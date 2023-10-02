package mpi

import (
	"bufio"
	"bytes"
	"crypto/md5"
	"fmt"
	"net"
	"os"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"
)

func init() {
	zap.ReplaceGlobals(zap.Must(zap.NewDevelopment()))
}

var (
	SelfRank                   uint64
	DispatcherToWorkerTCPConn  []*net.Conn
	WorkerToDispatcherTCPConn  *net.Conn
	DispatcherToWorkerListener []*net.Listener
	WorkerOutputs              []bytes.Buffer
	WorkerOutputsErr           []bytes.Buffer
	BytesSent                  uint64
	BytesReceived              uint64
	WorldSize                  uint64
)

func SetIPPool(filePath string, world *MPIWorld) error {
	// reading IP from file, the first IP is the dispatcher node
	// the rest are the worker nodes
	ipFile, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer ipFile.Close()
	scanner := bufio.NewScanner(ipFile)
	for scanner.Scan() {
		line := scanner.Text()
		//port and IP are separated by a :
		world.IPPool = append(world.IPPool, strings.Split(line, ":")[0])
		portNum, err := strconv.Atoi(strings.Split(line, ":")[1])
		if err != nil {
			return err
		}
		world.Port = append(world.Port, uint64(portNum))
		world.rank = append(world.rank, world.size)
		world.size++
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	return nil
}

func GetLocalIP() ([]string, error) {
	// get local IP address
	addrs, err := net.InterfaceAddrs()
	result := make([]string, 0)
	if err != nil {
		return result, err
	}
	for _, address := range addrs {
		// check the address type and if it is not a loopback the display it
		if ipnet, ok := address.(*net.IPNet); ok {
			if ipnet.IP.To4() != nil {
				result = append(result, ipnet.IP.String())
			}
		}
	}
	return result, nil
}

func isNodeWorker() bool {
	LastCommand := os.Args[len(os.Args)-1]
	return strings.ToLower(LastCommand) == "worker"
}

func WorldInit(IPfilePath, SSHKeyFilePath, SSHUserName string) *MPIWorld {
	world := new(MPIWorld)
	world.size = 0
	world.rank = make([]uint64, 0)
	world.IPPool = make([]string, 0)
	world.Port = make([]uint64, 0)

	// Initialize the worker pool
	err := SetIPPool(IPfilePath, world)
	if err != nil {
		zap.L().Error(err.Error())
		panic("Failed to initialize worker pool " + err.Error())
	}

	selfIP, _ := GetLocalIP()
	zap.L().Info(strings.Join(selfIP, ", "))

	isWorker := isNodeWorker()

	// Setup TCP connections dispatcher <--> workers
	if !isWorker {
		initDispatcher(SSHKeyFilePath, SSHUserName, world)
	} else {
		// Golang is stupid.
		var err error

		// Call initWorker and overwrite the world state
		world, err = initWorker()

		// Melt down on error
		if err != nil {
			panic(err.Error())
		}
	}

	WorldSize = world.size
	return world
}

// If Dispatcher calls this function, rank is required
// If Worker calls this function, rank is not required, it will send to Dispatcher
var sentBytes []byte
var recvBytes []byte

func SendBytes(buf []byte, rank uint64) error {
	var errorMsg error
	errorMsg = nil
	BytesSentInThisSession := 0
	sentBytes = append(sentBytes, buf...)
	for len(buf) > 0 {
		n := 0
		if SelfRank == 0 {
			n, errorMsg = (*DispatcherToWorkerTCPConn[rank]).Write(buf)
		} else {
			n, errorMsg = (*WorkerToDispatcherTCPConn).Write(buf)
		}
		if errorMsg != nil {
			zap.L().Info(string(debug.Stack()))
			return errorMsg
		}
		BytesSentInThisSession += n
		BytesSent += uint64(n)
		buf = buf[n:]
	}
	return errorMsg
}

// If Dispatcher calls this function, rank is required, it will receive from rank-th worker
// If Worker calls this function, rank is not required, it will receive from Dispatcher
func ReceiveBytes(size uint64, rank uint64) ([]byte, error) {
	buf := make([]byte, size)
	var errorMsg error
	errorMsg = nil
	BytesRead := uint64(0)
	for BytesRead < size {
		n := 0
		tmpBuf := make([]byte, size-BytesRead)
		if SelfRank == 0 {
			(*DispatcherToWorkerTCPConn[rank]).SetReadDeadline(time.Now().Add(10 * time.Second))
			n, errorMsg = (*DispatcherToWorkerTCPConn[rank]).Read(tmpBuf)
		} else {
			(*WorkerToDispatcherTCPConn).SetReadDeadline(time.Now().Add(10 * time.Second))
			n, errorMsg = (*WorkerToDispatcherTCPConn).Read(tmpBuf)
		}
		for i := BytesRead; i < BytesRead+uint64(n); i++ {
			buf[i] = tmpBuf[i-BytesRead]
		}
		if errorMsg != nil {
			if errorMsg.Error() == "EOF" {
				zap.L().Info("EOF")
			}
			zap.L().Info(string(debug.Stack()))
			return buf, errorMsg
		}
		BytesReceived += uint64(n)
		BytesRead += uint64(n)
	}
	recvBytes = append(recvBytes, buf...)
	return buf, errorMsg
}

func GetHash(str string) {
	zap.L().Info(str + " Bytes sent: " + strconv.Itoa(int(BytesSent)))
	zap.L().Info(str + " Bytes received: " + strconv.Itoa(int(BytesReceived)))
	zap.L().Info(str + " Sent hash: " + fmt.Sprintf("%x", md5.Sum(sentBytes)))
	zap.L().Info(str + " Received hash: " + fmt.Sprintf("%x", md5.Sum(recvBytes)))
}

func Close() {
	zap.L().Info("Bytes sent: " + strconv.Itoa(int(BytesSent)))
	zap.L().Info("Bytes received: " + strconv.Itoa(int(BytesReceived)))
	zap.L().Info("Sent hash: " + fmt.Sprintf("%x", md5.Sum(sentBytes)))
	zap.L().Info("Received hash: " + fmt.Sprintf("%x", md5.Sum(recvBytes)))
	if SelfRank == 0 {
		time.Sleep(1 * time.Second)
		for i := 1; i < len(DispatcherToWorkerTCPConn); i++ {
			(*DispatcherToWorkerTCPConn[i]).Close()
			(*DispatcherToWorkerListener[i]).Close()
		}
	} else {
		(*WorkerToDispatcherTCPConn).Close()
	}
}
