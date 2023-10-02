package mpi

import (
	"bufio"
	"bytes"
	"crypto/md5"
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"
	"golang.org/x/crypto/ssh"
)

type MPIWorld struct {
	size   uint64
	rank   []uint64
	IPPool []string
	Port   []uint64
}

func init() {
	zap.ReplaceGlobals(zap.Must(zap.NewDevelopment()))
}

func SerializeWorld(world *MPIWorld) []byte {
	// serialize the MPIWorld struct
	// format: size, rank, IPPool, Port
	// size: uint64
	buf := make([]byte, 0)
	sizebuf := make([]byte, 8)
	binary.LittleEndian.PutUint64(sizebuf, world.size)
	buf = append(buf, sizebuf...)

	// rank: []uint64
	for _, rank := range world.rank {
		rankBuf := make([]byte, 8)
		binary.LittleEndian.PutUint64(rankBuf, rank)
		buf = append(buf, rankBuf...)
	}

	// IPPool: []string
	for _, ip := range world.IPPool {
		IPBuf := make([]byte, 0)
		IPBuf = append(IPBuf, []byte(ip)...)
		IPBuf = append(IPBuf, 0)
		buf = append(buf, IPBuf...)
	}

	// Port: []uint64
	for _, port := range world.Port {
		portBuf := make([]byte, 8)
		binary.LittleEndian.PutUint64(portBuf, port)
		buf = append(buf, portBuf...)
	}
	return buf
}

func DeserializeWorld(buf []byte) *MPIWorld {
	// deserialize the MPIWorld struct
	// format: size, rank, IPPool, Port
	// size: uint64
	world := new(MPIWorld)
	world.size = binary.LittleEndian.Uint64(buf[:8])
	buf = buf[8:]

	// rank: []uint64
	world.rank = make([]uint64, world.size)
	for i := uint64(0); i < world.size; i++ {
		world.rank[i] = binary.LittleEndian.Uint64(buf[:8])
		buf = buf[8:]
	}

	// IPPool: []string
	world.IPPool = make([]string, world.size)
	for i := uint64(0); i < world.size; i++ {
		end := 0
		for end < len(buf) && buf[end] != 0 {
			end++
		}
		world.IPPool[i] = string(buf[:end])
		buf = buf[end+1:]
	}

	// Port: []uint64
	world.Port = make([]uint64, world.size)
	for i := uint64(0); i < world.size; i++ {
		world.Port[i] = binary.LittleEndian.Uint64(buf[:8])
		buf = buf[8:]
	}
	return world
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

func checkWorker() bool {
	LastCommand := os.Args[len(os.Args)-1]
	return strings.ToLower(LastCommand) == "worker"
}

func WorldInit(IPfilePath string, SSHKeyFilePath string, SSHUserName string) *MPIWorld {
	world := new(MPIWorld)
	world.size = 0
	world.rank = make([]uint64, 0)
	world.IPPool = make([]string, 0)
	world.Port = make([]uint64, 0)

	selfIP, _ := GetLocalIP()
	zap.L().Info(strings.Join(selfIP, ", "))

	isWorker := checkWorker()

	//Setup TCP connections dispatcher <--> workers

	if !isWorker {
		err := SetIPPool(IPfilePath, world)
		if err != nil {
			zap.L().Info(err.Error())
			panic(err)
		}
		DispatcherToWorkerTCPConn = make([]*net.Conn, world.size)
		WorkerOutputs = make([]bytes.Buffer, world.size)
		WorkerOutputsErr = make([]bytes.Buffer, world.size)
		DispatcherToWorkerListener = make([]*net.Listener, world.size)
		DispatcherToWorkerTCPConn[0] = nil
		selfFileLocation, _ := os.Executable()
		SelfRank = 0
		for i := 1; i < int(world.size); i++ {
			workerIP := world.IPPool[i]
			workerPort := world.Port[i]
			workerRank := uint64(i)

			// Start worker process via ssh
			key, err := ioutil.ReadFile(SSHKeyFilePath)
			if err != nil {
				fmt.Printf("unable to read private key: %v\n", err)
				panic("Failed to load key")
			}
			signer, err := ssh.ParsePrivateKey(key)
			if err != nil {
				fmt.Printf("unable to parse private key: %v\n", err)
				panic("Failed to parse key")
			}
			conn, err := ssh.Dial("tcp", workerIP+":"+strconv.Itoa(int(22)), &ssh.ClientConfig{
				User: SSHUserName,
				Auth: []ssh.AuthMethod{
					ssh.PublicKeys(signer),
				},
				HostKeyCallback: ssh.InsecureIgnoreHostKey(),
			})

			if err != nil {
				zap.L().Info(err.Error())
				panic("Failed to dial: " + err.Error())
			}

			session, err := conn.NewSession()
			if err != nil {
				zap.L().Info(err.Error())
				panic("Failed to create session: " + err.Error())
			}
			Command := selfFileLocation
			for j := 1; j < len(os.Args); j++ {
				Command += " " + os.Args[j]
			}
			Command += " " + world.IPPool[0] + " " + strconv.Itoa(int(world.Port[i]))
			Command += " Worker"

			//run the command async and panic when command return error
			go func() {
				defer session.Close()
				session.Stdout = &WorkerOutputs[i]
				session.Stderr = &WorkerOutputsErr[i]
				err := session.Run(Command)

				if err != nil {
					zap.L().Info(err.Error())
					panic(err)
				}
			}()

			go func(rank uint64) {
				// Print the output of the command
				for {
					data, _ := WorkerOutputs[rank].ReadString('\n')
					if data != "" {
						zap.L().Info("rank " + strconv.Itoa(int(rank)) + " " + data)
					}
					data, _ = WorkerOutputsErr[rank].ReadString('\n')
					if data != "" {
						ErrorColor := "\033[1;31m%s\033[0m"
						fmt.Printf(ErrorColor, "rank "+strconv.Itoa(int(rank))+" ERR "+data)
					}
					time.Sleep(1 * time.Microsecond)
				}
			}(uint64(i))

			// Listen to worker
			listener, err := net.Listen("tcp", ":"+strconv.Itoa(int(workerPort)))
			if err != nil {
				zap.L().Info(err.Error())
				panic("Failed to listen: " + err.Error())
			}
			// Accept a connection
			TCPConn, err := listener.Accept()

			DispatcherToWorkerTCPConn[i] = &TCPConn
			DispatcherToWorkerListener[i] = &listener
			if err != nil {
				zap.L().Info(err.Error())
				panic("Failed to connect via TCP: " + err.Error())
			}
			zap.L().Info("Connected to worker " + strconv.Itoa(i))

			// Send worker rank
			buf := make([]byte, 8)
			binary.LittleEndian.PutUint64(buf, uint64(workerRank))
			_, err = TCPConn.Write(buf)
			if err != nil {
				zap.L().Info(err.Error())
				panic("Failed to send rank: " + err.Error())
			}

			// Send the working directory
			{
				workingDir, err := os.Getwd()
				if err != nil {
					zap.L().Info(err.Error())
					panic("Failed to get working directory: " + err.Error())
				}
				//Send string length
				buf = make([]byte, 8)
				binary.LittleEndian.PutUint64(buf, uint64(len(workingDir)))
				_, err = TCPConn.Write(buf)
				if err != nil {
					zap.L().Info(err.Error())
					panic("Failed to send working directory length: " + err.Error())
				}
				//Send string
				_, err = TCPConn.Write([]byte(workingDir))
				if err != nil {
					zap.L().Info(err.Error())
					panic("Failed to send working directory: " + err.Error())
				}
				zap.L().Info("Sent working directory to worker " + strconv.Itoa(i))
			}

			// Sync the world state
			buf = SerializeWorld(world)

			//Send buf size
			bufSize := make([]byte, 8)
			binary.LittleEndian.PutUint64(bufSize, uint64(len(buf)))
			_, err = TCPConn.Write(bufSize)
			if err != nil {
				zap.L().Info(err.Error())
				panic("Failed to send buf size: " + err.Error())
			}

			//Send buf
			_, err = TCPConn.Write(buf)
			if err != nil {
				zap.L().Info(err.Error())
				panic("Failed to send world: " + err.Error())
			}

		}
	} else {
		// connect to dispatcher
		dispatcherIP := os.Args[len(os.Args)-3]
		workerPort := os.Args[len(os.Args)-2]
		TCPConn, err := net.Dial("tcp", dispatcherIP+":"+workerPort)
		WorkerToDispatcherTCPConn = &TCPConn
		if err != nil {
			zap.L().Info(err.Error())
			panic("Failed to accept: " + err.Error())
		}
		// Receive dispatcher rank
		buf := make([]byte, 8)
		_, err = TCPConn.Read(buf)
		if err != nil {
			zap.L().Info(err.Error())
			panic("Failed to receive rank: " + err.Error())
		}
		SelfRank = binary.LittleEndian.Uint64(buf)
		// Receive the working directory
		{
			//Receive string length
			buf = make([]byte, 8)
			_, err = TCPConn.Read(buf)
			if err != nil {
				zap.L().Info(err.Error())
				panic("Failed to receive working directory length: " + err.Error())
			}
			workingDirLength := binary.LittleEndian.Uint64(buf)
			//Receive string
			buf = make([]byte, workingDirLength)
			_, err = TCPConn.Read(buf)
			if err != nil {
				zap.L().Info(err.Error())
				panic("Failed to receive working directory: " + err.Error())
			}
			workingDir := string(buf)
			err = os.Chdir(workingDir)
			if err != nil {
				zap.L().Info(err.Error())
				panic("Failed to change working directory: " + err.Error())
			}
			workingDir, _ = os.Getwd()
			zap.L().Info("Changed working directory to " + workingDir)
		}

		// Sync the world state
		// Receive buf size
		bufSize := make([]byte, 8)
		_, err = TCPConn.Read(bufSize)
		if err != nil {
			zap.L().Info(err.Error())
			panic("Failed to receive buf size: " + err.Error())
		}
		buf = make([]byte, binary.LittleEndian.Uint64(bufSize))
		zap.L().Info("Received buf size " + strconv.Itoa(int(binary.LittleEndian.Uint64(bufSize))))

		_, err = TCPConn.Read(buf)
		if err != nil {
			zap.L().Info(err.Error())
			panic("Failed to receive world: " + err.Error())
		}
		world = DeserializeWorld(buf)
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
