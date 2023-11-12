package mpi

import (
	"bufio"
	"bytes"
	"context"
	"crypto/md5"
	"fmt"
	"net"
	"os"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
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

func SetIPPoolFromFile(filePath string, world *MPIWorld) error {
	// reading IP from file, the first IP is the dispatcher node
	// the rest are the worker nodes
	ipFile, err := os.Open(filePath)
	if err != nil {
		zap.L().Error("Failed to open IP file: " + err.Error())
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
			zap.L().Error("Failed to parse port number: " + err.Error())
			return err
		}
		world.Port = append(world.Port, uint64(portNum))
		world.rank = append(world.rank, world.size)
		world.size++
	}
	if err := scanner.Err(); err != nil {
		zap.L().Error("Failed to read IP file: " + err.Error())
		return err
	}
	return nil
}

// SetIPPoolFromKubernetes dynamically constructs the pool of IP addresses
// for the MPI world by querying the Kubernetes cluster for pods that match
// specific criteria. It utilizes the in-cluster configuration to create a
// clientset, which is then used to access the Kubernetes API and list pods
// based on a specified label selector. This function iterates over the list
// of worker pods, extracting their IP addresses and adding them to the world's
// IPPool. It is designed to run from within a pod inside a Kubernetes cluster.
// The function assumes that the pod has the necessary permissions to list
// pods in the Kubernetes API.
//
// Parameters:
//
//	world - A pointer to the MPIWorld structure where the pool of IP addresses
//	        will be stored.
//
// Returns:
//
//	error - Any error encountered while setting up the IPPool from Kubernetes.
//	        Returns nil if the operation is successful.
//
// Example Usage:
//
//	var myWorld MPIWorld
//	err := SetIPPoolFromKubernetes(&myWorld)
//	if err != nil {
//	    // handle error
//	}
func SetIPPoolFromKubernetes(world *MPIWorld) error {
	zap.L().Info("Attempting to load kubernetes configuration")
	// Creates the in-cluster config from the service account in the deployment.
	config, err := rest.InClusterConfig()
	if err != nil {
		zap.L().Error("Failed to create in-cluster config: " + err.Error())
		return err
	}

	// Now, take the config and make the clientset
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		zap.L().Error("Failed to create clientset: " + err.Error())
		return err
	}

	// Get the pods in the current namespace
	pods, err := clientset.CoreV1().Pods("").List(context.TODO(), metav1.ListOptions{
		LabelSelector: "app=krania-gnark,component=worker",
	})
	if err != nil {
		zap.L().Error("Failed to list pods: " + err.Error())
		return err
	}

	// Static port allocation
	port := 8000

	for _, pod := range pods.Items {
		zap.L().Info("Loading pod: " + pod.Status.PodIP)
		world.IPPool = append(world.IPPool, pod.Status.PodIP)
		world.Port = append(world.Port, uint64(port))
		world.rank = append(world.rank, world.size)
		world.size++
	}

	return nil
}

func GetLocalIP() ([]string, error) {
	// get local IP address
	addrs, err := net.InterfaceAddrs()
	result := make([]string, 0)
	if err != nil {
		zap.L().Error("Failed to get local IP address: " + err.Error())
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

	isWorker := isNodeWorker()

	selfIP, _ := GetLocalIP()
	zap.L().Info(strings.Join(selfIP, ", "))

	// Setup TCP connections dispatcher <--> workers
	if !isWorker {
		// Worker pool is initialized and passed down as the serialized world state, so we only need this info once.
		// Initialize the worker pool. First, try from kubernetes
		err := SetIPPoolFromKubernetes(world)
		if err != nil {
			// If we are not running within kubernetes, default to the ip.txt file.
			zap.L().Info("Failed to detect kuberenetes environment, defaulting to ip.txt")
			err := SetIPPoolFromFile(IPfilePath, world)

			// If something else breaks, die
			if err != nil {
				zap.L().Error(err.Error())
				panic("Failed to initialize worker pool from ip.txt" + err.Error())
			}
		}

		if world.size == 0 {
			panic("No world nodes found, exiting")
		}

		zap.L().Info("Node type: Dispatcher")
		initDispatcher(SSHKeyFilePath, SSHUserName, world)
	} else {
		zap.L().Info("Node type: Worker")
		// Golang is stupid.
		var err error

		// Call initWorker and overwrite the world state
		world, err = initWorker()

		if world.size == 0 {
			panic("world sent invalid state")
		}

		// Melt down on error
		if err != nil {
			panic(err.Error())
		}
	}

	WorldSize = world.size
	return world
}

var sentBytes []byte
var recvBytes []byte

// If Dispatcher calls this function, rank is required
// If Worker calls this function, rank is not required, it will send to Dispatcher
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
			n, errorMsg = (*DispatcherToWorkerTCPConn[rank]).Read(tmpBuf)
		} else {
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
