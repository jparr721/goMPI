package mpi

import (
	"encoding/binary"
	"net"
	"os"
	"strconv"
	"time"

	"go.uber.org/zap"
)

func initWorker() (*MPIWorld, error) {
	// connect to dispatcher
	dispatcherIP := os.Args[len(os.Args)-3]
	workerPort := os.Args[len(os.Args)-2]
	TCPConn, err := net.Dial("tcp", dispatcherIP+":"+workerPort)
	if err != nil {
		zap.L().Error("Failed to accept: " + err.Error())
		return nil, err
	}

	// Make sure there is no deadline for timeouts
	TCPConn.SetDeadline(time.Time{})

	WorkerToDispatcherTCPConn = &TCPConn
	zap.L().Info("Worker accepted TCP connection")

	// Receive dispatcher rank
	buf := make([]byte, 8)
	_, err = TCPConn.Read(buf)
	if err != nil {
		zap.L().Error("Failed to receive rank: " + err.Error())
		return nil, err
	}

	SelfRank = binary.LittleEndian.Uint64(buf)
	zap.L().Info("Worker rank " + strconv.Itoa(int(SelfRank)) + " starting work")
	// Receive the working directory
	{
		//Receive string length
		buf = make([]byte, 8)
		_, err = TCPConn.Read(buf)
		if err != nil {
			zap.L().Error("Failed to receive working directory length: " + err.Error())
			return nil, err
		}
		workingDirLength := binary.LittleEndian.Uint64(buf)

		//Receive string
		buf = make([]byte, workingDirLength)
		_, err = TCPConn.Read(buf)
		if err != nil {
			zap.L().Error("Failed to receive working directory: " + err.Error())
			return nil, err
		}
		workingDir := string(buf)
		zap.L().Info("Received working dir " + workingDir)

		err = os.Chdir(workingDir)
		if err != nil {
			zap.L().Error("Failed to change working directory: " + err.Error())
			return nil, err
		}
		workingDir, _ = os.Getwd()
		zap.L().Info("Changed working directory to " + workingDir)
	}

	// Sync the world state
	// Receive buf size
	bufSize := make([]byte, 8)
	_, err = TCPConn.Read(bufSize)
	if err != nil {
		zap.L().Error("Failed to receive buf size: " + err.Error())
		return nil, err
	}
	buf = make([]byte, binary.LittleEndian.Uint64(bufSize))
	zap.L().Info("Received buf size " + strconv.Itoa(int(binary.LittleEndian.Uint64(bufSize))))

	_, err = TCPConn.Read(buf)
	if err != nil {
		zap.L().Error("Failed to receive world: " + err.Error())
		return nil, err
	}
	return DeserializeWorld(buf), nil
}
