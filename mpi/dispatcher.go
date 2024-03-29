package mpi

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"strconv"
	"sync"
	"time"

	"go.uber.org/zap"
	"golang.org/x/crypto/ssh"
)

func initDispatcher(SSHKeyFilePath, SSHUserName string, world *MPIWorld) error {
	DispatcherToWorkerTCPConn = make([]*net.Conn, world.size)
	WorkerOutputs = make([]bytes.Buffer, world.size)
	WorkerOutputsErr = make([]bytes.Buffer, world.size)
	DispatcherToWorkerListener = make([]*net.Listener, world.size)
	DispatcherToWorkerTCPConn[0] = nil
	selfFileLocation, _ := os.Executable()
	SelfRank = 0

	var wg sync.WaitGroup
	errChan := make(chan error, world.size)

	for i := 1; i < int(world.size); i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			workerIP := world.IPPool[i]
			workerPort := world.Port[i]
			workerRank := uint64(i)
			fmt.Println("WORKER RANK", workerRank)

			// Start worker process via ssh
			key, err := os.ReadFile(SSHKeyFilePath)
			if err != nil {
				zap.L().Error("Failed to load key file: " + err.Error())
				errChan <- err
			}
			signer, err := ssh.ParsePrivateKey(key)
			if err != nil {
				zap.L().Error("Failed to parse key file: " + err.Error())
				errChan <- err
				return
			}
			conn, err := ssh.Dial("tcp", workerIP+":"+strconv.Itoa(int(22)), &ssh.ClientConfig{
				User: SSHUserName,
				Auth: []ssh.AuthMethod{
					ssh.PublicKeys(signer),
				},
				HostKeyCallback: ssh.InsecureIgnoreHostKey(),
			})

			if err != nil {
				zap.L().Error("Failed to dial: " + err.Error())
				errChan <- err
				return
			}

			session, err := conn.NewSession()
			if err != nil {
				zap.L().Error("Failed to create session: " + err.Error())
				errChan <- err
				return
			}
			Command := selfFileLocation
			for j := 1; j < len(os.Args); j++ {
				Command += " " + os.Args[j]
			}
			Command += " " + world.IPPool[0] + " " + strconv.Itoa(int(world.Port[i]))
			Command += " Worker"
			fmt.Println("SENDING COMMAND", Command)

			zap.L().Info("dispatcher sending command to worker " + Command)

			// run the command async and zap.L().Error when command return error
			go func() {
				defer session.Close()
				session.Stdout = &WorkerOutputs[i]
				session.Stderr = &WorkerOutputsErr[i]
				err := session.Run(Command)

				if err != nil {
					zap.L().Error("Command Run Error: " + err.Error())
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
						zap.L().Error("rank " + strconv.Itoa(int(rank)) + " " + data)
					}
					time.Sleep(1 * time.Microsecond)
				}
			}(uint64(i))

			// Listen to worker
			listener, err := net.Listen("tcp", ":"+strconv.Itoa(int(workerPort)))
			if err != nil {
				zap.L().Error("Failed to listen: " + err.Error())
				errChan <- err
				return
			}
			// Accept a connection
			TCPConn, err := listener.Accept()

			// Make sure there is no deadline for timeouts
			TCPConn.SetDeadline(time.Time{})

			DispatcherToWorkerTCPConn[i] = &TCPConn
			DispatcherToWorkerListener[i] = &listener
			if err != nil {
				zap.L().Error("Failed to connect via TCP: " + err.Error())
				errChan <- err
				return
			}
			zap.L().Info("Connected to worker " + strconv.Itoa(i))

			// Send worker rank
			buf := make([]byte, 8)
			binary.LittleEndian.PutUint64(buf, uint64(workerRank))
			_, err = TCPConn.Write(buf)
			if err != nil {
				zap.L().Error("Failed to send rank: " + err.Error())
				errChan <- err
				return
			}

			// Send the working directory
			{
				workingDir, err := os.Getwd()
				if err != nil {
					zap.L().Error("Failed to get working directory: " + err.Error())
					errChan <- err
					return
				}
				//Send string length
				buf = make([]byte, 8)
				binary.LittleEndian.PutUint64(buf, uint64(len(workingDir)))
				_, err = TCPConn.Write(buf)
				if err != nil {
					zap.L().Error("Failed to send working directory length: " + err.Error())
					errChan <- err
					return
				}
				//Send string
				_, err = TCPConn.Write([]byte(workingDir))
				if err != nil {
					zap.L().Error("Failed to send working directory: " + err.Error())
					errChan <- err
					return
				}
				zap.L().Info("Sent working directory to worker " + strconv.Itoa(i))
			}

			// Sync the world state
			buf = SerializeWorld(world)

			// Send buf size
			bufSize := make([]byte, 8)
			binary.LittleEndian.PutUint64(bufSize, uint64(len(buf)))
			_, err = TCPConn.Write(bufSize)
			if err != nil {
				zap.L().Error("Failed to send buf size: " + err.Error())
				errChan <- err
				return
			}

			// Send buf
			_, err = TCPConn.Write(buf)
			if err != nil {
				zap.L().Error("Failed to send world: " + err.Error())
				errChan <- err
				return
			}

		}(i)
	}
	wg.Wait()
	close(errChan)

	for err := range errChan {
		if err != nil {
			return err
		}
	}
	return nil
}
