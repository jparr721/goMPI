package main

import (
	"fmt"

	"github.com/jparr721/goMPI/mpi"
)

func main() {
	world := mpi.WorldInit("/root/test/ip.txt", "/root/.ssh/id_rsa", "root")
	fmt.Println(world)
	mpi.Close()
}
