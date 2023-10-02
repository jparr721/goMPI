package main

import (
	"github.com/jparr721/goMPI/mpi"
)

func main() {
	// wd, err := os.Getwd()

	// if err != nil {
	// 	panic(err)
	// }

	// ipJson := filepath.Join(wd, "ip.json")
	// configJson := filepath.Join(wd, "config.json")

	mpi.WorldInit("/root/test/ip.txt", "/root/.ssh/id_rsa", "root")
	mpi.Close()
}
