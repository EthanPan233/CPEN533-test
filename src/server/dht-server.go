package main

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"time"

	pa2lib "pa2/src/server/pa2lib"
)

// Print the usage of the program
func usage() {
	fmt.Println("Usage: go run src/server/pa2server.go [serverPortNum]")
}

var localPort = os.Args[1]

func main() {
	f, err := os.OpenFile("server.log", os.O_RDWR | os.O_CREATE | os.O_TRUNC, 0755)
	if err != nil {
		log.Fatalf("error opening file: %v", err)
	}
	defer f.Close()

	log.SetOutput(f)
	log.Println("This is a test log entry")

	rand.Seed(time.Now().UnixNano())
	// Check that number of arguments is correct
	if len(os.Args) != 3 {
		fmt.Println("Error: number of arguments is incorrect")
		usage()
		return
	}

	// Get the port number of the server
	port, err := strconv.Atoi(os.Args[1])
	if err != nil {
		fmt.Println("Error: port argument should be valid integer")
		usage()
		return
	}

	// Start the server
	pa2lib.StartServer(os.Args[2], port)
}
