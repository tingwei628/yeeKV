/*
todo: add more func() return error
todo: separate more data structures into small packages
todo: setup goroutine with a workpool (like in BLPop)
todo: safemap need to be active to check expired keys
*/
package main

import (
	"fmt"
	"net"
	"os"

	server "github.com/codecrafters-io/redis-starter-go/app/server"
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit

// var safeMap *datastore.SafeMap
// var safeList *datastore.SafeList
// var safeStream *datastore.SafeStream

// const NEVER_EXPIRED = -1

// type Server struct {
// 	safeMap    *datastore.SafeMap
// 	safeList   *datastore.SafeList
// 	safeStream *datastore.SafeStream
// }

//	func NewServer() *Server {
//		return &Server{
//			safeMap:    datastore.NewSafeMap(),
//			safeList:   datastore.NewSafeList(),
//			safeStream: datastore.NewSafeStream(),
//		}
//	}
func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	// fmt.Println("Logs from your program will appear here!")

	// Uncomment this block to pass the first stage

	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}

	defer l.Close()

	// safeMap = datastore.NewSafeMap()
	// safeList = datastore.NewSafeList()
	// safeStream = datastore.NewSafeStream()

	s := server.NewServer()

	for {
		// Accept connections in a loop
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			// os.Exit(1)
			continue // Continue to accept new connections even if one fails
		}

		// goroutine to handle multiple connections at the same time
		go s.HandleConnection(conn)
	}
}
