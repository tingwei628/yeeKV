package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	// Uncomment this block to pass the first stage

	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}

	defer l.Close()

	for {
		// Accept connections in a loop
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			// os.Exit(1)
			continue // Continue to accept new connections even if one fails
		}

		// goroutine to handle multiple connections at the same time
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()

	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		text := scanner.Text()

		fmt.Printf("Received: %q\n", text)

		// Handle the command
		text = strings.TrimSpace(text)
		commands := strings.Split(text, " ")
		len_args := len(commands)
		if len_args > 1 {
			if strings.EqualFold(commands[0], "ECHO") {
				conn.Write([]byte(fmt.Sprintf("+%s\r\n", commands[1])))
				continue
			}

			continue
		}
		if strings.EqualFold(commands[0], "PING") {
			// handlePing(conn)
			conn.Write([]byte("+PONG\r\n"))
		} else {
			// fmt.Println("Unknown command:", command)
			// conn.Write([]byte("-ERR unknown command\r\n"))
		}
	}
}
