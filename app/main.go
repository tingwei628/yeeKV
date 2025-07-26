package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
)

type SafeMap struct {
	mu sync.Mutex
	m  map[string]string
}

func NewSafeMap() *SafeMap {
	return &SafeMap{
		m: make(map[string]string),
	}
}

func (s *SafeMap) Set(key, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.m[key] = value
}

func (s *SafeMap) Get(key string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	value, ok := s.m[key]
	return value, ok
}

func (s *SafeMap) Delete(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.m, key)
}

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit
var safeMap *SafeMap

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	// fmt.Println("Logs from your program will appear here!")

	// Uncomment this block to pass the first stage

	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}

	safeMap = NewSafeMap()

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

	var commands []string
	var readBulkCommand bool
	var command_count int
	var err error
	for scanner.Scan() {
		text := scanner.Text()

		// fmt.Printf("Received: %q\n", text)

		// Handle the command
		text = strings.TrimSpace(text)

		if strings.HasPrefix(text, "*") {

			command_count, err = strconv.Atoi(text[1:])

			if err != nil {
				conn.Write([]byte("-ERR invalid number of arguments\r\n"))
				continue
			}

			if command_count == 0 {
				conn.Write([]byte("-ERR empty command\r\n"))
				continue
			}
			commands = make([]string, 0, command_count)

		} else if strings.HasPrefix(text, "$") {
			readBulkCommand = true
			continue
		}

		if readBulkCommand {
			commands = append(commands, text)
			readBulkCommand = false
		}

		if len(commands) == command_count {
			switch strings.ToUpper(commands[0]) {
			case "PING":
				conn.Write([]byte("+PONG\r\n"))
			case "ECHO":
				if len(commands) >= 2 {
					conn.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(commands[1]), commands[1])))
				}
				// } else {
				// 	conn.Write([]byte("-ERR wrong number of arguments for 'echo' command\r\n"))
				// }
			case "SET":
				if len(commands) >= 3 {
					safeMap.Set(commands[1], commands[2])
					conn.Write([]byte("+OK\r\n"))
				} else {
					// conn.Write([]byte("-ERR wrong number of arguments for 'set' command\r\n"))
				}
			case "GET":
				if len(commands) >= 2 {
					value, ok := safeMap.Get(commands[1])
					if ok {
						conn.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(value), value)))
					} else {
						conn.Write([]byte("$-1\r\n")) // nil response for non-existing key
					}
				} else {
					// conn.Write([]byte("-ERR wrong number of arguments for 'get' command\r\n"))
				}
			default:
				conn.Write([]byte("-ERR unknown command\r\n"))
			}

			readBulkCommand = false
			commands = nil
			command_count = 0

		} else {

		}

	}
}
