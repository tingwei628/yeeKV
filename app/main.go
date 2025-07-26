/*
todo: add more func() return error
todo: separate more data structures into small packages
todo: setup goroutine with a workpool
*/
package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Element struct {
	Value      string
	ExpiryTime time.Time
}

type ListItem struct {
	ItemValue Element
	Prev      *ListItem
	Next      *ListItem
}

type LinkedList struct {
	Head *ListItem
	Tail *ListItem
	Len  int
}

type SafeMap struct {
	mu sync.Mutex
	m  map[string]Element
}

type SafeList struct {
	mu sync.Mutex
	m  map[string]LinkedList
}

func NewSafeMap() *SafeMap {
	return &SafeMap{
		m: make(map[string]Element),
	}
}
func NewSafeList() *SafeList {
	return &SafeList{
		m: make(map[string]LinkedList),
	}
}

func (s *SafeMap) Set(key string, value string, px int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if px > 0 {
		expiryTime := time.Now().Add(time.Duration(px) * time.Millisecond)

		s.m[key] = Element{
			Value:      value,
			ExpiryTime: expiryTime,
		}
	} else {
		s.m[key] = Element{
			Value: value,
		}
	}
}

func (s *SafeMap) Get(key string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.m[key]
	if ok && !v.ExpiryTime.IsZero() {
		if time.Now().After(v.ExpiryTime) {
			delete(s.m, key)
			return "", false // Key has expired
		}
		return s.m[key].Value, true
	}

	if ok && v.ExpiryTime.IsZero() {
		// Key exists without expiry
		return s.m[key].Value, true
	}
	return "", false
}

func (s *SafeList) RPush(key string, values ...string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	m, ok := s.m[key]
	if ok {
		for _, value := range values {
			newItem := &ListItem{
				ItemValue: Element{
					Value: value,
				},
			}

			// If the list is empty, set both Head and Tail to the new item
			if m.Head == nil {
				m.Head = newItem
				m.Tail = newItem
			} else {
				// If the list is not empty, append the new item to the tail
				m.Tail.Next = newItem
				// Set the Prev pointer of the new item to the current tail
				newItem.Prev = m.Tail
				// Update the tail to point to the new item
				m.Tail = newItem
			}
			m.Len++

		}
	} else {
		// If the key does not exist, create a new list and add the values
		m = LinkedList{}
		for _, value := range values {
			newItem := &ListItem{
				ItemValue: Element{
					Value: value,
				},
			}
			if m.Head == nil {
				m.Head = newItem
				m.Tail = newItem
			} else {
				m.Tail.Next = newItem
				newItem.Prev = m.Tail
				m.Tail = newItem
			}
			m.Len++
		}
		s.m[key] = m
	}

	return m.Len
}

func (s *SafeList) LPush(key string, values ...string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	m, ok := s.m[key]
	if ok {
		for _, value := range values {
			newItem := &ListItem{
				ItemValue: Element{
					Value: value,
				},
			}

			// If the list is empty, set both Head and Tail to the new item
			if m.Head == nil {
				m.Head = newItem
				m.Tail = newItem
			} else {
				// If the list is not empty, prepend the new item to the head
				newItem.Next = m.Head
				// Set the Prev pointer of the current head to the new item
				m.Head.Prev = newItem
				// Update the head to point to the new item
				m.Head = newItem
			}
			m.Len++
		}
	} else {
		// If the key does not exist, create a new list and add the values
		m = LinkedList{}
		for _, value := range values {
			newItem := &ListItem{
				ItemValue: Element{
					Value: value,
				},
			}
			if m.Head == nil {
				m.Head = newItem
				m.Tail = newItem
			} else {
				newItem.Next = m.Head
				m.Head.Prev = newItem
				m.Head = newItem
			}
			m.Len++
		}
		s.m[key] = m
	}
	return m.Len
}

// func (s *SafeList) LRange(start, end int) int {
// 	s.mu.Lock()
// 	defer s.mu.Unlock()
// }

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit
var safeMap *SafeMap
var safeList *SafeList

const NEVER_EXPIRED = -1

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
	safeList = NewSafeList()

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
				if len(commands) == 3 {
					safeMap.Set(commands[1], commands[2], NEVER_EXPIRED)
					conn.Write([]byte("+OK\r\n"))
				} else if len(commands) == 5 && strings.ToUpper(commands[3]) == "PX" {
					px, err := strconv.ParseInt(commands[4], 10, 64)
					if err != nil {
						conn.Write([]byte("-ERR invalid PX value\r\n"))
					}
					safeMap.Set(commands[1], commands[2], px)
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
			case "RPUSH":
				if len(commands) >= 3 {
					v := safeList.RPush(commands[1], commands[2:]...)
					fmt.Println("RPUSH %v %v %v", commands[1], commands[2:], v)
					conn.Write([]byte(fmt.Sprintf(":%d\r\n", v)))
				} else {
					// conn.Write([]byte("-ERR wrong number of arguments for 'rpush' command\r\n"))
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
