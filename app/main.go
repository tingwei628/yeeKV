/*
todo: add more func() return error
todo: separate more data structures into small packages
todo: setup goroutine with a workpool (like in BLPop)
todo: safemap need to be active to check expired keys
*/
package main

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	ERR_STREAM_XADD_00      = "The ID specified in XADD must be greater than 0-0"
	ERR_STREAM_XADD_INVALID = "The ID specified in XADD is equal or smaller than the target stream top item"
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

type StreamElement struct {
	Value interface{}
}

type StreamItem struct {
	Id     string
	Fields map[string]StreamElement
}
type Stream struct {
	Items []StreamItem
}

type SafeMap struct {
	mu sync.Mutex
	m  map[string]Element
}

type SafeList struct {
	cond *sync.Cond
	mu   sync.Mutex
	m    map[string]*LinkedList
}

type SafeStream struct {
	mu sync.Mutex
	m  map[string]*Stream
}

func NewSafeMap() *SafeMap {
	return &SafeMap{
		m: make(map[string]Element),
	}
}
func NewSafeList() *SafeList {
	sl := &SafeList{
		m: make(map[string]*LinkedList),
	}
	sl.cond = sync.NewCond(&sl.mu)
	return sl
}
func NewSafeStream() *SafeStream {
	return &SafeStream{
		m: make(map[string]*Stream),
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
	// Check if the key exists and has an expiry time
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

func (s *SafeMap) Type(key string) (string, bool) {
	_, ok := s.Get(key) // Ensure the key is checked for expiry
	if ok {
		return "string", true // Assuming all values in SafeMap are strings
	}
	return "", false // Key does not exist or has expired
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
			fmt.Println("RPush: ", key, value, m.Len)
		}
	} else {
		// If the key does not exist, create a new list and add the values
		m = &LinkedList{}
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

	// Notify all waiting LPOP/BLPOP goroutines that new items are available
	// Avoid s.cond.Broadcast() here to prevent goroutines race consditions
	// Instead, we signal only once for each RPush operation
	for i := 0; i < len(values); i++ {
		s.cond.Signal()
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
		m = &LinkedList{}
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
func (s *SafeList) LPop(key string, popCount int) ([]string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	m, ok := s.m[key]
	var result []string

	if ok && m.Len > 0 {

		for i := 0; i < popCount && m.Len > 0; i++ {

			value := m.Head.ItemValue.Value
			result = append(result, value)

			// Move the head pointer to the next item
			m.Head = m.Head.Next
			// If the list becomes empty, set Tail to nil
			if m.Head == nil {
				m.Tail = nil
			} else {
				m.Head.Prev = nil // Set the Prev pointer of the new head to nil
			}

			m.Len--
		}

		return result, true
	}
	return result, false // Return empty string if the list is empty or key does not exist
}

// BLPop blocks until an item is available in the list or the timeout is reached.
func (s *SafeList) BLPop(key string, timeout time.Duration) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var (
		ctx    context.Context
		cancel context.CancelFunc
	)

	if timeout > 0 {
		ctx, cancel = context.WithTimeout(context.Background(), timeout)
		defer cancel()

		done := make(chan struct{})
		defer close(done)
		go func() {
			select {
			// Wait for the context to be done or the timeout to expire
			case <-ctx.Done():
				// If the context is done, signal the condition variable to wake up the waiting goroutine
				s.cond.Signal()
			// Avoid goroutine leak
			case <-done:
				return
			}
		}()
	}

	// block wait
	for {
		// Check if the key exists and has a non-empty list
		m, ok := s.m[key]
		if ok && m.Len > 0 {

			value := m.Head.ItemValue.Value
			// Move the head pointer to the next item
			m.Head = m.Head.Next
			// If the list becomes empty, set Tail to nil
			if m.Head == nil {
				m.Tail = nil
			} else {
				m.Head.Prev = nil // Set the Prev pointer of the new head to nil
			}
			m.Len--
			return value, true
		}

		if timeout > 0 {
			if ctx.Err() != nil {
				return "", false
			}
		}
		s.cond.Wait()
	}

}

func (s *SafeList) LRange(key string, start, stop int) []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	var result []string

	m, ok := s.m[key]
	if ok {

		// Return empty slice if the list is empty
		if m.Len == 0 {
			return result
		}

		// Adjust start and stop indices if they are negative
		if start < 0 {
			start = m.Len + start
		}
		if start < 0 {
			start = 0
		}

		if stop < 0 {
			stop = m.Len + stop
		}
		if stop < 0 {
			stop = 0
		}

		// start and stop are positives now
		// Ensure start and stop are within bounds
		if start >= m.Len || start > stop {
			return result
		}

		if stop > m.Len {
			stop = m.Len - 1
		}

		current := m.Head
		index := 0

		for current != nil {
			if index >= start && index <= stop {
				result = append(result, current.ItemValue.Value)
			}
			current = current.Next
			index++
		}
	}
	return result

}

func (s *SafeList) LLen(key string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	m, ok := s.m[key]
	if ok {
		return m.Len
	}
	return 0 // Return 0 if the key does not exist
}

func (s *SafeList) Type(key string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.m[key]; ok {
		return "list", true
	}
	return "", false
}

func (s *Stream) NewValidStreamId(id string) (string, bool, string) {
	if id == "0-0" {
		return "", false, ERR_STREAM_XADD_00
	}

	// generate valid id
	var ms, seq int64

	// Fully auto-generated IDs
	if id == "*" {
		ms = time.Now().UnixMilli()
		id = fmt.Sprintf("%d-0", ms)
	} else {

		parts := strings.Split(id, "-")
		if len(parts) != 2 {
			return "", false, ERR_STREAM_XADD_INVALID
		}
		if parts[0] == "" || parts[1] == "" {
			return "", false, ERR_STREAM_XADD_INVALID
		}
		ms, err1 := strconv.ParseInt(parts[0], 10, 64)
		if err1 != nil || ms < 0 {
			return "", false, ERR_STREAM_XADD_INVALID
		}

		fmt.Printf("parse ms from string: '%s' = %d\n", parts[0], ms)
		// Partially auto-generated IDs
		if parts[1] == "*" {

			if parts[0] == "0" && len(s.Items) == 0 {
				seq = 1
			}

			for i := len(s.Items) - 1; i >= 0; i-- {
				targetParts := strings.Split(s.Items[i].Id, "-")
				if targetParts[0] == parts[0] {
					targetSeq, _ := strconv.ParseInt(targetParts[1], 10, 64)
					seq = targetSeq + 1
					break
				}
			}

		} else {
			seq, err2 := strconv.ParseInt(parts[1], 10, 64)
			// Check if both parts are valid integers and non-negative
			if err2 != nil || seq < 0 {
				return "", false, ERR_STREAM_XADD_INVALID
			}
		}
	}
	id = fmt.Sprintf("%d-%d", ms, seq)
	fmt.Printf("parse ms from string: '%s' %d %d \n", id, ms, seq)

	// last stream id
	lastId := ""
	var lastMs, lastSeq int64

	if len(s.Items) > 0 {
		lastId = s.Items[len(s.Items)-1].Id
	}
	if lastId == "" {
		return id, true, ""
	} else {
		lastParts := strings.Split(lastId, "-")
		lastMs, _ = strconv.ParseInt(lastParts[0], 10, 64)
		lastSeq, _ = strconv.ParseInt(lastParts[1], 10, 64)
	}

	// compare
	if ms > lastMs {
		return id, true, ""
	}
	if ms == lastMs && seq > lastSeq {
		return id, true, ""
	}

	return "", false, ERR_STREAM_XADD_INVALID

}
func (s *SafeStream) Type(key string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.m[key]
	if ok {
		return "stream", true // Key does not exist
	}
	return "", false // Assuming all values in SafeMap are strings
}

func (s *SafeStream) XAdd(key string, id string, fields map[string]interface{}) (string, bool, string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	stream, ok := s.m[key]
	if !ok {
		stream = &Stream{Items: []StreamItem{}}
		s.m[key] = stream
	}

	// Create a new StreamItem with a unique ID
	newValidId, ok, errStr := stream.NewValidStreamId(id)
	if !ok {
		return "", false, errStr
	}

	item := StreamItem{
		Id:     newValidId,
		Fields: make(map[string]StreamElement),
	}

	for field, value := range fields {
		item.Fields[field] = StreamElement{Value: value}
	}

	stream.Items = append(stream.Items, item)
	return newValidId, true, ""
}

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit
var safeMap *SafeMap
var safeList *SafeList
var safeStream *SafeStream

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
	safeStream = NewSafeStream()

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
						continue
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
					conn.Write([]byte(fmt.Sprintf(":%d\r\n", v)))
				} else {
					// conn.Write([]byte("-ERR wrong number of arguments for 'rpush' command\r\n"))
				}
			case "LPUSH":
				if len(commands) >= 3 {
					v := safeList.LPush(commands[1], commands[2:]...)
					conn.Write([]byte(fmt.Sprintf(":%d\r\n", v)))

				} else {
					// conn.Write([]byte("-ERR wrong number of arguments for 'lpush' command\r\n"))
				}
			case "LPOP":
				if len(commands) >= 2 {
					popCount := 1
					if len(commands) == 3 {
						popCount, err = strconv.Atoi(commands[2])
						if err != nil {
							conn.Write([]byte("-ERR invalid popCount\r\n"))
							continue
						}
					}
					v, ok := safeList.LPop(commands[1], popCount)
					if ok && len(v) > 0 {
						if popCount == 1 {
							conn.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(v[0]), v[0])))
						} else {
							stringBuilder := strings.Builder{}
							stringBuilder.WriteString(fmt.Sprintf("*%d\r\n", len(v)))
							for _, value := range v {
								stringBuilder.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(value), value))
							}
							conn.Write([]byte(stringBuilder.String()))
						}
					} else {
						conn.Write([]byte("$-1\r\n")) // nil response for non-existing key or empty list
					}
				} else {
					// conn.Write([]byte("-ERR wrong number of arguments for 'lpop' command\r\n"))
				}
			case "BLPOP":
				if len(commands) == 3 {
					// BLPOP key timeout
					// timeout is in milliseconds
					timeout, err := strconv.ParseFloat(commands[2], 64)
					if err != nil {
						conn.Write([]byte("-ERR invalid timeout value\r\n"))
						continue
					}
					value, ok := safeList.BLPop(commands[1], time.Duration(timeout*float64(time.Second)))
					if ok {
						key := commands[1]
						conn.Write([]byte(fmt.Sprintf("*2\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(key), key, len(value), value)))
					} else {
						conn.Write([]byte("$-1\r\n"))
					}
				} else {
					conn.Write([]byte("-ERR wrong number of arguments for 'blpop' command\r\n"))
				}
			case "LRANGE":
				if len(commands) == 4 {
					start, err := strconv.Atoi(commands[2])
					stop, err := strconv.Atoi(commands[3])
					if err != nil {
						conn.Write([]byte("-ERR invalid start index or end index\r\n"))
						continue
					}

					v := safeList.LRange(commands[1], start, stop)
					v_len := len(v)

					stringBuilder := strings.Builder{}
					stringBuilder.WriteString(fmt.Sprintf("*%d\r\n", v_len))

					for _, value := range v {
						stringBuilder.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(value), value))
					}
					conn.Write([]byte(stringBuilder.String()))

				} else {
					// conn.Write([]byte("-ERR wrong number of arguments for 'lrange' command\r\n"))
				}
			case "LLEN":
				if len(commands) == 2 {
					v := safeList.LLen(commands[1])
					conn.Write([]byte(fmt.Sprintf(":%d\r\n", v)))
				} else {
					// conn.Write([]byte("-ERR wrong number of arguments for 'llen' command\r\n"))
				}
			case "TYPE":
				if len(commands) == 2 {
					var typeName string
					var ok bool

					if typeName, ok = safeMap.Type(commands[1]); ok {
					} else if typeName, ok = safeList.Type(commands[1]); ok {
					} else if typeName, ok = safeStream.Type(commands[1]); ok {
					} else {
						typeName = "none"
					}
					conn.Write([]byte(fmt.Sprintf("+%s\r\n", typeName)))
				} else {
					// conn.Write([]byte("-ERR wrong number of arguments for 'type' command\r\n"))
				}
			case "XADD":
				if len(commands) >= 5 {

					fields := make(map[string]interface{})

					for i := 3; i < len(commands); i += 2 {
						if i+1 < len(commands) {
							fields[commands[i]] = commands[i+1]
						} else {
							conn.Write([]byte("-ERR wrong number of arguments for 'xadd' command\r\n"))
							continue
						}
					}

					id, ok, errStr := safeStream.XAdd(commands[1], commands[2], fields)
					if ok {
						conn.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(id), id)))
					} else {
						conn.Write([]byte(fmt.Sprintf("-ERR %s\r\n", errStr)))
					}
				} else {
					// conn.Write([]byte("-ERR wrong number of arguments for 'xadd' command\r\n"))
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
