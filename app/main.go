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
	cond *sync.Cond
	mu   sync.Mutex
	m    map[string]*Stream
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
	ss := &SafeStream{
		m: make(map[string]*Stream),
	}
	ss.cond = sync.NewCond(&ss.mu)
	return ss
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
	var result []string = []string{}

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

	var result []string = []string{}

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

func parseStreamId(id string) (int64, int64, bool) {
	parts := strings.Split(id, "-")
	if len(parts) != 2 {
		return 0, 0, false
	}

	ms, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil || ms < 0 {
		return 0, 0, false
	}

	seq, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil || seq < 0 {
		return 0, 0, false
	}

	return ms, seq, true
}
func incrementStreamId(id string, increment int64) string {
	ms, seq, ok := parseStreamId(id)
	if !ok {
		return id // fallback
	}
	return fmt.Sprintf("%d-%d", ms, seq+increment)
}
func (s *Stream) NewValidStreamId(id string) (string, bool, string) {
	if id == "0-0" {
		return "", false, ERR_STREAM_XADD_00
	}

	// generate valid id
	var ms, seq int64
	var err error

	// Fully auto-generated IDs
	if id == "*" {
		ms = time.Now().UnixMilli()
	} else {

		parts := strings.Split(id, "-")
		if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
			return "", false, ERR_STREAM_XADD_INVALID
		}
		ms, err = strconv.ParseInt(parts[0], 10, 64)
		if err != nil || ms < 0 {
			return "", false, ERR_STREAM_XADD_INVALID
		}

		// Partially auto-generated IDs
		if parts[1] == "*" {

			for i := len(s.Items) - 1; i >= 0; i-- {
				targetParts := strings.Split(s.Items[i].Id, "-")
				if targetParts[0] == parts[0] {
					targetSeq, _ := strconv.ParseInt(targetParts[1], 10, 64)
					seq = targetSeq + 1
					break
				}
			}

			if ms == 0 {
				seq = 1
			}

		} else {
			seq, err = strconv.ParseInt(parts[1], 10, 64)
			// Check if both parts are valid integers and non-negative
			if err != nil || seq < 0 {
				return "", false, ERR_STREAM_XADD_INVALID
			}
		}
	}
	id = fmt.Sprintf("%d-%d", ms, seq)

	if len(s.Items) > 0 {
		lastId := s.Items[len(s.Items)-1].Id

		lastMs, lastSeq, ok := parseStreamId(lastId)

		if !ok {
			return "", false, ERR_STREAM_XADD_INVALID
		}

		if ms < lastMs || (ms == lastMs && seq <= lastSeq) {
			return "", false, ERR_STREAM_XADD_INVALID
		}
	}
	return id, true, ""

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

	s.cond.Signal()

	return newValidId, true, ""
}

func (s *SafeStream) xRangeHelper(key string, start, end string) ([]StreamItem, bool) {

	stream, ok := s.m[key]
	if !ok {
		return nil, false
	}

	if start == "-" && len(stream.Items) > 0 {
		start = stream.Items[0].Id
	}

	if end == "+" && len(stream.Items) > 0 {
		end = stream.Items[len(stream.Items)-1].Id
	}

	startMs, startSeq, ok1 := parseStreamId(start)
	endMs, endSeq, ok2 := parseStreamId(end)

	var result []StreamItem = []StreamItem{}

	if !ok1 || !ok2 {
		return result, false
	}

	for _, item := range stream.Items {
		ms, seq, ok := parseStreamId(item.Id)
		if !ok {
			continue
		}
		// validate stream id
		if (ms > startMs || (ms == startMs && seq >= startSeq)) &&
			(ms < endMs || (ms == endMs && seq <= endSeq)) {
			result = append(result, item)
		}
	}
	return result, true
}
func (s *SafeStream) XRange(key string, start, end string) ([]StreamItem, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.xRangeHelper(key, start, end)
}
func (s *SafeStream) XRead(keys []string, ids []string, timeout time.Duration) map[string][]StreamItem {

	fmt.Printf("ids %v \r\n", ids)
	// timeout < 0 no block
	// timeout = 0 block without timeout
	// timeout > 0 block with timeout

	// if timeout < 0 {
	// 	// 對於非阻塞情況，我們快速檢查一次。
	// 	s.mu.Lock()
	// 	defer s.mu.Unlock()
	// 	result := make(map[string][]StreamItem)
	// 	for i, key := range keys {
	// 		// 非阻塞時，$ 沒有意義，將其視為從頭開始。
	// 		startId := ids[i]
	// 		if startId == "$" {
	// 			startId = "0-0"
	// 		}
	// 		items, ok := s.xRangeHelper(key, incrementStreamId(startId, 1), "+")
	// 		if ok && len(items) > 0 {
	// 			result[key] = items
	// 		}
	// 	}
	// 	if len(result) > 0 {
	// 		return result
	// 	}
	// 	return nil
	// }

	// // --- 1. 設置超時和結果通道 ---
	// ctx := context.Background()
	// if timeout > 0 {
	// 	var cancel context.CancelFunc
	// 	ctx, cancel = context.WithTimeout(context.Background(), timeout)
	// 	defer cancel()
	// }

	// resultChan := make(chan map[string][]StreamItem, 1)

	// // --- 2. 啟動「等待者」goroutine ---
	// go func() {
	// 	s.mu.Lock()
	// 	defer s.mu.Unlock()

	// 	// 在 goroutine 內部解析 '$' ID，確保與等待操作在同一個鎖內。
	// 	effectiveIds := make([]string, len(ids))
	// 	for i, id := range ids {
	// 		if id == "$" {
	// 			stream, ok := s.m[keys[i]]
	// 			if ok && len(stream.Items) > 0 {
	// 				effectiveIds[i] = stream.Items[len(stream.Items)-1].Id
	// 			} else {
	// 				effectiveIds[i] = "0-0"
	// 			}
	// 		} else {
	// 			effectiveIds[i] = id
	// 		}
	// 	}

	// 	// 主等待迴圈
	// 	for {
	// 		// 檢查是否有新資料
	// 		result := make(map[string][]StreamItem)
	// 		for i, key := range keys {
	// 			items, ok := s.xRangeHelper(key, incrementStreamId(effectiveIds[i], 1), "+")
	// 			if ok && len(items) > 0 {
	// 				result[key] = items
	// 			}
	// 		}

	// 		// 如果找到資料，發送到 channel 並結束 goroutine
	// 		if len(result) > 0 {
	// 			resultChan <- result
	// 			return
	// 		}

	// 		// 在等待前，檢查是否已經超時，以避免不必要的等待
	// 		if ctx.Err() != nil {
	// 			// 不要發送任何東西，直接退出 goroutine
	// 			return
	// 		}

	// 		// 等待信號
	// 		s.cond.Wait()
	// 	}
	// }()

	// // --- 3. 等待結果或超時 ---
	// select {
	// case res := <-resultChan:
	// 	// 成功從等待者那裡收到了結果
	// 	return res
	// case <-ctx.Done():
	// 	// 超時發生。我們需要喚醒可能還在等待的 goroutine，讓它退出。
	// 	// 使用 Broadcast 以確保在複雜情況下所有等待者都能被喚醒。
	// 	s.cond.Broadcast()
	// 	return nil // 返回 nil 表示超時
	// }
	////////////////////////////////
	s.mu.Lock()
	validIds := make([]string, len(ids))
	for i, id := range ids {
		if id == "$" {
			// If the ID is '$', replace it with the current last ID of the stream.
			stream, ok := s.m[keys[i]]
			if ok && len(stream.Items) > 0 {
				validIds[i] = stream.Items[len(stream.Items)-1].Id
			} else {
				// If the stream is empty or doesn't exist, start from the beginning.
				validIds[i] = "0-0"
			}
		} else {
			validIds[i] = id
		}
	}

	fmt.Printf("validIds %v\r\n", validIds)

	result := make(map[string][]StreamItem)
	for i, key := range keys {
		items, ok := s.XRange(key, incrementStreamId(validIds[i], 1), "+")
		if ok && len(items) > 0 {
			result[key] = items
		}
	}

	s.mu.Unlock()
	fmt.Printf("result %v\r\n", result)

	if timeout < 0 {
		if len(result) > 0 {
			return result
		}
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	if timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, timeout)
	}

	defer cancel()

	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
			s.cond.Signal()
		case <-done:
			return
		}
	}()

	s.mu.Lock()
	defer s.mu.Unlock()

	for {
		// Check for data again inside the lock.
		for i, key := range keys {
			// Use the internal, non-locking version of XRange since we already hold the lock.
			items, ok := s.xRangeHelper(key, incrementStreamId(validIds[i], 1), "+")
			if ok && len(items) > 0 {
				result[key] = items
			}
		}

		// If we found data for any key, we are done.
		if len(result) > 0 {
			fmt.Println("after len(result) > 0 in for")
			return result
		}

		// If no data, check if the context was cancelled (i.e., we timed out).
		if ctx.Err() != nil {
			fmt.Println("after ctx.Err() in for")
			return nil // Timed out.
		}
		fmt.Println("before cond.Wait")
		s.cond.Wait()
		fmt.Println("after cond.Wait")
	}
}
func toRespString(val interface{}) string {
	switch v := val.(type) {
	case string:
		return v
	case []byte:
		return string(v)
	case int:
		return strconv.Itoa(v)
	case int64:
		return strconv.FormatInt(v, 10)
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64)
	case bool:
		if v {
			return "1"
		}
		return "0"
	default:
		return fmt.Sprint(v) // fallback for unknown types
	}
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
	var isReadFirstByte bool
	var command_count int
	var err error
	for scanner.Scan() {
		text := scanner.Text()

		// Handle the command
		text = strings.TrimSpace(text)
		fmt.Println(text)

		if strings.HasPrefix(text, "*") && !isReadFirstByte {

			command_count, err = strconv.Atoi(text[1:])
			if err != nil {
				conn.Write([]byte("-ERR invalid number of arguments\r\n"))
				continue
			}

			if command_count == 0 {
				conn.Write([]byte("-ERR empty command\r\n"))
				continue
			}
			isReadFirstByte = true
			commands = make([]string, 0, command_count)

		} else if strings.HasPrefix(text, "$") {
			//["ECHO", "hey"]
			//*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n
			readBulkCommand = true
			if len(text) > 1 {
				continue
			}
		}

		// $1 應該跳過
		// $  不應該跳過

		if readBulkCommand && isReadFirstByte {
			commands = append(commands, text)
			readBulkCommand = false
		}

		if len(commands) == command_count {

			fmt.Printf("commands %v\r\n", commands)

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
			case "XRANGE":
				if len(commands) == 4 {
					items, ok := safeStream.XRange(commands[1], commands[2], commands[3])
					if ok {
						stringBuilder := strings.Builder{}
						stringBuilder.WriteString(fmt.Sprintf("*%d\r\n", len(items)))

						for _, item := range items {
							stringBuilder.WriteString("*2\r\n")
							stringBuilder.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(item.Id), item.Id))

							// each item has key and value
							stringBuilder.WriteString(fmt.Sprintf("*%d\r\n", len(item.Fields)*2))
							for k, v := range item.Fields {
								valStr := toRespString(v.Value)
								stringBuilder.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(k), k))
								stringBuilder.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(valStr), valStr))
							}
						}

						conn.Write([]byte(stringBuilder.String()))

					} else {
						conn.Write([]byte("*0\r\n"))
					}

				} else {
					// conn.Write([]byte("-ERR wrong number of arguments for 'xrange' command\r\n"))
				}
			case "XREAD":

				var timeout float64
				var streamIndex = 1

				if strings.ToLower(commands[1]) == "block" {
					if len(commands) < 4 {
						conn.Write([]byte("-ERR syntax error\r\n"))
						continue
					}
					// t, err := strconv.Atoi(commands[2])
					timeout, err = strconv.ParseFloat(commands[2], 64)
					if err != nil || timeout < 0 {
						conn.Write([]byte("-ERR invalid timeout\r\n"))
						continue
					}
					streamIndex = 3
				} else {
					// no block
					timeout = -1
				}

				if len(commands) <= streamIndex || strings.ToLower(commands[streamIndex]) != "streams" {
					conn.Write([]byte("-ERR syntax error missing streams\r\n"))
					continue
				}

				streamIndex++
				streamCount := (len(commands) - streamIndex) / 2

				if (len(commands)-streamIndex)%2 != 0 || streamCount == 0 {
					conn.Write([]byte("-ERR wrong number of arguments for 'xread'\r\n"))
					continue
				}

				keys := commands[streamIndex : streamIndex+streamCount]
				ids := commands[streamIndex+streamCount:]
				if len(keys) != len(ids) {
					conn.Write([]byte("-ERR wrong number of key and Ids for 'xread' command\r\n"))
					continue
				}

				// timeout = 0
				result := safeStream.XRead(keys, ids, time.Duration(timeout*float64(time.Millisecond)))
				if result == nil {
					conn.Write([]byte("$-1\r\n"))
					continue
				}

				stringBuilder := strings.Builder{}
				stringBuilder.WriteString(fmt.Sprintf("*%d\r\n", len(result))) // top-level array

				for _, key := range keys {
					items, ok := result[key]
					if !ok {
						continue
					}
					stringBuilder.WriteString("*2\r\n")                                    // key + items
					stringBuilder.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(key), key)) // key

					stringBuilder.WriteString(fmt.Sprintf("*%d\r\n", len(items))) // entries
					for _, item := range items {
						stringBuilder.WriteString("*2\r\n") // id + field-value array
						stringBuilder.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(item.Id), item.Id))
						stringBuilder.WriteString(fmt.Sprintf("*%d\r\n", len(item.Fields)*2))
						for k, v := range item.Fields {
							valStr := toRespString(v.Value)
							stringBuilder.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(k), k))
							stringBuilder.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(valStr), valStr))
						}
					}
				}
				conn.Write([]byte(stringBuilder.String()))

			default:
				conn.Write([]byte("-ERR unknown command\r\n"))
			}

			readBulkCommand = false
			commands = nil
			command_count = 0
			isReadFirstByte = false

		} else {

		}

	}
}
