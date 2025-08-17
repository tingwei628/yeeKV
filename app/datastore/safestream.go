package datastore

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	ERR_STREAM_XADD_00      = "The ID specified in XADD must be greater than 0-0"
	ERR_STREAM_XADD_INVALID = "The ID specified in XADD is equal or smaller than the target stream top item"
)

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

type SafeStream struct {
	cond *sync.Cond
	mu   sync.Mutex
	m    map[string]*Stream
}

func NewSafeStream() *SafeStream {
	ss := &SafeStream{
		m: make(map[string]*Stream),
	}
	ss.cond = sync.NewCond(&ss.mu)
	return ss
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

	// timeout < 0 no block
	// timeout = 0 block without timeout
	// timeout > 0 block with timeout

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

	s.mu.Unlock()
	result := make(map[string][]StreamItem)
	for i, key := range keys {
		// XRange has mutexes
		items, ok := s.XRange(key, incrementStreamId(validIds[i], 1), "+")
		if ok && len(items) > 0 {
			result[key] = items
		}
	}

	if timeout < 0 {
		if len(result) > 0 {
			return result
		}
		return nil
	}

	ctx := context.Background()
	var cancel context.CancelFunc
	if timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

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
			return result
		}

		// If no data, check if the context was cancelled (i.e., we timed out).
		if ctx.Err() != nil {
			return nil // Timed out.
		}
		s.cond.Wait()
	}
}
