package datastore

import (
	"math"
	"strconv"
	"sync"
	"time"
)

type Element struct {
	Value      string
	ExpiryTime time.Time
}

type SafeMap struct {
	mu sync.Mutex
	m  map[string]Element
}

func NewSafeMap() *SafeMap {
	return &SafeMap{
		m: make(map[string]Element),
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
func (s *SafeMap) Incr(key string) (int64, bool, string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	val, ok := s.m[key]

	var num int64
	var err error
	if !ok {
		s.m[key] = Element{Value: "1"}
		return 1, true, ""
	}

	num, err = strconv.ParseInt(val.Value, 10, 64)
	if err != nil {
		return 0, false, "value is not an integer or out of range"
	}

	if num == math.MaxInt64 {
		return 0, false, "value is not an integer or out of range"
	}

	num++
	s.m[key] = Element{Value: strconv.FormatInt(num, 10)}
	return num, true, ""
}
