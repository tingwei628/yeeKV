package datastore

import (
	"context"
	"sync"
	"time"
)

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

type SafeList struct {
	cond *sync.Cond
	mu   sync.Mutex
	m    map[string]*LinkedList
}

func NewSafeList() *SafeList {
	sl := &SafeList{
		m: make(map[string]*LinkedList),
	}
	sl.cond = sync.NewCond(&sl.mu)
	return sl
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
