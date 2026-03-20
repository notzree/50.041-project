package node

import (
	"sync"
)

type LamportTimestamp struct {
	mu   sync.Mutex
	time int
}

func NewLamportTimestamp(timeargs ...int) *LamportTimestamp {
	var time int
	if len(timeargs) == 0 {
		time = 0
	} else {
		time = timeargs[0]
	}

	return &LamportTimestamp{
		time: time,
	}
}

// Tick increments the timestamp for a local event and returns the new value.
func (self *LamportTimestamp) Tick() int {
	self.mu.Lock()
	defer self.mu.Unlock()
	self.time++
	return self.time
}

// Recv will update self to get the latest timestamp.
func (self *LamportTimestamp) Recv(other int) {
	self.mu.Lock()
	defer self.mu.Unlock()
	self.time = max(self.time, other) + 1
}

// RecvIfNewer atomically checks if other > self, and if so updates.
// Returns (new value, true) if updated, (current value, false) if stale.
func (self *LamportTimestamp) RecvIfNewer(other int) (int, bool) {
	self.mu.Lock()
	defer self.mu.Unlock()
	if self.time > other {
		return self.time, false
	}
	self.time = max(self.time, other) + 1
	return self.time, true
}

func (self *LamportTimestamp) Val() int {
	self.mu.Lock()
	defer self.mu.Unlock()
	return self.time
}
