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

// Recv will update self to get the latest timestamp.

func (self *LamportTimestamp) Recv(other int) {
	self.mu.Lock()
	defer self.mu.Unlock()
	self.time = max(self.time, other) + 1
}
func (self *LamportTimestamp) Val() int {
	self.mu.Lock()
	defer self.mu.Unlock()
	return self.time
}
