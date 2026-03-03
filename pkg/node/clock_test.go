package node

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMerge(t *testing.T) {
	a := NewLamportTimestamp(1)
	b := NewLamportTimestamp(2)

	a.Recv(b.Val())
	assert.Equal(t, 3, a.Val())
}
