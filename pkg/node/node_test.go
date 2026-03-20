package node

import (
	"testing"

	"ds/v2/pkg/kv"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"
)

// newTestNode creates a minimal Node for unit testing handleIncomingToken.
// No registry or HTTP server needed.
func newTestNode(id string) *Node {
	return &Node{
		Id:        id,
		kv:        kv.NewKvStore(),
		timestamp: *NewLamportTimestamp(),
		token:     make(chan *Token, 1),
	}
}

func TestHandleIncomingToken_ReplaysUnseenLogs(t *testing.T) {
	n := newTestNode("n1")
	// Simulate that the node has already applied events 0 and 1
	n.lastApplied = 2

	token := &Token{
		HolderId:   "n0",
		LogOffset:  0,
		MinApplied: 10, // high so no compaction interferes
		Logs: []*kv.KvEvent{
			kv.NewKvEvent(kv.OpPut, "already-seen-0", structpb.NewStringValue("x")),
			kv.NewKvEvent(kv.OpPut, "already-seen-1", structpb.NewStringValue("x")),
			kv.NewKvEvent(kv.OpPut, "new-key", structpb.NewStringValue("hello")),
		},
	}

	err := n.handleIncomingToken(token)
	require.NoError(t, err)

	// Only the third event should have been applied
	_, ok := n.kv.Get("already-seen-0")
	assert.False(t, ok, "should not replay already-seen event")

	val, ok := n.kv.Get("new-key")
	require.True(t, ok)
	assert.Contains(t, string(val), "hello")

	assert.Equal(t, uint64(3), n.lastApplied)

	// drain token channel
	<-n.token
}

func TestHandleIncomingToken_RejectsStaleToken(t *testing.T) {
	n := newTestNode("n1")
	// Set the node's timestamp ahead
	n.timestamp.Recv(100)

	token := &Token{
		HolderId:  "n0",
		LogOffset: 0,
		Logs: []*kv.KvEvent{
			kv.NewKvEvent(kv.OpPut, "k", structpb.NewStringValue("v")),
		},
	}

	err := n.handleIncomingToken(token)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not ahead of this node")

	// Token channel should be empty — stale token not forwarded
	select {
	case <-n.token:
		t.Fatal("stale token should not be pushed to channel")
	default:
	}
}

func TestHandleIncomingToken_Compaction(t *testing.T) {
	n := newTestNode("n1")
	n.lastApplied = 5

	token := &Token{
		HolderId:   "n0",
		LogOffset:  3,
		MinApplied: 10, // higher than LogOffset+len(Logs), so will compact based on preReplayLastApplied
		Logs: []*kv.KvEvent{
			kv.NewKvEvent(kv.OpPut, "e3", structpb.NewStringValue("v3")),
			kv.NewKvEvent(kv.OpPut, "e4", structpb.NewStringValue("v4")),
			kv.NewKvEvent(kv.OpPut, "e5", structpb.NewStringValue("v5")),
			kv.NewKvEvent(kv.OpPut, "e6", structpb.NewStringValue("v6")),
		},
	}

	err := n.handleIncomingToken(token)
	require.NoError(t, err)

	// After compaction: MinApplied = min(10, 5) = 5
	// trimCount = 5 - 3 = 2, so first 2 logs trimmed, LogOffset becomes 5
	result := <-n.token
	assert.Equal(t, uint64(5), result.LogOffset)
	assert.Equal(t, uint64(5), result.MinApplied)
	assert.Len(t, result.Logs, 2, "should have trimmed first 2 entries")
	assert.Equal(t, "e5", result.Logs[0].Key)
	assert.Equal(t, "e6", result.Logs[1].Key)
}

func TestHandleIncomingToken_MinAppliedUpdated(t *testing.T) {
	n := newTestNode("n1")
	n.lastApplied = 3

	token := &Token{
		HolderId:   "n0",
		LogOffset:  0,
		MinApplied: 100, // artificially high
		Logs: []*kv.KvEvent{
			kv.NewKvEvent(kv.OpPut, "e0", structpb.NewStringValue("v")),
			kv.NewKvEvent(kv.OpPut, "e1", structpb.NewStringValue("v")),
			kv.NewKvEvent(kv.OpPut, "e2", structpb.NewStringValue("v")),
			kv.NewKvEvent(kv.OpPut, "e3", structpb.NewStringValue("v")),
		},
	}

	err := n.handleIncomingToken(token)
	require.NoError(t, err)

	result := <-n.token
	// MinApplied should be min(100, preReplayLastApplied=3) = 3
	assert.Equal(t, uint64(3), result.MinApplied)
}
