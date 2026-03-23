package kv

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestPersistence(t *testing.T) {
	logFile := "test_node.log"
	defer os.Remove(logFile) // Clean up after the test

	// --- PHASE 1: Write Data ---
	store1 := NewKvStore()
	err := store1.OpenLog(logFile)
	assert.NoError(t, err)

	event := NewKvEvent(OpPut, "sutd-key", structpb.NewStringValue("rocks"))
	_, err = store1.ApplyLog([]*KvEvent{event})
	assert.NoError(t, err)
	store1.wal.Close() // Simulate the process stopping

	// --- PHASE 2: Recovery ---
	store2 := NewKvStore()
	err = store2.OpenLog(logFile) // This should trigger recover()
	assert.NoError(t, err)

	val, exists := store2.Get("sutd-key")
	assert.True(t, exists)
	assert.Contains(t, string(val), "rocks")
}
