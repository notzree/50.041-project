package kv

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"
)

func strVal(s string) *structpb.Value {
	return structpb.NewStringValue(s)
}

func TestPutAndGet(t *testing.T) {
	store := NewKvStore()
	overwritten := store.Put("key1", []byte(`"hello"`))
	assert.False(t, overwritten)

	val, ok := store.Get("key1")
	require.True(t, ok)
	assert.Equal(t, []byte(`"hello"`), val)
}

func TestPutOverwrite(t *testing.T) {
	store := NewKvStore()
	store.Put("key1", []byte(`"a"`))
	overwritten := store.Put("key1", []byte(`"b"`))
	assert.True(t, overwritten)

	val, _ := store.Get("key1")
	assert.Equal(t, []byte(`"b"`), val)
}

func TestGetMissing(t *testing.T) {
	store := NewKvStore()
	_, ok := store.Get("nope")
	assert.False(t, ok)
}

func TestDelete(t *testing.T) {
	store := NewKvStore()
	store.Put("key1", []byte(`"v"`))
	deleted := store.Delete("key1")
	assert.True(t, deleted)

	_, ok := store.Get("key1")
	assert.False(t, ok)
}

func TestDeleteMissing(t *testing.T) {
	store := NewKvStore()
	deleted := store.Delete("nope")
	assert.False(t, deleted)
}

func TestApplyLogMixedOps(t *testing.T) {
	store := NewKvStore()
	events := []*KvEvent{
		NewKvEvent(OpPut, "a", strVal("1")),
		NewKvEvent(OpPut, "b", strVal("2")),
		NewKvEvent(OpDelete, "a", nil),
	}
	applied, err := store.ApplyLog(events)
	require.NoError(t, err)
	assert.Equal(t, 3, applied)

	_, ok := store.Get("a")
	assert.False(t, ok, "key 'a' should have been deleted")

	val, ok := store.Get("b")
	require.True(t, ok)
	// structpb string values marshal to JSON with quotes
	assert.Contains(t, string(val), "2")
}

func TestApplyLogEmpty(t *testing.T) {
	store := NewKvStore()
	applied, err := store.ApplyLog([]*KvEvent{})
	require.NoError(t, err)
	assert.Equal(t, 0, applied)
}

func TestApplyLogPartialFailure(t *testing.T) {
	store := NewKvStore()
	// A Put event with a nil Value will fail MarshalJSON
	events := []*KvEvent{
		NewKvEvent(OpPut, "good", strVal("ok")),
		{Op: OpPut, Key: "bad", Value: nil},
	}
	applied, err := store.ApplyLog(events)
	assert.Error(t, err)
	assert.Equal(t, 1, applied, "first event should succeed before failure")

	_, ok := store.Get("good")
	assert.True(t, ok)
}
