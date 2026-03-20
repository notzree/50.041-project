package node

import (
	"testing"

	"ds/v2/pkg/kv"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestTokenRoundtrip(t *testing.T) {
	original := &Token{
		HolderId:   "node-1",
		LogOffset:  5,
		MinApplied: 3,
		Logs: []*kv.KvEvent{
			kv.NewKvEvent(kv.OpPut, "k1", structpb.NewStringValue("v1")),
			kv.NewKvEvent(kv.OpDelete, "k2", nil),
		},
	}

	pb := original.ToProto()
	restored, err := TokenFromProto(pb)
	require.NoError(t, err)

	assert.Equal(t, original.HolderId, restored.HolderId)
	assert.Equal(t, original.LogOffset, restored.LogOffset)
	assert.Equal(t, original.MinApplied, restored.MinApplied)
	require.Len(t, restored.Logs, 2)
	assert.Equal(t, kv.OpPut, restored.Logs[0].Op)
	assert.Equal(t, "k1", restored.Logs[0].Key)
	assert.Equal(t, kv.OpDelete, restored.Logs[1].Op)
	assert.Equal(t, "k2", restored.Logs[1].Key)
}

func TestTokenSeqNumbers(t *testing.T) {
	tok := &Token{
		HolderId:  "n1",
		LogOffset: 10,
		Logs: []*kv.KvEvent{
			kv.NewKvEvent(kv.OpPut, "a", structpb.NewStringValue("x")),
			kv.NewKvEvent(kv.OpPut, "b", structpb.NewStringValue("y")),
			kv.NewKvEvent(kv.OpPut, "c", structpb.NewStringValue("z")),
		},
	}

	pb := tok.ToProto()
	assert.Equal(t, uint64(10), pb.Logs[0].Seq)
	assert.Equal(t, uint64(11), pb.Logs[1].Seq)
	assert.Equal(t, uint64(12), pb.Logs[2].Seq)
}

func TestTokenEmptyLogs(t *testing.T) {
	tok := &Token{
		HolderId:  "n1",
		LogOffset: 7,
		Logs:      []*kv.KvEvent{},
	}

	pb := tok.ToProto()
	restored, err := TokenFromProto(pb)
	require.NoError(t, err)
	assert.Equal(t, uint64(7), restored.LogOffset)
	assert.Empty(t, restored.Logs)
}
