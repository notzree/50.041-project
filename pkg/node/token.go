package node

import (
	"fmt"

	kvv1 "ds/v2/pkg/gen/kv/v1"
	"ds/v2/pkg/kv"
)

type Token struct {
	HolderId   string
	LogOffset  uint64
	MinApplied uint64
	Timestamp  uint64
	Logs       []*kv.KvEvent
}

func TokenFromProto(pb *kvv1.Token) (*Token, error) {
	logs := make([]*kv.KvEvent, 0, len(pb.GetLogs()))
	for i, e := range pb.GetLogs() {
		ev, err := kv.KvEventFromProto(e)
		if err != nil {
			return nil, fmt.Errorf("log entry %d: %w", i, err)
		}
		logs = append(logs, ev)
	}
	return &Token{
		HolderId:   pb.GetHolderId(),
		LogOffset:  pb.GetLogOffset(),
		MinApplied: pb.GetMinApplied(),
		Timestamp:  pb.GetTimestamp(),
		Logs:       logs,
	}, nil
}

func (t *Token) ToProto() *kvv1.Token {
	logs := make([]*kvv1.KvEvent, len(t.Logs))
	for i, log := range t.Logs {
		logs[i] = &kvv1.KvEvent{
			Op:    log.Op.ToProto(),
			Key:   log.Key,
			Value: log.Value,
			Seq:   uint64(i) + t.LogOffset,
		}
	}
	return &kvv1.Token{
		HolderId:   t.HolderId,
		LogOffset:  t.LogOffset,
		MinApplied: t.MinApplied,
		Timestamp:  t.Timestamp,
		Logs:       logs,
	}
}
