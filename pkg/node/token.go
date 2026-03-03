package node

import (
	"fmt"

	kvv1 "ds/v2/pkg/gen/kv/v1"
	"ds/v2/pkg/kv"
)

type Token struct {
	HolderID   string
	LogOffset  uint64
	MinApplied uint64
	Logs       []kv.KvEvent
}

func TokenFromProto(pb *kvv1.Token) (Token, error) {
	logs := make([]kv.KvEvent, 0, len(pb.GetLogs()))
	for i, e := range pb.GetLogs() {
		ev, err := kv.KvEventFromProto(e)
		if err != nil {
			return Token{}, fmt.Errorf("log entry %d: %w", i, err)
		}
		logs = append(logs, ev)
	}
	return Token{
		HolderID:   pb.GetHolderId(),
		LogOffset:  pb.GetLogOffset(),
		MinApplied: pb.GetMinApplied(),
		Logs:       logs,
	}, nil
}

func (t *Token) ToProto() *kvv1.Token {
	logs := make([]*kvv1.KvEvent, len(t.Logs))
	for i := range t.Logs {
		logs[i] = t.Logs[i].ToProto()
	}
	return &kvv1.Token{
		HolderId:   t.HolderID,
		LogOffset:  t.LogOffset,
		MinApplied: t.MinApplied,
		Logs:       logs,
	}
}
