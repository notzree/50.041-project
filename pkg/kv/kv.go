package kv

import (
	"fmt"
	"sync"

	kvv1 "ds/v2/pkg/gen/kv/v1"

	"google.golang.org/protobuf/types/known/structpb"
)

type Op int

const (
	OpPut    Op = 1
	OpDelete Op = 2
)

func OpFromProto(op kvv1.KvEvent_Op) (Op, error) {
	switch op {
	case kvv1.KvEvent_OP_PUT:
		return OpPut, nil
	case kvv1.KvEvent_OP_DELETE:
		return OpDelete, nil
	default:
		return 0, fmt.Errorf("unknown op: %v", op)
	}
}

func (o Op) ToProto() kvv1.KvEvent_Op {
	switch o {
	case OpPut:
		return kvv1.KvEvent_OP_PUT
	case OpDelete:
		return kvv1.KvEvent_OP_DELETE
	default:
		return kvv1.KvEvent_OP_UNSPECIFIED
	}
}

type KvEvent struct {
	Op    Op
	Key   string
	Value *structpb.Value
	Seq   uint64
}

func KvEventFromProto(pb *kvv1.KvEvent) (KvEvent, error) {
	op, err := OpFromProto(pb.GetOp())
	if err != nil {
		return KvEvent{}, err
	}
	return KvEvent{
		Op:    op,
		Key:   pb.GetKey(),
		Value: pb.GetValue(),
		Seq:   pb.GetSeq(),
	}, nil
}

func (e *KvEvent) ToProto() *kvv1.KvEvent {
	return &kvv1.KvEvent{
		Op:    e.Op.ToProto(),
		Key:   e.Key,
		Value: e.Value,
		Seq:   e.Seq,
	}
}

type KvStore[K comparable, V any] struct {
	mu    sync.RWMutex
	store map[K]V
}

func NewKvStore[K comparable, V any]() *KvStore[K, V] {
	return &KvStore[K, V]{
		store: make(map[K]V),
	}
}
