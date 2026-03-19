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
}

func NewKvEvent(op Op, key string, value *structpb.Value) *KvEvent {
	return &KvEvent{
		op,
		key,
		value,
	}
}

func KvEventFromProto(pb *kvv1.KvEvent) (*KvEvent, error) {
	op, err := OpFromProto(pb.GetOp())
	if err != nil {
		return nil, err
	}
	return &KvEvent{
		Op:    op,
		Key:   pb.GetKey(),
		Value: pb.GetValue(),
	}, nil
}

type KvStore struct {
	mu    sync.RWMutex
	store map[string][]byte
}

func NewKvStore() *KvStore {
	return &KvStore{
		store: make(map[string][]byte),
	}
}

// Put places a kv pair into the store. Returns true if it overwrote a value
func (kv *KvStore) put(key string, value []byte) bool {
	_, exist := kv.store[key]
	kv.store[key] = value
	return exist
}

// return true if key exist in kv
func (kv *KvStore) delete(key string) bool {
	if _, exist := kv.store[key]; !exist {
		return false
	}
	delete(kv.store, key)
	return true
}

func (kv *KvStore) get(key string) ([]byte, bool) {
	value, exist := kv.store[key]
	return value, exist
}

func (kv *KvStore) Put(key string, value []byte) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	return kv.put(key, value)

}

func (kv *KvStore) Get(key string) ([]byte, bool) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	return kv.get(key)
}
func (kv *KvStore) Delete(key string) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	return kv.delete(key)
}

// returns: count of events successfully applied, error
func (kv *KvStore) ApplyLog(events []*KvEvent) (int, error) {
	// lock once to avoid contention
	kv.mu.Lock()
	defer kv.mu.Unlock()
	for i, e := range events {
		switch e.Op {
		case OpPut:
			valueJson, err := e.Value.MarshalJSON()
			if err != nil {
				return i, err
			}
			kv.put(e.Key, valueJson)
		case OpDelete:
			kv.delete(e.Key)
		}
	}
	return len(events), nil
}
