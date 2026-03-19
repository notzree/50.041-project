package node

import (
	"context"
	kvv1 "ds/v2/pkg/gen/kv/v1"
	"ds/v2/pkg/gen/kv/v1/kvv1connect"
	"ds/v2/pkg/kv"
	"errors"
	"fmt"
	"log"
	"net/http"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
	"google.golang.org/protobuf/types/known/structpb"
)

type PendingEvent struct {
	Event *kv.KvEvent
	Done  chan error
}

type Node struct {
	Id   string
	addr string
	kv   *kv.KvStore
	// Zero Value if Node has never participated in an event
	timestamp     LamportTimestamp
	lastApplied   uint64
	token         chan *Token
	pendingEvents chan *PendingEvent
}

func NewNode(Id string, addr string, maxFlushCapacity int) *Node {
	return &Node{
		Id:   Id,
		addr: addr,
		// todo: persistence upon crash recovery
		kv:            kv.NewKvStore(),
		timestamp:     *NewLamportTimestamp(),
		token:         make(chan *Token, 1),
		pendingEvents: make(chan *PendingEvent, maxFlushCapacity),
	}
}

// Start registers all services, starts the background token loop, and serves HTTP.
// Blocks until ctx is cancelled, then shuts down gracefully.
func (n *Node) Start(ctx context.Context) error {
	mux := http.NewServeMux()

	kvPath, kvHandler := kvv1connect.NewKvServiceHandler(n)
	tokenPath, tokenHandler := kvv1connect.NewTokenServiceHandler(n)
	ringPath, ringHandler := kvv1connect.NewRingServiceHandler(n)
	mux.Handle(kvPath, kvHandler)
	mux.Handle(tokenPath, tokenHandler)
	mux.Handle(ringPath, ringHandler)

	srv := &http.Server{
		Addr:    n.addr,
		Handler: h2c.NewHandler(mux, &http2.Server{}),
	}

	// start background token loop
	go n.executeWithDistributedMutex(ctx)

	// run server in a goroutine so we can wait on ctx
	errCh := make(chan error, 1)
	go func() {
		log.Printf("node %s listening on %s", n.Id, n.addr)
		errCh <- srv.ListenAndServe()
	}()

	select {
	case <-ctx.Done():
		log.Printf("node %s shutting down", n.Id)
		return srv.Close()
	case err := <-errCh:
		return err
	}
}

// executeWithDistributedMutex should be spun up from within a goroutine
func (n *Node) executeWithDistributedMutex(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			for {
				select {
				case pe := <-n.pendingEvents:
					pe.Done <- ctx.Err()
				default:
					return
				}
			}
		case token := <-n.token:
		drainLoop:
			for {
				select {
				case pe := <-n.pendingEvents:
					token.Logs = append(token.Logs, pe.Event)
					n.lastApplied++
					pe.Done <- nil
				default:
					break drainLoop
				}
			}
			n.passToken()
		}
	}

}

// Implement kv.v1.TokenService
// the current token holder calls the ReceiveToken method on the next node
// to pass their current token onto the next node
func (n *Node) ReceiveToken(ctx context.Context, req *kvv1.ReceiveTokenRequest) (*kvv1.ReceiveTokenResponse, error) {
	// TODO: figure out token compaction here!!!
	// validate that the incoming token is in-fact logically AFTER!
	// If our lamport timestamp is ahead of the token, drop it, and warn.
	incomingTimestamp := int(req.Token.LogOffset) + len(req.Token.Logs)
	timestampVal := n.timestamp.Val()
	if timestampVal > incomingTimestamp {
		log.Println("warn: ignoring token with smaller lamport timestamp")
		return nil, fmt.Errorf("received token with timestamp: %v that is not ahead of this node: %v with timestamp: %v", incomingTimestamp, n.Id, n.timestamp.Val())
	}
	n.timestamp.Recv(incomingTimestamp)
	token, err := TokenFromProto(req.Token)
	if err != nil {
		return nil, fmt.Errorf("failed to parse token from proto: %w", err)
	}

	// replay only the log entries we haven't applied yet
	startIdx := n.lastApplied - token.LogOffset
	unseenLogs := token.Logs[startIdx:]
	applied, err := n.kv.ApplyLog(unseenLogs)
	// even on replay error, we still try to recover from partial failures, and continue the ring
	if applied > 0 {
		n.lastApplied = token.LogOffset + startIdx + uint64(applied)
	}
	// should not block if everything is correct
	n.token <- token

	if err != nil {
		log.Printf("warn: replay error... continuing with token transmission: %v \n", err)
		return &kvv1.ReceiveTokenResponse{}, nil
	}
	return &kvv1.ReceiveTokenResponse{}, nil
}

// Implement kv.v1.RingService
// Join registers a node to the caller node.
// the caller "joins" the server node's ring.
func (n *Node) Join(ctx context.Context, req *kvv1.JoinRequest) (*kvv1.JoinResponse, error) {

	return nil, nil
}

// Leave removes a node from the caller nodes registry.
// if a node calls Leave before Join, an error is raised.
func (n *Node) Leave(ctx context.Context, req *kvv1.LeaveRequest) (*kvv1.LeaveResponse, error) {
	return nil, nil
}

// Implement kv.v1.KvService
func (n *Node) Get(ctx context.Context, req *kvv1.GetRequest) (*kvv1.GetResponse, error) {
	jsonValue, exist := n.kv.Get(req.Key)
	if !exist {
		return &kvv1.GetResponse{Found: false}, nil
	}
	protoValue := &structpb.Value{}
	err := protoValue.UnmarshalJSON(jsonValue)
	if err != nil {
		return nil, err
	}
	return &kvv1.GetResponse{
		Value: protoValue,
		Found: exist,
	}, nil
}

func (n *Node) Put(ctx context.Context, req *kvv1.PutRequest) (*kvv1.PutResponse, error) {
	valueJson, err := req.Value.MarshalJSON()
	if err != nil {
		return nil, errors.New("failed to marshal value into valid json!")
	}
	event := kv.NewKvEvent(kv.OpPut, req.Key, req.Value)
	doneChan := make(chan error, 1)
	pe := &PendingEvent{Event: event, Done: doneChan}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case n.pendingEvents <- pe:
	}
	// wait for write to be flushed or for user to cancel
	select {
	case <-ctx.Done():
		return nil, ctx.Err()

	case err = <-doneChan:
	}
	if err != nil {
		return nil, fmt.Errorf("failed to append event to token log %w", err)
	}
	_ = n.kv.Put(req.Key, valueJson)
	return &kvv1.PutResponse{}, nil
}

func (n *Node) Delete(ctx context.Context, req *kvv1.DeleteRequest) (*kvv1.DeleteResponse, error) {
	// log even if its a no-op, other node might have kv pair in their local memory
	event := kv.NewKvEvent(kv.OpDelete, req.Key, nil)
	doneChan := make(chan error, 1)
	pe := &PendingEvent{Event: event, Done: doneChan}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case n.pendingEvents <- pe:
	}

	var err error
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case err = <-doneChan:
	}
	if err != nil {
		return nil, fmt.Errorf("failed to append event to token log %w", err)
	}
	_ = n.kv.Delete(req.Key)

	return &kvv1.DeleteResponse{}, nil
}
