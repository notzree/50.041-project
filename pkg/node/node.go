package node

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	kvv1 "ds/v2/pkg/gen/kv/v1"
	"ds/v2/pkg/gen/kv/v1/kvv1connect"
	"ds/v2/pkg/kv"
	"errors"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
	"google.golang.org/protobuf/types/known/structpb"
)

type PendingEvent struct {
	Event *kv.KvEvent
	Done  chan error
}

type Node struct {
	Id       string
	addr     string
	manual   bool
	hasToken atomic.Bool
	kv       *kv.KvStore
	registry *Registry
	// Zero Value if Node has never participated in an event
	timestamp   LamportTimestamp
	lastApplied atomic.Uint64
	// tokenMu serializes token processing (handleIncomingToken / handleTick)
	tokenMu       sync.Mutex
	token         chan *Token
	pendingEvents chan *PendingEvent
}

func NewNode(Id string, addr string, maxFlushCapacity int, ring []PeerConfig, manual bool) (*Node, error) {
	registry, err := NewRegistry(Id, ring)
	if err != nil {
		return nil, err
	}
	return &Node{
		Id:     Id,
		addr:   addr,
		manual: manual,
		// todo: persistence upon crash recovery
		kv:            kv.NewKvStore(),
		registry:      registry,
		timestamp:     *NewLamportTimestamp(),
		token:         make(chan *Token, 1),
		pendingEvents: make(chan *PendingEvent, maxFlushCapacity),
	}, nil
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

	// REST endpoints for manual testing
	mux.HandleFunc("/tick", n.handleTick)
	mux.HandleFunc("/status", n.handleStatus)

	srv := &http.Server{
		Addr:    n.addr,
		Handler: h2c.NewHandler(mux, &http2.Server{}),
	}

	// in auto mode, start the background token loop
	if !n.manual {
		go n.executeWithDistributedMutex(ctx)
	}

	// the node with the lowest ID initiates the token
	if n.registry.selfIdx == 0 {
		n.hasToken.Store(true)
		n.token <- &Token{
			HolderId: n.Id,
			Logs:     make([]*kv.KvEvent, 0),
		}
	}

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

// drainPendingInto drains all pending events into the token's log.
// Returns the number of events flushed.
func (n *Node) drainPendingInto(token *Token) int {
	flushed := 0
	for {
		select {
		case pe := <-n.pendingEvents:
			token.Logs = append(token.Logs, pe.Event)
			n.lastApplied.Add(1)
			pe.Done <- nil
			flushed++
		default:
			return flushed
		}
	}
}

// storeToken puts the token back into the node's channel and marks it as held.
func (n *Node) storeToken(token *Token) {
	n.hasToken.Store(true)
	n.token <- token
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
			n.drainPendingInto(token)
			err := n.passToken(ctx, token)
			if err != nil {
				log.Printf("warn: failed to pass token, retrying: %v\n", err)
				n.storeToken(token)
				time.Sleep(time.Second)
			}
		}
	}
}

func (n *Node) passToken(ctx context.Context, token *Token) error {
	n.hasToken.Store(false)
	// stamp the token with our current Lamport time before sending
	token.Timestamp = uint64(n.timestamp.Tick())

	nextNode := n.registry.Next()
	_, err := nextNode.Client.ReceiveToken(ctx, &kvv1.ReceiveTokenRequest{
		Token: token.ToProto(),
	})
	if err != nil {
		return fmt.Errorf("failed to pass token to next client: %w", err)
	}
	return nil
}

// handleIncomingToken validates, replays unseen logs, and compacts the token.
// Returns error if token is stale or replay fails partially.
func (n *Node) handleIncomingToken(token *Token) error {
	n.tokenMu.Lock()
	defer n.tokenMu.Unlock()

	// atomically check-and-update: reject stale tokens
	if _, ok := n.timestamp.RecvIfNewer(int(token.Timestamp)); !ok {
		return fmt.Errorf("stale token: token timestamp %d < node %s timestamp %d", token.Timestamp, n.Id, n.timestamp.Val())
	}

	preReplayLastApplied := n.lastApplied.Load()
	// replay only the log entries we haven't applied yet
	startIdx := preReplayLastApplied - token.LogOffset
	unseenLogs := token.Logs[startIdx:]
	applied, err := n.kv.ApplyLog(unseenLogs)
	// even on replay error, we still try to recover from partial failures, and continue the ring
	if applied > 0 {
		n.lastApplied.Store(token.LogOffset + startIdx + uint64(applied))
	}
	token.MinApplied = min(token.MinApplied, preReplayLastApplied)
	// compact: trim log entries that all nodes have applied
	if trimCount := token.MinApplied - token.LogOffset; trimCount > 0 {
		token.Logs = token.Logs[trimCount:]
		token.LogOffset = token.MinApplied
	}
	n.storeToken(token)

	return err
}

// Implement kv.v1.TokenService
// the current token holder calls the ReceiveToken method on the next node
// to pass their current token onto the next node
func (n *Node) ReceiveToken(ctx context.Context, req *kvv1.ReceiveTokenRequest) (*kvv1.ReceiveTokenResponse, error) {
	token, err := TokenFromProto(req.Token)
	if err != nil {
		return nil, fmt.Errorf("failed to parse token from proto: %w", err)
	}
	if err := n.handleIncomingToken(token); err != nil {
		log.Printf("warn: handleIncomingToken error: %v\n", err)
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

// handleTick is a REST endpoint (POST /tick) that manually advances the token.
// If this node holds the token, it drains pending events and passes the token.
// If not, it's a no-op. This allows sending tick to all nodes — only the holder acts.
func (n *Node) handleTick(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "application/json")

	select {
	case token := <-n.token:
		flushed := n.drainPendingInto(token)

		passedTo := n.registry.Next().Id
		err := n.passToken(r.Context(), token)
		if err != nil {
			n.storeToken(token)
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]any{
				"error": err.Error(),
			})
			return
		}

		json.NewEncoder(w).Encode(map[string]any{
			"held":           true,
			"passed_to":      passedTo,
			"events_flushed": flushed,
			"log_length":     len(token.Logs),
		})
	default:
		json.NewEncoder(w).Encode(map[string]any{
			"held": false,
		})
	}
}

// handleStatus is a REST endpoint (GET /status) returning node state.
func (n *Node) handleStatus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	json.NewEncoder(w).Encode(map[string]any{
		"id":           n.Id,
		"has_token":    n.hasToken.Load(),
		"last_applied": n.lastApplied.Load(),
		"timestamp":    n.timestamp.Val(),
		"manual":       n.manual,
	})
}
