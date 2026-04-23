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
	// Zero Value if Node has never participated in an event
	timestamp   LamportTimestamp
	lastApplied atomic.Uint64
	// tokenMu serializes token processing (handleIncomingToken / handleTick)
	tokenMu       sync.Mutex
	token         chan *Token
	freshestToken *Token
	tokenHeartbeatMutex sync.Mutex
	pendingEvents chan *PendingEvent
	// Membership
	registry *Registry
	electionMu sync.Mutex
	paxosMode atomic.Bool
	acceptedProposalLock sync.Mutex
	acceptedProposalRound uint64
	acceptedProposalRoundChanges uint64
	acceptedProposalNodeId string
}

func NewNode(Id string, addr string, maxFlushCapacity int, ring []PeerConfig, manual bool) (*Node, error) {
	registry, err := NewRegistry(Id, ring)
	if err != nil {
		return nil, err
	}
	store := kv.NewKvStore()
	logFile := fmt.Sprintf("%s.log", Id)
	if err := store.OpenLog(logFile); err != nil {
		// We log the error but allow the node to start in-memory if the file fails
		log.Printf("Warning: Persistence failed for %s: %v", Id, err)
	} else {
		log.Printf("Persistence is enabled for %s: writing to %s", Id, logFile)
	}

	n := Node{
        Id:               Id,
        addr:             addr,
        manual:           manual,
        kv:               store,
        timestamp:        *NewLamportTimestamp(),
        token:            make(chan *Token, 10),
        pendingEvents:    make(chan *PendingEvent, maxFlushCapacity),
        registry:         registry,
		acceptedProposalRound: 0,
		acceptedProposalRoundChanges: 0,
		acceptedProposalNodeId: "",
    }
	n.paxosMode.Store(false)
    return &n, nil
}

// Start registers all services, starts the background token loop, and serves HTTP.
// Blocks until ctx is cancelled, then shuts down gracefully.
func (n *Node) Start(ctx context.Context) error {
	fmt.Print("Starting node")
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

	// REST endpoints for switching to paxos
	mux.HandleFunc("/enable-paxos", n.handleEnablePaxos)
	mux.HandleFunc("/disable-paxos", n.handleDisablePaxos)

	srv := &http.Server{
		Addr:    n.addr,
		Handler: h2c.NewHandler(mux, &http2.Server{}),
	}

	// in auto mode, start the background token loop
	if !n.manual {
		go n.executeWithDistributedMutex(ctx)
	}

	// token keep alive service
	go func() {
		lastSeenFreshestToken := n.freshestToken
		for {
			time.Sleep(time.Minute)
			if n.freshestToken != nil && n.freshestToken == lastSeenFreshestToken {
				for n.freshestToken == lastSeenFreshestToken {
					log.Printf("Token keep alive goroutine triggered to send token to %s", n.registry.Next().Id)
					n.passToken(ctx, lastSeenFreshestToken)
					time.Sleep(10*time.Second)
				}
			} else {
				lastSeenFreshestToken = n.freshestToken
			}
		}
	}()

	// run server in a goroutine so we can wait on ctx
	errCh := make(chan error, 1)
	go func() {
		log.Printf("node %s listening on %s", n.Id, n.addr)
		errCh <- srv.ListenAndServe()
	}()

	// Connect to everyone
	n.registry.Connect(ctx)
	
	// the coordinator node initiates the token
	if n.registry.IsCoordinator() {
		peers := n.registry.GetPeers()
		peersLen := len(*peers)
		seenTokenCh := make(chan bool, peersLen)
		seenTokenReq := kvv1.SeenTokenRequest{}
		for i := range *peers {
			peer := &(*peers)[i]
			go func (peer *Peer) {
				res, err := peer.TokenClient.SeenToken(ctx, &seenTokenReq)
				if err != nil && res != nil {
					seenTokenCh <- res.Seen
				}
				seenTokenCh <- false
			}(peer)
		}
		responsesReceived := 0
		for responsesReceived < peersLen {
			select {
			case seen := <-seenTokenCh:
				if seen {
					break
				}
				responsesReceived++
				if responsesReceived >= peersLen {
					log.Printf("Initiating token because none saw")
					n.hasToken.Store(true)
					n.token <- &Token{
						HolderId: n.Id,
						Logs:     make([]*kv.KvEvent, 0),
					}
				}
			case <-time.After(10*time.Second):
				log.Printf("Initiating token because no one replied in time")
				n.hasToken.Store(true)
				n.token <- &Token{
					HolderId: n.Id,
					Logs:     make([]*kv.KvEvent, 0),
				}
			}
		}
	}

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
	var eventsToApply []*kv.KvEvent
	for {
		select {
		case pe := <-n.pendingEvents:
			token.Logs = append(token.Logs, pe.Event)
			eventsToApply = append(eventsToApply, pe.Event)
			n.lastApplied.Add(1)
			pe.Done <- nil
			flushed++
		default:
			if len(eventsToApply) > 0 {
				n.kv.ApplyLog(eventsToApply)
			}
			return flushed // The loop ends here when the queue is empty
		}
	}
}

// storeToken puts the token back into the node's channel and marks it as held.
func (n *Node) storeToken(token *Token) {
	n.hasToken.Store(true)
	n.token <- token
}

type StaleTokenError struct {
	Message string
}
func (e StaleTokenError) Error() string {
    return e.Message
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
			log.Printf("Passing token to %s in 5 seconds", n.registry.Next().Id)
			time.Sleep(5 * time.Second)
			err := n.passToken(ctx, token)
			if err != nil {
				if token == n.freshestToken {
					log.Printf("warn: failed to pass token to %s, retrying: %v\n", n.registry.Next().Id, err)
					n.storeToken(token)
				}
			}
		}
	}
}

func (n *Node) passToken(ctx context.Context, token *Token) error {
	n.hasToken.Store(false)
	// stamp the token with our current Lamport time before sending
	token.Timestamp = uint64(n.timestamp.Tick())

	nextNode := n.registry.Next()
	_, err := nextNode.TokenClient.ReceiveToken(ctx, &kvv1.ReceiveTokenRequest{
		Token: token.ToProto(),
	})
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

// handleIncomingToken validates, replays unseen logs, and compacts the token.
// Returns error if token is stale or replay fails partially.
func (n *Node) handleIncomingToken(token *Token) error {
	n.tokenMu.Lock()
	defer n.tokenMu.Unlock()

	// atomically check-and-update: reject stale tokens
	if _, ok := n.timestamp.RecvIfNewer(int(token.Timestamp)); !ok {
		return fmt.Errorf("stale token: token timestamp %d < node %s timestamp %d", token.Timestamp, n.Id, n.timestamp.Val())
	}
	n.freshestToken = token

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

// Implement kv.v1.RingService
// Join registers a node to the caller node.
// the caller "joins" the server node's ring.
func (n *Node) Join(ctx context.Context, req *kvv1.JoinRequest) (*kvv1.JoinResponse, error) {
	peers, coordinatorIdx := n.registry.GetPeersAndCoordinatorIdx()
	ring := make([]*kvv1.Node, len(*peers))
	for i := range *peers {
		ring[i] = &kvv1.Node{Id: (*peers)[i].Id, Addr: (*peers)[i].Addr}
	}
	if coordinatorIdx >= 0 && n.registry.SelfId == (*peers)[coordinatorIdx].Id {
		log.Printf("Coordinator received join request")
		log.Printf("%v", ring)
		for i := range ring {
			if (*ring[i]).Id == req.Node.Id {
				log.Printf("Join request detected from an existing member, no need to set peers for everyone")
				break
			} else if (*ring[i]).Id > req.Node.Id {
				ring = append(ring[:i], append([]*kvv1.Node{req.Node}, ring[i:]...)...)
				if i <= coordinatorIdx {
					coordinatorIdx++
				}
				break
			} else if i == len(ring)-1 && (*ring[i]).Id < req.Node.Id {
				ring = append(ring, req.Node)
				break
			}
		}
		log.Printf("Coordinator with idx %v now setting peers for everyone", coordinatorIdx)
		log.Printf("Peers: %v", peers)
		setPeersRequest := kvv1.SetPeersRequest{Nodes: ring, CoordinatorIdx: int64(coordinatorIdx)}
			for i := range *peers {
				go func(peer *Peer) {
					ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					defer cancel()
					_, err := peer.RingClient.SetPeers(ctx, &setPeersRequest)
					if err != nil {
						log.Printf("Error setting peers for %s: %v", peer.Id, err)
					}
				}(&(*peers)[i])
		}
	}

	joinResponse := kvv1.JoinResponse{
		Ring: ring,
		CoordinatorIdx: int64(coordinatorIdx),
	}
	log.Printf("Sent JoinResponse: %v", &joinResponse)
	return &joinResponse, nil
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
	if (n.paxosMode.Load() == true) {
		if n.registry.coordinatorId == n.registry.SelfId {
			// Blast out changes
			log.Printf("Sending put to all acceptors")
			n.acceptedProposalLock.Lock()
			defer n.acceptedProposalLock.Unlock()
			peerCount := len(n.registry.peers)
			ch := make(chan bool, peerCount)
			seq := n.acceptedProposalRoundChanges + 1
			kvEvent := kvv1.KvEvent{Op: kvv1.KvEvent_OP_PUT, Key: req.Key, Value: req.Value, Seq: seq}
			request := kvv1.ReceiveProposalRoundChangeRequest{NodeId: n.registry.SelfId, Round: n.acceptedProposalRound, KvEvent: &kvEvent}
			for idx, peer := range n.registry.peers {
				if (idx != n.registry.selfIdx) {
					go func(p *Peer) {
						_, err := p.RingClient.ReceiveProposalRoundChange(ctx, &request)
						if err == nil {
							log.Printf("Received accepted change response from %v", p.Id)
							ch <- true
						} else {
							log.Printf("Received declined change response from %v", p.Id)
							ch <- false
						}
					}(&peer)
				}
			}

			// Count accepted proposals
			promisedPeers := 1;
			responsesReceived := 1
			// majority := peerCount / 2 + 1
			majority := peerCount
			for res := range ch {
				responsesReceived++
				if res == true {
					promisedPeers++	
				}
				if promisedPeers >= majority  {
					valueJson, err := req.Value.MarshalJSON()
					if err != nil {
						return nil, errors.New("failed to marshal value into valid json!")
					}
					n.kv.Put(req.Key, valueJson)
					n.acceptedProposalRoundChanges++
					log.Printf("Majority accepted round change!")
					return &kvv1.PutResponse{}, nil
				} else if promisedPeers + peerCount - responsesReceived < majority {
					log.Printf("Majority did not accept")
				}
			}
		} 
		return nil, errors.New("This node is a non-coordinator and not allowed to write while paxos is active");
	} else {
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
	peers, coordinatorIdx := n.registry.GetPeersAndCoordinatorIdx()
	peerIds := make([]string, len(*peers))
	for i := range *peers {
		peerIds[i] = (*peers)[i].Id 
	}
	json.NewEncoder(w).Encode(map[string]any{
		"id":           n.Id,
		"has_token":    n.hasToken.Load(),
		"last_applied": n.lastApplied.Load(),
		"timestamp":    n.timestamp.Val(),
		"manual":       n.manual,
		"peers": 		peerIds,
		"paxos": 		n.paxosMode.Load(),
		"coordinator":  (*peers)[coordinatorIdx].Id,
		"acceptedProposalRound": n.acceptedProposalRound,
		"acceptedProposalRoundChanges": n.acceptedProposalRoundChanges,
		"acceptedProposalNodeId": n.acceptedProposalNodeId,
	})
}

func (n *Node) handleEnablePaxos(w http.ResponseWriter, r *http.Request) {
	n.paxosMode.Store(true)
	n.registry.mutex.Lock()
	defer n.registry.mutex.Unlock()
	if (n.registry.coordinatorId == n.registry.SelfId) {
		n.acceptedProposalLock.Lock()
		defer n.acceptedProposalLock.Unlock()

		// Prepare proposal
		ctx := r.Context()
		Round := n.acceptedProposalRound + 1
		RoundChanges := uint64(0)
		NodeId := n.registry.SelfId
		peerCount := len(n.registry.peers)
		ch := make(chan *kvv1.ReceivePrepareProposalRoundResponse, peerCount)
		request := kvv1.ReceivePrepareProposalRoundRequest{ Round: Round, RoundChanges: RoundChanges, NodeId: NodeId}
		for _, peer := range n.registry.peers {
			go func(p *Peer) {
				response, err := p.RingClient.ReceivePrepareProposalRound(ctx, &request)
				if (err == nil) {
					ch <- response
				}
			}(&peer)
		}

		// Count accepted proposals
		promisedPeers := 0;
		responsesReceived := 0
		majority := peerCount / 2 + 1
		for res := range ch {
			if res.Promised == true {
				promisedPeers++
			}
			responsesReceived++
			log.Printf("promisedPeers: %v/%v", promisedPeers, peerCount)
			if promisedPeers >= majority  {
				n.acceptedProposalRound = Round
				n.acceptedProposalRoundChanges = RoundChanges
				n.acceptedProposalNodeId = NodeId
				log.Printf("Majority accepted!")
				return
			} else if promisedPeers + peerCount - responsesReceived < majority {
				log.Printf("Majority did not accept")
				return
			}
		}
	}
}

func (n *Node) handleDisablePaxos(w http.ResponseWriter, r *http.Request) {
	n.paxosMode.Store(false)
}

func (n *Node) ReceiveElectionRequest(ctx context.Context, req *kvv1.ElectionRequest) (*kvv1.ElectionResponse, error) {
	log.Printf("Received election request from %v", req.NodeId)
	switch n.registry.RegistryState {
	case Stable:
		return &kvv1.ElectionResponse{
			NodeId: n.registry.coordinatorId,
		}, nil
	}
	return &kvv1.ElectionResponse{
		NodeId: "",
	}, nil
}

func (n *Node) ReceivePrepareProposalRound(ctx context.Context, req *kvv1.ReceivePrepareProposalRoundRequest) (*kvv1.ReceivePrepareProposalRoundResponse, error) {
	n.acceptedProposalLock.Lock()
	defer n.acceptedProposalLock.Unlock()
	log.Printf("Received proposal from %v for round %v", req.NodeId, req.Round)

	accepted := false
	if req.Round > n.acceptedProposalRound {
		accepted = true
	} else if req.Round == n.acceptedProposalRound && req.NodeId > n.acceptedProposalNodeId{
		accepted = true	
	}

	response := kvv1.ReceivePrepareProposalRoundResponse{
		Promised: true,
		PrevAcceptedRound: n.acceptedProposalRound,
		PrevAcceptedRoundChanges: n.acceptedProposalRoundChanges,
		PrevAcceptedNodeId: n.acceptedProposalNodeId,
	}
	if accepted == true {
		n.acceptedProposalRound = req.Round
		n.acceptedProposalRoundChanges = req.RoundChanges
		n.acceptedProposalNodeId = req.NodeId
		log.Printf("Accepted proposal")
	} else {
		response.Promised = false
		log.Printf("Rejected proposal")
	}
	return &response, nil
}

func (n *Node) ReceiveProposalRoundChange(ctx context.Context, req *kvv1.ReceiveProposalRoundChangeRequest) (*kvv1.ReceiveProposalRoundChangeResponse, error) {
	n.acceptedProposalLock.Lock()
	defer n.acceptedProposalLock.Unlock()

	if req.NodeId != n.registry.coordinatorId {
		return nil, errors.New("Not coordinator")
	} else if req.Round != n.acceptedProposalRound {
		return nil, errors.New("Not correct proposal round")
	} else if req.KvEvent.Seq != n.acceptedProposalRoundChanges + 1 {
		return nil, errors.New("Not correct proposal round change")
	}
	
	switch req.KvEvent.Op {
	case kvv1.KvEvent_OP_PUT:
		valueJson, err := req.KvEvent.Value.MarshalJSON()
		if err != nil {
			return nil, err
		}
		n.kv.Put(req.KvEvent.Key, valueJson)
	case kvv1.KvEvent_OP_DELETE:
		n.kv.Delete(req.KvEvent.Key)
	default:
		return nil, errors.New("Unsupported op")
	}
	
	n.acceptedProposalRoundChanges = req.KvEvent.Seq

	return &kvv1.ReceiveProposalRoundChangeResponse{}, nil
}

func (n *Node) Ping(ctx context.Context, req *kvv1.PingRequest) (*kvv1.PingResponse, error) {
	return nil, nil
}

func (n *Node) SetPeers(ctx context.Context, req *kvv1.SetPeersRequest) (*kvv1.SetPeersResponse, error) {
	_, err := n.registry.SetPeers(req)
	return nil, err
}

func (n *Node) SeenToken(ctx context.Context, req *kvv1.SeenTokenRequest) (*kvv1.SeenTokenResponse, error) {
	return &kvv1.SeenTokenResponse{Seen: n.freshestToken != nil}, nil
}