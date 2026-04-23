package node

import (
	"context"
	kvv1 "ds/v2/pkg/gen/kv/v1"
	"ds/v2/pkg/gen/kv/v1/kvv1connect"
	"errors"
	"fmt"
	"log"
	"net/http"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Peer struct {
	Id     string
	Addr   string
	TokenClient kvv1connect.TokenServiceClient
	RingClient kvv1connect.RingServiceClient
}
type RegistryState string
const (
    Stable RegistryState = ""
	CoordinatorUnreachable RegistryState = "CoordinatorUnreachable"
)

const ElectionWinTime = 10 * time.Second

type Registry struct {
	RegistryState RegistryState
	SelfId string
	selfIdx int
	selfAddr string
	peers   []Peer
	coordinatorId string
	coordinatorIdx int
	PingingPeriodically atomic.Bool
	mutex sync.RWMutex
	heartBeatMutex sync.Mutex
}

type PeerConfig struct {
	Id   string
	Addr string
}

// ParseRing parses "id1=addr1,id2=addr2" into PeerConfigs.
// Addresses without a scheme get "http://" prepended.
func ParseRing(s string) ([]PeerConfig, error) {
	parts := strings.Split(s, ",")
	peers := make([]PeerConfig, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		id, addr, ok := strings.Cut(p, "=")
		if !ok {
			return nil, fmt.Errorf("expected id=addr, got %q", p)
		}
		if !strings.HasPrefix(addr, "http://") && !strings.HasPrefix(addr, "https://") {
			addr = "http://" + addr
		}
		peers = append(peers, PeerConfig{Id: id, Addr: addr})
	}
	if len(peers) == 0 {
		return nil, fmt.Errorf("ring must have at least one peer")
	}
	return peers, nil
}

// NewRegistry builds a registry from a static config.
// The order of ring determines the token-passing order.
// selfId must be present in ring.
func NewRegistry(selfId string, ring []PeerConfig) (*Registry, error) {
	slices.SortFunc(ring, func(a, b PeerConfig) int {
		return strings.Compare(a.Id, b.Id)
	})
	peers := make([]Peer, len(ring))
	selfIdx := -1
	selfAddr := ""
	for i, cfg := range ring {
		peers[i] = Peer{
			Id:     cfg.Id,
			Addr:   cfg.Addr,
			TokenClient: kvv1connect.NewTokenServiceClient(http.DefaultClient, cfg.Addr),
			RingClient: kvv1connect.NewRingServiceClient(http.DefaultClient, cfg.Addr),
		}
		if cfg.Id == selfId {
			selfIdx = i
			selfAddr = cfg.Addr
		}
	}
	if selfIdx == -1 {
		return nil, fmt.Errorf("selfId %q not found in ring config", selfId)
	}
	return &Registry{
		SelfId: selfId,
		selfIdx: selfIdx,
		selfAddr: selfAddr,
		peers:   peers,
		coordinatorId: "",
		coordinatorIdx: -1,
	}, nil
}

// Next returns the next peer in the ring.
func (r *Registry) Next() *Peer {
	r.mutex.RLock()
	defer r.mutex.RUnlock()
	idx := (r.selfIdx + 1) % len(r.peers)
	return &r.peers[idx]
}

// Peer returns a peer by ID, or nil if not found.
func (r *Registry) Peer(id string) *Peer {
	r.mutex.RLock()
	defer r.mutex.RUnlock()
	for i := range r.peers {
		if r.peers[i].Id == id {
			return &r.peers[i]
		}
	}
	return nil
}

// Size returns the number of nodes in the ring.
func (r *Registry) Size() int {
	return len(r.peers)
}

// Called on startup to establish coordinator and verify if everyone is connected
func (r *Registry) Connect(ctx context.Context) error {
	// Check if by itself
	peersCount := len(r.peers);
	if peersCount == 1 {
		return nil
	}

	// Join a peer
	ch := make(chan *kvv1.JoinResponse, peersCount)
	req := kvv1.JoinRequest{Node: &kvv1.Node{Id: r.SelfId, Addr: r.peers[r.selfIdx].Addr}}
	const maxRetries = 2
	const pingInterval = 5*time.Second
	loop:
	for idx, peer := range r.peers {
		if idx != r.selfIdx {
			log.Printf("Joining %v", peer.Id)
			for attempt := 1; attempt <= maxRetries; attempt++ {
				res, err := peer.RingClient.Join(ctx, &req)
				if err == nil {
					log.Printf("Received join response from %v on attempt %d", peer.Id, attempt)
					ch <- res
					break loop
				}
				log.Printf("Join failed for %v (attempt %d/%d): %v", peer.Id, attempt, maxRetries, err)
				time.Sleep(pingInterval)
			}
			log.Printf("Join ultimately failed for %v after %d attempts", peer.Id, maxRetries)
		}
	}

	// Process join response and start election if needed
	select {
	case res := <-ch:
		log.Printf("JoinResponse: %v", res)
		newPeers := make([]Peer, len(res.Ring))
		for i := range res.Ring {
			newPeers[i] = *r.protobufNodeToPeer(res.Ring[i])
		}
		r.peers = newPeers
		log.Printf("Joined and discovered new peers: %v", r.peers)

		// Determine if need start election
		if res.CoordinatorIdx >= 0 {
			log.Printf("No need to call an election")
			joinResponse, _ := r.peers[res.CoordinatorIdx].RingClient.Join(ctx, &kvv1.JoinRequest{Node: &kvv1.Node{Id: r.SelfId, Addr: r.selfAddr}}) 
			r.SetPeers(&kvv1.SetPeersRequest{Nodes: joinResponse.Ring, CoordinatorIdx: joinResponse.CoordinatorIdx})
		} else {
			go r.StartElection(ctx)
			time.Sleep(2*ElectionWinTime)
		}
	case <-time.After((maxRetries + 1)*pingInterval):
		log.Fatalf("Failed to join any peers")
	}
	
	// Election manager
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			switch r.RegistryState {
			case Stable:
				if r.coordinatorIdx != r.selfIdx && r.coordinatorIdx >= 0 {
					ch := make(chan bool, 1)
					go func() {
						r.mutex.RLock()
						defer r.mutex.RUnlock()
						_, err := r.peers[r.coordinatorIdx].RingClient.Ping(ctx, &kvv1.PingRequest{ NodeId: r.SelfId })
						if err == nil {
							ch <- true
						} else {
							log.Printf("Error pinging coordinator: %v", err)
						}
					}()
					select {
					case <-ch:
					case <-time.After(10*time.Second):
						log.Println("Coordinator unreachable")
						r.RegistryState = CoordinatorUnreachable
					}
				}
			case CoordinatorUnreachable:
				r.StartElection(ctx)
			}
			time.Sleep(10*time.Second)
		}
	}()

	return nil
}

func (r *Registry) StartElection(ctx context.Context) error {
	log.Printf("Starting election")
	responseCh := make(chan *kvv1.ElectionResponse, 1) // buffer 1 is enough (we only care if ANY response arrives)

	// blast out election messages to peers with higher id in goroutines
	for _, peer := range r.peers {
		if peer.Id > r.SelfId {
			go func(p *Peer) {
				log.Printf("Sending election request to %v", peer.Id)
				resp, err := p.RingClient.ReceiveElectionRequest(ctx, &kvv1.ElectionRequest{NodeId: r.SelfId})
				if err == nil {
					// signal that someone responded
					select {
					case responseCh <- resp:
						log.Printf("Received election response from %v", peer.Id)
					default: // don't block if already sent
					}
				}
			}(&peer)
		}
	}

	// wait for either response or timeout
	select {
	case <-responseCh:
		// someone higher is alive → back off
		log.Println("Received response, backing off")
	case <-time.After(ElectionWinTime):
		// no one responded → proceed (become leader or next step)
		log.Println("No response, becoming leader")
		nodes := make([]*kvv1.Node, len(r.peers))
		for i := range r.peers {
			nodes[i] = &kvv1.Node{Id: r.peers[i].Id, Addr: r.peers[i].Addr}
		}
		setPeersRequest := kvv1.SetPeersRequest{Nodes: nodes, CoordinatorIdx: int64(r.selfIdx)}
		for _, peer := range r.peers {
			go func(p *Peer) {
				p.RingClient.SetPeers(ctx, &setPeersRequest)
			}(&peer)
		}
	}
	return nil
}

func (r *Registry) GetPeers() *[]Peer {
	r.mutex.RLock()
	defer r.mutex.RUnlock()
	return &r.peers
}

func (r *Registry) IsCoordinator() bool {
	r.mutex.RLock()
	defer r.mutex.RUnlock()
	return r.coordinatorId == r.SelfId
}

func (r *Registry) GetPeersAndCoordinatorIdx() (*[]Peer, int) {
	r.mutex.RLock()
	defer r.mutex.RUnlock()
	return &r.peers, r.coordinatorIdx
}

func (r *Registry) SetPeers(req *kvv1.SetPeersRequest) (bool, error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	newPeers := make([]Peer, len(req.Nodes))
	selfIdx := -1
	for i := range req.Nodes {
		newPeers[i] = *r.protobufNodeToPeer(req.Nodes[i])
		if req.Nodes[i].Id == r.SelfId {
			selfIdx = i
		}
	}
	if selfIdx < 0 {
		return false, errors.New("Self not in peer")
	}
	r.coordinatorIdx = int(req.CoordinatorIdx)
	r.coordinatorId = newPeers[req.CoordinatorIdx].Id
	r.RegistryState = Stable // If pass this point means that a coordinator is set so registrystate is stable
	r.selfIdx = selfIdx
	r.peers = newPeers
	peerIds := make([]string, len(r.peers))
	for i := range r.peers {
		peerIds[i] = r.peers[i].Id
	}
	log.Printf("Coordinator: %v", r.coordinatorId)
	log.Printf("Peers: %v", peerIds)

	// Spawn heartbeat goroutine
	if r.coordinatorId == r.SelfId {
		r.RegistryState = Stable
		r.coordinatorId = r.SelfId
		r.coordinatorIdx = r.selfIdx
		go func() {
			if (r.heartBeatMutex.TryLock() == false) {
				return
			}
			defer r.heartBeatMutex.Unlock()
			log.Printf("Pinging other nodes periodically")
			ctx := context.Background()
			for r.coordinatorId == r.SelfId {
				peers := r.GetPeers()
				successPings := make([]bool, len(*peers)) 
				successCh := make(chan int, len(*peers))
				pingReq := kvv1.PingRequest{ NodeId: r.SelfId }
				for i := range *peers {
					if i != r.selfIdx {
						go func(i int, p *Peer) {
							ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
							defer cancel()
							_, err := p.RingClient.Ping(ctx, &pingReq)
							if err == nil {
								successCh <- i
							} else {
								log.Printf("Error pinging %v: %v", p.Id, err)
							}
						}(i, &((*peers)[i]))
					} else {
						successPings[i] = true
						successCh <- i
					}
				}
				successPingCount := 0
				timeout := time.After(10 * time.Second)
				successPingCheckerLoop:
				for successPingCount < r.Size() {
					select {
					case i := <-successCh:
						successPingCount++
						successPings[i] = true
					case <-timeout:
						log.Printf("Timeout occured on one of the pings")
						newNodes := make([]*kvv1.Node, successPingCount)
						newNodesI := 0
						selfIdx := -1
						for i, success := range successPings {
							if (success == true) {
								newNodes[newNodesI] = &kvv1.Node{Id: (*peers)[i].Id, Addr: (*peers)[i].Addr}
								if ((*peers)[i].Id == r.SelfId) {
									selfIdx = newNodesI
								}
								newNodesI++
							} else {
								log.Printf("Ping to %v failed", (*peers)[i].Id)
							}
						}
						setPeersRequest := kvv1.SetPeersRequest{Nodes: newNodes, CoordinatorIdx: int64(selfIdx)}
						for i, success := range successPings {
							if (success == true) {
								go (*peers)[i].RingClient.SetPeers(ctx, &setPeersRequest)
							}
						}
						break successPingCheckerLoop
					}
				}
				time.Sleep(10*time.Second)
			}
			log.Printf("Not pinging other nodes periodically")
		}()
		return true, nil
	}
	return false, nil
}

func (n *Registry) protobufNodeToPeer(node *kvv1.Node) (*Peer) {
	return &Peer{
		Id:     node.Id,
		Addr:   node.Addr,
		TokenClient: kvv1connect.NewTokenServiceClient(http.DefaultClient, node.Addr),
		RingClient: kvv1connect.NewRingServiceClient(http.DefaultClient, node.Addr),
	}
}
