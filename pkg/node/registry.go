package node

import (
	"fmt"
	"net/http"
	"slices"
	"strings"

	"ds/v2/pkg/gen/kv/v1/kvv1connect"
)

type Peer struct {
	Id     string
	Addr   string
	Client kvv1connect.TokenServiceClient
}

type Registry struct {
	selfIdx int
	peers   []Peer
}

type PeerConfig struct {
	Id   string
	Addr string
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
	for i, cfg := range ring {
		peers[i] = Peer{
			Id:     cfg.Id,
			Addr:   cfg.Addr,
			Client: kvv1connect.NewTokenServiceClient(http.DefaultClient, cfg.Addr),
		}
		if cfg.Id == selfId {
			selfIdx = i
		}
	}
	if selfIdx == -1 {
		return nil, fmt.Errorf("selfId %q not found in ring config", selfId)
	}
	return &Registry{
		selfIdx: selfIdx,
		peers:   peers,
	}, nil
}

// Next returns the next peer in the ring.
func (r *Registry) Next() *Peer {
	idx := (r.selfIdx + 1) % len(r.peers)
	return &r.peers[idx]
}

// Peer returns a peer by ID, or nil if not found.
func (r *Registry) Peer(id string) *Peer {
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
