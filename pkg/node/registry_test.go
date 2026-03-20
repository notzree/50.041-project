package node

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRegistryDeterministicOrder(t *testing.T) {
	// Regardless of input order, peers should be sorted by ID.
	ring1 := []PeerConfig{
		{Id: "c", Addr: "addr-c"},
		{Id: "a", Addr: "addr-a"},
		{Id: "b", Addr: "addr-b"},
	}
	ring2 := []PeerConfig{
		{Id: "b", Addr: "addr-b"},
		{Id: "a", Addr: "addr-a"},
		{Id: "c", Addr: "addr-c"},
	}

	r1, err := NewRegistry("a", ring1)
	require.NoError(t, err)
	r2, err := NewRegistry("a", ring2)
	require.NoError(t, err)

	for i := 0; i < 3; i++ {
		assert.Equal(t, r1.peers[i].Id, r2.peers[i].Id)
	}
	assert.Equal(t, "a", r1.peers[0].Id)
	assert.Equal(t, "b", r1.peers[1].Id)
	assert.Equal(t, "c", r1.peers[2].Id)
}

func TestRegistryNextWrapsAround(t *testing.T) {
	ring := []PeerConfig{
		{Id: "a", Addr: "addr-a"},
		{Id: "b", Addr: "addr-b"},
	}

	// self is "b" (last in sorted order), next should wrap to "a"
	r, err := NewRegistry("b", ring)
	require.NoError(t, err)
	next := r.Next()
	assert.Equal(t, "a", next.Id)
}

func TestRegistryNextWith3Nodes(t *testing.T) {
	ring := []PeerConfig{
		{Id: "a", Addr: "addr-a"},
		{Id: "b", Addr: "addr-b"},
		{Id: "c", Addr: "addr-c"},
	}

	r, err := NewRegistry("a", ring)
	require.NoError(t, err)
	assert.Equal(t, "b", r.Next().Id)

	r, err = NewRegistry("b", ring)
	require.NoError(t, err)
	assert.Equal(t, "c", r.Next().Id)

	r, err = NewRegistry("c", ring)
	require.NoError(t, err)
	assert.Equal(t, "a", r.Next().Id)
}

func TestRegistrySelfNotInRing(t *testing.T) {
	ring := []PeerConfig{
		{Id: "a", Addr: "addr-a"},
		{Id: "b", Addr: "addr-b"},
	}

	_, err := NewRegistry("z", ring)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found in ring config")
}
