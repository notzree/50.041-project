package node

import "ds/v2/pkg/gen/kv/v1/kvv1connect"

type OtherNode struct {
	Id     string
	Client kvv1connect.TokenServiceClient
}

type Registry struct {
	registry map[string]OtherNode
	next     OtherNode
}

func NewRegistry(nodes []string) *Registry {

}
