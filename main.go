package main

import (
	"context"
	"ds/v2/pkg/node"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
)

func main() {
	id := flag.String("id", "", "node ID")
	addr := flag.String("addr", ":8080", "listen address")
	ring := flag.String("ring", "", "ring config: id1=addr1,id2=addr2,...")
	manual := flag.Bool("manual", false, "manual tick mode (token only advances on POST /tick)")
	flag.Parse()

	// env vars as fallback
	if *id == "" {
		*id = os.Getenv("NODE_ID")
	}
	if *addr == ":8080" {
		if v := os.Getenv("NODE_ADDR"); v != "" {
			*addr = v
		}
	}
	if *ring == "" {
		*ring = os.Getenv("RING")
	}
	if os.Getenv("MANUAL") == "true" {
		*manual = true
	}

	if *id == "" || *ring == "" {
		fmt.Fprintln(os.Stderr, "usage: node --id=<id> --ring=<id1=addr1,id2=addr2,...> [--addr=:8080] [--manual]")
		os.Exit(1)
	}

	peers, err := node.ParseRing(*ring)
	if err != nil {
		log.Fatalf("bad --ring: %v", err)
	}

	n, err := node.NewNode(*id, *addr, 100, peers, *manual)
	if err != nil {
		log.Fatalf("failed to create node: %v", err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	if err := n.Start(ctx); err != nil {
		log.Fatal(err)
	}
}
