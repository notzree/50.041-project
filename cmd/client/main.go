package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	kvv1 "ds/v2/pkg/gen/kv/v1"
	"ds/v2/pkg/gen/kv/v1/kvv1connect"
	"ds/v2/pkg/node"

	"google.golang.org/protobuf/types/known/structpb"
)

type peer struct {
	id    string
	addr  string // full URL, e.g. http://localhost:8080
	kvCli kvv1connect.KvServiceClient
}

func main() {
	nodes := flag.String("nodes", "", "node list: id1=addr1,id2=addr2,... (addrs are host:port, http:// added if missing)")
	flag.Parse()

	if *nodes == "" {
		*nodes = os.Getenv("NODES")
	}
	if *nodes == "" {
		fmt.Fprintln(os.Stderr, "usage: client --nodes=id1=addr1,id2=addr2,...")
		os.Exit(1)
	}

	configs, err := node.ParseRing(*nodes)
	if err != nil {
		fmt.Fprintf(os.Stderr, "bad --nodes: %v\n", err)
		os.Exit(1)
	}

	httpCli := &http.Client{Timeout: 30 * time.Second}

	peers := make([]peer, len(configs))
	for i, cfg := range configs {
		peers[i] = peer{
			id:    cfg.Id,
			addr:  cfg.Addr,
			kvCli: kvv1connect.NewKvServiceClient(httpCli, cfg.Addr),
		}
	}

	peerMap := make(map[string]*peer, len(peers))
	for i := range peers {
		peerMap[peers[i].id] = &peers[i]
	}

	fmt.Println("Connected to nodes:")
	for _, p := range peers {
		fmt.Printf("  %s → %s\n", p.id, p.addr)
	}
	fmt.Println("\nCommands:")
	fmt.Println("  put <key> <value> [node-id]   - Queue a put (blocks until tick flushes)")
	fmt.Println("  get <key> [node-id]            - Read from one or all nodes")
	fmt.Println("  delete <key> [node-id]         - Queue a delete (blocks until tick flushes)")
	fmt.Println("  tick                           - Advance token (sent to all nodes)")
	fmt.Println("  status                         - Show all node states")
	fmt.Println("  monitor           - Real-time dashboard of the token ring")
	fmt.Println("  exit")
	fmt.Println()

	ctx := context.Background()
	scanner := bufio.NewScanner(os.Stdin)

	for {
		fmt.Print("> ")
		if !scanner.Scan() {
			break
		}
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		args := strings.Fields(line)
		cmd := args[0]

		switch cmd {
		case "exit", "quit":
			return

		case "tick":
			doTick(ctx, httpCli, peers)

		case "status":
			doStatus(ctx, httpCli, peers)
		case "monitor":
			doMonitor(ctx, httpCli, peers)
		case "put":
			if len(args) < 3 {
				fmt.Println("usage: put <key> <value> [node-id]")
				continue
			}
			key, val := args[1], args[2]
			target := resolveTarget(args, 3, peers, peerMap)
			if target == nil {
				continue
			}
			doPut(ctx, target, key, val)

		case "get":
			if len(args) < 2 {
				fmt.Println("usage: get <key> [node-id]")
				continue
			}
			key := args[1]
			if len(args) >= 3 {
				if t, ok := peerMap[args[2]]; ok {
					doGet(ctx, []*peer{t}, key)
				} else {
					fmt.Printf("unknown node: %s\n", args[2])
				}
			} else {
				ptrs := make([]*peer, len(peers))
				for i := range peers {
					ptrs[i] = &peers[i]
				}
				doGet(ctx, ptrs, key)
			}

		case "delete":
			if len(args) < 2 {
				fmt.Println("usage: delete <key> [node-id]")
				continue
			}
			key := args[1]
			target := resolveTarget(args, 2, peers, peerMap)
			if target == nil {
				continue
			}
			doDelete(ctx, target, key)

		default:
			fmt.Printf("unknown command: %s\n", cmd)
		}
	}
}

func resolveTarget(args []string, idx int, peers []peer, peerMap map[string]*peer) *peer {
	if len(args) > idx {
		if t, ok := peerMap[args[idx]]; ok {
			return t
		}
		fmt.Printf("unknown node: %s\n", args[idx])
		return nil
	}
	return &peers[0]
}

func doPut(ctx context.Context, p *peer, key, val string) {
	pbVal := structpb.NewStringValue(val)
	// Put blocks until the node holds the token and flushes, so run in goroutine
	go func() {
		_, err := p.kvCli.Put(ctx, &kvv1.PutRequest{Key: key, Value: pbVal})
		if err != nil {
			fmt.Printf("\n[PUT %s FAILED on %s: %v]\n> ", key, p.id, err)
		} else {
			fmt.Printf("\n[PUT %s=%.20s OK on %s]\n> ", key, val, p.id)
		}
	}()
	fmt.Printf("PUT %s=%s queued on %s (waiting for tick...)\n", key, val, p.id)
}

func doDelete(ctx context.Context, p *peer, key string) {
	go func() {
		_, err := p.kvCli.Delete(ctx, &kvv1.DeleteRequest{Key: key})
		if err != nil {
			fmt.Printf("\n[DELETE %s FAILED on %s: %v]\n> ", key, p.id, err)
		} else {
			fmt.Printf("\n[DELETE %s OK on %s]\n> ", key, p.id)
		}
	}()
	fmt.Printf("DELETE %s queued on %s (waiting for tick...)\n", key, p.id)
}

func doGet(ctx context.Context, peers []*peer, key string) {
	for _, p := range peers {
		resp, err := p.kvCli.Get(ctx, &kvv1.GetRequest{Key: key})
		if err != nil {
			fmt.Printf("  %s: error: %v\n", p.id, err)
			continue
		}
		if !resp.Found {
			fmt.Printf("  %s: (not found)\n", p.id)
		} else {
			fmt.Printf("  %s: %s\n", p.id, resp.Value.GetStringValue())
		}
	}
}

func doTick(ctx context.Context, httpCli *http.Client, peers []peer) {
	for _, p := range peers {
		req, _ := http.NewRequestWithContext(ctx, http.MethodPost, p.addr+"/tick", nil)
		resp, err := httpCli.Do(req)
		if err != nil {
			fmt.Printf("  %s: error: %v\n", p.id, err)
			continue
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		var result map[string]any
		json.Unmarshal(body, &result)

		held, _ := result["held"].(bool)
		if held {
			passedTo, _ := result["passed_to"].(string)
			flushed, _ := result["events_flushed"].(float64)
			logLen, _ := result["log_length"].(float64)
			fmt.Printf("  %s → %s (flushed %d events, log len %d)\n",
				p.id, passedTo, int(flushed), int(logLen))
			return // one hop per tick
		} else {
			fmt.Printf("  %s: (no token)\n", p.id)
		}
	}
}

func doStatus(ctx context.Context, httpCli *http.Client, peers []peer) {
	for _, p := range peers {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, p.addr+"/status", nil)
		resp, err := httpCli.Do(req)
		if err != nil {
			fmt.Printf("  %s: error: %v\n", p.id, err)
			continue
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		var result map[string]any
		json.Unmarshal(body, &result)
		fmt.Printf("  %s: token=%v last_applied=%.0f timestamp=%.0f\n",
			p.id, result["has_token"], result["last_applied"], result["timestamp"])
	}
}

func doMonitor(ctx context.Context, httpCli *http.Client, peers []peer) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	fmt.Println("Entering Monitor Mode... (Press Ctrl+C to exit client)")

	for {
		select {
		case <-ticker.C:

			fmt.Print("\033[H\033[2J")
			fmt.Printf("=== TOKEN RING MONITOR | %s ===\n\n", time.Now().Format("15:04:05"))

			var ringVisual []string

			w := tabwriter.NewWriter(os.Stdout, 0, 8, 2, ' ', 0)
			fmt.Fprintln(w, "NODE ID\tTOKEN\tLAST APPLIED\tTIMESTAMP\tSTATUS")

			for _, p := range peers {
				req, _ := http.NewRequestWithContext(ctx, http.MethodGet, p.addr+"/status", nil)
				resp, err := httpCli.Do(req)

				if err != nil {
					ringVisual = append(ringVisual, fmt.Sprintf("[Offline] %s", p.id))
					fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\n", p.id, "-", "-", "-", "OFFLINE")
					continue
				}

				body, _ := io.ReadAll(resp.Body)
				resp.Body.Close()
				var res map[string]any
				json.Unmarshal(body, &res)

				hasToken, _ := res["has_token"].(bool)
				lastApp, _ := res["last_applied"].(float64)
				ts, _ := res["timestamp"].(float64)

				tokenStatus := "[ ]"
				tokenText := "no"
				if hasToken {
					tokenStatus = "[●]"
					tokenText = "YES"
				}

				ringVisual = append(ringVisual, fmt.Sprintf("%s %s", tokenStatus, p.id))
				fmt.Fprintf(w, "%s\t%s\t%.0f\t%.0f\t%s\n", p.id, tokenText, lastApp, ts, "ONLINE")
			}

			fmt.Println(strings.Join(ringVisual, "  -->  "))
			fmt.Println("\nNode Details:")
			w.Flush() //  prints the table
			fmt.Println("\nNote: Commands are disabled in Monitor Mode. Open a second terminal to send 'put' commands.")

		case <-ctx.Done():
			return
		}
	}
}
