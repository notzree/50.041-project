# Token Ring KV Store

Distributed key-value store using a token ring for mutual exclusion. Nodes pass a token around a ring — only the token holder can accept writes. Writes are logged on the token and replayed by each node as it receives it, achieving eventual consistency.

## Architecture

- **Token ring mutex**: nodes are sorted by ID into a ring. A single token circulates, carrying a log of uncommitted KV events.
- **Log replay + compaction**: each node replays unseen log entries on receive, then trims entries all nodes have applied.
- **Lamport timestamps**: token carries a Lamport timestamp for staleness detection.
- **Manual tick mode**: token only advances on explicit `POST /tick`, useful for demos and debugging.

## Quick Start

```bash
# start a 3-node cluster in manual mode
./run.sh 3 --manual

# connect the CLI client
go run ./cmd/client --nodes "node-1=localhost:8080,node-2=localhost:8081,node-3=localhost:8082"
```

## Client Commands

```
put <key> <value> [node-id]   Write a key (blocks until tick flushes)
get <key> [node-id]           Read from one or all nodes
delete <key> [node-id]        Delete a key (blocks until tick flushes)
tick                          Advance token one hop
status                        Show all node states
exit                          Quit
```

## Example Session

```
> put mykey hello
> tick                         # node-1 flushes write, passes to node-2
> get mykey
  node-1: hello
  node-2: (not found)          # hasn't received token yet
> tick                         # node-2 replays log, passes to node-3
> get mykey
  node-1: hello
  node-2: hello
  node-3: (not found)
> tick                         # node-3 replays log
> get mykey                    # all nodes consistent
```

## Testing

```bash
go test ./pkg/kv/... ./pkg/node/...        # unit tests
go test -race ./pkg/kv/... ./pkg/node/...  # with race detector
```

## Run Options

```bash
./run.sh <num-nodes> [--manual]  # generate docker-compose + start cluster
docker compose down              # tear down
```

Nodes can also be run directly:

```bash
go run . --id=node-1 --addr=:8080 --ring="node-1=localhost:8080,node-2=localhost:8081" --manual
```

## TODOs

- **Persistence**: write-ahead log or snapshot for crash recovery
- **Leader election**: currently the lowest-ID node seeds the token; needs proper election on node failure
- **Failure detection**: detect crashed nodes and skip them in the ring
- **Dynamic membership**: `Join`/`Leave` RPCs are stubbed but not implemented
- **Token loss recovery**: if the token is lost (holder crashes mid-pass), regenerate it
