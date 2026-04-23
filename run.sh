#!/usr/bin/env bash
set -euo pipefail

usage() {
  echo "Usage: $0 <num-nodes> [--manual]"
  echo ""
  echo "Generates docker-compose.yml and starts a token-ring cluster."
  echo "Also prints the client command to connect."
  echo ""
  echo "Examples:"
  echo "  $0 3 --manual    # 3 nodes, manual tick mode"
  echo "  $0 5             # 5 nodes, automatic token passing"
  exit 1
}

if [[ $# -lt 1 ]]; then
  usage
fi

NUM_NODES="$1"
MANUAL="false"

if [[ "${2:-}" == "--manual" ]]; then
  MANUAL="true"
fi

if ! [[ "$NUM_NODES" =~ ^[0-9]+$ ]] || [[ "$NUM_NODES" -lt 2 ]]; then
  echo "Error: need at least 2 nodes"
  exit 1
fi

# Build the RING string and node names
RING=""
for i in $(seq 1 "$NUM_NODES"); do
  NAME="node-${i}"
  if [[ -n "$RING" ]]; then
    RING="${RING},"
  fi
  RING="${RING}${NAME}=${NAME}:8080"
done

# Generate docker-compose.yml
COMPOSE="docker-compose.yml"
cat > "$COMPOSE" <<EOF
services:
EOF

CLIENT_NODES=""
for i in $(seq 1 "$NUM_NODES"); do
  NAME="node-${i}"
  HOST_PORT=$((8079 + i))

  cat >> "$COMPOSE" <<EOF
  ${NAME}:
    build: .
    environment:
      NODE_ID: ${NAME}
      NODE_ADDR: ":8080"
      RING: "${RING}"
      MANUAL: "${MANUAL}"
    ports:
      - "${HOST_PORT}:8080"

EOF

  if [[ -n "$CLIENT_NODES" ]]; then
    CLIENT_NODES="${CLIENT_NODES},"
  fi
  CLIENT_NODES="${CLIENT_NODES}${NAME}=localhost:${HOST_PORT}"
done

echo "Generated ${COMPOSE} with ${NUM_NODES} nodes (manual=${MANUAL})"
echo ""
echo "Starting cluster..."
docker compose up --build -d

echo ""
echo "Connect with:"
echo "  go run ./cmd/client --nodes \"${CLIENT_NODES}\""
go run ./cmd/client --nodes "${CLIENT_NODES}"