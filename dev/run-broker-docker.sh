#!/usr/bin/env bash
# Run a source-built Scorpio image (dev/build-image.sh) for ETSI testing, against the dockerized
# Postgres + Kafka started by dev/start-deps.sh.
#
# Usage: dev/run-broker-docker.sh [tag]   (default tag: scorpio-local:dev)
#
# Why `--network container:$(hostname)`:
#   This dev box is itself a container (docker-out-of-docker). The Robot suite AND its mock context
#   source server run *inside this container* (localhost:9090 for the broker, localhost:8086 for the
#   mock). A normal sibling container can't see those. Sharing THIS container's network namespace
#   makes the broker bind the same localhost the suite and mock use — so ContextSource /
#   DistributedOperations (which forward to the host-run mock) work with zero suite changes — while
#   still resolving the deps by their compose-network hostnames (this container is attached to
#   compose-files_default by dev/start-deps.sh).
#
# Stop any source-run broker first (dev/stop-broker.sh) — :9090 can't be shared.
set -euo pipefail
TAG="${1:-scorpio-local:dev}"
NAME=scorpio-broker-local

docker rm -f "$NAME" >/dev/null 2>&1 || true
echo ">>> starting $NAME ($TAG) sharing this container's netns"
docker run -d --name "$NAME" \
  --network "container:$(hostname)" \
  -e DBHOST=scorpio-dev-postgres \
  -e BUSHOST=scorpio-dev-kafka \
  -e QUARKUS_HTTP_HOST=0.0.0.0 \
  "$TAG" >/dev/null

echo ">>> waiting for :9090 health"
for i in $(seq 1 90); do
  if curl -sf -m2 http://localhost:9090/q/health >/dev/null 2>&1; then
    echo "UP after ~$((i*2))s — broker at http://localhost:9090"
    exit 0
  fi
  sleep 2
done
echo "timed out; last 30 log lines:"; docker logs --tail 30 "$NAME"; exit 1
