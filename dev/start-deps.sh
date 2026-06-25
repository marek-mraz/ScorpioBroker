#!/usr/bin/env bash
# Bring up the dev dependencies (Postgres + Kafka) and attach THIS container to their
# network so the source-run broker can reach them by hostname.
#
# Env quirk (docker-out-of-docker): this dev box shares the host's docker socket, so
# published container ports are NOT on this container's localhost. The broker must reach
# Postgres/Kafka by their container hostnames over the shared compose network.
set -euo pipefail
cd "$(dirname "$0")/.."

COMPOSE_FILE=compose-files/docker-compose-dev-deps.yml
NETWORK=compose-files_default

echo ">>> starting postgres + kafka"
docker compose -f "$COMPOSE_FILE" up -d

echo ">>> waiting for postgres health"
for _ in $(seq 1 30); do
  if docker inspect -f '{{.State.Health.Status}}' scorpio-dev-postgres 2>/dev/null | grep -q healthy; then
    echo "postgres healthy"; break
  fi
  sleep 2
done

# Attach this container to the deps network (not persistent across container recreate).
HOST_CONTAINER="$(hostname)"
if docker network connect "$NETWORK" "$HOST_CONTAINER" 2>/dev/null; then
  echo ">>> connected $HOST_CONTAINER to $NETWORK"
else
  echo ">>> $HOST_CONTAINER already on $NETWORK (or not a container) — ok"
fi

echo "=== resolves ==="
getent hosts scorpio-dev-postgres scorpio-dev-kafka || true
