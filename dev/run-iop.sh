#!/usr/bin/env bash
# Bring up the SINGLE federation test stack (5 Scorpio brokers) for the ETSI IOP
# and DistributedOperations suites — ALWAYS BUILT FROM THE LOCAL REPOSITORY.
#
# Usage:
#   dev/run-iop.sh            # mvn package (in-memory) + docker compose up --build + health
#   dev/run-iop.sh --no-mvn   # skip mvn (reuse existing target/quarkus-app), still rebuild image
#   dev/run-iop.sh down       # tear the stack down (and its volumes)
#
# Brokers reachable from this dev container by hostname: http://scorpio1:9090 .. scorpio5:9090
# (published 9081-9085 land on the VM host, not this container's localhost).
set -euo pipefail
cd "$(dirname "$0")/.."

PROJECT=iop
COMPOSE=compose-files/docker-compose-iop.yml

if [ "${1:-}" = "down" ]; then
  docker compose -p "$PROJECT" -f "$COMPOSE" down -v
  exit 0
fi

# 1. Build the broker image FROM LOCAL CODE as scorpio-local:latest (the tag the compose references),
#    unless told to skip. Each AAIO broker uses IN-MEMORY (synchronous in-VM) messaging — federation
#    between brokers is HTTP (Context Source Registrations), so no Kafka is needed inside a broker, and
#    in-memory avoids the create->notify/temporal race that breaks the ETSI suite under async Kafka.
if [ "${1:-}" != "--no-mvn" ]; then
  echo ">>> build scorpio-local:latest from local repo (in-memory profile)"
  ./dev/build-image.sh in-memory scorpio-local:latest
fi

# 2. Bring the stack up (5 brokers, each its own postgres + kafka + mqtt). No build: block in the
#    compose — it uses the scorpio-local:latest image built in step 1.
echo ">>> docker compose up (5 brokers)"
docker compose -p "$PROJECT" -f "$COMPOSE" up -d

# 2b. Attach THIS dev container to the stack network so it can reach the brokers by hostname
#     (scorpio1..5:9090) and the brokers can POST callbacks back to the suite's mock servers here.
#     No-op when not running inside a container (e.g. a CI runner using published ports).
if docker network inspect "${PROJECT}_scorpio-net" >/dev/null 2>&1; then
  docker network connect "${PROJECT}_scorpio-net" "$(hostname)" 2>/dev/null \
    && echo ">>> attached $(hostname) to ${PROJECT}_scorpio-net" \
    || true
fi

# 2c. MQTT notification suite (058_*): the suite launches a mosquitto container on the
#     `compose-files_default` network (hardcoded in resources/mqttUtils/MqttUtils.resource) named
#     `ngsi-ld-test-suite-mosquitto-container`. For the brokers to deliver to it and the suite's MQTT
#     client (running here) to reach it, every broker + this container must share that network. Ensure
#     it exists and attach them. Idempotent; harmless when MQTT tests aren't run.
docker network create compose-files_default >/dev/null 2>&1 || true
for n in 1 2 3 4 5; do
  cid=$(docker compose -p "$PROJECT" -f "$COMPOSE" ps -q "scorpio${n}" 2>/dev/null)
  [ -n "$cid" ] && docker network connect compose-files_default "$cid" 2>/dev/null || true
done
docker network connect compose-files_default "$(hostname)" 2>/dev/null || true
echo ">>> attached brokers + $(hostname) to compose-files_default (MQTT 058_* reachability)"

# 3. Health-check each broker. Use BN base URL if set (CI reaches the brokers on their published
#    ports via host.docker.internal), else by in-network hostname (docker-out-of-docker dev box,
#    where published ports land on the VM host, not this container's localhost).
echo ">>> waiting for all 5 brokers' health"
for n in 1 2 3 4 5; do
  base_var="B${n}"; base="${!base_var:-http://scorpio${n}:9090/ngsi-ld/v1}"
  health="${base%/ngsi-ld/v1}/q/health"
  ok=
  for i in $(seq 1 90); do
    if curl -sf -m2 "$health" >/dev/null 2>&1; then
      echo "  scorpio${n} UP after ~$((i*2))s  ($health)"; ok=1; break
    fi
    sleep 2
  done
  [ -n "$ok" ] || { echo "  scorpio${n} TIMED OUT ($health)"; docker compose -p "$PROJECT" -f "$COMPOSE" logs --tail 30 "scorpio${n}"; exit 1; }
done

echo "=== IOP / DistributedOperations stack up (built from local repo) ==="
echo "  scorpio1..5 : http://scorpio{1..5}:9090  (host VM: localhost:9081..9085)"
echo "  IOP suite:              run ./IOP_TP with b1_url..b5_url -> http://host.docker.internal:908{1..5}/ngsi-ld/v1"
echo "  DistributedOperations:  point url at http://scorpio1:9090/ngsi-ld/v1 (broker1 is the SUT)"
