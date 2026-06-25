#!/usr/bin/env bash
# Build & run the Scorpio AllInOneRunner from source in Quarkus dev mode.
#
# Usage:
#   dev/run-broker.sh in-memory   # synchronous in-process messaging (DEFAULT; what the ETSI suite expects)
#   dev/run-broker.sh kafka       # async messaging via the scorpio-dev-kafka broker
#
# Profiles:
#   in-memory  -> SmallRye in-VM channels, core->temporal propagation is synchronous.
#                 Use this for ETSI conformance: tests create via the core API then immediately
#                 query the temporal API, which races under async messaging.
#   kafka      -> production-like async event bus (needs scorpio-dev-kafka up).
#
# Notes:
#  - Needs JDK 21 (dev/install-tools.sh) and the deps up (dev/start-deps.sh).
#  - Low free memory on this box: build serially with a capped heap (-Xmx1500m), no -T parallel.
#  - Broker serves on :9090. Health: curl localhost:9090/q/health
#  - Logs go to /tmp/scorpio.log. Runs in the background; use dev/stop-broker.sh to stop it.
set -euo pipefail
cd "$(dirname "$0")/.."

PROFILE="${1:-in-memory}"
export MAVEN_OPTS="-Xmx1500m"

# Kill any previous source-run broker (pkill is unreliable here; scan /proc).
for p in /proc/[0-9]*; do
  cmd=$(tr '\0' ' ' < "$p/cmdline" 2>/dev/null || true)
  case "$cmd" in *quarkus:dev*|*aio-runner*) kill -9 "${p#/proc/}" 2>/dev/null || true;; esac
done
sleep 2

case "$PROFILE" in
  kafka)
    ARGS=(-Dkafka -Dquarkus.profile=kafka -Dbushost=scorpio-dev-kafka) ;;
  in-memory)
    ARGS=(-Din-memory -Dquarkus.profile=in-memory) ;;
  *)
    echo "unknown profile '$PROFILE' (use 'in-memory' or 'kafka')"; exit 1 ;;
esac

echo ">>> building + starting broker (profile=$PROFILE)"
( cd AllInOneRunner && nohup mvn quarkus:dev "${ARGS[@]}" \
    -Ddbhost=scorpio-dev-postgres -Dquarkus.http.host=0.0.0.0 > /tmp/scorpio.log 2>&1 & )

echo ">>> waiting for :9090 (first build can take ~1-2 min)"
for i in $(seq 1 60); do
  if curl -s -m2 http://localhost:9090/q/health 2>/dev/null | grep -q UP; then
    echo "HEALTH UP after ~$((i*5))s"; exit 0
  fi
  if grep -qiE 'BUILD FAILURE|Failed to start|Unresolved compilation' /tmp/scorpio.log 2>/dev/null; then
    echo "STARTUP FAILED — tail of /tmp/scorpio.log:"; tail -15 /tmp/scorpio.log; exit 1
  fi
  sleep 5
done
echo "timed out waiting for health; see /tmp/scorpio.log"; exit 1
