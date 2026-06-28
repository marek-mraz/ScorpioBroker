#!/usr/bin/env bash
# ONE ETSI serial-test environment, identical locally and in CI.
#
# Always the SAME stack: compose-files/docker-compose-iop.yml — 5 self-contained Scorpio brokers,
# each with its own postgres + kafka + mqtt, built from the local working tree (scorpio-local:latest).
# Single-broker suites run against broker1 (scorpio1); the federation suites (DistributedOperations,
# IOP) use all five. No second/alternate compose, no kafka-vs-in-memory split — one config everywhere.
#
# Rules baked in: reset state between suites with clean_db.sh (NEVER drop the DB); keep https @context
# URLs (only the broker/callback hosts are http).
#
# The ONLY config knob is a handful of env vars (defaults = this docker-out-of-docker dev box; CI
# overrides them). This is one config + overrides, not multiple configurations.
#   B1..B5         broker base URLs   (default http://scorpioN:9090/ngsi-ld/v1, reached by hostname)
#   CALLBACK_HOST  host the brokers POST notifications / csource callbacks to (default this hostname)
#   SUITE          test-suite dir     (default ngsi-ld-test-suite)
#   SKIP_UP=1      the 5-broker stack is already up (skip build + compose up)
#   INCLUDE_MQTT=1 also run the MQTT suites (emqx is part of the stack)
# CI example:
#   B1=http://host.docker.internal:9081/ngsi-ld/v1 ... B5=http://host.docker.internal:9085/ngsi-ld/v1 \
#   CALLBACK_HOST=host.docker.internal dev/etsi-serial.sh
set -uo pipefail
cd "$(dirname "$0")/.."

B1="${B1:-http://scorpio1:9090/ngsi-ld/v1}"
B2="${B2:-http://scorpio2:9090/ngsi-ld/v1}"
B3="${B3:-http://scorpio3:9090/ngsi-ld/v1}"
B4="${B4:-http://scorpio4:9090/ngsi-ld/v1}"
B5="${B5:-http://scorpio5:9090/ngsi-ld/v1}"
SUITE="${SUITE:-ngsi-ld-test-suite}"
# Default callback host = this container's IP on the stack network, so the brokers can reach the
# suite's notification/context mock servers. (This container has >1 network, so its hostname may
# resolve to the wrong interface — use the stack-network IP explicitly.) CI overrides this with
# CALLBACK_HOST=host.docker.internal.
if [ -z "${CALLBACK_HOST:-}" ]; then
  CALLBACK_HOST=$(docker inspect -f '{{(index .NetworkSettings.Networks "iop_scorpio-net").IPAddress}}' \
    "$(hostname)" 2>/dev/null)
  CALLBACK_HOST="${CALLBACK_HOST:-$(hostname)}"
fi

# 1. Bring up the single stack (build scorpio-local:latest from source + 5 brokers + health).
[ "${SKIP_UP:-}" = 1 ] || ./dev/run-iop.sh

# 2. Point the suite at broker1 and at the callback host; @context URLs stay https.
( cd "$SUITE/resources"
  sed -i "s|^url = .*|url = '$B1'|" variables.py
  sed -i "s|^temporal_api_url = .*|temporal_api_url = '$B1'|" variables.py
  sed -i "s|^notification_server_host = .*|notification_server_host = '$CALLBACK_HOST'|" variables.py
  sed -i "s|^context_source_host = .*|context_source_host = '$CALLBACK_HOST'|" variables.py
  sed -i "s|^context_server_host = .*|context_server_host = '$CALLBACK_HOST'|" variables.py )

# 3. Run every suite serially. clean_db.sh resets broker1's DB between suites (no DB drops); the IOP
#    suite resets all five brokers itself via libraries/FederationReset.py.
cd "$SUITE"
[ -d .venv ] || { python3 -m venv .venv && .venv/bin/pip install -q -r requirements.txt; }
ROBOT=.venv/bin/robot
export CLEAN_DB_CONTAINER="${CLEAN_DB_CONTAINER:-scorpio-postgres-1}"
EXCLUDE=(--exclude iop)
[ "${INCLUDE_MQTT:-}" = 1 ] || EXCLUDE+=(--exclude mqtt)

# Debug fix-loop: STOP_ON_ERROR=1 makes robot abort at the FIRST failing test (--exitonfailure) and
# halts the whole serial run right there, writing etsi-failures.md so the error surfaces immediately.
# Default (unset) runs every suite to completion, unchanged.
DBG=()
[ "${STOP_ON_ERROR:-}" = 1 ] && DBG+=(--exitonfailure)
stop_if_failed() {  # $1=robot exit code  $2=suite label  $3=results dir
  { [ "${STOP_ON_ERROR:-}" = 1 ] && [ "$1" -ne 0 ]; } || return 0
  echo ">>> STOP_ON_ERROR: first failing test in '$2' (robot rc=$1) — halting serial run."
  python3 report_failures.py "$3/output.xml" etsi-failures.md || true
  echo "  inspect: $SUITE/$3/log.html   failures: $SUITE/etsi-failures.md"
  exit 1
}

rm -rf results && mkdir -p results
for s in CommonBehaviours \
         ContextInformation/Consumption ContextInformation/Provision ContextInformation/Subscription \
         ContextSource jsonldContext; do
  ./clean_db.sh >/dev/null 2>&1 || true
  name="${s//\//-}"
  $ROBOT "${DBG[@]}" "${EXCLUDE[@]}" --outputdir "results/$name" "./TP/NGSI-LD/$s"; rc=$?
  stop_if_failed "$rc" "$s" "results/$name"
done

# DistributedOperations: broker1 is the SUT, the other brokers are federated context sources.
./clean_db.sh >/dev/null 2>&1 || true
$ROBOT "${DBG[@]}" "${EXCLUDE[@]}" --outputdir results/DistributedOperations ./TP/NGSI-LD/DistributedOperations; rc=$?
stop_if_failed "$rc" DistributedOperations results/DistributedOperations

# IOP: exercises all five brokers (self-resetting suite).
$ROBOT "${DBG[@]}" --variable b1_url:"$B1" --variable b2_url:"$B2" --variable b3_url:"$B3" \
       --variable b4_url:"$B4" --variable b5_url:"$B5" --outputdir results/IOP IOP_TP; rc=$?
stop_if_failed "$rc" IOP results/IOP

# ---- Unified output (IDENTICAL for local and CI) -------------------------------------------------
# 1) ONE combined report: merge every suite's output.xml into results/{output.xml,report.html,log.html}.
.venv/bin/rebot --nostatusrc --name "ETSI NGSI-LD (5-broker stack)" \
  --output results/output.xml --report results/report.html --log results/log.html \
  results/*/output.xml || true
# 2) ONE failures-only file (same generator/format everywhere): etsi-failures.md.
python3 report_failures.py 'results/*/output.xml' etsi-failures.md || true

echo "=== ETSI serial run complete ==="
echo "  full report : $SUITE/results/report.html  (+ output.xml, log.html)"
echo "  failures    : $SUITE/etsi-failures.md"
