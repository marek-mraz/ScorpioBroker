#!/usr/bin/env bash
# Stop the source-run Scorpio broker (Quarkus dev). pkill is unreliable in this container,
# so kill by scanning /proc for the quarkus:dev / aio-runner process.
set -euo pipefail
killed=0
for p in /proc/[0-9]*; do
  cmd=$(tr '\0' ' ' < "$p/cmdline" 2>/dev/null || true)
  case "$cmd" in
    *quarkus:dev*|*aio-runner*) kill -9 "${p#/proc/}" 2>/dev/null && killed=1 || true ;;
  esac
done
sleep 1
if curl -s -m2 -o /dev/null http://localhost:9090/q/health 2>/dev/null; then
  echo "warning: something still answering on :9090"
else
  echo "broker stopped${killed:+}"
fi
