#!/bin/bash
# ponytail: pkill is unavailable in this container; kill via /proc by saved PID + scan.
set +e
cd /workspace/AllInOneRunner/target/quarkus-app
for p in /proc/[0-9]*; do
  c=$(tr '\0' ' ' < "$p/cmdline" 2>/dev/null)
  case "$c" in *quarkus-run.jar*) kill -9 "$(basename $p)" 2>/dev/null;; esac
done
sleep 3
nohup java -Dquarkus.profile=in-memory -Ddbhost=scorpio-dev-postgres -Dquarkus.http.host=0.0.0.0 -jar quarkus-run.jar > /tmp/scorpio.log 2>&1 &
echo $! > /tmp/scorpio.pid
for i in $(seq 1 60); do
  code=$(curl -s -o /dev/null -w "%{http_code}" --max-time 3 http://localhost:9090/ngsi-ld/v1/entities?type=Test 2>/dev/null)
  [ "$code" = "200" ] && { echo "UP (pid $(cat /tmp/scorpio.pid)) after ${i}x2s"; exit 0; }
  sleep 2
done
echo "FAILED to come up"; tail -15 /tmp/scorpio.log; exit 1
