#!/usr/bin/env bash
# ponytail: restart the packaged in-memory broker to clear in-VM subscription/registry state (lesson 3).
for p in /proc/[0-9]*; do cmd=$(tr '\0' ' ' < "$p/cmdline" 2>/dev/null); case "$cmd" in *quarkus-run.jar*) kill -9 "${p#/proc/}" 2>/dev/null;; esac; done
sleep 2
cd /workspace
nohup java -Dquarkus.profile=in-memory -Din-memory -Ddbhost=scorpio-dev-postgres -Dquarkus.http.host=0.0.0.0 \
  -jar AllInOneRunner/target/quarkus-app/quarkus-run.jar > /tmp/scorpio.log 2>&1 &
for i in $(seq 1 40); do
  curl -s -m2 http://localhost:9090/q/health 2>/dev/null | grep -q UP && { echo "broker UP (~$((i*3))s)"; exit 0; }
  sleep 3
done
echo "broker DID NOT come up; tail:"; tail -8 /tmp/scorpio.log; exit 1
