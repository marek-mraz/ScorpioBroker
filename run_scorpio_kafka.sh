#!/bin/bash
# Launch Scorpio AllInOneRunner (kafka profile) against dockerized postgres+kafka.
set +e
cd /workspace/AllInOneRunner/target/quarkus-app || exit 2
for p in /proc/[0-9]*; do
  c=$(tr '\0' ' ' < "$p/cmdline" 2>/dev/null)
  case "$c" in *quarkus-run.jar*) kill -9 "$(basename "$p")" 2>/dev/null;; esac
done
nohup java -Dquarkus.profile=kafka -Ddbhost=scorpio-dev-postgres -Dbushost=scorpio-dev-kafka \
  -Dquarkus.http.host=0.0.0.0 -jar quarkus-run.jar > /tmp/scorpio.log 2>&1 &
echo $! > /tmp/scorpio.pid
echo "started pid $(cat /tmp/scorpio.pid)"
