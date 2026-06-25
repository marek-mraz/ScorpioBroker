#!/usr/bin/env bash
# Build a Scorpio AllInOneRunner docker image from the CURRENT working tree (your local fixes).
#
# Usage:
#   dev/build-image.sh [kafka|in-memory] [tag]
#   dev/build-image.sh                      # kafka profile, tag scorpio-local:dev (default)
#   dev/build-image.sh in-memory            # in-memory profile
#
# Quarkus bakes the messaging profile at BUILD time, so the profile chosen here is the messaging
# the image will use. kafka -> reads BUSHOST/DBHOST; in-memory -> synchronous in-VM (ETSI-correct,
# no Kafka needed). Image copies target/quarkus-app via AllInOneRunner/src/main/docker/Dockerfile.jvm.
#
# NOTE: this runs `mvn clean install`, which rewrites target/. Do NOT run it while a `quarkus:dev`
# source broker is live (it will hot-reload and hit the ECJ trap). Stop it first (dev/stop-broker.sh).
set -euo pipefail
cd "$(dirname "$0")/.."

PROFILE="${1:-kafka}"
TAG="${2:-scorpio-local:dev}"
case "$PROFILE" in
  kafka)     ARGS=(-Dkafka -Dquarkus.profile=kafka) ;;
  in-memory) ARGS=(-Din-memory -Dquarkus.profile=in-memory) ;;
  *) echo "profile must be 'kafka' or 'in-memory'"; exit 1 ;;
esac

export MAVEN_OPTS=-Xmx1500m
echo ">>> mvn clean install (profile=$PROFILE) — serial, capped heap"
mvn -q -DskipTests "${ARGS[@]}" clean install

echo ">>> docker build $TAG (from AllInOneRunner/target/quarkus-app)"
docker build -f AllInOneRunner/src/main/docker/Dockerfile.jvm -t "$TAG" AllInOneRunner

echo "=== built $TAG (profile=$PROFILE) ==="
docker images "$TAG" --format '{{.Repository}}:{{.Tag}} {{.Size}}'
