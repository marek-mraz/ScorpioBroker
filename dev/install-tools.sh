#!/usr/bin/env bash
# Install the toolchain needed to build & run Scorpio from source:
#   Temurin JDK 21, Maven, Docker CLI + compose plugin, gnupg.
# Idempotent: safe to re-run. Debian/Ubuntu (apt) only.
# This container is recreated fresh each session, so the toolchain is NOT persisted —
# run this once at the start of every session.
set -euo pipefail

sudo apt-get update -qq
sudo apt-get install -y -qq gnupg ca-certificates curl
sudo install -m 0755 -d /etc/apt/keyrings

# --- Docker apt repo (CLI + compose plugin; the daemon is the host's /var/run/docker.sock) ---
if [ ! -f /etc/apt/keyrings/docker.gpg ]; then
  curl -fsSL https://download.docker.com/linux/debian/gpg -o /tmp/docker.gpg.asc
  sudo gpg --dearmor --yes -o /etc/apt/keyrings/docker.gpg /tmp/docker.gpg.asc
  sudo chmod a+r /etc/apt/keyrings/docker.gpg
  echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/debian $(. /etc/os-release && echo "$VERSION_CODENAME") stable" \
    | sudo tee /etc/apt/sources.list.d/docker.list >/dev/null
fi

# --- Adoptium apt repo (Temurin JDK 21; Debian 12 only ships JDK 17) ---
if [ ! -f /etc/apt/keyrings/adoptium.gpg ]; then
  curl -fsSL https://packages.adoptium.net/artifactory/api/gpg/key/public -o /tmp/adoptium.asc
  sudo gpg --dearmor --yes -o /etc/apt/keyrings/adoptium.gpg /tmp/adoptium.asc
  sudo chmod a+r /etc/apt/keyrings/adoptium.gpg
  echo "deb [signed-by=/etc/apt/keyrings/adoptium.gpg] https://packages.adoptium.net/artifactory/deb $(. /etc/os-release && echo "$VERSION_CODENAME") main" \
    | sudo tee /etc/apt/sources.list.d/adoptium.list >/dev/null
fi

sudo apt-get update -qq
sudo apt-get install -y -qq docker-ce-cli docker-compose-plugin temurin-21-jdk maven

# The mounted docker socket is owned by root; open it so `docker` works without sudo this session.
sudo chmod 666 /var/run/docker.sock 2>/dev/null || true

echo "=== installed ==="
java -version 2>&1 | head -1
mvn -version 2>&1 | head -1
docker --version
docker compose version | head -1
