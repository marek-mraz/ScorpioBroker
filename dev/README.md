# Scorpio dev environment & ETSI test bed

This box is recreated fresh each session, so the toolchain is not persisted — run the setup steps each time.

## The one environment (dev AND CI)

Everything — single-broker suites, federation, MQTT — runs against **one** stack:
`compose-files/docker-compose-iop.yml` — **5 self-contained Scorpio brokers**, each with its own
Postgres + Kafka + MQTT (emqx), built from the working tree (`scorpio-local:latest`, **in-memory**
messaging). Single-broker suites target **broker1 (`scorpio1`)**; federation suites use all five.
There is no separate single-broker / kafka compose — local and CI use the same stack, so results match.

```bash
./dev/install-tools.sh      # JDK 21 + Maven + Docker CLI (once per session)
./dev/etsi-serial.sh        # build image + bring up 5 brokers + run EVERY suite serially + report
```

`dev/etsi-serial.sh` is the **single entrypoint CI also uses** (`.github/workflows/etsi-serial-test.yml`).
It produces the **same two outputs** locally and in CI, under `ngsi-ld-test-suite/`:
- `results/report.html` (+ `output.xml`, `log.html`) — the one combined report.
- `etsi-failures.md` — the failures-only file.

It points the suite at broker1, resets state between suites with `clean_db.sh` (never drops the DB),
keeps **https** `@context` URLs, and configures reachability from a few env vars (defaults = this dev
box; CI overrides `B1..B5` + `CALLBACK_HOST`).

## Scripts (grouped)

**Setup**
- `install-tools.sh` — Temurin JDK 21, Maven, Docker CLI + compose plugin.
- `start-deps.sh` — Postgres + Kafka for the single-broker inner loop only (not the 5-broker stack).

**The stack / testing (canonical)**
- `build-image.sh [in-memory|kafka] [tag]` — build `scorpio-local` from the working tree. The stack
  uses `in-memory` (default tag `scorpio-local:dev`; the stack wants `scorpio-local:latest`).
- `run-iop.sh [--no-mvn|down]` — build the image + bring up the 5 brokers + health-check, and attach
  this container to the stack network. `--no-mvn` reuses the existing image; `down` tears it all down.
- `etsi-serial.sh` — the unified runner described above (calls `run-iop.sh`, runs every suite, reports).

**Single-broker inner loop (fast Java iteration only — NOT the authoritative result)**
- `run-broker.sh [in-memory|kafka]` — build + run one source broker on `localhost:9090` (quarkus:dev).
- `stop-broker.sh` — stop it.

**Other**
- `scorpiobroker.code-workspace` — VS Code multi-module workspace.

## Gotchas (cost real time to learn)

- **Build the in-memory image/jar with `-Dquarkus.profile=in-memory`**, not only `-Din-memory`: the
  in-memory `@Incoming` consumers are `@IfBuildProfile("in-memory")` (evaluated at build), else startup
  dies with `SRMSG00019: Unable to connect an emitter with the channel 'entity'`.
- **AAIO = All-In-One**: each broker packs every microservice into one JVM, so it uses synchronous
  in-VM messaging. Kafka inside a broker is unnecessary and async Kafka makes the ETSI suite race
  (create→notify / create→temporal-query) — that's why the image is built in-memory.
- **`clean_db.sh` resets state, never drop the DB.** It truncates data tables (schema/Flyway stay).
  Set `CLEAN_DB_CONTAINER` to target a specific Postgres (the stack uses broker1's `scorpio-postgres-1`).
  It does NOT clear in-VM subscription/registry caches — restart the broker for measured runs.
- **Callbacks (broker → suite mocks):** this container has >1 network, so its hostname may resolve to
  the wrong interface. `etsi-serial.sh` uses its **stack-network IP** as `CALLBACK_HOST` so the brokers
  can reach the mock servers (Connection-refused on `:8085` otherwise).
- **Prefer `https`** for `@context`/endpoints; `http` only for the local broker/mock hosts.
- Low free memory historically: build serially with `MAVEN_OPTS=-Xmx1500m` (no `-T`).
