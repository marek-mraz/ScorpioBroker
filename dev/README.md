# Scorpio dev environment & ETSI test bed — quick start

Fast, repeatable setup for building/running **Scorpio (AllInOneRunner)** from source against
**Postgres + Kafka**, and running the **ETSI NGSI-LD conformance suite**.

> This dev box is a container that shares the host's docker socket (docker-out-of-docker) and is
> **recreated fresh each session** — the toolchain is not persisted, so step 1 runs every time.

## TL;DR

```bash
dev/install-tools.sh          # JDK 21, Maven, Docker CLI (once per session)
dev/start-deps.sh             # Postgres + Kafka, attach this container to their network
dev/run-broker.sh in-memory   # build + run broker on :9090 (in-memory = ETSI-correct)
# ... hack ...
cd ngsi-ld-test-suite && ./etsi_report.sh        # run the suites + build the failures report
```

## 1. Install the toolchain — `dev/install-tools.sh`

Installs Temurin **JDK 21** (Debian ships only 17), **Maven**, **Docker CLI + compose plugin**,
and opens the docker socket for this session. Idempotent.

## 2. Start dependencies — `dev/start-deps.sh`

Brings up `scorpio-dev-postgres` (db/user/pass all `ngb`) and `scorpio-dev-kafka` from
`compose-files/docker-compose-dev-deps.yml`, waits for Postgres health, and **connects this
container to the deps network** (`compose-files_default`) so the broker can reach them by
hostname. The network attach is **not** persistent across container recreation — re-run after a
fresh session.

Why hostnames: published ports are on the *host*, not this container's localhost, so the broker
talks to Postgres/Kafka over the shared compose network (`scorpio-dev-postgres`, `scorpio-dev-kafka`).

## 3. Run the broker — `dev/run-broker.sh [in-memory|kafka]`

Builds and runs the AllInOneRunner in Quarkus dev mode on **:9090**, logging to `/tmp/scorpio.log`,
in the background. Stop it with `dev/stop-broker.sh`.

| profile | messaging | when |
|---|---|---|
| **in-memory** (default) | SmallRye in-VM channels, **synchronous** core→temporal propagation | **ETSI conformance.** Tests create via the core API then immediately query the temporal API — async messaging races that to empty results. |
| **kafka** | async event bus via `scorpio-dev-kafka` | production-like; needs Kafka up |

Health check: `curl localhost:9090/q/health` → `{"status":"UP",...}`.

## 4. Run the ETSI suite — `ngsi-ld-test-suite/etsi_report.sh`

```bash
cd ngsi-ld-test-suite
./etsi_report.sh                 # run all configured suites, then build etsi-failures.md
./etsi_report.sh report          # skip running; rebuild the report from existing results-*/
./etsi_report.sh run 1200        # run with a per-suite timeout of 1200s
```

- Each suite wipes the DB first (`clean_db.sh`) and runs Robot against `TP/NGSI-LD/<path>`.
- Which suites run is the `SUITES=(...)` list at the top of `etsi_report.sh` (matches the CI matrix
  in `.github/workflows/ci-cd-github.yml`).
- Output: **`ngsi-ld-test-suite/etsi-failures.md`** — failures only, with the request under test,
  expected-vs-actual, and the spec/requirement tags. Per-suite pass/fail prints to stdout.
- Run a single suite directly: `./run_suite.sh <name> <relpath> [timeout]`
  (e.g. `./run_suite.sh Consumption-Entity ContextInformation/Consumption/Entity`).
- Run one Robot file in isolation: `./clean_db.sh && .venv/bin/robot --outputdir /tmp/x TP/NGSI-LD/<file>.robot`.

The Robot venv lives at `ngsi-ld-test-suite/.venv`. The suite targets `http://localhost:9090/ngsi-ld/v1`.

## Known gotchas — read this before debugging "broken" tests

Hard-won traps that look like broker bugs but aren't. Each one cost real time to rediscover.

1. **`clean_db.sh` does NOT clear in-memory state.** In the `in-memory` profile the broker keeps active
   (registry-)subscriptions in in-VM tables; `clean_db.sh` only truncates Postgres. A subscription left
   over from a manual `curl` probe or a prior test file keeps matching and sending notifications, so a
   `cSourceNotification` arrives with a **stale `subscriptionId`** and the assertion fails. **Restart the
   broker (fresh JVM) before any measured ContextSource/Subscription run**, and don't hand-create subs on
   the broker you then measure.

2. **`jsonldContext` hangs ~30 min** (test 053_05_01 waits on an external context server that is
   unreachable here / in CI). It is **excluded** from the CI matrix and the serial workflow on purpose; the
   local `etsi_report.sh` `SUITES=(...)` list also omits it. Don't add it back without a per-test timeout.

3. **`uri.etsi.org` is 403 from inside broker containers.** The ETSI `@context` chains to
   `ngsi-ld-core-context-v1.8`/`v1.9`. A broker that doesn't recognise those as *core* contexts tries to
   fetch them → 403 → JSON-LD compaction fails → GETs come back fully **expanded**
   (`https://ngsi-ld-test-suite/context#OffStreetParking`). The current source lists v1.8/v1.9 in
   `NGSIConstants.CORE_CONTEXT_URLS` and resolves them locally, so **always run images built FROM SOURCE**
   (the published `java-latest` is older and fails this). The IOP compose now `build:`s from source for
   exactly this reason — see §6.

4. **Federated GET-by-id needs `operations` on the registration.** A `ContextSourceRegistration` without an
   `operations` member is **not** consulted for `GET /entities/{id}` — broker A returns 404 with no forward
   and it looks like federation is broken. Add `"operations": ["federationOps"]` (or `["retrieveOps"]`) and
   the federated read works (verified across the source-built 5-broker IOP stack).

5. **The IOP `IOP_CNF_*` create/retrieve assertions are a harness mismatch, not a broker bug.** They
   `Retrieve Entity ... local=true` *without* a context and `Should Be Equal` the (Core-Context, expanded)
   response against a data file that is *compacted* with an inline `@context`. No conformant broker can
   reproduce that inline `@context`, so the equality can't pass. Don't chase it in Java.

## Build & gotchas (this box)

- **Low free memory** — build serially with `MAVEN_OPTS=-Xmx1500m`; `mvn -T1C` gets OOM-killed.
- **Editing `Commons/` needs a full restart**, not a live reload: the dev-mode ECJ compiler can't
  resolve some transitive deps (e.g. spatial4j) on a partial recompile and dies with
  "Unresolved compilation problems". A clean `mvn install` uses the real classpath — see below.
- **Stale broken classes**: after a failed live reload, `target/` can hold a broken `.class` that a
  later `quarkus:dev` start picks up ("cannot find symbol" on code that's actually fine). Fix with a
  clean rebuild:
  ```bash
  MAVEN_OPTS=-Xmx1500m mvn clean install -DskipTests -Din-memory -Dquarkus.profile=in-memory
  ```
  (use `-Dkafka -Dquarkus.profile=kafka` for the kafka profile).
- **Kill the broker by /proc scan**, not `pkill` (unavailable here) — `dev/stop-broker.sh` does this.
- A leftover broker from a prior session can keep holding :9090 and silently serve the OLD jar —
  always stop-then-start.

## 5. Docker-image testing (Kafka + Postgres) — `dev/build-image.sh` + `dev/run-broker-docker.sh`

Run the broker as a **container built from your working tree**, against the dockerized
Kafka + Postgres. Same `localhost:9090` target as source-run, so the ETSI suite is unchanged.

```bash
dev/start-deps.sh                  # postgres + kafka (if not already up) + network attach
dev/stop-broker.sh                 # free :9090 (a source-run broker can't share the port)
dev/build-image.sh kafka           # mvn clean install (kafka profile) + docker build -> scorpio-local:dev
dev/run-broker-docker.sh           # run the image; waits for /q/health
cd ngsi-ld-test-suite && ./etsi_report.sh
```

- `dev/build-image.sh [kafka|in-memory] [tag]` packages **your local fixes** and builds an image via
  `AllInOneRunner/src/main/docker/Dockerfile.jvm` (base `eclipse-temurin:21-jre-ubi9-minimal`).
  The messaging profile is **baked at build time** — `kafka` reads `BUSHOST`/`DBHOST`,
  `in-memory` is synchronous in-VM. **Do not run it while a `quarkus:dev` broker is live** (it
  rewrites `target/` → ECJ hot-reload trap); `dev/stop-broker.sh` first.
- `dev/run-broker-docker.sh [tag]` runs the image with **`--network container:$(hostname)`** — it
  shares *this dev container's* network namespace. That is the crucial trick on this
  docker-out-of-docker box: the Robot suite and its mock context source both run *inside this
  container*, so the broker must bind the same `localhost` they use (`:9090` broker, `:8086` mock)
  while still resolving `scorpio-dev-postgres` / `scorpio-dev-kafka` over the attached deps network.
- Inspect / stop: `docker logs -f scorpio-broker-local`, `docker rm -f scorpio-broker-local`.

> ETSI note: `kafka` introduces async core→temporal propagation, which races a handful of
> create-then-immediately-query-temporal tests to empty. Use `in-memory` for the cleanest temporal
> conformance numbers; use `kafka` for production-like / messaging-path validation.

## 6. Testing ContextSource & DistributedOperations (federation)

**These suites do NOT need a second Scorpio.** They register a Context Source whose `endpoint` is
rewritten to a **mock HTTP server the suite starts itself** (`resources/MockServerUtils.resource`,
`HttpCtrl.Server` on `0.0.0.0:8086`, see `resources/variables.py`). The broker under test forwards
the distributed operation to that mock; the test asserts the merged result (e.g. a `207`
BatchOperationResult combining the local op with the mock's reply).

So the requirement is simply **the broker must reach the mock on `localhost:8086`**, which holds for
both run modes here because robot + mock + broker all share this container's localhost:
- **source-run** (`dev/run-broker.sh`): broker is a process in this container → reaches `:8086`.
- **docker image** (`dev/run-broker-docker.sh`): `--network container:$(hostname)` puts the broker
  on the same localhost → reaches `:8086`.

Run them:
```bash
cd ngsi-ld-test-suite
./run_suite.sh ContextSource        ContextSource        1800
./run_suite.sh DistributedOperations DistributedOperations 1800
# single test (watch the broker forward to the mock):
./clean_db.sh && .venv/bin/robot --outputdir /tmp/d \
  TP/NGSI-LD/DistributedOperations/Provision/Entities/DeleteEntity/D002_02_01_inc.robot
```
Remaining failures here are **real broker federation-semantics gaps** (e.g. should return `207`
multi-status but returns `204`), not infrastructure — they are now reproducible and fixable locally.

**Real multi-broker interop** (true peer-to-peer, no mock) is covered by
`compose-files/docker-compose-iop-test.yml` (5 brokers, each own Postgres + single-node Kafka,
`GATEWAY`/`BUSHOST`/`DBHOST` per broker). The brokers are now **built from source** (image
`scorpio-local:dev`, `build:` → `AllInOneRunner/src/main/docker/Dockerfile.jvm`) instead of the
published `java-latest` — so IOP always exercises the current working tree. Run it:

```bash
dev/build-image.sh kafka scorpio-local:dev      # mvn clean install (kafka) + docker build (do this first)
docker compose -f compose-files/docker-compose-iop-test.yml \
               -f compose-files/docker-compose-iop-test.override.yml up -d   # override = heap/mem caps
# from this dev container you reach the brokers by hostname (scorpioN:9090), not localhost:908N:
cd ngsi-ld-test-suite
.venv/bin/robot --variable b1_url:http://scorpio1:9090/ngsi-ld/v1 \
  --variable b2_url:http://scorpio2:9090/ngsi-ld/v1 --variable b3_url:http://scorpio3:9090/ngsi-ld/v1 \
  --variable b4_url:http://scorpio4:9090/ngsi-ld/v1 --variable b5_url:http://scorpio5:9090/ngsi-ld/v1 \
  --variable url:http://scorpio1:9090/ngsi-ld/v1 --variable temporal_api_url:http://scorpio1:9090/ngsi-ld/v1 \
  --outputdir results-iop ./IOP_TP
```

The Dockerfile only **COPYs** `target/quarkus-app`, so the jar must be built (kafka profile) before
`compose ... up`; `docker compose build` then reuses the single `scorpio-local:dev` image for all
five brokers. **Why from source matters for IOP:** the ETSI IOP `@context` chains to
`ngsi-ld-core-context-v1.8`/`v1.9`, and `uri.etsi.org` is **403-blocked from inside the containers**.
The current source recognises v1.8/v1.9 as core contexts (`NGSIConstants.CORE_CONTEXT_URLS`) and
resolves them locally; the older published image does not, so it fetches → 403 → no compaction →
every IOP test fails on an expanded-vs-compacted body diff. The ETSI ContextSource /
DistributedOperations suites do **not** require this stack — the mock path above is the supported way.

## 6a. Debugging distributed-op forwarding (what the broker actually sends)

The ETSI mock (`HttpCtrl.Server`) is **picky**, and that masquerades as broker bugs. Things learned
the hard way (2026-06-23):

- **Stub matching is EXACT** — method + full path **including the query string**, compared as a
  lowercased string (`HttpCtrl/http_stub.py`). `/attrs` ≠ `/attrs/`; `/attrs/speed?deleteAll=false`
  ≠ `/attrs/speed`. The broker must send the **canonical short** form the stub registered.
- **On no stub match, the mock BLOCKS** (waits for a manual `Reply By`) instead of 404ing. The broker
  then times out and reports `UnprocessableContextSourceRegistration` / **"Connection was closed"**
  (422) → a spurious `207`. So "Connection was closed" almost always means *path/method/query
  mismatch*, not a network problem.
- **`Set Stub Reply` and `Wait For Request` are mutually exclusive for the same request**: a request
  that matches a stub is auto-replied and **not** recorded, so a test that sets a stub *and* calls
  `Wait For Request` for that path will time out unless the broker's request misses the stub. Tests
  mixing both can be a test-tool quirk — note in `error.md`, don't chase in Java.

**Fastest way to see what the broker forwards** — run a logging mock on `:8086` instead of the suite's:
```bash
cat > /tmp/mock.py <<'PY'
from http.server import BaseHTTPRequestHandler, HTTPServer
import sys
class H(BaseHTTPRequestHandler):
    def _h(self):
        n=int(self.headers.get('Content-Length',0) or 0)
        sys.stderr.write(f"GOT {self.command} {self.path} body={self.rfile.read(n)[:200]!r}\n"); sys.stderr.flush()
        self.send_response(204); self.end_headers()
    do_GET=do_POST=do_PATCH=do_PUT=do_DELETE=_h
    def log_message(self,*a): pass
HTTPServer(('0.0.0.0',8086),H).serve_forever()
PY
python3 /tmp/mock.py 2>/tmp/mock.err &
# create entity + an inclusive registration whose endpoint is http://0.0.0.0:8086, then do the op;
# `cat /tmp/mock.err` shows the exact method/path/body the broker sent. Forward path bugs jump right out.
```
Note: the canonical attr-collection endpoint Scorpio sends is `/entities/{id}/attrs/` and Scorpio
**accepts the trailing slash on its own endpoints**, so matching the suite's `/attrs/` stub is
federation-safe.

## Editing the broker while `quarkus:dev` is running — the ECJ trap

If you edit a source file (e.g. `EntityService.java`) while `dev/run-broker.sh` is live, Quarkus
live-reload recompiles incrementally with **ECJ**, which here throws **bogus** errors like
`cannot access UpdateEntityRequest / ReplaceEntityRequest — class file not found` and
`incompatible types ... cannot be converted to ...` (the same type to itself). These are **not** real
— the broker just wedges in "Error restarting Quarkus". Recovery:
```bash
# kill every dev/maven JVM, do a real javac build (authoritative), then cold-start
for p in /proc/[0-9]*; do c=$(tr '\0' ' ' </"$p"/cmdline 2>/dev/null); case "$c" in *quarkus*|*serialized-app*) kill -9 "${p#/proc/}";; esac; done
mvn -q -pl Commons,EntityManager -am clean install -DskipTests -Din-memory   # surfaces REAL compile errors
dev/run-broker.sh in-memory
```
The wrapper may print `exit 1` from its health-wait pipefail even when the broker is fine — verify with
`curl -s -o /dev/null -w '%{http_code}' localhost:9090/q/health` (200 = up), not the exit code.
Also: this box is memory-tight (~2 GB free); a broker can OOM-die seconds after "started" — re-check
health, don't assume it stayed up.

## Layout

```
dev/install-tools.sh       # toolchain
dev/start-deps.sh          # postgres + kafka + network attach
dev/run-broker.sh          # build + run AllInOneRunner from source (in-memory | kafka)
dev/stop-broker.sh         # stop a source-run broker
dev/build-image.sh         # build a docker image from the working tree (kafka | in-memory)
dev/run-broker-docker.sh   # run that image, sharing this container's netns (for the suite + mock)
compose-files/docker-compose-dev-deps.yml   # postgres + kafka services
compose-files/docker-compose-iop-test.yml   # 5-broker real federation/interop stack
ngsi-ld-test-suite/etsi_report.sh           # run suites + build failures report
ngsi-ld-test-suite/etsi-failures.md         # generated report
ngsi-ld-test-suite/resources/MockServerUtils.resource  # mock context source (federation tests)
error.md                                     # running log of ETSI findings/fixes
```
