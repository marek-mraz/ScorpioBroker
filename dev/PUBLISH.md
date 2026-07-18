# Publishing the production-ready Scorpio image

**Definition of "production-ready" here:** the *exact* image binary that passed the full ETSI
NGSI-LD serial run — all 8 suites (CommonBehaviours, Consumption, Provision, Subscription,
ContextSource, jsonldContext, DistributedOperations, IOP across all 5 federated brokers) with
**zero failed, zero skipped, zero robot execution errors**, and every suite provably ran
(a missing `output.xml` counts as not green). The image is never rebuilt between test and
publish — what was tested is what ships.

## Automatic publish via CI (recommended)

1. Make sure both repos are pushed: this repo (`development-quarkus`) and, if you changed the
   suite, `marek-mraz/ngsi-ld-test-suite` (`main`). CI clones both from GitHub.
2. GitHub → **Actions** → **Build and ETSI Test (Serial)** → **Run workflow** (manual dispatch,
   branch `development-quarkus`). Takes roughly 30–45 min.
3. Read the run's **Summary** page. It shows the failures report (`etsi-failures.md`) and the
   gate verdict:
   - `Publish gate: GREEN — N tests passed …` → the image was pushed automatically to
     `ghcr.io/marek-mraz/scorpio-broker:<sha7>` and `:latest` (`<sha7>` = the commit tested).
   - `Publish gate: NOT green` → nothing is published; the summary lists exactly why
     (per-suite fail/skip counts, missing suites). The job itself stays green by design —
     the gate only controls publishing.
4. Full HTML reports are in the `ETSI-Serial-Reports` artifact (`results/report.html`).

Auth is the built-in `GITHUB_TOKEN` (job has `packages: write`) — no secrets to configure.
The first publish creates the GHCR package; if you want anonymous pulls, set the package to
public once: GitHub → Packages → `scorpio-broker` → Package settings → visibility.

## Manual publish (local)

Only when CI is unavailable. Same rule: publish only the tested binary.

```bash
./dev/etsi-serial.sh                     # full run — no STOP_ON_ERROR, no SKIP_UP
# Verify 100% green: etsi-failures.md must list no failures, and all 8
# ngsi-ld-test-suite/results/*/output.xml must exist (incl. IOP).
docker login ghcr.io -u marek-mraz       # PAT with write:packages scope
docker tag scorpio-local:latest ghcr.io/marek-mraz/scorpio-broker:$(git rev-parse --short HEAD)
docker tag scorpio-local:latest ghcr.io/marek-mraz/scorpio-broker:latest
docker push ghcr.io/marek-mraz/scorpio-broker:$(git rev-parse --short HEAD)
docker push ghcr.io/marek-mraz/scorpio-broker:latest
```

Do **not** run `mvn` or `dev/build-image.sh` between the test run and the push — that would
replace `scorpio-local:latest` with an untested binary.

## Running the published image

All-in-one broker on port 9090; **in-memory messaging profile baked in at build time**
(no Kafka needed). Requires PostGIS.

```yaml
services:
  postgres:
    image: postgis/postgis:18-3.6
    environment: { POSTGRES_USER: ngb, POSTGRES_PASSWORD: ngb, POSTGRES_DB: ngb }
  scorpio:
    image: ghcr.io/marek-mraz/scorpio-broker:latest
    ports: ["9090:9090"]
    environment:
      DBHOST: postgres
      GATEWAY: "http://your-public-host:9090"   # external URL the broker advertises
    depends_on: [postgres]
```

Health check: `GET /q/health` → 200.

## Scope caveats (what "100% green" covers)

- The certified profile is **in-memory** messaging. A Kafka-profile image
  (`dev/build-image.sh kafka`) is a *different binary* — it has NOT passed the gate; run the
  suite against it before treating it as production.
- CI excludes the MQTT notification suites (`*mqtt*`, need the suite's mosquitto wiring that
  only works on the dev box) and tests tagged `brokerok-harness` (proven ETSI-harness bugs,
  see `error.md`) / `deferred-broker-fix` (documented open broker bugs). Green certifies
  everything else.
