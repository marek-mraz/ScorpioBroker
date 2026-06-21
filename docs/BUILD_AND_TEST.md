# Building the AAIO image on GitLab & running the API test suite

This manual covers building the **All-In-One-Runner (AAIO)** `java-kafka` Docker
image — `scorpiobroker/all-in-one-runner:java-kafka-7.0.0` — and running the full
`api-test.json` Postman/newman suite against it, both **locally** and in **GitLab CI**.

> Current project version: **7.0.0** (all 14 modules). The image tag is independent of
> the Maven version and is controlled with `-Ddocker-tag=...`.

---

## 1. How the image is built

The image is produced by Quarkus' container-image extension
(`quarkus-container-image-docker`, declared in `BrokerParent/pom.xml`), which builds
from `AllInOneRunner/src/main/docker/Dockerfile.jvm` using the local Docker daemon.

The `docker` Maven profile in `AllInOneRunner/pom.xml` sets:

| Property | Value |
| --- | --- |
| `quarkus.container-image.registry` | `scorpiobroker` |
| `quarkus.container-image.name` | `${project.artifactId}` → `all-in-one-runner` |
| `quarkus.container-image.tag` | `${docker-tag}` |

Resulting reference: **`scorpiobroker/all-in-one-runner:${docker-tag}`**.

The messaging flavour is selected by two flags that must agree:

| Flag | Effect |
| --- | --- |
| `-Dkafka` | activates the Maven profile that adds the Kafka connector dependency |
| `-Dquarkus.profile=kafka` | bakes the Kafka (de)serializer config (`application-kafka.properties`) at **build** time |

> ⚠️ Both are required. A plain build has no connector → `SRMSG00019: Unable to connect
> emitter 'entity'`. Building with the wrong/empty quarkus profile but running with
> `-Dquarkus.profile=kafka` fails at startup with `value.deserializer ... must be set`
> (Quarkus auto-detects Kafka serializers at build time).

### Build command (produces the java-kafka-7.0.0 image)

```bash
mvn -B -ntp clean package \
  -Dmaven.test.skip=true \
  -Dkafka -Dquarkus.profile=kafka \
  -Ddocker -Ddocker-tag=java-kafka-7.0.0
# -> scorpiobroker/all-in-one-runner:java-kafka-7.0.0
```

Push it:

```bash
docker push scorpiobroker/all-in-one-runner:java-kafka-7.0.0
# or add -Dquarkus.container-image.push=true to the mvn command above
```

---

## 2. Building on GitLab CI

The pipeline is defined in [`.gitlab-ci.yml`](../.gitlab-ci.yml) with three stages:

1. **build** – `mvn clean install` of the whole reactor (Temurin 21).
2. **image** – builds & pushes `scorpiobroker/all-in-one-runner:java-kafka-7.0.0`
   using `docker:dind` and the build command above.
3. **test** – brings up the stack and runs the api-test suite (see §3), publishing a
   JUnit report so failures show up in the GitLab MR widget.

### Required CI/CD variables (Settings → CI/CD → Variables)

| Variable | Purpose |
| --- | --- |
| `DOCKERHUB_USER` | Docker Hub user with push rights to the `scorpiobroker` org |
| `DOCKERHUB_TOKEN` | Docker Hub access token (masked/protected) |

To cut a different version, override `IMAGE_VERSION` (e.g. run the pipeline with
`IMAGE_VERSION=7.0.1`) — the tag becomes `java-kafka-7.0.1`.

> **GitLab Runner note:** the `image`/`test` jobs need a runner that allows the
> privileged `docker:dind` service (or a socket-bind runner). On shared SaaS runners
> dind is available by default.

---

## 3. Running the api-test.json suite

The suite is a Postman collection (`api-test.json`) run with **newman**. It needs:

| Dependency | Why |
| --- | --- |
| Postgres + PostGIS | entity storage / geo queries |
| Kafka | internal event bus (the broker is built with the kafka profile) |
| Scorpio AAIO on `:9090` | the system under test |
| **Notification receiver on `:8080`** | subscription tests POST notifications here and poll `GET /<subId>` to verify delivery. Source: [`testserver/testserver.py`](../testserver/testserver.py). **Without it every subscription test fails with `ECONNREFUSED 127.0.0.1:8080`.** |

### Environment file

The stock `api-test-aaio-localhost-environment.json` is missing two host variables the
collection uses (`history-query-manager`, `history-entity-manager`). Use
[`api-test-newman-env.json`](../api-test-newman-env.json) instead — it maps every
microservice host variable to `http://localhost:9090` and the notification server to
`http://localhost:8080`.

### One-shot run against the built image

```bash
# 1. bring up postgres + kafka + AAIO image + notification receiver
IMAGE_TAG=java-kafka-7.0.0 \
  docker compose -f compose-files/docker-compose-aaio-test.yml up -d

# 2. wait until the broker is ready
until curl -sf http://localhost:9090/q/health/ready; do sleep 3; done

# 3. run the suite
newman run api-test.json \
  -e api-test-newman-env.json \
  --reporters cli,junit \
  --reporter-junit-export newman-report.xml

# 4. tear down
docker compose -f compose-files/docker-compose-aaio-test.yml down -v
```

### Running against a from-source dev broker (no image build)

```bash
# deps
docker compose -f compose-files/docker-compose-dev-deps.yml up -d
# notification receiver
python3 testserver/testserver.py 8080 &
# broker (dev mode, kafka profile)
cd AllInOneRunner
mvn quarkus:dev -Dkafka -Dquarkus.profile=kafka \
  -Ddbhost=scorpio-dev-postgres -Dbushost=scorpio-dev-kafka \
  -Dquarkus.http.host=0.0.0.0
# then, from the repo root:
newman run api-test.json -e api-test-newman-env.json
```

---

## 4. Interpreting results

- A green run requires the **notification receiver on :8080**. If you skip it, expect
  ~73 `ECONNREFUSED 127.0.0.1:8080` request errors plus the assertion failures that
  cascade from them (`JSONError: No data, empty input`), almost all under
  `/subscriptions`.
- The remaining handful of `Error Tests / Dist ops` registration cases exercise
  distributed Context-Source federation; they need matching CSource registrations to be
  meaningful and may be expected-fail in a single-broker run.
