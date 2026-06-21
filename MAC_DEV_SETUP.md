# Mac Development Setup for Scorpio Broker

This guide explains how to set up the development environment on a Mac using Docker for Postgres and Kafka, and Java for developing the Scorpio Broker.

## Prerequisites

1.  **Homebrew**: Package manager for Mac.
    ```bash
    /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
    ```
2.  **Docker Desktop**: To run Postgres and Kafka.
    ```bash
    brew install --cask docker
    ```
3.  **Java (JDK 17+)**: Recommended version for Scorpio.
    ```bash
    brew install openjdk@17
    ```
4.  **Maven**: For building the Java projects.
    ```bash
    brew install maven
    ```

## Step 1: Run Postgres and Kafka via Docker

Create a `docker-compose.dev.yml` file in your preferred directory (or run the existing compose files inside the `compose-files/` folder) with the minimum required services for local development:

```yaml
version: "3.8"
services:
  postgres:
    image: postgis/postgis:14-3.4
    environment:
      POSTGRES_USER: ngb
      POSTGRES_PASSWORD: ngb
      POSTGRES_DB: ngb
    ports:
      - "5432:5432"

  zookeeper:
    image: zookeeper:3.8
    ports:
      - "2181:2181"

  kafka:
    image: wurstmeister/kafka:2.13-2.8.1
    ports:
      - "9092:9092"
    environment:
      KAFKA_ADVERTISED_HOST_NAME: localhost
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_CREATE_TOPICS: "REGISTRY:1:1,ENTITY:1:1,SUB_ALIVE:1:1,SUB_SYNC:1:1,HIST_SUB_SYNC:1:1,TEMPORAL:1:1"
    depends_on:
      - zookeeper
```

Start the containers:
```bash
docker-compose -f docker-compose.dev.yml up -d
```

## Step 2: Build Scorpio

Clone the repository and build the parent and child projects:

```bash
git clone https://github.com/ScorpioBroker/ScorpioBroker.git
cd ScorpioBroker
mvn clean install -DskipTests
```

## Step 3: Run Microservices Locally

You can run individual microservices (e.g., EntityManager, SubscriptionManager) using Quarkus dev mode. This enables hot-reloading.

Open a terminal for a microservice and run:
```bash
cd EntityManager
mvn compile quarkus:dev -Dquarkus.profile=kafka
```

For the Subscription Manager:
```bash
cd SubscriptionManager
mvn compile quarkus:dev -Dquarkus.profile=kafka
```

*Note: The `-Dquarkus.profile=kafka` flag ensures the application reads the properties defined in `application-kafka.properties` to connect to your local Docker-hosted Kafka broker.*

## Step 4: Testing the Setup

Send a basic request to verify it's working:
```bash
curl -X POST http://localhost:9090/ngsi-ld/v1/entities/ \
  -H "Content-Type: application/json" \
  -d '{
        "id": "urn:ngsi-ld:Test:001",
        "type": "Test",
        "name": {
            "type": "Property",
            "value": "Mac Dev Test"
        }
      }'
```