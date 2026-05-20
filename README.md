# Kafka Order System

## Overview

Two independent Spring Boot services communicating via Apache Kafka. **Order API**
accepts orders via REST and publishes events; **Inventory Service** consumes events
and approves/rejects based on in-memory stock.

## Architecture

```
Client → POST /orders → [Order API :8080] → Kafka (orders) → [Inventory Service :8081]
                              ↓ 503 if Kafka down              ↓ APPROVE / REJECT / DUPLICATE
                                                    ↓ non-retryable
                                             Kafka (orders-dead-letter)
```

## Production-Grade Mechanisms

| Mechanism | Where | Why it matters |
|---|---|---|
| Idempotent producer (`acks=all`, `enable.idempotence=true`) | `OrderEventProducer` | Prevents duplicate publishes on retry |
| Synchronous publish with timeout | `OrderEventProducer` | Client gets an honest 503, not a false 202 |
| `ErrorHandlingDeserializer` | consumer `application.yaml` | Bad payload → DLQ, not a poison-pill loop |
| `use.type.headers=false` | consumer `application.yaml` | Services fully decoupled — no shared package dependency |
| Manual offset acknowledgment | consumer `application.yaml` | No message loss if the consumer crashes mid-processing |
| Retry with fixed backoff (3×, 1s) | `KafkaConsumerConfig` | Transient failures retried before the DLQ |
| Dead Letter Queue | `orders-dead-letter` topic | Non-retryable failures captured, not lost |
| Consumer-side idempotency (atomic `Set.add`) | `InventoryStore` | Duplicate delivery → no double reservation |
| Correlation ID (MDC + Kafka header) | `CorrelationFilter` → producer → consumer | Full request traceability across services |
| Actuator health endpoints | both services | Container-level health checks |

## How to Run

```bash
docker compose up --build
```

Docker is the only prerequisite — the images build the jars themselves. The first run
builds both service images (a few minutes); the services start once the Kafka
healthcheck passes (about 30s).

## How to Test

Order API is on `:8080`; Inventory Service is on `:8081`.

**1. Successful order**
```bash
curl -i -X POST http://localhost:8080/orders \
  -H 'Content-Type: application/json' \
  -d '{"orderId":"1","itemId":"item-1","quantity":2}'
```
Expected: `202 Accepted`; `item-1` stock decreases 10 → 8.

**2. Rejected order** — `item-3` has stock 0
```bash
curl -i -X POST http://localhost:8080/orders \
  -H 'Content-Type: application/json' \
  -d '{"orderId":"2","itemId":"item-3","quantity":1}'
```
Expected: `202` accepted by the API; Inventory Service logs `REJECTED`.

**3. Duplicate** — send the identical request twice
```bash
curl -i -X POST http://localhost:8080/orders \
  -H 'Content-Type: application/json' \
  -d '{"orderId":"3","itemId":"item-1","quantity":1}'
```
Expected: the second delivery logs `DUPLICATE`; stock unchanged.

**4. Kafka down**
```bash
docker compose stop kafka
curl -i -X POST http://localhost:8080/orders \
  -H 'Content-Type: application/json' \
  -d '{"orderId":"4","itemId":"item-1","quantity":1}'
```
Expected: `503 Service Unavailable` within ~5s (not a false `202`). Restore with
`docker compose start kafka`.

**5. Check stock**
```bash
curl http://localhost:8081/inventory/item-1
```
Expected: `{"itemId":"item-1","availableStock":8}` (after test 1).

**6. Check order result**
```bash
curl http://localhost:8081/orders/1/status
```
Expected: `200` with the order result (`APPROVED` / `REJECTED`), or `404` if it has
not been processed yet.

**7. Kafka UI** — browse topics, messages and consumer groups at http://localhost:8090

## Initial Inventory State

| Item | Stock |
|---|---|
| item-1 | 10 |
| item-2 | 5 |
| item-3 | 0 — intentionally empty; use to test REJECT |

## Assumptions & Trade-offs

- **In-memory storage** — acceptable per the task requirements; state resets on restart.
- **Idempotency is session-scoped** — duplicate protection does not survive a service
  restart (production: a Redis- or DB-backed dedup store).
- **Single Kafka broker** — sufficient for local dev; production needs 3+ nodes.
- **Fixed backoff (not exponential)** — simpler; exponential with jitter is preferred
  for production.
- **No shared contracts module** — each service owns its `OrderEvent` copy, decoupled
  via `use.type.headers=false`.
