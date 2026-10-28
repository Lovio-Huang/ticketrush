# TicketRush

[![CI](https://github.com/Lovio-Huang/ticketrush/actions/workflows/ci.yml/badge.svg)](https://github.com/Lovio-Huang/ticketrush/actions/workflows/ci.yml)
![Java 21](https://img.shields.io/badge/Java-21-orange)
![Spring Boot 4](https://img.shields.io/badge/Spring%20Boot-4.1-brightgreen)

A ticketing backend built for **flash sales**: thousands of people hit "buy" the
second tickets go on sale, and the system has to sell **exactly** the tickets it
has. No overselling, no lost orders, and response times stay flat.

**Stack:** Java 21 (virtual threads) · Spring Boot 4 · MySQL 8.4 · Redis (Lua scripts) · Kafka 4 (KRaft) · Flyway · Testcontainers · Docker Compose · GitHub Actions · Micrometer/Prometheus

## Results

[`scripts/load_test.py`](scripts/load_test.py): **20,000 users race for 2,000 tickets** (3 runs after a warm-up run).

| Metric | Result |
|---|---|
| Throughput | **8,500 – 9,000 requests/s** (20,000 requests in 2.2 – 2.4 s) |
| Latency (median run) | p50 16 ms · p95 47 ms · p99 65 ms |
| Tickets sold | **exactly 2,000 in every run**; the other 18,000 got `SOLD_OUT` |
| Orders persisted to MySQL | 2,000 / 2,000 |

> Measured on one Apple M5 laptop (10 cores, 24 GB) with the app, MySQL, Redis,
> Kafka *and* the load generator all on the same machine, so treat it as a lower
> bound, not a production benchmark. CI runs a smaller version (5,000 users / 500
> tickets) on every push and fails the build if a single ticket is oversold.

## Architecture

```mermaid
flowchart LR
    C([Client]) -->|"POST /reservations<br/>Idempotency-Key"| API[Spring Boot API]
    API -->|"① rate_limit.lua<br/>② reserve.lua (atomic)"| R[(Redis)]
    API -->|"③ OrderRequested<br/>acks=all"| K[[Kafka<br/>ticket-orders]]
    API -->|"202 Accepted + orderId"| C
    K --> W[Order consumer]
    W -->|"④ conditional UPDATE<br/>+ INSERT order"| DB[(MySQL)]
    J[Expiry job] -->|"FOR UPDATE SKIP LOCKED"| DB
    J -->|release.lua| R
    C -.->|"GET /orders/{id} (poll)"| API
```

A reservation never waits on MySQL:

1. **Rate limit.** A per-user token bucket in Redis rejects bots before they cost anything.
2. **Reserve in Redis.** One Lua script checks the idempotency key, the stock and the per-user limit, then decrements. Redis runs it as a single command, so concurrent buyers can't interleave.
3. **Queue the order.** The order goes to Kafka, and the client gets `202 Accepted` only after the broker acknowledges it (`acks=all`). If Kafka is down, the tickets go straight back.
4. **Persist.** A consumer writes the order to MySQL with a conditional `UPDATE ... WHERE available_tickets >= ?`. MySQL is the source of truth, and Redis is a fast copy of it.
5. **Pay or expire.** The order waits 10 minutes for payment. After that, a scheduled job expires it and puts the tickets back on sale in both stores.

## Design decisions

| Problem | Approach | Trade-off / why |
|---|---|---|
| Overselling under concurrency | Atomic check-and-decrement in a **Redis Lua script** | Locking a MySQL row (`SELECT ... FOR UPDATE`) makes every buyer queue on one row. Optimistic locking under contention means retry storms. Redis does ~100k ops/s on a single thread with no locks. |
| What if Redis is wrong? | **Defense in depth**: a conditional `UPDATE` plus a `CHECK (available_tickets >= 0)` constraint in MySQL | If Redis drifts (restart, bad manual fix), the order is `REJECTED` and nothing is oversold. Covered by the `mysqlStillPreventsOversellingWhenRedisIsOutOfSync` test. |
| Slow DB writes during the spike | **Async persistence via Kafka** | The client polls `GET /orders/{id}` instead of getting the order back right away (eventual consistency), but latency no longer depends on MySQL. |
| Client retries / double clicks | **`Idempotency-Key` header**, stored atomically with the reservation | A retry returns the original order (`Idempotent-Replayed: true`) instead of buying twice. |
| Kafka delivers at least once | **Idempotent consumer**: check by order id, with the primary key as backstop | A redelivered message is a no-op. Poison messages go to a dead-letter topic after 3 retries. |
| Ordering and contention in MySQL | Kafka messages **keyed by event id** | All writes for one event go through a single partition and consumer, so they never contend on the event row. The cost: one very hot event is limited to one partition's throughput (see next steps). |
| Paying vs expiring at the same moment | **Optimistic locking** (`@Version`) on orders | Whichever transaction commits second gets `409 CONCURRENT_MODIFICATION` instead of silently overwriting the first. |
| Running the expiry job on many instances | `SELECT ... FOR UPDATE SKIP LOCKED` in small batches | Instances split the work instead of blocking each other. No leader election needed. |
| Rate limiting across instances | Token bucket in Redis using the **Redis server clock** (`TIME`) | Limits hold cluster-wide and don't depend on app hosts having synchronized clocks. |
| Cache stampede when a sale opens | `@Cacheable(sync = true)` on event metadata | Hundreds of simultaneous first requests trigger one MySQL load instead of hundreds (measured: 50 concurrent misses dropped to 1). |
| Thread-per-request blocking I/O | **Java 21 virtual threads** (`spring.threads.virtual.enabled`) | Plain blocking code that still scales to thousands of concurrent requests. |

## API

| Method | Path | Description |
|---|---|---|
| `POST` | `/api/events` | Create an event and load its stock into Redis |
| `GET` | `/api/events/{id}` | Event details with live availability |
| `POST` | `/api/events/{id}/reservations` | Reserve tickets. Headers: `X-User-Id`, `Idempotency-Key`. Returns `202` |
| `GET` | `/api/orders/{id}` | Order status: `PROCESSING` → `PENDING_PAYMENT` → `PAID` / `EXPIRED` / `CANCELLED` |
| `POST` | `/api/orders/{id}/payment` | Simulated payment |
| `POST` | `/api/orders/{id}/cancellation` | Cancel and release the tickets |
| `GET` | `/actuator/prometheus` | Metrics, including `ticketrush_reservations_total{outcome=...}` |

Errors use [RFC 9457](https://www.rfc-editor.org/rfc/rfc9457) problem details with a stable `code`:

```json
{ "status": 409, "code": "SOLD_OUT", "title": "SOLD_OUT", "detail": "No tickets left" }
```

> `X-User-Id` stands in for authentication. In a real deployment it would come from a JWT validated at the gateway.

## Run it

You need Docker.

```bash
docker compose up --build -d
```

```bash
# create an event whose sale is already open
curl -X POST localhost:8080/api/events -H 'Content-Type: application/json' -d '{
  "name": "Demo Concert", "venue": "SAP Center",
  "startsAt": "2027-06-01T02:00:00Z", "saleStartsAt": "2026-01-01T00:00:00Z",
  "totalTickets": 100, "priceCents": 4999, "perUserLimit": 2 }'

# reserve two tickets
curl -i -X POST localhost:8080/api/events/1/reservations \
  -H 'Content-Type: application/json' -H 'X-User-Id: alice' -H 'Idempotency-Key: 7f1c' \
  -d '{"quantity": 2}'

# run the flash-sale load test
python3 scripts/load_test.py --tickets 500 --users 5000 --concurrency 200
```

`docker compose --profile observability up -d` also starts Prometheus on `localhost:9090`.

## Tests

```bash
./mvnw verify
```

| Layer | What it proves |
|---|---|
| Unit tests (`OrderTest`, `ReservationServiceTest`) | The order state machine; Kafka failures return tickets; duplicates aren't republished; stock is reseeded from MySQL after a Redis restart |
| Integration tests (Testcontainers: real MySQL, Redis and Kafka) | **1,000 concurrent buyers for 100 tickets sell exactly 100**; per-user limits hold under parallel requests; idempotent replays; expiry and cancellation restore stock in both stores; redelivered messages are no-ops; MySQL catches Redis drift |
| CI load test (Docker Compose) | The real containerized stack sells exactly N tickets under load |

The integration tests skip themselves on machines without Docker. CI always runs them.

## Project layout

```
src/main/java/com/yitaihuang/ticketrush/
├── reservation/   hot path: ReservationService, StockGateway (Lua), RateLimiter, Kafka publisher
├── order/         Kafka consumer, order state machine, expiry job
├── event/         events + Caffeine-cached metadata
├── common/        RFC 9457 error handling, after-commit hooks
└── config/
src/main/resources/
├── redis/         reserve.lua · release.lua · rate_limit.lua
└── db/migration/  Flyway schema
```

## Known limitations and next steps

- **Undersell after a crash.** If the app dies between committing an expiry to MySQL and releasing the tickets in Redis, Redis shows fewer tickets than really exist. It never shows more, so this can't oversell. A periodic reconciliation job comparing the two stores would fix it.
- **Very hot events.** One event lives on one Redis key and one Kafka partition. For a stadium-tour-scale drop, I'd shard the stock across N keys (`stock:{event:1}:0..N`) and spread orders across partitions.
- **Publish-then-timeout.** If Kafka stores the message but the acknowledgment times out, the API returns tickets that were actually sold. MySQL's conditional update still prevents overselling. A transactional outbox would close this gap.
- **Auth and payments are stubbed** to keep the focus on concurrency.

## License

MIT
