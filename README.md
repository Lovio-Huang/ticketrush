# TicketRush

[![CI](https://github.com/Bastardki11er/ticketrush/actions/workflows/ci.yml/badge.svg)](https://github.com/Bastardki11er/ticketrush/actions/workflows/ci.yml)
![Java 21](https://img.shields.io/badge/Java-21-orange)
![Spring Boot 4](https://img.shields.io/badge/Spring%20Boot-4.1-brightgreen)

A ticketing backend built for **flash sales**: thousands of people hit "buy" the
second tickets go on sale, and the system has to sell **exactly** the tickets it
has. No overselling, no lost orders, and response times stay flat.

**Stack:** Java 21 (virtual threads) · Spring Boot 4 · Spring Security (JWT) · MySQL 8.4 · Redis (Lua scripts) · Kafka 4 (KRaft) · Flyway · Testcontainers · Docker Compose · GitHub Actions · Micrometer/Prometheus

## Results

[`scripts/load_test.py`](scripts/load_test.py) against the Docker Compose stack: 20,000 signed-in users (each request carries a JWT the app verifies), 200 in flight at a time, one ticket each. Two scenarios: a **sell-out** (2,000 tickets) and **enough for everyone** (20,000 tickets, so every request becomes an order). Medians of 6 runs per scenario, ranges in brackets.

| | Sell-out: 2,000 tickets | Enough for everyone: 20,000 tickets |
|---|---|---|
| HTTP requests/s (all answers) | **8,800** [6,700 – 10,200] | **8,100** [6,600 – 8,600] |
| Accepted (202) per second during the burst | 880 | 8,100 |
| Latency of 202 answers | p50 26 ms · p99 111 ms | p50 23 ms · p99 59 ms |
| Orders in MySQL per second, start to last insert | 450 | **930** |
| Kafka backlog peak (consumer lag) | ~1,960 | ~19,940 |
| Backlog drained after the last response | 2.1 s | 19.1 s |
| 202 → `GET /orders/{id}` shows the order (sampled) | p50 3.1 s · p99 4.1 s | p50 10.7 s · p99 18.9 s |
| Correctness, every run | exactly 2,000 sold, 2,000 persisted, stores agree | 20,000 sold and persisted, stores agree |

What this says: the HTTP side answers at ~8–9k requests/s with flat latency, but the order writer persists ~1,000 orders/s, because all orders for one event share one Kafka partition and one consumer thread, with one MySQL transaction each. In a sell-out that hardly matters (2,000 orders drain in ~2 s). When nearly every request succeeds, the backlog is the whole burst, and an order takes up to ~20 s to show up; until then `GET` says `PROCESSING`. Batching the consumer's inserts is the obvious next step.

The recovery work cost little on the hot path. Same script, same machine, run alternately against the previous commit: requests/s −4% (sell-out) and −7% (enough for everyone), within run-to-run noise for the sell-out. The consumer is ~10% slower (931 vs 1,039 orders/s, one more Redis call per order to confirm it).

> Measured on one Apple M5 laptop (10 cores, 24 GB, macOS 27), Docker Desktop 4.94 with
> 10 CPUs and 8 GB for the VM, running the app, MySQL, Redis (AOF on), Kafka *and* the load
> generator together. The script also polls order status, the stats endpoint and consumer lag
> while it runs, which costs some throughput (the earlier, simpler script saw ~10k requests/s).
> Treat these as a single-machine lower bound, not a production benchmark. CI runs a smaller
> version of both scenarios on every push and fails on a single oversold or lost order.

## Architecture

```mermaid
flowchart LR
    C([Client]) -->|"POST /reservations<br/>Idempotency-Key"| API[Spring Boot API]
    API -->|"① rate_limit.lua<br/>② reserve.lua: stock, per-user count,<br/>idempotency key, order + outbox entry"| R[(Redis)]
    API -->|"③ OrderRequested<br/>acks=all"| K[[Kafka<br/>ticket-orders]]
    API -->|"202 Accepted + orderId"| C
    K --> W[Order consumer]
    W -->|"④ conditional UPDATE<br/>+ INSERT order"| DB[(MySQL)]
    W -->|"confirm.lua / release.lua"| R
    O[Outbox relay] -->|"resend unconfirmed orders"| K
    O -.-> R
    J[Expiry job] -->|"FOR UPDATE SKIP LOCKED"| DB
    J -->|"release.lua (by order id)"| R
    X[Reconciler] -.->|"release finished orders,<br/>recount from held orders"| R
    C -.->|"GET /orders/{id} (poll)"| API
```

A reservation never waits on MySQL:

1. **Rate limit.** A per-user token bucket in Redis rejects bots before they cost anything.
2. **Reserve in Redis.** One Lua script checks the idempotency key, the stock and the per-user limit, then decrements the stock and records the order itself (the exact Kafka message) as *held* and *unconfirmed*. Redis runs it as a single command, so concurrent buyers can't interleave.
3. **Queue the order.** The order goes to Kafka. Normally the client gets `202 Accepted` after the broker acknowledges it (`acks=all`). If the producer fails before the record is even queued, the tickets go straight back (`503`). If the ack just doesn't come back, the outcome is unknown: the tickets stay held, the client still gets its `202`, and the outbox relay resends the same message until Kafka confirms it.
4. **Persist.** A consumer writes the order to MySQL with a conditional `UPDATE ... WHERE available_tickets >= ?`, then confirms it in Redis. MySQL is the source of truth for orders; Redis is the fast, self-consistent copy the hot path reads.
5. **Pay or expire.** The order waits 10 minutes for payment. After that, a scheduled job expires it and puts the tickets back on sale in both stores. Releases are keyed by order id, so they happen at most once.

## Design decisions

| Problem | Approach | Trade-off / why |
|---|---|---|
| Overselling under concurrency | Atomic check-and-decrement in a **Redis Lua script** | Locking a MySQL row (`SELECT ... FOR UPDATE`) makes every buyer queue on one row. Optimistic locking under contention means retry storms. Redis does ~100k ops/s on a single thread with no locks. |
| What if Redis is wrong? | **Defense in depth**: a conditional `UPDATE` plus a `CHECK (available_tickets >= 0)` constraint in MySQL | If Redis drifts (restart, bad manual fix), the order is `REJECTED` and nothing is oversold. Covered by the `mysqlStillPreventsOversellingWhenRedisIsOutOfSync` test. |
| Kafka ack times out, or the app dies right after reserving | **The Redis order record doubles as an outbox**; a relay resends unconfirmed orders under the same order id | A transactional outbox in MySQL would put a database write on every reservation, the one thing the hot path avoids. The order is already recorded atomically with its reservation in Redis, so the only new hot-path cost is one `ZREM` after the ack. The price: an unconfirmed order is only as durable as Redis (AOF, fsync every second). |
| Releasing the same tickets twice | **Release by order id**: the script flips the order to `R` and only the first call returns tickets | Cancel, expiry, a rejected order, the reconciler and retries can all try to release the same order; only one wins. The marker lives with the event's other keys and has no TTL of its own, so it can't lapse while a retry is still possible. |
| Slow DB writes during the spike | **Async persistence via Kafka** | The client polls `GET /orders/{id}` instead of getting the order back right away (eventual consistency), but latency no longer depends on MySQL. |
| Client retries / double clicks | **`Idempotency-Key` header**, stored atomically with the reservation | A retry returns the original order (`Idempotent-Replayed: true`) instead of buying twice. |
| Kafka delivers at least once | **Idempotent consumer**: check by order id, with the primary key as backstop | A redelivery of the *same* order is a no-op (it only re-syncs Redis). A constraint error is skipped only if the stored row is that exact order; anything else (a foreign key, the same idempotency key under another order id, the same id with different details) is retried and then dead-lettered instead of silently dropped. |
| Ordering and contention in MySQL | Kafka messages **keyed by event id** | All writes for one event go through a single partition and consumer, so they never contend on the event row. The cost: one very hot event is limited to one partition's throughput (see next steps). |
| Paying vs expiring at the same moment | **Optimistic locking** (`@Version`) on orders | Whichever transaction commits second gets `409 CONCURRENT_MODIFICATION` instead of silently overwriting the first. |
| Running the expiry job on many instances | `SELECT ... FOR UPDATE SKIP LOCKED` in small batches | Instances split the work instead of blocking each other. No leader election needed. |
| Rate limiting across instances | Token bucket in Redis using the **Redis server clock** (`TIME`) | Limits hold cluster-wide and don't depend on app hosts having synchronized clocks. |
| Cache stampede when a sale opens | `@Cacheable(sync = true)` on event metadata | Hundreds of simultaneous first requests trigger one MySQL load instead of hundreds (measured: 50 concurrent misses dropped to 1). |
| Thread-per-request blocking I/O | **Java 21 virtual threads** (`spring.threads.virtual.enabled`) | Plain blocking code that still scales to thousands of concurrent requests. |

## API

Interactive docs: [`/swagger-ui.html`](http://localhost:8080/swagger-ui.html). Authenticated routes take `Authorization: Bearer <token>`.

| Method | Path | Who | Description |
|---|---|---|---|
| `POST` | `/api/auth/register` | anyone | Create an account (username is case-insensitive, password 8 chars to 72 bytes) |
| `POST` | `/api/auth/login` | anyone | Returns a 1-hour HS256 access token |
| `GET` | `/api/auth/me` | user | Who the token belongs to |
| `GET` | `/api/events` | anyone | Upcoming events with live availability, paged |
| `GET` | `/api/events/{id}` | anyone | Event details with live availability |
| `POST` | `/api/events` | admin | Create an event and load its stock into Redis |
| `GET` | `/api/events/{id}/stats` | admin | Tickets sold, revenue, totals per order status, and whether Redis and MySQL agree |
| `POST` | `/api/events/{id}/reservations` | user | Reserve tickets. Header `Idempotency-Key`. Returns `202` |
| `GET` | `/api/orders` | user | My orders, newest first. `?status=PAID&page=0&size=20` |
| `GET` | `/api/orders/{id}` | user | Order status: `PROCESSING` → `PENDING_PAYMENT` → `PAID` / `EXPIRED` / `CANCELLED` |
| `POST` | `/api/orders/{id}/payment` | user | Simulated payment |
| `POST` | `/api/orders/{id}/cancellation` | user | Cancel and release the tickets |
| `GET` | `/actuator/prometheus` | anyone | Metrics, including `ticketrush_reservations_total{outcome=...}` |

Errors use [RFC 9457](https://www.rfc-editor.org/rfc/rfc9457) problem details with a stable `code`, including the ones Spring itself raises (`INVALID_REQUEST`, `UNAUTHORIZED`, `FORBIDDEN`):

```json
{ "status": 409, "code": "SOLD_OUT", "title": "SOLD_OUT", "detail": "No tickets left" }
```

**Auth.** The app signs its own JWTs and checks the signature, issuer and expiry on every request; the `scope` claim carries `user` or `user admin`. Passwords are BCrypt hashes. Logins are throttled to 5 attempts per minute per username, and sign-ups and logins to 30 per minute per client address, both with the Redis token bucket. Wrong passwords and unknown users get the same answer.

`503` answers are safe to retry with the same `Idempotency-Key`: `ORDER_QUEUE_UNAVAILABLE` (Kafka unreachable or not confirming orders) and `STOCK_RECOVERING` (Redis is being rebuilt, see below).

## Consistency and recovery

Redis and MySQL never hold the same number: between the Redis reservation and the consumer's insert, an order exists only in Redis and Kafka. So instead of comparing stock counts, the system keeps three invariants and repairs only what breaks them.

| Invariant | Where it is kept |
|---|---|
| **I1** Redis: `stock + tickets of held orders = total`, and each user's count = the tickets they hold | Every Redis script changes stock, per-user counts and the order record together, atomically |
| **I2** MySQL: `available + tickets of PENDING_PAYMENT/PAID orders = total` | Each transaction changes the stock row and the order row together |
| **I3** Orders Redis holds = orders active in MySQL + orders in flight | The consumer confirms or releases each order after it commits; the reconciler applies the releases that got lost |

Each event's Redis state: `stock`, `buyers` (user → tickets), `idem:<user>:<key>` → order id, `orders` (order id → the order message while held, `R` once released) and `outbox` (orders Kafka hasn't confirmed, scored by accept time).

| Failure | What happens |
|---|---|
| Kafka ack times out (message may or may not be stored) | Tickets stay held, client gets `202`. The relay resends after 10 s under the same order id; the consumer keeps the first copy. The order leaves the outbox when Kafka acks or the consumer stores it. |
| Producer fails before queueing the record (brokers unreachable for `max.block.ms`) | Definitely not sent: tickets released, idempotency key forgotten, `503`. |
| Redis ran the reservation but the reply was lost, the client retries | The retry gets the original order id (`Idempotent-Replayed: true`); the relay publishes the order, which was waiting in the outbox. |
| App crashes between reserving and publishing | Same: the outbox entry is resent by the relay. |
| An order sits unconfirmed for 30 s | New reservations for that event get `503 ORDER_QUEUE_UNAVAILABLE` until Kafka catches up, instead of piling up orders only Redis knows about. |
| Kafka redelivers a message, or two consumers race on it | One order row; Redis is re-synced idempotently. |
| App dies after MySQL commits a cancel/expiry, before Redis hears of it | The reconciler (every 30 s) finds orders Redis still holds that MySQL has finished and releases them, at most once. |
| Someone edits the stock key, or a bug breaks I1 | The reconciler recounts stock and per-user counts from the held orders in one Lua call. It never copies MySQL's number, which would put orders in flight on sale twice. |
| Redis loses an event's data (restart without AOF, flush) | Reservations get `503 STOCK_RECOVERING`. Once the order topic is drained (consumer lag 0, so every accepted order is in MySQL), one instance rebuilds from MySQL: held orders, released markers, per-user counts and idempotency keys, then recounts stock. Orders consumed meanwhile are confirmed into the same hash, so none is lost. |

Every row has an integration test in [`ConsistencyIntegrationTest`](src/test/java/com/yitaihuang/ticketrush/reservation/ConsistencyIntegrationTest.java) that drives the failure through real Redis, Kafka and MySQL and then checks I1–I3, the outbox and every idempotency key together.

## Run it

You need Docker.

```bash
docker compose up --build -d
```

Ports bind to `127.0.0.1` and can be moved if something already uses them, e.g. `APP_PORT=18080 MYSQL_PORT=13306 docker compose up -d` (also `REDIS_PORT`, `KAFKA_PORT`). Compose creates an admin account `admin` / `ticketrush-admin`; set `ADMIN_PASSWORD` to change it, and `JWT_SECRET` (base64, 32+ bytes) for anything beyond a local demo.

Open [localhost:8080](http://localhost:8080) for a small console: sign in, browse events, buy, pay, see your orders and, as admin, the sales stats.

```bash
login() { curl -s localhost:8080/api/auth/login -H 'Content-Type: application/json' \
  -d "{\"username\":\"$1\",\"password\":\"$2\"}" | python3 -c 'import json,sys; print(json.load(sys.stdin)["accessToken"])'; }
ADMIN=$(login admin ticketrush-admin)

# create an event whose sale is already open
curl -X POST localhost:8080/api/events -H "Authorization: Bearer $ADMIN" -H 'Content-Type: application/json' -d '{
  "name": "Demo Concert", "venue": "SAP Center",
  "startsAt": "2027-06-01T02:00:00Z", "saleStartsAt": "2026-01-01T00:00:00Z",
  "totalTickets": 100, "priceCents": 4999, "perUserLimit": 2 }'

# sign up, then reserve two tickets
curl -X POST localhost:8080/api/auth/register -H 'Content-Type: application/json' \
  -d '{"username": "alice", "password": "correct horse"}'
ALICE=$(login alice 'correct horse')
curl -i -X POST localhost:8080/api/events/1/reservations -H "Authorization: Bearer $ALICE" \
  -H 'Content-Type: application/json' -H 'Idempotency-Key: 7f1c' \
  -d '{"quantity": 2}'

# run the flash-sale load test (signs its own tokens with the dev secret, or --jwt-secret)
python3 scripts/load_test.py --tickets 500 --users 5000 --concurrency 200
```

`docker compose --profile observability up -d` also starts Prometheus on `localhost:9090`.

## Tests

```bash
./mvnw verify
```

| Layer | What it proves |
|---|---|
| Unit tests (`OrderTest`, `ReservationServiceTest`, `OrderRequestPublisherTest`, `StockReconcilerTest`) | The order state machine; "not sent" vs "maybe stored" Kafka results and what each does to the tickets; duplicates aren't republished; a missing Redis state is rebuilt or refused; the reconciler only releases orders MySQL has finished and never copies MySQL's stock |
| Integration tests (Testcontainers: real MySQL, Redis and Kafka) | **1,000 concurrent buyers for 100 tickets sell exactly 100**; the reconciler running non-stop during a sale with cancellations never sells a ticket twice; every failure window in the table above; per-user limits hold under parallel requests; idempotent replays; expiry and cancellation restore stock in both stores; MySQL catches Redis drift; forged, expired and foreign-issuer tokens are rejected; login guessing is throttled; non-admins can't create events or read stats; order history paging and filters |
| CI load test (Docker Compose) | The real containerized stack sells exactly N tickets under load, and persists every order when there is enough for everyone |

66 tests in all. The integration tests skip themselves on machines without Docker. CI always runs them.

## Project layout

```
src/main/java/com/yitaihuang/ticketrush/
├── auth/          users, BCrypt, JWT issuing, admin bootstrap
├── reservation/   hot path: ReservationService, StockGateway (Lua), RateLimiter, Kafka publisher;
│                  OrderOutboxRelay, StockReconciler, StockRecovery
├── order/         Kafka consumer, order state machine, expiry job, history, sales stats
├── event/         events + Caffeine-cached metadata
├── common/        RFC 9457 error handling, after-commit hooks
└── config/        security (JWT, CSP), OpenAPI
src/main/resources/
├── redis/         reserve · release · confirm · due (outbox) · seed + recount (rebuild, repair) · rate_limit
├── static/        demo console
└── db/migration/  Flyway schema
```

## Known limitations and next steps

- **Very hot events.** One event lives on one Redis key and one Kafka partition. For a stadium-tour-scale drop, I'd shard the stock across N keys (`stock:{event:1}:0..N`) and spread orders across partitions.
- **An unconfirmed order is only as durable as Redis.** Orders Kafka acknowledged are safe. One that was accepted on an unknown ack, or during a crash, lives in the Redis outbox until it is resent; with AOF `everysec` a Redis crash can lose the last second of those. A MySQL outbox would close this, at the cost of a database write per reservation.
- **Dead-lettered orders need a human.** If a message keeps failing (say, an idempotency-key clash after Redis lost data mid-flight), it goes to `ticket-orders.DLT` and its tickets stay held in Redis until someone decides. Nothing is oversold, but those tickets don't sell.
- **One stuck order pauses its event.** An order Kafka won't confirm for 30 s stops new reservations for that event. That's deliberate (it bounds how much only Redis knows), but there's no automatic give-up yet.
- **A narrow resend race.** If the relay resends an order while the original request thread is still deciding the send failed before queueing, the order can be stored and released. The relay waits 10 s while that decision takes at most `max.block.ms` (1 s), and MySQL still prevents overselling; the order then simply expires unpaid.
- **Recount cost grows with orders.** The repair reads an event's whole orders hash in one Lua call every 30 s. Fine for tens of thousands of orders per event; a stadium-scale event would want the hash sharded or the recount done incrementally.
- **Per-address limits behind a proxy.** The sign-up and login limits key on the connecting address. Behind a load balancer every client shares one address, so the app would need to trust `X-Forwarded-For` from that proxy (`server.forward-headers-strategy`).
- **One shared signing key.** HS256 means whoever verifies tokens can also mint them. With more services I'd move to RS256 with a JWKS endpoint, or an identity provider.
- **Role changes wait for the token to expire.** Tokens are stateless, so revoking admin takes up to the 1-hour TTL. Short tokens plus refresh tokens would shrink that.
- **Payments are simulated** to keep the focus on concurrency.

## License

MIT
