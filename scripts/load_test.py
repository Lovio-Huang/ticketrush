#!/usr/bin/env python3
"""Flash-sale load test for ticketrush. Standard library only.

Creates an event with N tickets, then has M distinct users try to buy one
ticket each at the same time. With M > N it is a sell-out race; with M <= N
every request should succeed, which stresses the order pipeline instead.

It reports the two sides of the system separately:

  * the HTTP burst: all requests per second, and latency of the 202s alone
  * the order pipeline: accepted orders per second until all are in MySQL,
    a sample of "202 -> GET /orders/{id} shows it", the peak Kafka backlog
    (ticketrush_orders_queue_lag) and how long it took to drain

and then checks the invariants: exactly min(N, M) accepted, every accepted
order persisted, the expected stock left, and Redis and MySQL in agreement.

Every user has its own JWT. The script signs them itself with the app's HS256
key, the way an identity provider would, so the run doesn't spend minutes on
20,000 BCrypt sign-ups first. The key defaults to $JWT_SECRET, or to the
development key the app falls back to when JWT_SECRET isn't set.

Usage:
  python3 scripts/load_test.py --tickets 500 --users 5000 --concurrency 200      # sell-out
  python3 scripts/load_test.py --tickets 20000 --users 20000 --concurrency 200   # enough for everyone
"""

import argparse
import base64
import hashlib
import hmac
import http.client
import json
import os
import statistics
import sys
import threading
import time
import uuid
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse

_local = threading.local()

# Same fallback as application.yml: only for local runs and CI.
DEV_JWT_SECRET = "46nTHWwbEfvhyRNt4EVzb0fs8bOHvuq7D2Wao7h9qn0="


def b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


def make_token(secret_b64, subject, scope):
    """An HS256 access token shaped like the ones /api/auth/login issues."""
    now = int(time.time())
    header = b64url(json.dumps({"alg": "HS256", "typ": "JWT"}).encode())
    claims = {"iss": "ticketrush", "sub": subject, "iat": now, "exp": now + 3600, "scope": scope}
    payload = b64url(json.dumps(claims).encode())
    signature = hmac.new(base64.b64decode(secret_b64), f"{header}.{payload}".encode(), hashlib.sha256).digest()
    return f"Bearer {header}.{payload}.{b64url(signature)}"


def connection(base):
    # One keep-alive connection per worker thread.
    conn = getattr(_local, "conn", None)
    if conn is None:
        conn = http.client.HTTPConnection(base.hostname, base.port or 80, timeout=30)
        _local.conn = conn
    return conn


def request(base, method, path, body=None, headers=None):
    headers = {"Content-Type": "application/json", **(headers or {})}
    payload = json.dumps(body) if body is not None else None
    for attempt in range(2):
        conn = connection(base)
        try:
            conn.request(method, path, body=payload, headers=headers)
            resp = conn.getresponse()
            data = resp.read()
            return resp.status, json.loads(data) if data else None
        except (http.client.HTTPException, ConnectionError):
            conn.close()
            _local.conn = None
            if attempt == 1:
                raise


def create_event(base, tickets, admin_token):
    now = datetime.now(timezone.utc)
    status, body = request(base, "POST", "/api/events", {
        "name": f"Load test {now:%H:%M:%S}",
        "venue": "Benchmark Arena",
        "startsAt": (now + timedelta(days=30)).isoformat(),
        "saleStartsAt": (now - timedelta(minutes=1)).isoformat(),
        "totalTickets": tickets,
        "priceCents": 5000,
        "perUserLimit": 1,
    }, {"Authorization": admin_token})
    if status != 201:
        sys.exit(f"Could not create event: {status} {body}")
    return body["id"]


def reserve(base, event_id, user, on_accepted):
    started = time.perf_counter()
    status, body = request(base, "POST", f"/api/events/{event_id}/reservations", {"quantity": 1}, {
        "Authorization": user,
        "Idempotency-Key": str(uuid.uuid4()),
    })
    done = time.perf_counter()
    outcome = "accepted" if status == 202 else (body or {}).get("code", f"HTTP_{status}")
    order_id = (body or {}).get("orderId")
    if outcome == "accepted":
        on_accepted(order_id, user, done)
    return outcome, (done - started) * 1000, user, order_id


def percentile(values, pct):
    ordered = sorted(values)
    index = min(len(ordered) - 1, round(pct / 100 * (len(ordered) - 1)))
    return ordered[index]


def summary(values):
    if not values:
        return "n/a"
    return (f"p50 {statistics.median(values):.1f}   p95 {percentile(values, 95):.1f}"
            f"   p99 {percentile(values, 99):.1f}   max {max(values):.1f}")


class VisibilitySampler:
    """Polls every k-th accepted order until GET shows it, timing 202 -> visible.

    Only a sample, polled every POLL_S, so the extra GETs barely load the app.
    The result includes up to one poll interval plus one GET of measurement error.
    """

    POLL_S = 0.02

    def __init__(self, base, every, pool):
        self.base, self.every, self.pool = base, every, pool
        self.seen, self.delays_ms, self.lock = 0, [], threading.Lock()
        self.futures = []

    def on_accepted(self, order_id, user, accepted_at):
        with self.lock:
            self.seen += 1
            if self.seen % self.every:
                return
        self.futures.append(self.pool.submit(self._poll, order_id, user, accepted_at))

    def _poll(self, order_id, user, accepted_at):
        deadline = accepted_at + 120
        while time.perf_counter() < deadline:
            status, body = request(self.base, "GET", f"/api/orders/{order_id}", headers={"Authorization": user})
            if status == 200 and body["status"] != "PROCESSING":
                with self.lock:
                    self.delays_ms.append((time.perf_counter() - accepted_at) * 1000)
                return
            time.sleep(self.POLL_S)


def queue_metrics(base):
    """Consumer lag and messages the consumer has written so far, from the app's
    Prometheus endpoint; None for what it couldn't read."""
    conn = http.client.HTTPConnection(base.hostname, base.port or 80, timeout=10)
    lag = writes = None
    try:
        conn.request("GET", "/actuator/prometheus")
        for line in conn.getresponse().read().decode().splitlines():
            if line.startswith("ticketrush_orders_queue_lag"):
                lag = float(line.rsplit(" ", 1)[1])
            elif line.startswith("ticketrush_orders_consumed_total"):
                writes = float(line.rsplit(" ", 1)[1])
            elif writes is None and line.startswith("ticketrush_orders_write_seconds_count"):
                # Before the batch consumer, one write was one message.
                writes = float(line.rsplit(" ", 1)[1])
    except (OSError, http.client.HTTPException):
        pass
    finally:
        conn.close()
    return lag, writes


class LagWatcher(threading.Thread):
    """Samples the Kafka backlog and the consumer's write count every 200 ms in the background."""

    def __init__(self, base):
        super().__init__(daemon=True)
        self.base, self.samples, self.running = base, [], True

    def run(self):
        while self.running:
            lag, writes = queue_metrics(self.base)
            if lag is not None and lag >= 0:
                self.samples.append((time.perf_counter(), lag, writes))
            time.sleep(0.2)


def persisted_orders(base, event_id, admin_token):
    _, stats = request(base, "GET", f"/api/events/{event_id}/stats", headers={"Authorization": admin_token})
    return stats, sum(totals["orders"] for totals in stats["byStatus"].values())


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--base-url", default="http://localhost:8080")
    parser.add_argument("--tickets", type=int, default=500)
    parser.add_argument("--users", type=int, default=5000)
    parser.add_argument("--concurrency", type=int, default=200)
    parser.add_argument("--jwt-secret", default=os.environ.get("JWT_SECRET", DEV_JWT_SECRET),
                        help="base64 HS256 key the app verifies tokens with (default: $JWT_SECRET or the dev key)")
    parser.add_argument("--visibility-samples", type=int, default=200,
                        help="how many accepted orders to time from 202 until GET shows them")
    parser.add_argument("--json", action="store_true", help="also print a one-line JSON summary")
    args = parser.parse_args()
    base = urlparse(args.base_url)

    admin = make_token(args.jwt_secret, "1", "user admin")
    event_id = create_event(base, args.tickets, admin)
    expected = min(args.tickets, args.users)
    print(f"Event {event_id}: {args.tickets} tickets, {args.users} users, concurrency {args.concurrency}")

    # Tokens are signed before the race starts, so signing isn't part of the measured latency.
    users = [make_token(args.jwt_secret, str(10_000_000 + i), "user") for i in range(args.users)]
    watcher = LagWatcher(base)
    watcher.start()
    with ThreadPoolExecutor(max_workers=20) as poll_pool:
        sampler = VisibilitySampler(base, max(1, expected // args.visibility_samples), poll_pool)
        started = time.perf_counter()
        with ThreadPoolExecutor(max_workers=args.concurrency) as pool:
            results = list(pool.map(lambda u: reserve(base, event_id, u, sampler.on_accepted), users))
        burst_end = time.perf_counter()

        outcomes = Counter(outcome for outcome, *_ in results)
        accepted = sum(1 for outcome, *_ in results if outcome == "accepted")
        # Drain: wait until every accepted order is in MySQL.
        stats, persisted = persisted_orders(base, event_id, admin)
        deadline = time.time() + 120
        while persisted < accepted and time.time() < deadline:
            time.sleep(0.05)
            stats, persisted = persisted_orders(base, event_id, admin)
        all_persisted = time.perf_counter()
        for future in sampler.futures:
            future.result()
    time.sleep(0.5)  # one more lag sample after the drain
    watcher.running = False
    watcher.join()
    stats, persisted = persisted_orders(base, event_id, admin)

    burst_s = burst_end - started
    ok_ms = [ms for outcome, ms, *_ in results if outcome == "accepted"]
    other_ms = [ms for outcome, ms, *_ in results if outcome != "accepted"]
    peak_lag = max((lag for _, lag, _ in watcher.samples), default=float("nan"))
    lag_zero = next((t for t, lag, _ in watcher.samples if t >= burst_end and lag == 0), None)
    # The consumer's own rate while it had a backlog: writes between the first sample
    # with lag and the first one back at 0. Not limited by how fast requests arrived.
    backlog = [(t, w) for t, lag, w in watcher.samples if w is not None and lag > 0]
    end = next(((t, w) for t, lag, w in watcher.samples if w is not None and t >= burst_end and lag == 0), None)
    consumer_rate = ((end[1] - backlog[0][1]) / (end[0] - backlog[0][0])
                     if backlog and end and end[0] > backlog[0][0] else None)

    print(f"\nHTTP burst    {len(results)} requests in {burst_s:.2f}s  ->  {len(results) / burst_s:,.0f} req/s"
          f"  ({accepted / burst_s:,.0f} accepted/s)")
    print(f"Latency (ms)  202 only:  {summary(ok_ms)}")
    print(f"              rejected:  {summary(other_ms)}")
    print("Outcomes      " + ", ".join(f"{k}={v}" for k, v in sorted(outcomes.items())))
    print(f"\nPipeline      {persisted}/{accepted} orders in MySQL {all_persisted - started:.2f}s after the start"
          f"  ->  {persisted / (all_persisted - started):,.0f} orders/s end to end;"
          f" drained {all_persisted - burst_end:.2f}s after the last response")
    print(f"202->visible  (ms, {len(sampler.delays_ms)} sampled)  {summary(sampler.delays_ms)}")
    print(f"Kafka backlog peak {peak_lag:,.0f} messages; lag back to 0 "
          + (f"{lag_zero - burst_end:.2f}s after the last response" if lag_zero else "not observed")
          + (f"; consumer wrote {consumer_rate:,.0f} msgs/s while backlogged" if consumer_rate else ""))

    left = stats["availableInDatabase"]
    print(f"\nLeft on sale  MySQL {left}, Redis {stats['availableInRedis']}, in sync: {stats['storesInSync']}")
    ok = (accepted == expected and persisted == accepted
          and stats["byStatus"].get("PENDING_PAYMENT", {}).get("orders", 0) == expected
          and left == args.tickets - expected and stats["storesInSync"])
    if args.json:
        print(json.dumps({
            "tickets": args.tickets, "users": args.users, "concurrency": args.concurrency,
            "req_per_s": round(len(results) / burst_s), "accepted_per_s": round(accepted / burst_s),
            "orders_per_s_end_to_end": round(persisted / (all_persisted - started)),
            "drain_s": round(all_persisted - burst_end, 2), "peak_lag": peak_lag,
            "lag_zero_s": round(lag_zero - burst_end, 2) if lag_zero else None,
            "consumer_per_s": round(consumer_rate) if consumer_rate else None,
            "ok_p50": statistics.median(ok_ms) if ok_ms else None,
            "ok_p95": percentile(ok_ms, 95) if ok_ms else None,
            "ok_p99": percentile(ok_ms, 99) if ok_ms else None,
            "visible_p50": statistics.median(sampler.delays_ms) if sampler.delays_ms else None,
            "visible_p95": percentile(sampler.delays_ms, 95) if sampler.delays_ms else None,
            "visible_p99": percentile(sampler.delays_ms, 99) if sampler.delays_ms else None,
            "outcomes": outcomes, "pass": ok,
        }))
    if ok:
        print(f"\nPASS: sold exactly {expected} tickets, every order persisted, stores agree")
    else:
        print("\nFAIL: invariants violated")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
