#!/usr/bin/env python3
"""Flash-sale load test for ticketrush. Standard library only.

Creates an event with N tickets, then has M distinct users try to buy one
ticket each at the same time. Afterwards it checks the invariants that matter:

  * exactly N reservations were accepted (no overselling, no underselling)
  * every accepted order was persisted to MySQL by the Kafka consumer
  * the event shows 0 tickets left

Every user has its own JWT. The script signs them itself with the app's HS256
key, the way an identity provider would, so the run doesn't spend minutes on
20,000 BCrypt sign-ups first. The key defaults to $JWT_SECRET, or to the
development key the app falls back to when JWT_SECRET isn't set.

Usage:
  python3 scripts/load_test.py --tickets 500 --users 5000 --concurrency 200
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


def reserve(base, event_id, user):
    started = time.perf_counter()
    status, body = request(base, "POST", f"/api/events/{event_id}/reservations", {"quantity": 1}, {
        "Authorization": user,
        "Idempotency-Key": str(uuid.uuid4()),
    })
    elapsed_ms = (time.perf_counter() - started) * 1000
    outcome = "accepted" if status == 202 else (body or {}).get("code", f"HTTP_{status}")
    return outcome, elapsed_ms, user, (body or {}).get("orderId")


def percentile(values, pct):
    ordered = sorted(values)
    index = min(len(ordered) - 1, round(pct / 100 * (len(ordered) - 1)))
    return ordered[index]


def wait_for_orders(base, accepted, timeout_s=60):
    """Polls every accepted order until the consumer has written it to MySQL."""
    deadline = time.time() + timeout_s
    pending = dict(accepted)
    statuses = Counter()
    while pending and time.time() < deadline:
        for order_id, user in list(pending.items()):
            status, body = request(base, "GET", f"/api/orders/{order_id}", headers={"Authorization": user})
            if status == 200 and body["status"] != "PROCESSING":
                statuses[body["status"]] += 1
                del pending[order_id]
        if pending:
            time.sleep(0.5)
    return statuses, len(pending)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--base-url", default="http://localhost:8080")
    parser.add_argument("--tickets", type=int, default=500)
    parser.add_argument("--users", type=int, default=5000)
    parser.add_argument("--concurrency", type=int, default=200)
    parser.add_argument("--jwt-secret", default=os.environ.get("JWT_SECRET", DEV_JWT_SECRET),
                        help="base64 HS256 key the app verifies tokens with (default: $JWT_SECRET or the dev key)")
    args = parser.parse_args()
    base = urlparse(args.base_url)

    event_id = create_event(base, args.tickets, make_token(args.jwt_secret, "1", "user admin"))
    print(f"Event {event_id}: {args.tickets} tickets, {args.users} users, concurrency {args.concurrency}")

    # Tokens are signed before the race starts, so signing isn't part of the measured latency.
    users = [make_token(args.jwt_secret, str(10_000_000 + i), "user") for i in range(args.users)]
    started = time.perf_counter()
    with ThreadPoolExecutor(max_workers=args.concurrency) as pool:
        results = list(pool.map(lambda u: reserve(base, event_id, u), users))
    wall_s = time.perf_counter() - started

    outcomes = Counter(outcome for outcome, *_ in results)
    latencies = [ms for _, ms, *_ in results]
    accepted = {order_id: user for outcome, _, user, order_id in results if outcome == "accepted"}

    print(f"\nRequests      {len(results)} in {wall_s:.2f}s  ->  {len(results) / wall_s:,.0f} req/s")
    print(f"Latency (ms)  p50 {statistics.median(latencies):.1f}   p95 {percentile(latencies, 95):.1f}"
          f"   p99 {percentile(latencies, 99):.1f}   max {max(latencies):.1f}")
    print("Outcomes      " + ", ".join(f"{k}={v}" for k, v in sorted(outcomes.items())))

    statuses, missing = wait_for_orders(base, accepted)
    _, event = request(base, "GET", f"/api/events/{event_id}")
    print(f"Persisted     {dict(statuses)}" + (f", {missing} never persisted" if missing else ""))
    print(f"Left on sale  {event['availableTickets']}")

    ok = (len(accepted) == args.tickets and missing == 0
          and statuses.get("PENDING_PAYMENT", 0) == args.tickets and event["availableTickets"] == 0)
    if ok:
        print(f"\nPASS: sold exactly {args.tickets} tickets, no oversell")
    else:
        print("\nFAIL: invariants violated")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
