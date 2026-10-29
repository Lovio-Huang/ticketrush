# Benchmark results

Raw output, one folder per label. Two kinds, kept apart:

**Historical runs, snapshot reconstructed afterwards.** These runs saved only
`git diff HEAD` (`code.diff`), which leaves out files git didn't track yet. The
`untracked-sources.tar.gz` next to them was made *after* the runs, from the
working tree; the files' modification times predate each run, which is a hint
that they are the measured versions, not proof. Treat these snapshots as
reconstructed from the diff and the timestamps, and not fully verifiable.

| Label | Driver | Code | Consumer |
|---|---|---|---|
| `baseline` | `scripts/consumer_bench.py` | commit `2e4a4ae` + `code.diff` + reconstructed `untracked-sources.tar.gz` (round 4's reliability work, incl. its timestamp-based late-order rule, since replaced) | one transaction per message |
| `batched` | `scripts/consumer_bench.py` | the same + the batch consumer (the two `code.diff` files differ exactly by the batching change and the load test reading `ticketrush_orders_consumed_total`) | one transaction per poll, at most 200 messages |
| `mixed-batch200` | `run.sh` + `scripts/mixed_load.py` | `code.diff` + reconstructed `untracked-sources.tar.gz`, taken right after the runs; the code changed again afterwards (the atomic restart check, default deny) | the same batch consumer |

**Delivered code, verified snapshot.**

| Label | Driver | Code | Runs |
|---|---|---|---|
| `delivered` | `scripts/consumer_bench.py` | `source.tar.gz`: every file under `src`, `scripts`, `pom.xml`, `compose.yaml`, `Dockerfile`, archived and hashed (`source.sha256`) before the build; `source-check.txt` confirms the hashes still matched after the run | 1 round per scenario: a check that the delivered build behaves like the `batched` runs, not a new measurement of the speed-up |

`baseline` and `batched`: same laptop (`env.txt`), fresh compose stack each
(project `trbench`, ports 18080/13306/16379/19092), one warm-up of 2,000
orders, then 5 rounds of sell-out (2,000 tickets, 20,000 users) and
enough-for-everyone (20,000 / 20,000), concurrency 200, run one after the other
on 2026-10-28. `delivered`: the same setup, one round. Files: `env.txt`
(commit, machine, Docker), `code.diff`, `runs.jsonl` (per run: the load test's
JSON, consumer timer deltas, MySQL statement digests), `run<N>-<scenario>.txt`
(the load test's printout).

```bash
python3 perf/compare.py baseline batched
python3 perf/compare.py batched delivered
```

`mixed-batch200`: 4 rounds of a 20,000-order burst on one event while other
users pay (1/3) and cancel (1/3) each order as soon as it shows up, and the
rest expire (payment window 15 s, expiry scan 1 s: `compose-override.yml`).
`locks.txt` has MySQL's InnoDB lock counters before and after each round. In
rounds 1-3 the script's `pass` flag and `all_settled_s` are wrong (it counted
statuses with 0 orders as unsettled and waited out its 180 s deadline); the
final statuses and everything else in those rounds are right. Round 4 ran with
the fix. Maximum waits in it are the largest seen in these runs, not bounds.
