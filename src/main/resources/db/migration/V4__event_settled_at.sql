-- Lets the background jobs (outbox relay, reconciler) stop visiting an event once
-- nothing about it can still need them, instead of visiting every past event forever.
--
-- settled_at:     set once the sale is over and a reconciler run found nothing left
--                 to do; the jobs skip the event from then on.
-- upkeep_version: bumped, with settled_at cleared, by every write that may need the
--                 jobs later (a stock change, a rejected order, a dead letter, a void).
--                 The reconciler settles only if the version is still the one it saw
--                 before checking, so a write that lands in between keeps the event in.
ALTER TABLE events
    ADD COLUMN settled_at DATETIME(6) NULL,
    ADD COLUMN upkeep_version BIGINT NOT NULL DEFAULT 0,
    ADD KEY idx_events_upkeep (settled_at, sale_starts_at);
