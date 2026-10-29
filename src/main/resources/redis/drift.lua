-- The reconciler's cheap check, O(1): every script that moves stock moves the
-- held counter by the same amount, so stock + held = total unless something
-- outside the scripts (a hand edit, a bug) broke it. Only then does the
-- reconciler pay for the full recount, which reads the whole orders hash and
-- blocks Redis while it does (tens of ms per 20k orders).
--
-- KEYS[1] stock counter
-- KEYS[2] held counter
-- ARGV[1] total tickets of the event
--
-- Returns 1 if a recount is needed (also when the counter is missing, e.g. a
-- new event or data from before the counter existed), 0 otherwise. A missing
-- stock key is the rebuild's business, not drift: 0.
local stock = redis.call('GET', KEYS[1])
if not stock then
    return 0
end
local held = redis.call('GET', KEYS[2])
if not held or tonumber(stock) + tonumber(held) ~= tonumber(ARGV[1]) then
    return 1
end
return 0
