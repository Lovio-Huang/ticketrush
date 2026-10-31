-- Rebuild after Redis lost an event's data: puts orders known to MySQL back into
-- the orders hash and restores their idempotency keys. Entries already present
-- win (HSETNX / SET NX), so an order claimed or released during the rebuild
-- keeps its newer state.
--
-- Fenced: runs only while KEYS[1] still holds this rebuilder's token (and renews
-- it), so a rebuilder whose lease expired, or whose Redis was wiped again, can't
-- write into someone else's rebuild.
--
-- KEYS[1]   rebuild lease
-- KEYS[2]   orders hash
-- KEYS[2+i] idempotency key of order i
-- ARGV[1]   token, ARGV[2] lease in ms
-- ARGV      then per order: orderId, value (order JSON if active, 'R' otherwise), idempotency TTL in seconds,
--           quantity (the idempotency key records it: a retry with another quantity is a conflict)
--
-- Returns the number of orders seeded, or -1 if fenced off.
if redis.call('GET', KEYS[1]) ~= ARGV[1] then
    return -1
end
redis.call('PEXPIRE', KEYS[1], tonumber(ARGV[2]))
for i = 1, #KEYS - 2 do
    local base = 4 * i - 1
    local id, value, ttl, qty = ARGV[base], ARGV[base + 1], tonumber(ARGV[base + 2]), ARGV[base + 3]
    redis.call('HSETNX', KEYS[2], id, value)
    if ttl > 0 then
        redis.call('SET', KEYS[2 + i], id .. '|' .. qty, 'NX', 'EX', ttl)
    end
end
return #KEYS - 2
