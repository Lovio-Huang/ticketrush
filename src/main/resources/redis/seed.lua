-- Rebuild after Redis lost an event's data: puts orders known to MySQL back into
-- the orders hash and restores their idempotency keys. Entries already present
-- win (HSETNX / SET NX), so an order confirmed or released during the rebuild
-- keeps its newer state.
--
-- KEYS[1]   orders hash
-- KEYS[1+i] idempotency key of order i
-- ARGV      per order: orderId, value (order JSON if active, 'R' otherwise), idempotency TTL in seconds
for i = 1, #KEYS - 1 do
    local id, value, ttl = ARGV[3 * i - 2], ARGV[3 * i - 1], tonumber(ARGV[3 * i])
    redis.call('HSETNX', KEYS[1], id, value)
    if ttl > 0 then
        redis.call('SET', KEYS[1 + i], id, 'NX', 'EX', ttl)
    end
end
return #KEYS - 1
