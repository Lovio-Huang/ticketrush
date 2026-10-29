-- Returns an order's tickets to the pool (expired, cancelled, rejected by MySQL,
-- or never handed to Kafka). Keyed by order id, so it is idempotent: the first
-- call flips the order to 'R' and every later call (a retry, the reconciler, a
-- redelivered message) finds 'R' and changes nothing. The 'R' marker lives in
-- the same hash as the event's other state and never expires on its own, so it
-- can't lapse while a retry is still possible.
--
-- KEYS[1] stock counter
-- KEYS[2] buyers hash
-- KEYS[3] orders hash
-- KEYS[4] outbox
-- KEYS[5] (optional) idempotency key to forget, so the client can retry
-- ARGV[1] orderId
--
-- Returns 1 if tickets were returned, 0 if there was nothing to release.

local held = redis.call('HGET', KEYS[3], ARGV[1])
redis.call('HSET', KEYS[3], ARGV[1], 'R')
redis.call('ZREM', KEYS[4], ARGV[1])
if KEYS[5] and redis.call('GET', KEYS[5]) == ARGV[1] then
    redis.call('DEL', KEYS[5])
end

if not held or held == 'R' then
    return 0
end
-- No stock key means Redis is being rebuilt; the rebuild recounts from the orders hash.
if redis.call('EXISTS', KEYS[1]) == 0 then
    return 0
end

local order = cjson.decode(held)
redis.call('INCRBY', KEYS[1], order.quantity)
local left = redis.call('HINCRBY', KEYS[2], order.userId, -order.quantity)
if left <= 0 then
    redis.call('HDEL', KEYS[2], order.userId)
end
return 1
