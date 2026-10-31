-- The consumer's claim, taken before it writes the order to MySQL. Atomic with
-- release.lua, so an order is either released or claimed, never both:
--   * released already ('R')  -> returns -1; the consumer must not give it tickets
--   * held                     -> marks it sent (no not-sent release from now on),
--                                 takes it out of the outbox, returns 0
--   * unknown, first delivery, stock key present -> Redis lost the order and was
--                                 rebuilt without it: its send came after the rebuild's
--                                 barrier (a request that paused before its send), so
--                                 its tickets and its user's limit may be someone
--                                 else's now. Marks it released, returns -1. No clock is
--                                 consulted: any Redis restart is turned into a full
--                                 loss and a rebuild (RedisRestartGuard), so an unknown
--                                 order here is always older than the current state.
--   * unknown otherwise (a rebuild is still running, or MySQL already has the
--                                 order) -> holds it now, restores its idempotency key,
--                                 counts its tickets, returns 1
--
-- KEYS[1] stock counter
-- KEYS[2] buyers hash
-- KEYS[3] orders hash
-- KEYS[4] outbox
-- KEYS[5] sent set
-- KEYS[6] held counter
-- KEYS[7] idempotency key of the order
-- ARGV[1] orderId
-- ARGV[2] order JSON
-- ARGV[3] seconds left on the idempotency key (0 = don't restore it)
-- ARGV[4] 'new' if MySQL doesn't have the order yet, '' for a stored order's redelivery

local current = redis.call('HGET', KEYS[3], ARGV[1])
if current == 'R' then
    return -1
end
if not current and ARGV[4] == 'new' and redis.call('EXISTS', KEYS[1]) == 1 then
    redis.call('HSET', KEYS[3], ARGV[1], 'R')
    redis.call('ZREM', KEYS[4], ARGV[1])
    return -1
end
redis.call('SADD', KEYS[5], ARGV[1])
redis.call('ZREM', KEYS[4], ARGV[1])
if current then
    return 0
end

redis.call('HSET', KEYS[3], ARGV[1], ARGV[2])
if tonumber(ARGV[3]) > 0 then
    redis.call('SET', KEYS[7], ARGV[1] .. '|' .. cjson.decode(ARGV[2]).quantity, 'NX', 'EX', tonumber(ARGV[3]))
end
-- No stock key means a rebuild is running; its final recount counts this order.
if redis.call('EXISTS', KEYS[1]) == 1 then
    local order = cjson.decode(ARGV[2])
    -- May go below zero if Redis already resold these tickets; MySQL then rejects the extra order.
    redis.call('DECRBY', KEYS[1], order.quantity)
    redis.call('INCRBY', KEYS[6], order.quantity)
    redis.call('HINCRBY', KEYS[2], order.userId, order.quantity)
end
return 1
