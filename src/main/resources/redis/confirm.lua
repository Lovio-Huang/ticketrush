-- Called once MySQL holds an active order. Kafka has delivered it, so it leaves
-- the outbox. If Redis has never seen the order (it lost its data while the
-- order was in flight), the order is held now so Redis counts its tickets.
-- A released order ('R') stays released.
--
-- KEYS[1] stock counter
-- KEYS[2] buyers hash
-- KEYS[3] orders hash
-- KEYS[4] outbox
-- KEYS[5] idempotency key of the order
-- ARGV[1] orderId
-- ARGV[2] order JSON
-- ARGV[3] seconds left on the idempotency key (0 = don't restore it)
--
-- Returns 1 if the order was newly held, 0 if Redis already knew it.

redis.call('ZREM', KEYS[4], ARGV[1])
if redis.call('HSETNX', KEYS[3], ARGV[1], ARGV[2]) == 0 then
    return 0
end
if tonumber(ARGV[3]) > 0 then
    redis.call('SET', KEYS[5], ARGV[1], 'NX', 'EX', tonumber(ARGV[3]))
end
if redis.call('EXISTS', KEYS[1]) == 1 then
    local order = cjson.decode(ARGV[2])
    -- May go below zero if Redis already resold these tickets; MySQL then rejects the extra order.
    redis.call('DECRBY', KEYS[1], order.quantity)
    redis.call('HINCRBY', KEYS[2], order.userId, order.quantity)
end
return 1
