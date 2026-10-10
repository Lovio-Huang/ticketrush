-- Atomically reserves tickets for one user. Runs as a single Redis command, so
-- concurrent requests can never observe a half-applied reservation.
--
-- KEYS[1] stock counter      ticketrush:{event:<id>}:stock
-- KEYS[2] buyers hash        ticketrush:{event:<id>}:buyers   (userId -> tickets held)
-- KEYS[3] idempotency key    ticketrush:{event:<id>}:idem:<userId>:<key>
-- ARGV[1] userId
-- ARGV[2] quantity
-- ARGV[3] per-user limit
-- ARGV[4] new order id
-- ARGV[5] idempotency key TTL in seconds
--
-- Returns {outcome, orderId}

local existing = redis.call('GET', KEYS[3])
if existing then
    return {'DUPLICATE', existing}
end

local stock = redis.call('GET', KEYS[1])
if not stock then
    return {'NOT_INITIALIZED', ''}
end

local qty = tonumber(ARGV[2])
if tonumber(stock) < qty then
    return {'SOLD_OUT', ''}
end

local held = tonumber(redis.call('HGET', KEYS[2], ARGV[1]) or '0')
if held + qty > tonumber(ARGV[3]) then
    return {'LIMIT_EXCEEDED', ''}
end

redis.call('DECRBY', KEYS[1], qty)
redis.call('HINCRBY', KEYS[2], ARGV[1], qty)
redis.call('SET', KEYS[3], ARGV[4], 'EX', tonumber(ARGV[5]))
return {'RESERVED', ARGV[4]}
