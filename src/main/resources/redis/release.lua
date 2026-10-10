-- Returns previously reserved tickets to the pool (order expired, cancelled,
-- or the reservation could not be handed off to Kafka).
--
-- KEYS[1] stock counter
-- KEYS[2] buyers hash
-- KEYS[3] (optional) idempotency key to forget, so the client can retry
-- ARGV[1] userId
-- ARGV[2] quantity

local qty = tonumber(ARGV[2])
redis.call('INCRBY', KEYS[1], qty)

local held = redis.call('HINCRBY', KEYS[2], ARGV[1], -qty)
if held <= 0 then
    redis.call('HDEL', KEYS[2], ARGV[1])
end

if KEYS[3] then
    redis.call('DEL', KEYS[3])
end
return 1
