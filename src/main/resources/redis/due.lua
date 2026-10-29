-- Orders that have sat in the outbox longer than ARGV[1] ms: Kafka may never
-- have stored them (an ack that timed out, a crash before the send). Returns
-- {orderId, order JSON, ...} for the relay to publish again. Entries whose order
-- was released meanwhile are dropped.
--
-- KEYS[1] outbox
-- KEYS[2] orders hash
-- ARGV[1] min age in ms
-- ARGV[2] max orders to return
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local ids = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', now - tonumber(ARGV[1]), 'LIMIT', 0, tonumber(ARGV[2]))
local due = {}
for _, id in ipairs(ids) do
    local order = redis.call('HGET', KEYS[2], id)
    if order and order ~= 'R' then
        due[#due + 1] = id
        due[#due + 1] = order
    else
        redis.call('ZREM', KEYS[1], id)
    end
end
return due
