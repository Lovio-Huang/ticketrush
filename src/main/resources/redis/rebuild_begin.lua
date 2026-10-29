-- Decides whether this caller may rebuild an event's Redis state now.
--
-- The first caller after the loss records when the loss was noticed. Rebuilding
-- waits a quiet period from then: sends that started before the loss (requests
-- in flight, a relay mid-batch, on any instance) either reach Kafka within the
-- producer's delivery timeout or never will, so after the quiet period plus
-- consumer lag 0 they are all in MySQL. Then one caller at a time gets a lease
-- identified by its token.
--
-- KEYS[1] stock counter
-- KEYS[2] rebuild lease
-- KEYS[3] recovering-since (ms, Redis clock)
-- ARGV[1] token
-- ARGV[2] lease in ms
-- ARGV[3] quiet period in ms
--
-- Returns {'DONE'|'BUSY'|'WAIT'|'GO', ms to wait}

if redis.call('EXISTS', KEYS[1]) == 1 then
    return {'DONE', 0}
end
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
redis.call('SET', KEYS[3], now, 'NX', 'PX', 86400000)
local holder = redis.call('GET', KEYS[2])
if holder and holder ~= ARGV[1] then
    return {'BUSY', redis.call('PTTL', KEYS[2])}
end
local waited = now - tonumber(redis.call('GET', KEYS[3]))
if waited < tonumber(ARGV[3]) then
    return {'WAIT', tonumber(ARGV[3]) - waited}
end
redis.call('SET', KEYS[2], ARGV[1], 'PX', tonumber(ARGV[2]))
return {'GO', 0}
