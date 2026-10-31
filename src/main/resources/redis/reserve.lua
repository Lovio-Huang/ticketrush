-- Atomically reserves tickets for one user. Runs as a single Redis command, so
-- concurrent requests can never observe a half-applied reservation.
--
-- Besides the counters it records the order itself, which keeps the invariant
--   stock + (tickets of all held orders) = total tickets
-- true after every script, and puts the order in the outbox until Kafka has it.
--
-- KEYS[1] stock counter      ticketrush:{event:<id>}:stock
-- KEYS[2] buyers hash        ticketrush:{event:<id>}:buyers   (userId -> tickets held)
-- KEYS[3] idempotency key    ticketrush:{event:<id>}:idem:<userId>:<key>   ('<orderId>|<quantity>')
-- KEYS[4] orders hash        ticketrush:{event:<id>}:orders   (orderId -> order JSON while held, 'R' once released)
-- KEYS[5] outbox             ticketrush:{event:<id>}:outbox   (orderId scored by accept time, until Kafka confirms)
-- KEYS[6] held counter       ticketrush:{event:<id>}:held     (tickets of held orders; stock + held = total)
-- ARGV[1] userId
-- ARGV[2] quantity
-- ARGV[3] per-user limit
-- ARGV[4] new order id
-- ARGV[5] idempotency key TTL in seconds
-- ARGV[6] order JSON (the Kafka message)
-- ARGV[7] max age in ms of an unconfirmed order before new reservations are paused
-- ARGV[8] end of the sale (the event's start), epoch ms, compared with Redis's clock
--
-- Returns {outcome, orderId}

-- The key belongs to one request: this user, this event (both in the key's name)
-- and this quantity. The same key with another quantity is a conflict, never the
-- original order and never a second reservation. A key written before quantities
-- were recorded holds the order id alone: then the held order says what it was
-- for, and if Redis no longer has it, the caller checks the order in MySQL.
local existing = redis.call('GET', KEYS[3])
if existing then
    local sep = string.find(existing, '|', 1, true)
    local id, qty
    if sep then
        id = string.sub(existing, 1, sep - 1)
        qty = tonumber(string.sub(existing, sep + 1))
    else
        id = existing
        local held = redis.call('HGET', KEYS[4], id)
        if held and held ~= 'R' then
            qty = tonumber(cjson.decode(held).quantity)
        end
    end
    if not qty then
        return {'DUPLICATE_UNVERIFIED', id}
    end
    if qty ~= tonumber(ARGV[2]) then
        return {'IDEMPOTENCY_CONFLICT', id}
    end
    return {'DUPLICATE', id}
end

local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
-- After the duplicate check, so a retry of an order accepted before the end still finds it.
-- The app checked its own clock already; this catches a request that paused past the end.
if now >= tonumber(ARGV[8]) then
    return {'SALE_ENDED', ''}
end

local stock = redis.call('GET', KEYS[1])
if not stock then
    return {'NOT_INITIALIZED', ''}
end

local oldest = redis.call('ZRANGE', KEYS[5], 0, 0, 'WITHSCORES')
if oldest[2] and now - tonumber(oldest[2]) > tonumber(ARGV[7]) then
    -- Kafka hasn't confirmed an order for a while: stop taking new ones until it catches up.
    return {'QUEUE_BACKLOGGED', ''}
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
redis.call('INCRBY', KEYS[6], qty)
redis.call('HINCRBY', KEYS[2], ARGV[1], qty)
redis.call('SET', KEYS[3], ARGV[4] .. '|' .. ARGV[2], 'EX', tonumber(ARGV[5]))
redis.call('HSET', KEYS[4], ARGV[4], ARGV[6])
redis.call('ZADD', KEYS[5], now, ARGV[4])
return {'RESERVED', ARGV[4]}
