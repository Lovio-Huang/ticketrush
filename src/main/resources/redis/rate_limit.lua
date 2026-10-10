-- Token bucket rate limiter. Uses the Redis server clock so that every app
-- instance agrees on time, regardless of clock skew between hosts.
--
-- KEYS[1] bucket key        ticketrush:ratelimit:<userId>
-- ARGV[1] bucket capacity   (max burst)
-- ARGV[2] refill rate       (tokens per second)
--
-- Returns 1 if the request is allowed, 0 otherwise.

local capacity = tonumber(ARGV[1])
local rate = tonumber(ARGV[2])

local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)

local bucket = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens = tonumber(bucket[1]) or capacity
local ts = tonumber(bucket[2]) or now

local elapsed = math.max(0, now - ts)
tokens = math.min(capacity, tokens + elapsed * rate / 1000)

local allowed = 0
if tokens >= 1 then
    tokens = tokens - 1
    allowed = 1
end

redis.call('HSET', KEYS[1], 'tokens', tokens, 'ts', now)
redis.call('PEXPIRE', KEYS[1], math.ceil(capacity / rate * 1000) + 1000)
return allowed
