-- Recomputes stock and per-user counts from the held orders, in one atomic step:
--   stock = total - (tickets of held orders),  buyers[u] = tickets held by u
-- Nothing can interleave, so this is safe while a sale is running, and orders
-- still in flight to MySQL are counted because they are held.
--
-- Two modes. Live (ARGV[2] = ''): only fixes drift (a hand edit, a bug) and
-- never creates a missing stock key, because a missing key means a rebuild is
-- seeding and a count of a half-seeded hash would put tickets on sale twice.
-- Finishing a rebuild (ARGV[2] = token): fenced by the lease, creates the stock
-- key, and ends the recovery.
--
-- KEYS[1] stock counter
-- KEYS[2] buyers hash
-- KEYS[3] orders hash
-- KEYS[4] rebuild lease
-- KEYS[5] recovering-since
-- KEYS[6] held counter (set to the recounted total)
-- ARGV[1] total tickets of the event
-- ARGV[2] '' or the rebuild token
--
-- Returns {previous stock or -1 if missing, new stock, 1 if per-user counts changed},
-- {-1, -1, 0} if live and there is no stock key, {-2, -2, 0} if fenced off.

if ARGV[2] == '' then
    if redis.call('EXISTS', KEYS[1]) == 0 then
        return {-1, -1, 0}
    end
elseif redis.call('GET', KEYS[4]) ~= ARGV[2] then
    return {-2, -2, 0}
end

local entries = redis.call('HGETALL', KEYS[3])
local held, perUser = 0, {}
for i = 2, #entries, 2 do
    if entries[i] ~= 'R' then
        local order = cjson.decode(entries[i])
        held = held + order.quantity
        perUser[order.userId] = (perUser[order.userId] or 0) + order.quantity
    end
end

redis.call('SET', KEYS[6], held)
local expected = tonumber(ARGV[1]) - held
local previous = tonumber(redis.call('GET', KEYS[1]) or '-1')
if previous ~= expected then
    redis.call('SET', KEYS[1], expected)
end

local buyersChanged = 0
local current = redis.call('HGETALL', KEYS[2])
local seen = 0
for i = 1, #current, 2 do
    seen = seen + 1
    if perUser[current[i]] ~= tonumber(current[i + 1]) then
        buyersChanged = 1
    end
end
for _ in pairs(perUser) do
    seen = seen - 1
end
if buyersChanged == 1 or seen ~= 0 then
    buyersChanged = 1
    redis.call('DEL', KEYS[2])
    for user, qty in pairs(perUser) do
        redis.call('HSET', KEYS[2], user, qty)
    end
end
if ARGV[2] ~= '' then
    redis.call('DEL', KEYS[4], KEYS[5])
end
return {previous, expected, buyersChanged}
