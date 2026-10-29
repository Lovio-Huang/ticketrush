-- Recomputes stock and per-user counts from the held orders, in one atomic step:
--   stock = total - (tickets of held orders),  buyers[u] = tickets held by u
-- Nothing can interleave, so this is safe while a sale is running, and orders
-- still in flight to MySQL are counted because they are held. Creates the stock
-- key at the end of a rebuild; on a live event it only fixes drift (a hand edit,
-- a bug), which it reports.
--
-- KEYS[1] stock counter
-- KEYS[2] buyers hash
-- KEYS[3] orders hash
-- ARGV[1] total tickets of the event
--
-- Returns {previous stock or -1 if missing, new stock, 1 if per-user counts changed}

local entries = redis.call('HGETALL', KEYS[3])
local held, perUser = 0, {}
for i = 2, #entries, 2 do
    if entries[i] ~= 'R' then
        local order = cjson.decode(entries[i])
        held = held + order.quantity
        perUser[order.userId] = (perUser[order.userId] or 0) + order.quantity
    end
end

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
return {previous, expected, buyersChanged}
