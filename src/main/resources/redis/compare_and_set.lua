-- Sets a key only if it still holds the value we last observed (compare-and-set),
-- so a correction never overwrites a reservation that happened in the meantime.
--
-- KEYS[1] stock counter
-- ARGV[1] expected current value
-- ARGV[2] new value
--
-- Returns 1 if the value was replaced, 0 if it had changed.

if redis.call('GET', KEYS[1]) == ARGV[1] then
    redis.call('SET', KEYS[1], ARGV[2])
    return 1
end
return 0
