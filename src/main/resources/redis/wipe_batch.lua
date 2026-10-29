-- One batch of RedisRestartGuard's wipe, only while the caller still owns the
-- wipe lock. The ownership check, the delete and the lease renewal are one
-- step: a wiper that stalled past its lease and was taken over deletes nothing
-- more, even keys its successor has rebuilt since.
--
-- KEYS[1]  wipe lock
-- KEYS[2..] keys to delete
-- ARGV[1] owner token
-- ARGV[2] wipe lease in ms
--
-- Returns the number of keys deleted, or -1 if the caller no longer owns the lock.
if redis.call('GET', KEYS[1]) ~= ARGV[1] then
    return -1
end
redis.call('PEXPIRE', KEYS[1], tonumber(ARGV[2]))
if #KEYS > 1 then
    redis.call('UNLINK', unpack(KEYS, 2))
end
return #KEYS - 1
