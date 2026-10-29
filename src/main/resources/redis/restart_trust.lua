-- The end of RedisRestartGuard's wipe: trusts the server and frees the lock,
-- only if the caller still owns it. A wiper that lost its lease neither marks
-- the server trusted nor frees the lock of the one that took over.
--
-- KEYS[1] trusted server
-- KEYS[2] wipe lock
-- ARGV[1] the server's run_id
-- ARGV[2] owner token
--
-- Returns 1 if trusted, 0 if the caller no longer owns the lock.
if redis.call('GET', KEYS[2]) ~= ARGV[2] then
    return 0
end
redis.call('SET', KEYS[1], ARGV[1])
redis.call('DEL', KEYS[2])
return 1
