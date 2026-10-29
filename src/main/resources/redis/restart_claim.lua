-- One step of RedisRestartGuard, atomic so that several instances noticing the
-- same restart wipe it exactly once: an instance that looked before another one
-- finished, and acts after, finds the record already updated and does nothing.
--
-- KEYS[1] trusted server (run_id whose state the app last trusted)
-- KEYS[2] wipe lock
-- ARGV[1] the server's current run_id
-- ARGV[2] wipe lease in ms
-- ARGV[3] this caller's owner token, unique per attempt: the lock names who
--         holds it, so a wiper whose lease ran out can't act for its successor
--
-- Returns 0 if the current server is trusted, 1 if this caller must wipe (it
-- now holds the lock), 2 if another caller is wiping.
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return 0
end
if redis.call('SET', KEYS[2], ARGV[3], 'NX', 'PX', tonumber(ARGV[2])) then
    return 1
end
return 2
