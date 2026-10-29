-- Shortens (ARGV[2] ms) or drops (ARGV[2] = 0) a rebuild lease, but only the
-- caller's own: a lease another rebuilder took over is left alone.
--
-- KEYS[1] rebuild lease
-- ARGV[1] token
if redis.call('GET', KEYS[1]) ~= ARGV[1] then
    return 0
end
if tonumber(ARGV[2]) > 0 then
    return redis.call('PEXPIRE', KEYS[1], tonumber(ARGV[2]))
end
return redis.call('DEL', KEYS[1])
