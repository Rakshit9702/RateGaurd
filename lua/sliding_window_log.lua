-- KEYS[1] = base key (ZSET of request timestamps)
-- ARGV[1] = limit
-- ARGV[2] = window (seconds)
-- returns {allowed, remaining, limit, reset, retry_after}

local key = KEYS[1]
local limit = tonumber(ARGV[1])
local window = tonumber(ARGV[2])

local time_result = redis.call('TIME')
local now = tonumber(time_result[1]) + tonumber(time_result[2]) / 1000000

redis.call('ZREMRANGEBYSCORE', key, '-inf', now - window)

local count = redis.call('ZCARD', key)
local allowed = count < limit

if allowed then
    local member = tostring(now) .. '-' .. tostring(math.random())
    redis.call('ZADD', key, now, member)
    count = count + 1
end

redis.call('EXPIRE', key, window)

local remaining = limit - count
if remaining < 0 then
    remaining = 0
end

local oldest = redis.call('ZRANGE', key, 0, 0, 'WITHSCORES')
local reset
local retry_after = 0

if #oldest > 0 then
    reset = math.ceil(tonumber(oldest[2]) + window)
else
    reset = math.ceil(now + window)
end

if not allowed then
    retry_after = math.ceil(tonumber(oldest[2]) + window - now)
    if retry_after < 0 then
        retry_after = 0
    end
end

return {allowed and 1 or 0, remaining, limit, reset, retry_after}
