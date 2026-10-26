-- KEYS[1] = base key (HASH: tokens, ts)
-- ARGV[1] = capacity
-- ARGV[2] = refill_rate (tokens per second)
-- returns {allowed, remaining, limit, reset, retry_after}

local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local refill_rate = tonumber(ARGV[2])

local time_result = redis.call('TIME')
local now = tonumber(time_result[1]) + tonumber(time_result[2]) / 1000000

local stored = redis.call('HMGET', key, 'tokens', 'ts')
local tokens = tonumber(stored[1])
local ts = tonumber(stored[2])

if tokens == nil then
    tokens = capacity
    ts = now
end

local elapsed = now - ts
if elapsed < 0 then
    elapsed = 0
end

tokens = math.min(capacity, tokens + elapsed * refill_rate)

local allowed = tokens >= 1
if allowed then
    tokens = tokens - 1
end

redis.call('HMSET', key, 'tokens', tokens, 'ts', now)

local ttl = math.ceil(capacity / refill_rate)
redis.call('EXPIRE', key, ttl)

local remaining = math.floor(tokens)
if remaining < 0 then
    remaining = 0
end

local reset = math.ceil(now + (capacity - tokens) / refill_rate)

local retry_after = 0
if not allowed then
    retry_after = math.ceil((1 - tokens) / refill_rate)
end

return {allowed and 1 or 0, remaining, capacity, reset, retry_after}
