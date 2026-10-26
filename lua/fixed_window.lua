-- KEYS[1] = base key (already namespaced/quoted by Python)
-- ARGV[1] = limit
-- ARGV[2] = window (seconds)
-- returns {allowed, remaining, limit, reset, retry_after}

local base_key = KEYS[1]
local limit = tonumber(ARGV[1])
local window = tonumber(ARGV[2])

local time_result = redis.call('TIME')
local now = tonumber(time_result[1]) + tonumber(time_result[2]) / 1000000

local window_index = math.floor(now / window)
local full_key = base_key .. ':' .. window_index

local count = redis.call('INCR', full_key)
if count == 1 then
    redis.call('EXPIRE', full_key, window)
end

local allowed = count <= limit
local remaining = limit - count
if remaining < 0 then
    remaining = 0
end

local reset = (window_index + 1) * window
local retry_after = 0
if not allowed then
    retry_after = math.ceil(reset - now)
end

return {allowed and 1 or 0, remaining, limit, reset, retry_after}
