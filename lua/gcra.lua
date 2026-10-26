-- KEYS[1] = base key (STRING: theoretical arrival time, TAT)
-- ARGV[1] = limit
-- ARGV[2] = window (seconds)
-- ARGV[3] = burst (already resolved in Python, defaults to limit)
-- returns {allowed, remaining, limit, reset, retry_after}

local key = KEYS[1]
local limit = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local burst = tonumber(ARGV[3])

local time_result = redis.call('TIME')
local now = tonumber(time_result[1]) + tonumber(time_result[2]) / 1000000

local emission_interval = window / limit
-- (burst - 1), not burst: burst_tolerance = emission_interval * burst lets
-- burst+1 requests through in an instant flood (off-by-one at the boundary).
-- This is the standard GCRA formula that admits exactly `burst` instantly.
local burst_tolerance = emission_interval * (burst - 1)

local raw_tat = redis.call('GET', key)
local tat = tonumber(raw_tat) or now

local allow_at = tat - burst_tolerance
local allowed = now >= allow_at

local effective_tat = tat
if allowed then
    effective_tat = math.max(tat, now) + emission_interval
    redis.call('SET', key, effective_tat)
end

redis.call('EXPIRE', key, math.ceil(burst_tolerance + emission_interval))

-- remaining has no natural GCRA definition; approximated from how much
-- burst slack is left before the next permitted-arrival time.
local remaining = math.floor((burst_tolerance - math.max(0, effective_tat - now)) / emission_interval)
if remaining < 0 then
    remaining = 0
end
if remaining > burst then
    remaining = burst
end

local reset = math.ceil(effective_tat)

local retry_after = 0
if not allowed then
    retry_after = math.ceil(allow_at - now)
    if retry_after < 0 then
        retry_after = 0
    end
end

return {allowed and 1 or 0, remaining, limit, reset, retry_after}
