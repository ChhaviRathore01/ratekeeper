-- GCRA (Generic Cell Rate Algorithm) rate limiter.
--
-- Instead of storing a token count and a timestamp, GCRA stores a single
-- value per key: the Theoretical Arrival Time (TAT), the time at which the
-- bucket would be completely full again. A request for n tokens is allowed
-- if pushing TAT forward by n * interval keeps it within `tolerance`
-- (burst * interval) of now. This is equivalent to a token bucket but needs
-- only one GET and one SET, and the key can expire exactly when it is full.
--
-- Precision: Lua numbers are 64-bit floats. A Unix timestamp in
-- microseconds (~1.7e15) only has ~0.25us resolution, which is enough to
-- break exact arithmetic with fractional intervals. So timestamps are kept
-- relative to a recent epoch (much smaller numbers, ~0.01us resolution) and
-- all calculations are done on small offsets from `now`.
--
-- KEYS[1]  rate limit key
-- ARGV[1]  interval: microseconds per token (may be fractional)
-- ARGV[2]  burst: bucket capacity
-- ARGV[3]  n: tokens requested
-- ARGV[4]  now in microseconds since EPOCH, or "server" to use Redis TIME
--
-- Returns {allowed (0/1), remaining, retry_after_us, reset_after_us}

local EPOCH = 1735689600 -- 2025-01-01T00:00:00Z, in seconds

local key = KEYS[1]
local interval = tonumber(ARGV[1])
local burst = tonumber(ARGV[2])
local n = tonumber(ARGV[3])
local now

if ARGV[4] == 'server' then
  -- Required before Redis 5 to allow non-deterministic commands (TIME)
  -- followed by writes; a no-op on newer versions.
  if redis.replicate_commands then
    redis.replicate_commands()
  end
  local t = redis.call('TIME')
  now = (tonumber(t[1]) - EPOCH) * 1000000 + tonumber(t[2])
else
  now = tonumber(ARGV[4])
end

local tolerance = interval * burst

-- Offset of the stored TAT from now. A missing key means the bucket is full
-- (offset 0). If the clock moved backwards the offset is simply larger, so
-- skew can never grant extra tokens.
local tat_offset = 0
local stored = tonumber(redis.call('GET', key))
if stored and stored > now then
  tat_offset = stored - now
end

-- Tokens available given a TAT offset. The epsilon stops floating point
-- error from turning 2.9999999 into 2.
local function tokens(offset)
  local free = math.floor((tolerance - offset) / interval + 1e-6)
  if free < 0 then return 0 end
  if free > burst then return burst end
  return free
end

local new_offset = tat_offset + n * interval
local wait = new_offset - tolerance

if wait > 0 then
  return {0, tokens(tat_offset), math.ceil(wait), math.ceil(tat_offset)}
end

redis.call('SET', key, string.format('%.3f', now + new_offset),
  'PX', math.ceil(new_offset / 1000))
return {1, tokens(new_offset), 0, math.ceil(new_offset)}
