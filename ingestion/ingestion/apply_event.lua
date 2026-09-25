-- Applies one event atomically: dedup, last-activity, counters, busiest, pairing.
-- Returns 1 if applied, 0 if the event is a duplicate.
--
-- KEYS[1] ride:{ride_id}      KEYS[2] station:{station_id}      KEYS[3] stations:busiest
-- ARGV[1] event_type          ARGV[2] station_id                ARGV[3] station_name
-- ARGV[4] timestamp (epoch ms, as a string)                     ARGV[5] ride TTL seconds
--
-- Note: pairing also updates the *other* station's hash (read from the ride hash),
-- which is fine on a single Redis node but not Redis Cluster-safe.

local ride, station, busiest = KEYS[1], KEYS[2], KEYS[3]
local event_type, station_id, station_name = ARGV[1], ARGV[2], ARGV[3]
local ts_str, ttl = ARGV[4], ARGV[5]
local ts = tonumber(ts_str)

local half = (event_type == 'trip_start') and 'start' or 'end'
local other = (half == 'start') and 'end' or 'start'

-- 1. Dedup: each half of a ride is applied once.
if redis.call('HSETNX', ride, half .. '_ts', ts_str) == 0 then
  return 0
end
redis.call('HSET', ride, half .. '_station', station_id)
redis.call('EXPIRE', ride, ttl)

-- 2. Station name.
if station_name ~= '' then
  redis.call('HSET', station, 'name', station_name)
end

-- 3. Last activity: only move forward in event time. On equal timestamps
--    trip_end wins, so the result doesn't depend on arrival order.
local last = tonumber(redis.call('HGET', station, 'last_ts_ms'))
if (not last) or ts > last or (ts == last and event_type == 'trip_end') then
  redis.call('HSET', station, 'last_ts_ms', ts_str, 'last_type', event_type)
end

-- 4. Bike balance counters (balance = arrivals - departures, computed on read).
redis.call('HINCRBY', station, (half == 'start') and 'departures' or 'arrivals', 1)

-- 5. Busiest.
redis.call('ZINCRBY', busiest, 1, station_id)

-- 6. Pairing: if the other half is already here, record the trip duration.
local other_ts = redis.call('HGET', ride, other .. '_ts')
if other_ts then
  local start_ts = tonumber(redis.call('HGET', ride, 'start_ts'))
  local end_ts = tonumber(redis.call('HGET', ride, 'end_ts'))
  local duration = end_ts - start_ts
  if duration > 0 then
    local d = string.format('%d', duration)
    local start_station = 'station:' .. redis.call('HGET', ride, 'start_station')
    local end_station = 'station:' .. redis.call('HGET', ride, 'end_station')
    redis.call('HINCRBY', start_station, 'dep_trips', 1)
    redis.call('HINCRBY', start_station, 'dep_sum_ms', d)
    redis.call('HINCRBY', end_station, 'arr_trips', 1)
    redis.call('HINCRBY', end_station, 'arr_sum_ms', d)
  end
end

return 1
