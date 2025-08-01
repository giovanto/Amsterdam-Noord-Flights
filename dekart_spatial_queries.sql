-- SPATIAL QUERIES FOR DEKART VISUALIZATION

-- 1. FLIGHT PATHS - Connect points to show aircraft trajectories
SELECT 
    icao24,
    callsign,
    collection_time,
    latitude as lat,
    longitude as lon,
    altitude,
    noise_db,
    -- Create a path_order to connect points chronologically
    ROW_NUMBER() OVER (PARTITION BY icao24 ORDER BY collection_time) as path_order,
    -- Group flights into sessions (new session if > 30 min gap)
    SUM(CASE 
        WHEN LAG(collection_time) OVER (PARTITION BY icao24 ORDER BY collection_time) IS NULL 
          OR collection_time - LAG(collection_time) OVER (PARTITION BY icao24 ORDER BY collection_time) > INTERVAL '30 minutes'
        THEN 1 
        ELSE 0 
    END) OVER (PARTITION BY icao24 ORDER BY collection_time) as flight_session
FROM simple_flights
WHERE collection_time > NOW() - INTERVAL '3 hours'
  AND latitude IS NOT NULL
ORDER BY icao24, collection_time;

-- 2. APPROACH/DEPARTURE CORRIDORS - Identify flight patterns
SELECT 
    latitude as lat,
    longitude as lon,
    COUNT(*) as flight_count,
    AVG(altitude) as avg_altitude,
    AVG(noise_db) as avg_noise,
    MAX(noise_db) as max_noise,
    -- Round coordinates to create grid cells (approximately 100m x 100m)
    ROUND(latitude::numeric, 3) as lat_grid,
    ROUND(longitude::numeric, 3) as lon_grid
FROM simple_flights
WHERE noise_db > 50
GROUP BY ROUND(latitude::numeric, 3), ROUND(longitude::numeric, 3), latitude, longitude
HAVING COUNT(*) > 2;

-- 3. NOISE HEATMAP - Aggregate by location
SELECT 
    AVG(latitude) as lat,
    AVG(longitude) as lon,
    COUNT(*) as measurements,
    AVG(noise_db) as avg_noise_db,
    MAX(noise_db) as max_noise_db,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY noise_db) as p95_noise_db,
    -- Create hexagon-like grid (approximately 250m cells)
    ROUND(latitude::numeric * 400) / 400 as lat_cell,
    ROUND(longitude::numeric * 400) / 400 as lon_cell
FROM simple_flights
WHERE noise_db > 0
GROUP BY lat_cell, lon_cell
HAVING COUNT(*) > 5;

-- 4. TIME-BASED FLIGHT DENSITY
SELECT 
    date_trunc('hour', collection_time) as hour,
    COUNT(DISTINCT icao24) as unique_aircraft,
    COUNT(*) as total_observations,
    AVG(noise_db) as avg_noise,
    -- Most active coordinates this hour
    MODE() WITHIN GROUP (ORDER BY ROUND(latitude::numeric, 2)) as common_lat,
    MODE() WITHIN GROUP (ORDER BY ROUND(longitude::numeric, 2)) as common_lon
FROM simple_flights
WHERE collection_time > NOW() - INTERVAL '24 hours'
GROUP BY date_trunc('hour', collection_time)
ORDER BY hour DESC;

-- 5. AMSTERDAM NOORD FOCUS - Your house area
SELECT 
    collection_time,
    icao24,
    callsign,
    latitude as lat,
    longitude as lon,
    altitude,
    noise_db,
    distance_km,
    area_type,
    -- Calculate bearing/direction
    DEGREES(ATAN2(
        longitude - 4.895168,
        latitude - 52.385157
    )) as bearing_from_noord
FROM simple_flights
WHERE latitude BETWEEN 52.37 AND 52.40  -- Amsterdam Noord bounds
  AND longitude BETWEEN 4.87 AND 4.92
  AND noise_db > 60
ORDER BY noise_db DESC;

-- 6. RUNWAY ALIGNMENT ANALYSIS - Detect approach/departure paths
WITH runway_ends AS (
    -- Approximate Schiphol runway coordinates
    SELECT 'Polderbaan' as runway, 52.378 as lat, 4.729 as lon
    UNION ALL SELECT 'Kaagbaan', 52.308, 4.741
    UNION ALL SELECT 'Aalsmeerbaan', 52.285, 4.744
    UNION ALL SELECT 'Buitenveldertbaan', 52.304, 4.776
    UNION ALL SELECT 'Zwanenburgbaan', 52.318, 4.745
)
SELECT 
    s.latitude as lat,
    s.longitude as lon,
    s.altitude,
    s.noise_db,
    r.runway as nearest_runway,
    -- Distance to runway
    SQRT(POWER(s.latitude - r.lat, 2) + POWER(s.longitude - r.lon, 2)) * 111 as distance_to_runway_km
FROM simple_flights s
CROSS JOIN runway_ends r
WHERE s.altitude < 3000  -- Low altitude flights only
ORDER BY distance_to_runway_km
LIMIT 1000;