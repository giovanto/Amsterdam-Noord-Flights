-- Aviation Impact Analysis Database Schema
-- Designed for 2-week data collection and future correlation analysis

-- Flight data schema
CREATE SCHEMA IF NOT EXISTS flight_data;
CREATE SCHEMA IF NOT EXISTS analysis;
CREATE SCHEMA IF NOT EXISTS monitoring;

-- Core flight records table
CREATE TABLE flight_data.flights (
    id SERIAL PRIMARY KEY,
    collection_time TIMESTAMPTZ NOT NULL,
    icao24 VARCHAR(10) NOT NULL,
    callsign VARCHAR(10),
    origin_country VARCHAR(50),
    latitude DECIMAL(10,7),
    longitude DECIMAL(11,7),
    baro_altitude INTEGER,
    velocity DECIMAL(6,2),
    true_track DECIMAL(6,2),
    vertical_rate DECIMAL(6,2),
    area_type VARCHAR(20) NOT NULL,
    
    -- Analysis fields
    distance_to_house_km DECIMAL(8,3),
    estimated_noise_db DECIMAL(5,2),
    noise_impact_level VARCHAR(20),
    schiphol_operation VARCHAR(30),
    approach_corridor VARCHAR(30),
    aircraft_category VARCHAR(30),
    
    -- Time analysis
    hour_of_day INTEGER,
    day_of_week INTEGER,
    is_weekend BOOLEAN,
    time_period VARCHAR(20),
    
    -- Pattern flags
    is_over_house BOOLEAN DEFAULT FALSE,
    is_low_altitude BOOLEAN DEFAULT FALSE,
    is_high_noise BOOLEAN DEFAULT FALSE,
    
    -- Metadata
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Aircraft reference data
CREATE TABLE flight_data.aircraft_types (
    icao24 VARCHAR(10) PRIMARY KEY,
    aircraft_model VARCHAR(50),
    aircraft_manufacturer VARCHAR(50),
    engine_type VARCHAR(30),
    noise_category VARCHAR(10),
    max_takeoff_weight INTEGER,
    first_seen TIMESTAMPTZ DEFAULT NOW(),
    last_seen TIMESTAMPTZ DEFAULT NOW()
);

-- Future correlation preparation
CREATE TABLE analysis.demographic_zones (
    zone_id SERIAL PRIMARY KEY,
    zone_name VARCHAR(100),
    municipality_code VARCHAR(10),
    geometry GEOMETRY(POLYGON, 4326),
    population_density DECIMAL(10,2),
    socioeconomic_index DECIMAL(5,2),
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE analysis.flight_zone_correlations (
    id SERIAL PRIMARY KEY,
    flight_id INTEGER REFERENCES flight_data.flights(id),
    zone_id INTEGER REFERENCES analysis.demographic_zones(zone_id),
    impact_score DECIMAL(5,2),
    correlation_type VARCHAR(30),
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Monitoring and data quality
CREATE TABLE monitoring.collection_stats (
    id SERIAL PRIMARY KEY,
    collection_date DATE NOT NULL,
    flights_collected INTEGER DEFAULT 0,
    api_calls_made INTEGER DEFAULT 0,
    errors_encountered INTEGER DEFAULT 0,
    avg_collection_interval INTEGER,
    data_quality_score DECIMAL(5,2),
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE monitoring.data_quality_issues (
    id SERIAL PRIMARY KEY,
    flight_id INTEGER REFERENCES flight_data.flights(id),
    issue_type VARCHAR(50),
    issue_description TEXT,
    severity VARCHAR(20),
    resolved BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Indexes for performance
CREATE INDEX idx_flights_collection_time ON flight_data.flights(collection_time);
CREATE INDEX idx_flights_icao24_time ON flight_data.flights(icao24, collection_time);
CREATE INDEX idx_flights_location ON flight_data.flights(latitude, longitude);
CREATE INDEX idx_flights_noise_analysis ON flight_data.flights(estimated_noise_db, is_high_noise);
CREATE INDEX idx_flights_house_proximity ON flight_data.flights(is_over_house, distance_to_house_km);
CREATE INDEX idx_flights_temporal ON flight_data.flights(hour_of_day, day_of_week, is_weekend);

-- Spatial index for future geographic analysis
CREATE INDEX idx_demographic_zones_geom ON analysis.demographic_zones USING GIST(geometry);

-- Views for common analysis patterns
CREATE VIEW analysis.high_impact_flights AS
SELECT 
    f.*,
    CASE 
        WHEN f.is_high_noise AND f.is_over_house THEN 'Critical'
        WHEN f.is_high_noise OR f.is_over_house THEN 'High'
        WHEN f.estimated_noise_db > 60 THEN 'Medium'
        ELSE 'Low'
    END as impact_category
FROM flight_data.flights f
WHERE f.collection_time >= NOW() - INTERVAL '24 hours';

CREATE VIEW analysis.collection_summary AS
SELECT 
    DATE(collection_time) as collection_date,
    COUNT(*) as total_flights,
    COUNT(DISTINCT icao24) as unique_aircraft,
    AVG(estimated_noise_db) as avg_noise_level,
    SUM(CASE WHEN is_high_noise THEN 1 ELSE 0 END) as high_noise_flights,
    SUM(CASE WHEN is_over_house THEN 1 ELSE 0 END) as flights_over_house
FROM flight_data.flights
GROUP BY DATE(collection_time)
ORDER BY collection_date DESC;

-- Functions for data validation
CREATE OR REPLACE FUNCTION flight_data.validate_flight_record()
RETURNS TRIGGER AS $$
BEGIN
    -- Validate coordinates
    IF NEW.latitude < -90 OR NEW.latitude > 90 THEN
        INSERT INTO monitoring.data_quality_issues (flight_id, issue_type, issue_description, severity)
        VALUES (NEW.id, 'Invalid Latitude', 'Latitude out of valid range', 'High');
    END IF;
    
    IF NEW.longitude < -180 OR NEW.longitude > 180 THEN
        INSERT INTO monitoring.data_quality_issues (flight_id, issue_type, issue_description, severity)
        VALUES (NEW.id, 'Invalid Longitude', 'Longitude out of valid range', 'High');
    END IF;
    
    -- Validate altitude
    IF NEW.baro_altitude < -1000 OR NEW.baro_altitude > 50000 THEN
        INSERT INTO monitoring.data_quality_issues (flight_id, issue_type, issue_description, severity)
        VALUES (NEW.id, 'Invalid Altitude', 'Altitude out of reasonable range', 'Medium');
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger for data validation
CREATE TRIGGER validate_flight_data
    AFTER INSERT ON flight_data.flights
    FOR EACH ROW
    EXECUTE FUNCTION flight_data.validate_flight_record();

-- Function to update collection statistics
CREATE OR REPLACE FUNCTION monitoring.update_daily_stats()
RETURNS void AS $$
BEGIN
    INSERT INTO monitoring.collection_stats (
        collection_date, 
        flights_collected, 
        data_quality_score
    )
    SELECT 
        CURRENT_DATE,
        COUNT(*),
        100.0 - (COUNT(DISTINCT dqi.flight_id) * 100.0 / COUNT(f.id))
    FROM flight_data.flights f
    LEFT JOIN monitoring.data_quality_issues dqi ON f.id = dqi.flight_id 
        AND dqi.created_at >= CURRENT_DATE
    WHERE f.collection_time >= CURRENT_DATE
    ON CONFLICT (collection_date) DO UPDATE SET
        flights_collected = EXCLUDED.flights_collected,
        data_quality_score = EXCLUDED.data_quality_score,
        updated_at = NOW();
END;
$$ LANGUAGE plpgsql;