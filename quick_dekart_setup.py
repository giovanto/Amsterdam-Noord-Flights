#!/usr/bin/env python3
"""
Quick Dekart Setup for Flight Visualization
Bypasses ETL pipeline issues to get data into PostgreSQL for immediate visualization
"""

import sqlite3
import psycopg2
from datetime import datetime
import json

# Database connections
sqlite_path = "/opt/flight-collector/amsterdam_flight_patterns_2week.db"
pg_params = {
    'host': '172.17.0.1',
    'user': 'gio',
    'password': 'alpinism',
    'database': 'aviation_impact_analysis'
}

def setup_dekart_view():
    """Create a simple view for Dekart visualization"""
    
    # Connect to PostgreSQL
    pg_conn = psycopg2.connect(**pg_params)
    pg_cursor = pg_conn.cursor()
    
    # Create a simplified table for Dekart
    pg_cursor.execute("""
        CREATE TABLE IF NOT EXISTS flight_data.dekart_flights (
            id SERIAL PRIMARY KEY,
            icao24 VARCHAR(10),
            callsign VARCHAR(20),
            latitude FLOAT,
            longitude FLOAT,
            altitude FLOAT,
            velocity FLOAT,
            vertical_rate FLOAT,
            collection_time TIMESTAMP,
            distance_km FLOAT,
            estimated_db FLOAT,
            over_house BOOLEAN,
            aircraft_type VARCHAR(50),
            origin VARCHAR(10),
            destination VARCHAR(10),
            airline VARCHAR(50),
            collection_hour INTEGER,
            is_weekend BOOLEAN
        );
    """)
    
    # Clear existing data
    pg_cursor.execute("TRUNCATE flight_data.dekart_flights;")
    
    # Connect to SQLite
    sqlite_conn = sqlite3.connect(sqlite_path)
    sqlite_cursor = sqlite_conn.cursor()
    
    # Get last 1000 flights for visualization
    sqlite_cursor.execute("""
        SELECT 
            icao24, callsign, latitude, longitude, baro_altitude,
            velocity, vertical_rate, collection_time, distance_to_house_km,
            estimated_noise_db, 
            CASE WHEN area_type = 'house' THEN 1 ELSE 0 END as over_house,
            aircraft_type, 
            SUBSTR(flight_number, 1, 3) as origin,
            runway as destination,
            airline,
            CAST(strftime('%H', collection_time) as INTEGER) as collection_hour,
            CASE WHEN strftime('%w', collection_time) IN ('0', '6') THEN 1 ELSE 0 END as is_weekend
        FROM flights
        WHERE latitude IS NOT NULL AND longitude IS NOT NULL
        ORDER BY collection_time DESC
        LIMIT 5000
    """)
    
    flights = sqlite_cursor.fetchall()
    print(f"Found {len(flights)} flights to transfer")
    
    # Insert into PostgreSQL with proper type conversion
    for flight in flights:
        try:
            # Convert is_weekend from 0/1 to boolean
            flight_data = list(flight)
            flight_data[16] = bool(flight_data[16])  # is_weekend
            flight_data[10] = bool(flight_data[10])  # over_house
            
            pg_cursor.execute("""
                INSERT INTO flight_data.dekart_flights (
                    icao24, callsign, latitude, longitude, altitude,
                    velocity, vertical_rate, collection_time, distance_km,
                    estimated_db, over_house, aircraft_type, origin,
                    destination, airline, collection_hour, is_weekend
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, flight_data)
        except Exception as e:
            print(f"Error inserting flight: {e}")
            continue
    
    # Create visualization-friendly views
    pg_cursor.execute("""
        CREATE OR REPLACE VIEW flight_data.noise_heatmap AS
        SELECT 
            date_trunc('hour', collection_time) as hour,
            AVG(latitude) as lat,
            AVG(longitude) as lon,
            COUNT(*) as flight_count,
            AVG(estimated_db) as avg_noise,
            MAX(estimated_db) as max_noise
        FROM flight_data.dekart_flights
        WHERE estimated_db > 0
        GROUP BY date_trunc('hour', collection_time);
    """)
    
    pg_cursor.execute("""
        CREATE OR REPLACE VIEW flight_data.flight_paths AS
        SELECT 
            icao24,
            callsign,
            latitude,
            longitude,
            altitude,
            estimated_db,
            collection_time,
            aircraft_type,
            airline,
            CASE 
                WHEN estimated_db > 70 THEN 'high'
                WHEN estimated_db > 60 THEN 'medium'
                ELSE 'low'
            END as noise_level
        FROM flight_data.dekart_flights
        WHERE collection_time > NOW() - INTERVAL '24 hours'
        ORDER BY collection_time DESC;
    """)
    
    pg_cursor.execute("""
        CREATE OR REPLACE VIEW flight_data.hourly_patterns AS
        SELECT 
            collection_hour as hour,
            COUNT(*) as flights,
            AVG(estimated_db) as avg_noise,
            SUM(CASE WHEN over_house THEN 1 ELSE 0 END) as flights_over_house
        FROM flight_data.dekart_flights
        GROUP BY collection_hour
        ORDER BY collection_hour;
    """)
    
    pg_conn.commit()
    print("✅ Dekart tables and views created successfully")
    
    # Print sample queries for Dekart
    print("\n🎯 Dekart SQL Queries to Try:")
    print("\n1. Flight paths with noise levels:")
    print("SELECT * FROM flight_data.flight_paths;")
    
    print("\n2. Noise heatmap by hour:")
    print("SELECT * FROM flight_data.noise_heatmap;")
    
    print("\n3. Daily patterns:")
    print("SELECT * FROM flight_data.hourly_patterns;")
    
    print("\n4. High noise events:")
    print("""SELECT 
    collection_time, callsign, aircraft_type, 
    estimated_db, distance_km
FROM flight_data.dekart_flights
WHERE estimated_db > 65
ORDER BY estimated_db DESC
LIMIT 100;""")
    
    sqlite_conn.close()
    pg_cursor.close()
    pg_conn.close()

if __name__ == "__main__":
    setup_dekart_view()