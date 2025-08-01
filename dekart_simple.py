#!/usr/bin/env python3
"""
Simple Dekart Setup - Works with actual SQLite schema
"""

import sqlite3
import psycopg2
from datetime import datetime

# Database connections
sqlite_path = "/opt/flight-collector/amsterdam_flight_patterns_2week.db"
pg_params = {
    'host': '172.17.0.1',
    'user': 'gio',
    'password': 'alpinism',
    'database': 'aviation_impact_analysis'
}

def setup_dekart():
    """Create simple Dekart visualizations"""
    
    # Connect to PostgreSQL
    pg_conn = psycopg2.connect(**pg_params)
    pg_cursor = pg_conn.cursor()
    
    # Create a simple flights table
    pg_cursor.execute("""
        DROP TABLE IF EXISTS flight_data.simple_flights CASCADE;
        CREATE TABLE flight_data.simple_flights (
            id SERIAL PRIMARY KEY,
            collection_time TIMESTAMP,
            icao24 VARCHAR(10),
            callsign VARCHAR(20),
            latitude FLOAT,
            longitude FLOAT,
            altitude FLOAT,
            velocity FLOAT,
            distance_km FLOAT,
            noise_db FLOAT,
            area_type VARCHAR(20),
            hour INTEGER
        );
    """)
    
    # Connect to SQLite
    sqlite_conn = sqlite3.connect(sqlite_path)
    sqlite_cursor = sqlite_conn.cursor()
    
    # Get recent flights
    sqlite_cursor.execute("""
        SELECT 
            collection_time,
            icao24,
            callsign,
            latitude,
            longitude,
            baro_altitude,
            velocity,
            distance_to_house_km,
            estimated_noise_db,
            area_type,
            CAST(strftime('%H', collection_time) as INTEGER) as hour
        FROM flights
        WHERE latitude IS NOT NULL 
        AND longitude IS NOT NULL
        AND collection_time > datetime('now', '-2 days')
        ORDER BY collection_time DESC
        LIMIT 10000
    """)
    
    flights = sqlite_cursor.fetchall()
    print(f"Found {len(flights)} flights to visualize")
    
    # Insert into PostgreSQL
    inserted = 0
    for flight in flights:
        try:
            pg_cursor.execute("""
                INSERT INTO flight_data.simple_flights (
                    collection_time, icao24, callsign, latitude, longitude,
                    altitude, velocity, distance_km, noise_db, area_type, hour
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, flight)
            inserted += 1
        except Exception as e:
            print(f"Error: {e}")
            continue
    
    # Create views for Dekart
    pg_cursor.execute("""
        CREATE OR REPLACE VIEW flight_data.dekart_map AS
        SELECT 
            latitude as lat,
            longitude as lon,
            noise_db,
            callsign,
            altitude,
            collection_time,
            CASE 
                WHEN noise_db > 70 THEN 'red'
                WHEN noise_db > 60 THEN 'orange'
                ELSE 'green'
            END as color
        FROM flight_data.simple_flights
        WHERE noise_db > 0;
    """)
    
    pg_cursor.execute("""
        CREATE OR REPLACE VIEW flight_data.dekart_timeline AS
        SELECT 
            date_trunc('hour', collection_time) as hour,
            COUNT(*) as flights,
            AVG(noise_db) as avg_noise,
            MAX(noise_db) as max_noise
        FROM flight_data.simple_flights
        WHERE noise_db > 0
        GROUP BY date_trunc('hour', collection_time)
        ORDER BY hour;
    """)
    
    pg_cursor.execute("""
        CREATE OR REPLACE VIEW flight_data.dekart_patterns AS
        SELECT 
            hour,
            COUNT(*) as flight_count,
            AVG(noise_db) as avg_noise_db,
            COUNT(CASE WHEN area_type = 'house' THEN 1 END) as over_house
        FROM flight_data.simple_flights
        GROUP BY hour
        ORDER BY hour;
    """)
    
    pg_conn.commit()
    print(f"✅ Inserted {inserted} flights successfully")
    
    print("\n🎯 Dekart Queries:")
    print("\n1. Map visualization:")
    print("SELECT * FROM flight_data.dekart_map ORDER BY collection_time DESC LIMIT 1000;")
    
    print("\n2. Timeline:")
    print("SELECT * FROM flight_data.dekart_timeline;")
    
    print("\n3. Daily patterns:")
    print("SELECT * FROM flight_data.dekart_patterns;")
    
    print("\n4. High noise events:")
    print("SELECT * FROM flight_data.simple_flights WHERE noise_db > 65 ORDER BY noise_db DESC LIMIT 100;")
    
    print(f"\n📊 Dekart URL: http://85.214.63.233:8088/")
    print("Database: aviation_impact_analysis")
    print("User: gio")
    
    sqlite_conn.close()
    pg_cursor.close()
    pg_conn.close()

if __name__ == "__main__":
    setup_dekart()