#!/usr/bin/env python3
"""
ETL Pipeline for Aviation Impact Analysis
Transfers data from SQLite collection to PostgreSQL analysis database

Author: Giovanni Antoniazzi (Studio Bereikbaar)
Purpose: Robust data pipeline for 2-week collection and future analysis
"""

import sqlite3
import psycopg2
import psycopg2.extras
from datetime import datetime, timedelta
import logging
import sys
from typing import Dict, List, Optional
import json

class AviationETLPipeline:
    """ETL Pipeline for transferring flight data to analysis database"""
    
    def __init__(self, 
                 sqlite_path: str = "/opt/flight-collector/amsterdam_flight_patterns_2week.db",
                 pg_host: str = "172.17.0.1",
                 pg_user: str = "gio", 
                 pg_password: str = "alpinism",
                 pg_database: str = "aviation_impact_analysis"):
        
        self.sqlite_path = sqlite_path
        self.pg_params = {
            'host': pg_host,
            'user': pg_user, 
            'password': pg_password,
            'database': pg_database
        }
        
        self.logger = self._setup_logging()
        
    def _setup_logging(self) -> logging.Logger:
        """Configure logging for ETL operations"""
        logger = logging.getLogger('AviationETL')
        logger.setLevel(logging.INFO)
        
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        
        return logger
    
    def extract_new_flights(self, last_processed_id: int = 0) -> List[Dict]:
        """Extract new flight records from SQLite"""
        conn = sqlite3.connect(self.sqlite_path)
        conn.row_factory = sqlite3.Row
        
        query = """
        SELECT * FROM flights 
        WHERE id > ? 
        ORDER BY id
        """
        
        cursor = conn.execute(query, (last_processed_id,))
        flights = [dict(row) for row in cursor.fetchall()]
        conn.close()
        
        self.logger.info(f"Extracted {len(flights)} new flights from SQLite")
        return flights
    
    def transform_flight_data(self, flights: List[Dict]) -> List[Dict]:
        """Transform flight data for PostgreSQL schema"""
        transformed = []
        
        for flight in flights:
            # Convert timestamp format
            collection_time = datetime.fromisoformat(flight['collection_time'].replace('Z', '+00:00'))
            
            transformed_flight = {
                'collection_time': collection_time,
                'icao24': flight['icao24'],
                'callsign': flight.get('callsign'),
                'origin_country': flight.get('origin_country'),
                'latitude': flight.get('latitude'),
                'longitude': flight.get('longitude'),
                'baro_altitude': flight.get('baro_altitude'),
                'velocity': flight.get('velocity'),
                'true_track': flight.get('true_track'),
                'vertical_rate': flight.get('vertical_rate'),
                'area_type': flight['area_type'],
                'distance_to_house_km': flight.get('distance_to_house_km'),
                'estimated_noise_db': flight.get('estimated_noise_db'),
                'noise_impact_level': flight.get('noise_impact_level'),
                'schiphol_operation': flight.get('schiphol_operation'),
                'approach_corridor': flight.get('approach_corridor'),
                'aircraft_category': flight.get('aircraft_category'),
                'hour_of_day': flight.get('hour_of_day'),
                'day_of_week': flight.get('day_of_week'),
                'is_weekend': flight.get('is_weekend'),
                'time_period': flight.get('time_period'),
                'is_over_house': flight.get('is_over_house', False),
                'is_low_altitude': flight.get('is_low_altitude', False),
                'is_high_noise': flight.get('is_high_noise', False)
            }
            
            transformed.append(transformed_flight)
        
        return transformed
    
    def load_flights_to_postgresql(self, flights: List[Dict]) -> int:
        """Load transformed flight data into PostgreSQL"""
        if not flights:
            return 0
            
        conn = psycopg2.connect(**self.pg_params)
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        insert_query = """
        INSERT INTO flight_data.flights (
            collection_time, icao24, callsign, origin_country,
            latitude, longitude, baro_altitude, velocity, true_track, vertical_rate,
            area_type, distance_to_house_km, estimated_noise_db, noise_impact_level,
            schiphol_operation, approach_corridor, aircraft_category,
            hour_of_day, day_of_week, is_weekend, time_period,
            is_over_house, is_low_altitude, is_high_noise
        ) VALUES (
            %(collection_time)s, %(icao24)s, %(callsign)s, %(origin_country)s,
            %(latitude)s, %(longitude)s, %(baro_altitude)s, %(velocity)s, 
            %(true_track)s, %(vertical_rate)s, %(area_type)s, 
            %(distance_to_house_km)s, %(estimated_noise_db)s, %(noise_impact_level)s,
            %(schiphol_operation)s, %(approach_corridor)s, %(aircraft_category)s,
            %(hour_of_day)s, %(day_of_week)s, %(is_weekend)s, %(time_period)s,
            %(is_over_house)s, %(is_low_altitude)s, %(is_high_noise)s
        ) RETURNING id
        """
        
        loaded_count = 0
        for flight in flights:
            try:
                cursor.execute(insert_query, flight)
                loaded_count += 1
            except Exception as e:
                self.logger.error(f"Failed to load flight {flight.get('icao24')}: {e}")
                conn.rollback()
                continue
        
        conn.commit()
        cursor.close()
        conn.close()
        
        self.logger.info(f"Loaded {loaded_count} flights into PostgreSQL")
        return loaded_count
    
    def get_last_processed_id(self) -> int:
        """Get the last processed flight ID from PostgreSQL"""
        try:
            conn = psycopg2.connect(**self.pg_params)
            cursor = conn.cursor()
            
            cursor.execute("SELECT MAX(id) FROM flight_data.flights")
            result = cursor.fetchone()
            
            cursor.close()
            conn.close()
            
            return result[0] if result[0] is not None else 0
            
        except Exception as e:
            self.logger.warning(f"Could not get last processed ID: {e}")
            return 0
    
    def update_monitoring_stats(self):
        """Update daily monitoring statistics"""
        try:
            conn = psycopg2.connect(**self.pg_params)
            cursor = conn.cursor()
            
            cursor.execute("SELECT monitoring.update_daily_stats()")
            conn.commit()
            
            cursor.close()
            conn.close()
            
            self.logger.info("Updated monitoring statistics")
            
        except Exception as e:
            self.logger.error(f"Failed to update monitoring stats: {e}")
    
    def run_etl_cycle(self) -> Dict:
        """Execute complete ETL cycle"""
        start_time = datetime.now()
        
        try:
            # Get last processed ID to avoid duplicates
            last_id = self.get_last_processed_id()
            
            # Extract new flights
            flights = self.extract_new_flights(last_id)
            
            if not flights:
                self.logger.info("No new flights to process")
                return {'status': 'success', 'flights_processed': 0}
            
            # Transform data
            transformed_flights = self.transform_flight_data(flights)
            
            # Load into PostgreSQL
            loaded_count = self.load_flights_to_postgresql(transformed_flights)
            
            # Update monitoring
            self.update_monitoring_stats()
            
            duration = (datetime.now() - start_time).total_seconds()
            
            result = {
                'status': 'success',
                'flights_processed': loaded_count,
                'duration_seconds': duration,
                'timestamp': datetime.now().isoformat()
            }
            
            self.logger.info(f"ETL cycle completed: {loaded_count} flights in {duration:.1f}s")
            return result
            
        except Exception as e:
            self.logger.error(f"ETL cycle failed: {e}")
            return {
                'status': 'error',
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }

def main():
    """Command line interface for ETL pipeline"""
    pipeline = AviationETLPipeline()
    
    if len(sys.argv) > 1 and sys.argv[1] == '--continuous':
        # Continuous mode for production
        import time
        
        print("Starting continuous ETL pipeline...")
        while True:
            result = pipeline.run_etl_cycle()
            
            if result['status'] == 'error':
                print(f"ETL Error: {result['error']}")
                time.sleep(60)  # Wait 1 minute on error
            else:
                print(f"Processed {result['flights_processed']} flights")
                time.sleep(300)  # Run every 5 minutes
                
    else:
        # Single run mode
        result = pipeline.run_etl_cycle()
        print(json.dumps(result, indent=2))

if __name__ == "__main__":
    main()