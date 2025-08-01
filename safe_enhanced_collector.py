#!/usr/bin/env python3
"""
Safe Enhanced Flight Collector - Preserves working interfaces while adding improvements
Based on project analyst findings to ensure compatibility with existing pipeline
"""

import sqlite3
import signal
import sys
import time
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
import os
import json

# Import existing components with CORRECT interfaces
from opensky_fetcher import OpenSkyFetcher
from schiphol_analyzer import SchipholFlightAnalyzer

class SafeEnhancedFlightCollector:
    """Enhanced collector that preserves ALL working method signatures"""
    
    def __init__(self, db_path: str = "safe_enhanced_amsterdam_flights.db"):
        self.db_path = db_path
        self.start_time = datetime.now()
        self.end_time = self.start_time + timedelta(days=14)
        
        # SAFE API LIMITS - Based on analysis
        self.DAILY_API_LIMIT = 3800
        self.api_calls_today = 0
        self.last_reset_date = datetime.now().date()
        
        # ENHANCED SETTINGS - Compatible with working system
        self.collection_settings = {
            # Preserve original structure but enhance timing
            'peak_interval_minutes': 0.5,    # 30 seconds (was 3 minutes)
            'night_interval_minutes': 5,     # 5 minutes (was 10 minutes)
            
            # Preserve house coordinates (CRITICAL)
            'house_coords': (52.395, 4.915),  # Amsterdam Noord 1032 center
            
            # EXPANDED bounds (key improvement)
            'local_bounds': {
                'lat_min': 52.35, 'lat_max': 52.45,  # Expanded from 52.37-52.42
                'lon_min': 4.85, 'lon_max': 4.95     # Expanded from 4.89-4.94
            },
            'schiphol_bounds': {  # MAJOR EXPANSION for trajectory coverage
                'lat_min': 52.0, 'lat_max': 52.6,    # Was 52.0-52.6 (keep)
                'lon_min': 4.2, 'lon_max': 5.2       # Expanded from 4.2-5.1
            }
        }
        
        # Initialize components (PRESERVE WORKING PATTERN)
        self.fetcher = None
        self.analyzer = SchipholFlightAnalyzer()
        self.running = False
        
        # Enhanced statistics tracking
        self.stats = {
            'total_collections': 0,
            'api_calls_made': 0,
            'flights_over_house': 0,
            'high_noise_events': 0,
            'unique_aircraft_spotted': set(),
            'start_time': self.start_time,
            'progress_percentage': 0,
            # New enhanced tracking
            'trajectory_points': {},  # Track points per aircraft
            'coverage_stats': {'local': 0, 'schiphol': 0, 'extended': 0}
        }
        
        # Setup
        self.setup_logging()
        self.setup_database()
        self.load_credentials()
        
        # Graceful shutdown (PRESERVE PATTERN)
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
        
    def setup_logging(self):
        """Enhanced logging with trajectory tracking"""
        log_path = '/opt/flight-collector/safe_enhanced_collector.log' if os.path.exists('/opt/flight-collector') else 'safe_enhanced_collector.log'
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_path),
                logging.StreamHandler()
            ]
        )
        
    def setup_database(self):
        """ENHANCED database schema - PRESERVES original + adds trajectory fields"""
        conn = sqlite3.connect(self.db_path)
        
        # PRESERVE original schema structure but enhance for trajectories
        conn.execute('''
            CREATE TABLE IF NOT EXISTS flights (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                collection_time TIMESTAMP NOT NULL,
                icao24 TEXT NOT NULL,
                callsign TEXT,
                origin_country TEXT,
                latitude REAL,
                longitude REAL,
                baro_altitude REAL,
                velocity REAL,
                true_track REAL,
                vertical_rate REAL,
                area_type TEXT NOT NULL,
                
                -- Enhanced analysis fields (PRESERVE FROM WORKING SYSTEM)
                distance_to_house_km REAL,
                estimated_noise_db REAL,
                noise_impact_level TEXT,
                schiphol_operation TEXT,
                approach_corridor TEXT,
                
                -- NEW: Enhanced trajectory fields
                collection_interval_minutes REAL,
                points_for_aircraft INTEGER,
                coverage_zone TEXT,  -- 'local', 'schiphol', 'extended'
                
                -- NEW: Schiphol enrichment fields
                aircraft_type TEXT,
                airline TEXT,
                flight_number TEXT,
                runway TEXT,
                gate TEXT,
                aircraft_category TEXT
            )
        ''')
        
        # Enhanced indexes for trajectory analysis
        conn.execute('CREATE INDEX IF NOT EXISTS idx_icao24_time ON flights (icao24, collection_time)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_trajectory_points ON flights (icao24, points_for_aircraft)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_coverage_zone ON flights (coverage_zone)')
        
        # API usage tracking table
        conn.execute('''
            CREATE TABLE IF NOT EXISTS daily_api_usage (
                date DATE PRIMARY KEY,
                api_calls INTEGER DEFAULT 0,
                collections INTEGER DEFAULT 0,
                flights_collected INTEGER DEFAULT 0
            )
        ''')
        
        conn.commit()
        conn.close()
        
    def load_credentials(self):
        """PRESERVE working credential loading pattern"""
        try:
            creds_path = '/opt/flight-collector/credentials.json'
            if os.path.exists(creds_path):
                with open(creds_path, 'r') as f:
                    self.credentials = json.load(f)
            else:
                self.credentials = {}
        except Exception as e:
            logging.warning(f"Could not load credentials: {e}")
            self.credentials = {}
            
    def initialize_fetcher(self):
        """PRESERVE working fetcher initialization pattern"""
        if not self.fetcher:
            try:
                self.fetcher = OpenSkyFetcher()
                # Dynamically set bounds for enhanced coverage
                self.fetcher.AMSTERDAM_NOORD_BOUNDS = self.collection_settings['schiphol_bounds']
                logging.info(f"🎯 Enhanced coverage area: {self.collection_settings['schiphol_bounds']}")
            except Exception as e:
                logging.error(f"Failed to initialize fetcher: {e}")
                
    def determine_coverage_zone(self, lat: float, lon: float) -> str:
        """Determine which coverage zone a flight falls into"""
        local_bounds = self.collection_settings['local_bounds']
        
        # Check local area (Amsterdam Noord focus)
        if (local_bounds['lat_min'] <= lat <= local_bounds['lat_max'] and
            local_bounds['lon_min'] <= lon <= local_bounds['lon_max']):
            return 'local'
            
        # Check if in expanded Schiphol area
        schiphol_bounds = self.collection_settings['schiphol_bounds']
        if (schiphol_bounds['lat_min'] <= lat <= schiphol_bounds['lat_max'] and
            schiphol_bounds['lon_min'] <= lon <= schiphol_bounds['lon_max']):
            return 'schiphol'
            
        return 'extended'
        
    def collect_flight_data(self):
        """ENHANCED collection using PRESERVED method signatures"""
        collection_time = datetime.now()
        
        try:
            if not self.fetcher:
                self.initialize_fetcher()
                
            # PRESERVE working method call: get_current_flights()
            flights_df = self.fetcher.get_current_flights()
            
            if flights_df.empty:
                logging.info("No flights found in enhanced coverage area")
                return
                
            # Track API calls
            self.api_calls_today += 1
            self.update_daily_stats(api_calls=1)
            
            # PRESERVE working method signature: calculate_noise_impact(df, coords)
            house_coords = self.collection_settings['house_coords']
            flights_with_noise = self.analyzer.calculate_noise_impact(flights_df, house_coords)
            
            # PRESERVE working method: identify_schiphol_operations
            flights_analyzed = self.analyzer.identify_schiphol_operations(flights_with_noise)
            
            # Enhanced processing: add trajectory tracking
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            flights_collected = 0
            for _, flight in flights_analyzed.iterrows():
                if flight['latitude'] is None or flight['longitude'] is None:
                    continue
                    
                # Track trajectory points per aircraft
                icao24 = flight['icao24']
                if icao24 not in self.stats['trajectory_points']:
                    self.stats['trajectory_points'][icao24] = 0
                self.stats['trajectory_points'][icao24] += 1
                
                # Determine coverage zone
                coverage_zone = self.determine_coverage_zone(flight['latitude'], flight['longitude'])
                self.stats['coverage_stats'][coverage_zone] += 1
                
                # Enhanced data insertion (PRESERVE original fields + add new ones)
                cursor.execute('''
                    INSERT INTO flights (
                        collection_time, icao24, callsign, origin_country,
                        latitude, longitude, baro_altitude, velocity, true_track, vertical_rate,
                        area_type, distance_to_house_km, estimated_noise_db, noise_impact_level,
                        schiphol_operation, approach_corridor,
                        collection_interval_minutes, points_for_aircraft, coverage_zone,
                        aircraft_type, airline, flight_number, runway, gate, aircraft_category
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    collection_time.isoformat(), flight['icao24'], flight.get('callsign', ''),
                    flight.get('origin_country', ''), flight['latitude'], flight['longitude'],
                    flight.get('baro_altitude'), flight.get('velocity'), flight.get('true_track'),
                    flight.get('vertical_rate'), flight.get('area_type', 'unknown'),
                    flight.get('distance_to_house_km', 0), flight.get('estimated_noise_db', 0),
                    flight.get('noise_impact_level', 'low'), flight.get('schiphol_operation', ''),
                    flight.get('approach_corridor', ''),
                    # Enhanced fields
                    self.collection_settings['peak_interval_minutes'], 
                    self.stats['trajectory_points'][icao24], coverage_zone,
                    flight.get('aircraft_type', ''), flight.get('airline', ''),
                    flight.get('flight_number', ''), flight.get('runway', ''),
                    flight.get('gate', ''), flight.get('aircraft_category', '')
                ))
                
                flights_collected += 1
                
                # Track enhanced statistics
                if flight.get('area_type') == 'house':
                    self.stats['flights_over_house'] += 1
                    
                if flight.get('estimated_noise_db', 0) > 65:
                    self.stats['high_noise_events'] += 1
                    
                self.stats['unique_aircraft_spotted'].add(icao24)
                
            conn.commit()
            conn.close()
            
            # Update collection stats
            self.stats['total_collections'] += 1
            self.update_daily_stats(collections=1, flights=flights_collected)
            
            # Enhanced logging
            high_quality_tracks = len([k for k, v in self.stats['trajectory_points'].items() if v >= 15])
            remaining_api = self.DAILY_API_LIMIT - self.api_calls_today
            
            logging.info(f"✅ Enhanced collection: {flights_collected} flights | "
                        f"API: {self.api_calls_today}/{self.DAILY_API_LIMIT} | "
                        f"High-quality trajectories: {high_quality_tracks} | "
                        f"Coverage - Local: {self.stats['coverage_stats']['local']}, "
                        f"Schiphol: {self.stats['coverage_stats']['schiphol']}, "
                        f"Extended: {self.stats['coverage_stats']['extended']}")
                        
        except Exception as e:
            logging.error(f"Collection error: {e}")
            
    def update_daily_stats(self, api_calls: int = 0, collections: int = 0, flights: int = 0):
        """Update daily statistics tracking"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        today = datetime.now().date().isoformat()
        cursor.execute('''
            INSERT OR REPLACE INTO daily_api_usage 
            (date, api_calls, collections, flights_collected)
            VALUES (?, 
                COALESCE((SELECT api_calls FROM daily_api_usage WHERE date = ?), 0) + ?,
                COALESCE((SELECT collections FROM daily_api_usage WHERE date = ?), 0) + ?,
                COALESCE((SELECT flights_collected FROM daily_api_usage WHERE date = ?), 0) + ?
            )
        ''', (today, today, api_calls, today, collections, today, flights))
        
        conn.commit()
        conn.close()
        
    def run(self):
        """ENHANCED run loop with preserved working patterns"""
        logging.info(f"🚀 Safe Enhanced Flight Collector starting")
        logging.info(f"📊 Enhanced 30-second collection intervals during peak hours")
        logging.info(f"🎯 Expanded coverage area: {self.collection_settings['schiphol_bounds']}")
        logging.info(f"🏠 House monitoring: {self.collection_settings['house_coords']}")
        
        self.running = True
        
        # Main collection loop with enhanced timing
        while self.running and datetime.now() < self.end_time:
            current_hour = datetime.now().hour
            
            # Enhanced scheduling - more frequent during peak hours
            if 6 <= current_hour <= 20:  # Peak hours
                interval_minutes = self.collection_settings['peak_interval_minutes']  # 0.5 = 30 seconds
            else:  # Night hours
                interval_minutes = self.collection_settings['night_interval_minutes']  # 5 minutes
                
            # Check API limits
            if self.api_calls_today >= self.DAILY_API_LIMIT:
                logging.warning("⚠️ Daily API limit reached, switching to night interval")
                interval_minutes = 10  # Slow down if hitting limits
                
            # Collect data
            self.collect_flight_data()
            
            # Wait for next collection
            time.sleep(interval_minutes * 60)
            
        logging.info("Enhanced collection completed")
        self.print_final_stats()
        
    def signal_handler(self, signum, frame):
        """PRESERVE working signal handling"""
        logging.info("\n📛 Shutdown signal received")
        self.running = False
        
    def print_final_stats(self):
        """Enhanced statistics display"""
        duration = datetime.now() - self.start_time
        
        print(f"\n=== SAFE ENHANCED COLLECTION STATISTICS ===")
        print(f"Duration: {duration}")
        print(f"Total collections: {self.stats['total_collections']}")
        print(f"API calls made: {self.api_calls_today}")
        print(f"Flights over house: {self.stats['flights_over_house']}")
        print(f"High noise events: {self.stats['high_noise_events']}")
        print(f"Unique aircraft: {len(self.stats['unique_aircraft_spotted'])}")
        
        # Enhanced trajectory statistics
        print(f"\n=== TRAJECTORY QUALITY ===")
        excellent_tracks = len([k for k, v in self.stats['trajectory_points'].items() if v >= 30])
        good_tracks = len([k for k, v in self.stats['trajectory_points'].items() if v >= 15])
        fair_tracks = len([k for k, v in self.stats['trajectory_points'].items() if v >= 5])
        
        print(f"Excellent trajectories (30+ points): {excellent_tracks}")
        print(f"Good trajectories (15+ points): {good_tracks}")
        print(f"Fair trajectories (5+ points): {fair_tracks}")
        
        print(f"\n=== COVERAGE STATISTICS ===")
        for zone, count in self.stats['coverage_stats'].items():
            print(f"{zone.title()} zone flights: {count}")

if __name__ == "__main__":
    collector = SafeEnhancedFlightCollector()
    
    # Validate expected API usage
    expected_daily = (24 * 60 / 0.5) * 0.6  # 30s intervals * 60% active (peak hours)
    print(f"📊 Expected daily API usage: ~{expected_daily:.0f} calls")
    
    if expected_daily > 3800:
        print("❌ Expected usage may exceed daily limit")
        print("🔧 Adjusting to safer intervals...")
        collector.collection_settings['peak_interval_minutes'] = 1.0  # 1 minute instead of 30s
        
    collector.run()