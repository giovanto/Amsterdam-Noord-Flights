#!/usr/bin/env python3
"""
Enhanced Flight Collector - Optimized for Trajectory Reconstruction
Based on analysis: Option 2 (Balanced Coverage) selected
"""

import sqlite3
import signal
import sys
import schedule
import time
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
import os

# Import existing components
from opensky_fetcher import OpenSkyFetcher
from schiphol_analyzer import SchipholFlightAnalyzer

class EnhancedFlightCollector:
    """Enhanced collector with optimal frequency and expanded coverage"""
    
    def __init__(self, db_path: str = "enhanced_flight_data.db"):
        self.db_path = db_path
        self.start_time = datetime.now()
        self.end_time = self.start_time + timedelta(days=14)
        
        # ENHANCED SETTINGS - Based on our analysis
        self.collection_settings = {
            # Variable intervals based on time of day
            'schedules': {
                'peak': {
                    'hours': list(range(6, 21)),  # 6:00-20:59
                    'interval_seconds': 30,
                    'description': 'Peak hours - 30 second intervals'
                },
                'off_peak': {
                    'hours': [5, 21, 22, 23],
                    'interval_seconds': 60,
                    'description': 'Off-peak - 60 second intervals'
                },
                'night': {
                    'hours': [0, 1, 2, 3, 4],
                    'interval_seconds': 300,  # 5 minutes
                    'description': 'Night - 5 minute intervals'
                }
            },
            
            # EXPANDED GEOGRAPHIC COVERAGE
            'coverage_areas': {
                'core': {  # Amsterdam Noord - your area
                    'center': (52.385157, 4.895168),
                    'bounds': {
                        'lat_min': 52.35, 'lat_max': 52.42,
                        'lon_min': 4.84, 'lon_max': 4.95
                    }
                },
                'extended': {  # Greater Amsterdam
                    'bounds': {
                        'lat_min': 52.25, 'lat_max': 52.45,
                        'lon_min': 4.70, 'lon_max': 5.05
                    }
                },
                'full': {  # Complete coverage for trajectories
                    'bounds': {
                        'lat_min': 52.0, 'lat_max': 52.6,
                        'lon_min': 4.2, 'lon_max': 5.2
                    }
                }
            },
            
            # Approach corridors for trajectory capture
            'approach_zones': {
                'polderbaan_north': {
                    'lat_min': 52.35, 'lat_max': 52.50,
                    'lon_min': 4.65, 'lon_max': 4.85
                },
                'kaagbaan_south': {
                    'lat_min': 52.15, 'lat_max': 52.30,
                    'lon_min': 4.70, 'lon_max': 4.80
                },
                'zwanenburg_east': {
                    'lat_min': 52.28, 'lat_max': 52.38,
                    'lon_min': 4.75, 'lon_max': 5.00
                }
            }
        }
        
        # Initialize components
        self.fetcher = None
        self.analyzer = SchipholFlightAnalyzer()
        self.running = False
        
        # Enhanced statistics
        self.stats = {
            'total_collections': 0,
            'api_calls_made': 0,
            'flights_tracked': {},  # Track points per aircraft
            'coverage_stats': {area: 0 for area in self.collection_settings['coverage_areas']},
            'schedule_stats': {sched: 0 for sched in self.collection_settings['schedules']},
            'start_time': self.start_time
        }
        
        # Setup
        self.setup_logging()
        self.setup_database()
        
        # Graceful shutdown
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
        
    def setup_logging(self):
        """Configure logging"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('enhanced_collector.log'),
                logging.StreamHandler()
            ]
        )
        
    def setup_database(self):
        """Enhanced database schema for trajectory analysis"""
        conn = sqlite3.connect(self.db_path)
        
        # Enhanced schema with trajectory support
        conn.execute('''
            CREATE TABLE IF NOT EXISTS flights (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                collection_time TIMESTAMP NOT NULL,
                collection_schedule TEXT NOT NULL,
                
                -- Aircraft identification
                icao24 TEXT NOT NULL,
                callsign TEXT,
                
                -- Position data (critical for trajectories)
                latitude REAL,
                longitude REAL,
                baro_altitude REAL,
                
                -- Movement data
                velocity REAL,
                true_track REAL,
                vertical_rate REAL,
                
                -- Coverage zones
                in_core_zone BOOLEAN,
                in_extended_zone BOOLEAN,
                in_approach_zone TEXT,
                
                -- Analysis fields
                distance_to_house_km REAL,
                estimated_noise_db REAL,
                
                -- Trajectory support
                points_collected INTEGER,  -- Running count per aircraft
                time_since_last_seen INTEGER  -- Seconds since last observation
            )
        ''')
        
        # Create indexes separately
        conn.execute('CREATE INDEX IF NOT EXISTS idx_icao24_time ON flights (icao24, collection_time)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_collection_time ON flights (collection_time)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_noise ON flights (estimated_noise_db)')
        
        # Aircraft tracking table
        conn.execute('''
            CREATE TABLE IF NOT EXISTS aircraft_tracks (
                icao24 TEXT PRIMARY KEY,
                first_seen TIMESTAMP,
                last_seen TIMESTAMP,
                total_points INTEGER,
                avg_interval_seconds REAL,
                trajectory_quality TEXT  -- 'excellent', 'good', 'fair', 'poor'
            )
        ''')
        
        conn.commit()
        conn.close()
        
    def get_current_schedule(self) -> Dict:
        """Determine which schedule to use based on current hour"""
        current_hour = datetime.now().hour
        
        for schedule_name, schedule in self.collection_settings['schedules'].items():
            if current_hour in schedule['hours']:
                return {
                    'name': schedule_name,
                    'interval': schedule['interval_seconds'],
                    'description': schedule['description']
                }
        
        # Default to off-peak if not in any schedule
        return {
            'name': 'off_peak',
            'interval': 60,
            'description': 'Default off-peak schedule'
        }
        
    def check_coverage_zone(self, lat: float, lon: float) -> Dict[str, bool]:
        """Check which coverage zones a position falls into"""
        zones = {}
        
        # Check each zone
        for zone_name, zone_config in self.collection_settings['coverage_areas'].items():
            bounds = zone_config['bounds']
            zones[zone_name] = (
                bounds['lat_min'] <= lat <= bounds['lat_max'] and
                bounds['lon_min'] <= lon <= bounds['lon_max']
            )
            
        # Check approach zones
        approach_zone = None
        for zone_name, bounds in self.collection_settings['approach_zones'].items():
            if (bounds['lat_min'] <= lat <= bounds['lat_max'] and
                bounds['lon_min'] <= lon <= bounds['lon_max']):
                approach_zone = zone_name
                break
                
        return {
            'in_core': zones.get('core', False),
            'in_extended': zones.get('extended', False),
            'in_full': zones.get('full', True),
            'approach_zone': approach_zone
        }
        
    def update_aircraft_tracking(self, conn: sqlite3.Connection, icao24: str, 
                               collection_time: datetime):
        """Update aircraft tracking statistics"""
        cursor = conn.cursor()
        
        # Get existing track info
        cursor.execute('''
            SELECT first_seen, last_seen, total_points 
            FROM aircraft_tracks 
            WHERE icao24 = ?
        ''', (icao24,))
        
        result = cursor.fetchone()
        
        if result:
            first_seen, last_seen_str, total_points = result
            last_seen = datetime.fromisoformat(last_seen_str)
            
            # Calculate interval
            interval_seconds = (collection_time - last_seen).total_seconds()
            
            # Update tracking
            cursor.execute('''
                UPDATE aircraft_tracks 
                SET last_seen = ?, 
                    total_points = total_points + 1,
                    avg_interval_seconds = 
                        (avg_interval_seconds * ? + ?) / (? + 1)
                WHERE icao24 = ?
            ''', (collection_time.isoformat(), total_points, interval_seconds, 
                  total_points, icao24))
            
            return total_points + 1, interval_seconds
        else:
            # New aircraft
            cursor.execute('''
                INSERT INTO aircraft_tracks 
                (icao24, first_seen, last_seen, total_points, avg_interval_seconds, trajectory_quality)
                VALUES (?, ?, ?, 1, 0, 'poor')
            ''', (icao24, collection_time.isoformat(), collection_time.isoformat()))
            
            return 1, 0
            
    def assess_trajectory_quality(self, points: int, avg_interval: float) -> str:
        """Assess trajectory reconstruction quality"""
        if points >= 30 and avg_interval <= 35:
            return 'excellent'
        elif points >= 20 and avg_interval <= 60:
            return 'good'
        elif points >= 10 and avg_interval <= 120:
            return 'fair'
        else:
            return 'poor'
            
    def collect_data(self):
        """Enhanced data collection with trajectory support"""
        schedule = self.get_current_schedule()
        collection_time = datetime.now()
        
        logging.info(f"📡 Collection starting - Schedule: {schedule['description']}")
        
        try:
            # Get flight data for expanded area
            bounds = self.collection_settings['coverage_areas']['full']['bounds']
            flights = self.fetcher.get_flights_in_area(
                bounds['lat_min'], bounds['lat_max'],
                bounds['lon_min'], bounds['lon_max']
            )
            
            if not flights:
                logging.warning("No flights received")
                return
                
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            flights_collected = 0
            trajectory_updates = 0
            
            for flight in flights:
                if flight.latitude is None or flight.longitude is None:
                    continue
                    
                # Check coverage zones
                zones = self.check_coverage_zone(flight.latitude, flight.longitude)
                
                # Update aircraft tracking
                points_collected, time_gap = self.update_aircraft_tracking(
                    conn, flight.icao24, collection_time
                )
                
                # Calculate noise (if in range)
                noise_data = self.analyzer.calculate_noise_impact(
                    flight.latitude, flight.longitude, 
                    flight.baro_altitude or flight.geo_altitude or 0
                )
                
                # Insert flight data
                cursor.execute('''
                    INSERT INTO flights (
                        collection_time, collection_schedule, icao24, callsign,
                        latitude, longitude, baro_altitude, velocity, 
                        true_track, vertical_rate, in_core_zone, in_extended_zone,
                        in_approach_zone, distance_to_house_km, estimated_noise_db,
                        points_collected, time_since_last_seen
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    collection_time.isoformat(), schedule['name'],
                    flight.icao24, flight.callsign, flight.latitude, flight.longitude,
                    flight.baro_altitude, flight.velocity, flight.true_track,
                    flight.vertical_rate, zones['in_core'], zones['in_extended'],
                    zones['approach_zone'], noise_data['distance_km'],
                    noise_data['estimated_db'], points_collected, int(time_gap)
                ))
                
                flights_collected += 1
                
                # Update stats
                if flight.icao24 not in self.stats['flights_tracked']:
                    self.stats['flights_tracked'][flight.icao24] = 0
                self.stats['flights_tracked'][flight.icao24] += 1
                
            # Update trajectory quality assessments
            cursor.execute('''
                UPDATE aircraft_tracks 
                SET trajectory_quality = 
                    CASE 
                        WHEN total_points >= 30 AND avg_interval_seconds <= 35 THEN 'excellent'
                        WHEN total_points >= 20 AND avg_interval_seconds <= 60 THEN 'good'
                        WHEN total_points >= 10 AND avg_interval_seconds <= 120 THEN 'fair'
                        ELSE 'poor'
                    END
            ''')
            
            conn.commit()
            conn.close()
            
            # Update statistics
            self.stats['total_collections'] += 1
            self.stats['api_calls_made'] += 2  # OpenSky + Schiphol
            self.stats['schedule_stats'][schedule['name']] += 1
            
            # Log summary
            high_quality_tracks = len([k for k, v in self.stats['flights_tracked'].items() if v >= 20])
            logging.info(f"✅ Collected {flights_collected} flights | "
                        f"High-quality tracks: {high_quality_tracks} | "
                        f"API calls today: {self.stats['api_calls_made']}")
            
        except Exception as e:
            logging.error(f"Collection error: {e}")
            
    def calculate_daily_api_usage(self) -> int:
        """Calculate expected daily API usage with current schedule"""
        daily_calls = 0
        
        for schedule_name, schedule in self.collection_settings['schedules'].items():
            hours = len(schedule['hours'])
            collections_per_hour = 3600 / schedule['interval_seconds']
            # 2 API calls per collection (OpenSky + Schiphol)
            daily_calls += hours * collections_per_hour * 2
            
        return int(daily_calls)
        
    def run(self):
        """Run enhanced collector with dynamic scheduling"""
        logging.info(f"🚀 Enhanced Flight Collector starting")
        logging.info(f"📊 Expected daily API usage: {self.calculate_daily_api_usage()} calls")
        logging.info(f"🎯 Collection area: 52.0-52.6°N, 4.2-5.2°E")
        
        # Initialize OpenSky connection
        self.fetcher = OpenSkyFetcher()
        if not self.fetcher.authenticate():
            logging.error("Failed to authenticate with OpenSky")
            return
            
        self.running = True
        
        # Initial collection
        self.collect_data()
        
        # Dynamic scheduling loop
        while self.running and datetime.now() < self.end_time:
            schedule = self.get_current_schedule()
            
            # Wait for next collection
            time.sleep(schedule['interval'])
            
            # Collect if still running
            if self.running:
                self.collect_data()
                
        logging.info("Collection completed")
        self.print_final_stats()
        
    def signal_handler(self, signum, frame):
        """Handle graceful shutdown"""
        logging.info("\n📛 Shutdown signal received")
        self.running = False
        
    def print_final_stats(self):
        """Print comprehensive statistics"""
        duration = datetime.now() - self.start_time
        
        print("\n=== ENHANCED COLLECTION STATISTICS ===")
        print(f"Duration: {duration}")
        print(f"Total collections: {self.stats['total_collections']}")
        print(f"Total API calls: {self.stats['api_calls_made']}")
        print(f"Unique aircraft tracked: {len(self.stats['flights_tracked'])}")
        
        # Trajectory quality breakdown
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT trajectory_quality, COUNT(*) 
            FROM aircraft_tracks 
            GROUP BY trajectory_quality
        ''')
        
        print("\nTrajectory Quality:")
        for quality, count in cursor.fetchall():
            print(f"  {quality}: {count} aircraft")
            
        # Best tracked aircraft
        cursor.execute('''
            SELECT icao24, total_points, avg_interval_seconds 
            FROM aircraft_tracks 
            ORDER BY total_points DESC 
            LIMIT 10
        ''')
        
        print("\nBest Tracked Aircraft:")
        for icao24, points, interval in cursor.fetchall():
            print(f"  {icao24}: {points} points, {interval:.0f}s avg interval")
            
        conn.close()

if __name__ == "__main__":
    collector = EnhancedFlightCollector()
    collector.run()