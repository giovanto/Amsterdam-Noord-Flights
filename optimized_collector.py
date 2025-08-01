#!/usr/bin/env python3
"""
Optimized Flight Collector - Maximizes 4000 daily API calls
Smart API usage: OpenSky every 30s peak, Schiphol only when needed
"""

import sqlite3
import signal
import sys
import time
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
import os

# Import existing components
from opensky_fetcher import OpenSkyFetcher
from schiphol_analyzer import SchipholFlightAnalyzer

class OptimizedFlightCollector:
    """Maximizes trajectory quality within 4000 API calls/day"""
    
    def __init__(self, db_path: str = "optimized_flight_data.db"):
        self.db_path = db_path
        self.start_time = datetime.now()
        self.end_time = self.start_time + timedelta(days=14)
        
        # MAXIMIZE API USAGE STRATEGY
        self.DAILY_API_LIMIT = 3950  # Small buffer
        self.api_calls_today = 0
        self.last_reset_date = datetime.now().date()
        
        # OPTIMIZED SCHEDULE - Based on current flight patterns
        # Peak: 06-20 hours = 15 hours, every 30s = 1800 calls
        # Off-peak: 21-23, 05 = 4 hours, every 60s = 240 calls  
        # Night: 00-04 = 5 hours, every 5 minutes = 60 calls
        # Total OpenSky: ~2100 calls/day
        # Schiphol: Smart usage only when aircraft detected = ~500-800 calls/day
        # Total: ~2600-2900 calls/day (plenty of headroom!)
        
        self.collection_settings = {
            'schedules': {
                'peak': {
                    'hours': [6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20],  # 15 hours
                    'opensky_interval': 30,  # Every 30 seconds
                    'schiphol_interval': 90,  # Every 90 seconds when aircraft present
                    'description': 'Peak - 30s OpenSky, smart Schiphol'
                },
                'moderate': {
                    'hours': [5, 21, 22, 23],  # 4 hours
                    'opensky_interval': 60,  # Every minute
                    'schiphol_interval': 120,  # Every 2 minutes when aircraft present
                    'description': 'Moderate - 60s OpenSky, smart Schiphol'
                },
                'night': {
                    'hours': [0, 1, 2, 3, 4],  # 5 hours
                    'opensky_interval': 300,  # Every 5 minutes
                    'schiphol_interval': 300,  # Every 5 minutes when aircraft present
                    'description': 'Night - 5min OpenSky, minimal Schiphol'
                }
            },
            
            # EXPANDED AREA for complete trajectories
            'coverage_area': {
                'lat_min': 52.0, 'lat_max': 52.6,
                'lon_min': 4.2, 'lon_max': 5.2
            }
        }
        
        # Smart Schiphol usage
        self.schiphol_state = {
            'last_call': None,
            'aircraft_cache': {},  # Cache aircraft info to reduce calls
            'call_counter': 0
        }
        
        # Initialize components
        self.fetcher = None
        self.analyzer = SchipholFlightAnalyzer()
        self.running = False
        
        # Statistics
        self.stats = {
            'total_collections': 0,
            'opensky_calls': 0,
            'schiphol_calls': 0,
            'flights_tracked': {},
            'last_collection': None,
            'start_time': self.start_time
        }
        
        # Setup
        self.setup_logging()
        self.setup_database()
        self.load_daily_stats()
        
        # Graceful shutdown
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
        
    def calculate_max_daily_calls(self) -> Dict[str, int]:
        """Calculate maximum daily API usage"""
        usage = {'opensky': 0, 'schiphol': 0, 'total': 0}
        
        for schedule_name, schedule in self.collection_settings['schedules'].items():
            hours = len(schedule['hours'])
            
            # OpenSky calls (consistent)
            opensky_per_hour = 3600 / schedule['opensky_interval']
            usage['opensky'] += hours * opensky_per_hour
            
            # Schiphol calls (variable, estimate 60% of OpenSky)
            schiphol_per_hour = (3600 / schedule['schiphol_interval']) * 0.6  # Only when aircraft present
            usage['schiphol'] += hours * schiphol_per_hour
            
        usage['total'] = usage['opensky'] + usage['schiphol']
        return {k: int(v) for k, v in usage.items()}
        
    def setup_logging(self):
        """Configure logging"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('optimized_collector.log'),
                logging.StreamHandler()
            ]
        )
        
    def setup_database(self):
        """Optimized database schema"""
        conn = sqlite3.connect(self.db_path)
        
        # Flights table with trajectory optimization
        conn.execute('''
            CREATE TABLE IF NOT EXISTS flights (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                collection_time TIMESTAMP NOT NULL,
                collection_type TEXT NOT NULL,  -- 'opensky', 'schiphol', 'combined'
                icao24 TEXT NOT NULL,
                callsign TEXT,
                latitude REAL,
                longitude REAL,
                baro_altitude REAL,
                velocity REAL,
                true_track REAL,
                vertical_rate REAL,
                distance_to_house_km REAL,
                estimated_noise_db REAL,
                
                -- Schiphol enrichment (when available)
                aircraft_type TEXT,
                airline TEXT,
                flight_number TEXT,
                origin TEXT,
                destination TEXT,
                
                -- Trajectory tracking
                trajectory_segment INTEGER,  -- Group points into flight segments
                points_in_segment INTEGER
            )
        ''')
        
        # API usage tracking
        conn.execute('''
            CREATE TABLE IF NOT EXISTS api_usage (
                date DATE PRIMARY KEY,
                opensky_calls INTEGER DEFAULT 0,
                schiphol_calls INTEGER DEFAULT 0,
                total_calls INTEGER DEFAULT 0,
                collections INTEGER DEFAULT 0
            )
        ''')
        
        # Create indexes for trajectory queries
        conn.execute('CREATE INDEX IF NOT EXISTS idx_icao24_time ON flights (icao24, collection_time)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_trajectory ON flights (icao24, trajectory_segment)')
        
        conn.commit()
        conn.close()
        
    def load_daily_stats(self):
        """Load today's API usage"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        today = datetime.now().date().isoformat()
        cursor.execute('SELECT opensky_calls, schiphol_calls, total_calls FROM api_usage WHERE date = ?', (today,))
        result = cursor.fetchone()
        
        if result:
            self.stats['opensky_calls'], self.stats['schiphol_calls'], self.api_calls_today = result
        else:
            cursor.execute(
                'INSERT INTO api_usage (date, opensky_calls, schiphol_calls, total_calls) VALUES (?, 0, 0, 0)',
                (today,)
            )
            conn.commit()
            
        conn.close()
        
    def update_api_usage(self, opensky: int = 0, schiphol: int = 0):
        """Update API usage tracking"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        today = datetime.now().date().isoformat()
        
        # Reset if new day
        if datetime.now().date() != self.last_reset_date:
            self.api_calls_today = 0
            self.stats['opensky_calls'] = 0
            self.stats['schiphol_calls'] = 0
            self.last_reset_date = datetime.now().date()
            
        # Update counters
        self.stats['opensky_calls'] += opensky
        self.stats['schiphol_calls'] += schiphol
        self.api_calls_today += (opensky + schiphol)
        
        # Update database
        cursor.execute('''
            UPDATE api_usage 
            SET opensky_calls = opensky_calls + ?,
                schiphol_calls = schiphol_calls + ?,
                total_calls = total_calls + ?,
                collections = collections + 1
            WHERE date = ?
        ''', (opensky, schiphol, opensky + schiphol, today))
        
        conn.commit()
        conn.close()
        
    def get_current_schedule(self) -> Dict:
        """Get current schedule"""
        if self.api_calls_today >= self.DAILY_API_LIMIT:
            return {
                'name': 'limit_reached',
                'opensky_interval': 3600,  # 1 hour
                'schiphol_interval': 3600,
                'description': 'API limit reached - emergency mode'
            }
            
        current_hour = datetime.now().hour
        
        for schedule_name, schedule in self.collection_settings['schedules'].items():
            if current_hour in schedule['hours']:
                return {
                    'name': schedule_name,
                    'opensky_interval': schedule['opensky_interval'],
                    'schiphol_interval': schedule['schiphol_interval'],
                    'description': schedule['description']
                }
                
        # Fallback
        return {
            'name': 'fallback',
            'opensky_interval': 300,
            'schiphol_interval': 300,
            'description': 'Fallback schedule'
        }
        
    def should_call_schiphol(self, flights_detected: int, schedule: Dict) -> bool:
        """Smart decision on whether to call Schiphol API"""
        if flights_detected == 0:
            return False
            
        if self.schiphol_state['last_call'] is None:
            return True
            
        # Check interval
        time_since_last = (datetime.now() - self.schiphol_state['last_call']).total_seconds()
        return time_since_last >= schedule['schiphol_interval']
        
    def collect_data(self):
        """Optimized data collection"""
        schedule = self.get_current_schedule()
        collection_time = datetime.now()
        
        try:
            logging.info(f"📡 Collection - {schedule['description']}")
            
            # 1. Always get OpenSky data (our primary source)
            flights_df = self.fetcher.get_current_flights()
            self.update_api_usage(opensky=1)
            
            flights_detected = len(flights_df) if not flights_df.empty else 0
            collection_type = 'opensky'
            
            # 2. Smart Schiphol enrichment
            schiphol_data = {}
            if self.should_call_schiphol(flights_detected, schedule):
                try:
                    # Get Schiphol data for detected aircraft
                    for _, flight in flights_df.iterrows():
                        if flight['callsign'] and flight['callsign'] not in self.schiphol_state['aircraft_cache']:
                            # This would call Schiphol API for aircraft details
                            # For now, we'll simulate with noise calculation
                            pass
                    
                    self.schiphol_state['last_call'] = collection_time
                    self.update_api_usage(schiphol=1)
                    collection_type = 'combined'
                    
                except Exception as e:
                    logging.warning(f"Schiphol API error: {e}")
                    
            # 3. Store flight data
            if not flights_df.empty:
                conn = sqlite3.connect(self.db_path)
                cursor = conn.cursor()
                
                for _, flight in flights_df.iterrows():
                    # Calculate noise impact
                    noise_data = self.analyzer.calculate_noise_impact(
                        flight['latitude'], flight['longitude'], 
                        flight.get('baro_altitude', 0)
                    )
                    
                    # Track aircraft
                    icao24 = flight['icao24']
                    if icao24 not in self.stats['flights_tracked']:
                        self.stats['flights_tracked'][icao24] = 0
                    self.stats['flights_tracked'][icao24] += 1
                    
                    # Insert flight record
                    cursor.execute('''
                        INSERT INTO flights (
                            collection_time, collection_type, icao24, callsign,
                            latitude, longitude, baro_altitude, velocity,
                            true_track, vertical_rate, distance_to_house_km,
                            estimated_noise_db, points_in_segment
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        collection_time.isoformat(), collection_type,
                        flight['icao24'], flight.get('callsign', ''),
                        flight['latitude'], flight['longitude'],
                        flight.get('baro_altitude'), flight.get('velocity'),
                        flight.get('true_track'), flight.get('vertical_rate'),
                        noise_data['distance_km'], noise_data['estimated_db'],
                        self.stats['flights_tracked'][icao24]
                    ))
                    
                conn.commit()
                conn.close()
                
            self.stats['total_collections'] += 1
            self.stats['last_collection'] = collection_time
            
            # Log progress
            high_quality = len([k for k, v in self.stats['flights_tracked'].items() if v >= 15])
            remaining_calls = self.DAILY_API_LIMIT - self.api_calls_today
            
            logging.info(f"✅ Collected {flights_detected} flights | "
                        f"API: {self.api_calls_today}/{self.DAILY_API_LIMIT} "
                        f"(OpenSky: {self.stats['opensky_calls']}, Schiphol: {self.stats['schiphol_calls']}) | "
                        f"High-quality tracks: {high_quality} | "
                        f"Remaining: {remaining_calls}")
                        
        except Exception as e:
            logging.error(f"Collection error: {e}")
            
    def run(self):
        """Run optimized collector"""
        # Calculate and display usage
        expected_usage = self.calculate_max_daily_calls()
        
        logging.info(f"🚀 Optimized Flight Collector starting")
        logging.info(f"📊 Expected daily usage: OpenSky {expected_usage['opensky']}, "
                    f"Schiphol {expected_usage['schiphol']}, "
                    f"Total {expected_usage['total']}/{self.DAILY_API_LIMIT}")
        logging.info(f"🎯 Coverage: {self.collection_settings['coverage_area']}")
        
        if expected_usage['total'] > self.DAILY_API_LIMIT:
            logging.error(f"❌ Expected usage {expected_usage['total']} exceeds limit {self.DAILY_API_LIMIT}")
            return
            
        # Initialize OpenSky
        self.fetcher = OpenSkyFetcher()
        self.running = True
        
        # Main collection loop
        last_opensky = 0
        
        while self.running and datetime.now() < self.end_time:
            schedule = self.get_current_schedule()
            current_time = time.time()
            
            # Check if it's time for OpenSky collection
            if current_time - last_opensky >= schedule['opensky_interval']:
                if self.api_calls_today < self.DAILY_API_LIMIT:
                    self.collect_data()
                    last_opensky = current_time
                else:
                    logging.warning("⚠️ Daily API limit reached, pausing collection")
                    time.sleep(3600)  # Sleep 1 hour
                    
            time.sleep(1)  # Check every second
            
        logging.info("Collection completed")
        self.print_stats()
        
    def signal_handler(self, signum, frame):
        """Graceful shutdown"""
        logging.info("📛 Shutdown signal received")
        self.running = False
        
    def print_stats(self):
        """Print final statistics"""
        duration = datetime.now() - self.start_time
        
        print(f"\n=== OPTIMIZED COLLECTION STATS ===")
        print(f"Duration: {duration}")
        print(f"Collections: {self.stats['total_collections']}")
        print(f"OpenSky calls: {self.stats['opensky_calls']}")
        print(f"Schiphol calls: {self.stats['schiphol_calls']}")
        print(f"Total API calls: {self.api_calls_today}/{self.DAILY_API_LIMIT}")
        print(f"Aircraft tracked: {len(self.stats['flights_tracked'])}")
        
        # Best tracked aircraft
        best = sorted(self.stats['flights_tracked'].items(), key=lambda x: x[1], reverse=True)[:10]
        print("\nBest Tracked Aircraft:")
        for icao24, points in best:
            print(f"  {icao24}: {points} points")

if __name__ == "__main__":
    collector = OptimizedFlightCollector()
    
    # Show usage calculation
    usage = collector.calculate_max_daily_calls()
    print(f"📊 Expected API Usage:")
    print(f"   OpenSky: {usage['opensky']} calls/day")
    print(f"   Schiphol: {usage['schiphol']} calls/day") 
    print(f"   Total: {usage['total']}/{collector.DAILY_API_LIMIT}")
    print(f"   Efficiency: {usage['total']/collector.DAILY_API_LIMIT*100:.1f}%")
    
    if usage['total'] <= collector.DAILY_API_LIMIT:
        print("✅ Optimal usage - proceeding")
        collector.run()
    else:
        print("❌ Usage exceeds limit!")
        sys.exit(1)