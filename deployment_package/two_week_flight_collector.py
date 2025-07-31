#!/usr/bin/env python3
"""
2-Week Intensive Flight Data Collector for Amsterdam Noord 1032
Optimized collection strategy to capture all flight patterns efficiently
"""
import json
import time
import schedule
import sqlite3
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
import logging
from typing import Dict, List
import os
import signal
import sys

# Import our modules
from opensky_fetcher import OpenSkyFetcher
from schiphol_analyzer import SchipholFlightAnalyzer


class TwoWeekFlightCollector:
    """Smart 2-week flight data collector optimized for pattern discovery"""
    
    def __init__(self, db_path: str = "amsterdam_flight_patterns_2week.db"):
        """Initialize the 2-week collector"""
        
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('two_week_collector.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        
        # Collection period
        self.start_time = datetime.now()
        self.end_time = self.start_time + timedelta(days=14)
        
        self.logger.info(f"🎯 2-Week Collection Period: {self.start_time.strftime('%Y-%m-%d %H:%M')} to {self.end_time.strftime('%Y-%m-%d %H:%M')}")
        
        # Database setup
        self.db_path = db_path
        self.setup_database()
        
        # Optimized collection settings for 2-week intensive study
        self.collection_settings = {
            # More frequent during peak hours, less frequent at night
            'peak_interval_minutes': 3,    # 06:00-23:00 (busy hours)
            'night_interval_minutes': 10,  # 23:00-06:00 (quiet hours)
            
            # Geographic areas (your house + Schiphol area)
            'house_coords': (52.395, 4.915),  # Amsterdam Noord 1032 center
            'local_bounds': {  # Tight focus around your area
                'lat_min': 52.37, 'lat_max': 52.42,
                'lon_min': 4.89, 'lon_max': 4.94
            },
            'schiphol_bounds': {  # Broader Schiphol operations area
                'lat_min': 52.0, 'lat_max': 52.6,
                'lon_min': 4.2, 'lon_max': 5.1
            }
        }
        
        # Initialize components
        self.fetcher = None
        self.analyzer = SchipholFlightAnalyzer()
        self.running = False
        
        # Enhanced statistics for 2-week analysis
        self.stats = {
            'total_collections': 0,
            'api_calls_made': 0,
            'flights_over_house': 0,
            'high_noise_events': 0,
            'unique_aircraft_spotted': set(),
            'start_time': self.start_time,
            'progress_percentage': 0
        }
        
        # Graceful shutdown handling
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
    
    def setup_database(self):
        """Setup optimized database for 2-week pattern analysis"""
        conn = sqlite3.connect(self.db_path)
        
        # Enhanced flights table with pattern analysis fields
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
                
                -- Enhanced analysis fields
                distance_to_house_km REAL,
                estimated_noise_db REAL,
                noise_impact_level TEXT,
                schiphol_operation TEXT,
                approach_corridor TEXT,
                aircraft_category TEXT,
                
                -- Time-based analysis
                hour_of_day INTEGER,
                day_of_week INTEGER,
                is_weekend BOOLEAN,
                time_period TEXT,  -- 'early_morning', 'morning', 'afternoon', 'evening', 'night'
                
                -- Pattern detection
                is_over_house BOOLEAN,
                is_low_altitude BOOLEAN,
                is_high_noise BOOLEAN
            )
        ''')
        
        # Optimized indexes for pattern analysis
        conn.execute('CREATE INDEX IF NOT EXISTS idx_time_analysis ON flights(collection_time, hour_of_day, day_of_week)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_house_flights ON flights(is_over_house, collection_time)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_noise_events ON flights(is_high_noise, estimated_noise_db)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_aircraft_tracking ON flights(icao24, collection_time)')
        
        # Pattern summary table
        conn.execute('''
            CREATE TABLE IF NOT EXISTS pattern_summary (
                analysis_date DATE,
                total_flights INTEGER,
                flights_over_house INTEGER,
                high_noise_events INTEGER,
                unique_aircraft INTEGER,
                busiest_hour INTEGER,
                quietest_hour INTEGER,
                peak_noise_time TEXT,
                most_common_corridor TEXT,
                weekend_vs_weekday_ratio REAL
            )
        ''')
        
        # Real-time insights table
        conn.execute('''
            CREATE TABLE IF NOT EXISTS insights (
                timestamp TIMESTAMP,
                insight_type TEXT,
                description TEXT,
                data_points INTEGER,
                confidence_level TEXT
            )
        ''')
        
        conn.commit()
        conn.close()
        self.logger.info(f"📊 Database initialized: {self.db_path}")
    
    def is_peak_hours(self) -> bool:
        """Check if current time is during peak flight hours"""
        current_hour = datetime.now().hour
        return 6 <= current_hour <= 23  # 6 AM to 11 PM
    
    def get_collection_interval(self) -> int:
        """Get appropriate collection interval based on time of day"""
        if self.is_peak_hours():
            return self.collection_settings['peak_interval_minutes']
        else:
            return self.collection_settings['night_interval_minutes']
    
    def load_credentials(self) -> Dict:
        """Load OpenSky credentials"""
        try:
            with open("credentials.json", 'r') as f:
                creds = json.load(f)
                return {
                    'client_id': creds['clientId'],
                    'client_secret': creds['clientSecret']
                }
        except Exception as e:
            self.logger.error(f"Error loading credentials: {e}")
            return {}
    
    def collect_enhanced_flight_data(self, area_type: str) -> pd.DataFrame:
        """Enhanced flight data collection with immediate analysis"""
        if not self.fetcher:
            creds = self.load_credentials()
            self.fetcher = OpenSkyFetcher(**creds)
        
        # Set appropriate bounds
        if area_type == 'local':
            self.fetcher.AMSTERDAM_NOORD_BOUNDS = self.collection_settings['local_bounds']
        else:  # schiphol
            self.fetcher.AMSTERDAM_NOORD_BOUNDS = self.collection_settings['schiphol_bounds']
        
        try:
            flights = self.fetcher.get_current_flights()
            self.stats['api_calls_made'] += 1
            
            if not flights.empty:
                # Add collection metadata
                now = datetime.now()
                flights['collection_time'] = now
                flights['area_type'] = area_type
                
                # Time-based analysis
                flights['hour_of_day'] = now.hour
                flights['day_of_week'] = now.weekday()  # 0=Monday, 6=Sunday
                flights['is_weekend'] = now.weekday() >= 5
                
                # Time period classification
                hour = now.hour
                if 5 <= hour < 9:
                    time_period = 'early_morning'
                elif 9 <= hour < 12:
                    time_period = 'morning'
                elif 12 <= hour < 17:
                    time_period = 'afternoon'
                elif 17 <= hour < 22:
                    time_period = 'evening'
                else:
                    time_period = 'night'
                flights['time_period'] = time_period
                
                # Enhanced analysis
                flights = self.enhance_with_pattern_analysis(flights)
                
                # Update real-time statistics
                over_house = len(flights[flights['is_over_house'] == True])
                high_noise = len(flights[flights['is_high_noise'] == True])
                
                self.stats['flights_over_house'] += over_house
                self.stats['high_noise_events'] += high_noise
                self.stats['unique_aircraft_spotted'].update(flights['icao24'].tolist())
                
                if over_house > 0:
                    self.logger.info(f"🏠 {over_house} flights detected over your house!")
                if high_noise > 0:
                    self.logger.info(f"🔊 {high_noise} high-noise flights detected!")
                
                self.logger.info(f"✅ Collected {len(flights)} flights ({area_type})")
                return flights
            else:
                self.logger.debug(f"No flights in {area_type} area")
                return pd.DataFrame()
                
        except Exception as e:
            self.logger.error(f"Error collecting {area_type} data: {e}")
            return pd.DataFrame()
    
    def enhance_with_pattern_analysis(self, flights: pd.DataFrame) -> pd.DataFrame:
        """Enhanced real-time pattern analysis"""
        if flights.empty:
            return flights
        
        house_coords = self.collection_settings['house_coords']
        
        # Distance calculations
        def calc_distance(row):
            if pd.isna(row['latitude']) or pd.isna(row['longitude']):
                return float('inf')
            from geopy.distance import geodesic
            return geodesic((row['latitude'], row['longitude']), house_coords).kilometers
        
        flights['distance_to_house_km'] = flights.apply(calc_distance, axis=1)
        
        # Pattern detection flags
        flights['is_over_house'] = flights['distance_to_house_km'] <= 1.0  # Within 1km of house
        flights['is_low_altitude'] = flights['baro_altitude'] < 3000  # Low altitude flights
        
        # Enhanced noise analysis
        flights = self.analyzer.calculate_noise_impact(flights, house_coords)
        flights['is_high_noise'] = flights['estimated_noise_db'] >= 65
        
        # Schiphol operations analysis
        flights = self.analyzer.identify_schiphol_operations(flights)
        
        # Aircraft classification
        if 'icao24' in flights.columns:
            classifications = flights.apply(
                lambda row: self.analyzer.classify_aircraft_by_icao(
                    row.get('icao24', ''), 
                    row.get('callsign', '')
                ), axis=1
            )
            flights['aircraft_category'] = [c.get('category', 'Unknown') for c in classifications]
        
        return flights
    
    def store_enhanced_data(self, flights: pd.DataFrame):
        """Store data with enhanced pattern analysis"""
        if flights.empty:
            return
        
        conn = sqlite3.connect(self.db_path)
        
        try:
            # Define columns for database
            db_columns = [
                'collection_time', 'icao24', 'callsign', 'origin_country',
                'latitude', 'longitude', 'baro_altitude', 'velocity', 
                'true_track', 'vertical_rate', 'area_type',
                'distance_to_house_km', 'estimated_noise_db', 'noise_impact_level',
                'schiphol_operation', 'approach_corridor', 'aircraft_category',
                'hour_of_day', 'day_of_week', 'is_weekend', 'time_period',
                'is_over_house', 'is_low_altitude', 'is_high_noise'
            ]
            
            # Ensure all required columns exist
            for col in db_columns:
                if col not in flights.columns:
                    flights[col] = None
            
            # Store to database
            flights[db_columns].to_sql('flights', conn, if_exists='append', index=False)
            
            self.logger.debug(f"📁 Stored {len(flights)} flights to database")
            
        except Exception as e:
            self.logger.error(f"Error storing data: {e}")
        finally:
            conn.close()
    
    def generate_daily_insights(self):
        """Generate daily pattern insights"""
        conn = sqlite3.connect(self.db_path)
        
        today = datetime.now().date()
        
        # Get today's flight patterns
        today_data = pd.read_sql_query('''
            SELECT * FROM flights 
            WHERE DATE(collection_time) = ?
        ''', conn, params=(today,))
        
        if not today_data.empty:
            insights = []
            
            # Flight frequency analysis
            total_flights = len(today_data)
            house_flights = len(today_data[today_data['is_over_house'] == 1])
            
            if house_flights > 0:
                insights.append({
                    'type': 'daily_summary',
                    'description': f"{house_flights} flights detected over your house today ({house_flights/total_flights*100:.1f}% of area traffic)",
                    'data_points': house_flights,
                    'confidence': 'high'
                })
            
            # Peak hours analysis
            hourly_house_flights = today_data[today_data['is_over_house'] == 1]['hour_of_day'].value_counts()
            if not hourly_house_flights.empty:
                peak_hour = hourly_house_flights.index[0]
                insights.append({
                    'type': 'peak_activity',
                    'description': f"Peak flight activity over your house: {peak_hour}:00-{peak_hour+1}:00 ({hourly_house_flights.iloc[0]} flights)",
                    'data_points': hourly_house_flights.iloc[0],
                    'confidence': 'medium'
                })
            
            # Noise impact analysis
            high_noise = today_data[today_data['is_high_noise'] == 1]
            if not high_noise.empty:
                avg_noise = high_noise['estimated_noise_db'].mean()
                insights.append({
                    'type': 'noise_analysis',
                    'description': f"{len(high_noise)} high-noise events today (avg: {avg_noise:.1f} dB)",
                    'data_points': len(high_noise),
                    'confidence': 'high'
                })
            
            # Store insights
            for insight in insights:
                conn.execute('''
                    INSERT INTO insights (timestamp, insight_type, description, data_points, confidence_level)
                    VALUES (?, ?, ?, ?, ?)
                ''', (datetime.now(), insight['type'], insight['description'], 
                      insight['data_points'], insight['confidence']))
            
            conn.commit()
        
        conn.close()
    
    def run_collection_cycle(self):
        """Run enhanced collection cycle with real-time analysis"""
        # Check if collection period is over
        if datetime.now() >= self.end_time:
            self.logger.info("🏁 2-week collection period completed!")
            self.running = False
            return
        
        # Update progress
        elapsed = datetime.now() - self.start_time
        total_duration = self.end_time - self.start_time
        progress = (elapsed.total_seconds() / total_duration.total_seconds()) * 100
        self.stats['progress_percentage'] = progress
        
        cycle_start = datetime.now()
        
        # Collect local area (your house vicinity)
        local_flights = self.collect_enhanced_flight_data('local')
        if not local_flights.empty:
            self.store_enhanced_data(local_flights)
        
        time.sleep(2)  # Rate limiting
        
        # Collect broader Schiphol area
        schiphol_flights = self.collect_enhanced_flight_data('schiphol')
        if not schiphol_flights.empty:
            self.store_enhanced_data(schiphol_flights)
        
        self.stats['total_collections'] += 1
        
        # Generate insights periodically
        if self.stats['total_collections'] % 20 == 0:  # Every 20 cycles
            self.generate_daily_insights()
        
        # Progress logging
        if self.stats['total_collections'] % 50 == 0:  # Every ~3-4 hours
            remaining = self.end_time - datetime.now()
            self.logger.info(f"📈 Progress: {progress:.1f}% | "
                           f"Remaining: {remaining.days}d {remaining.seconds//3600}h | "
                           f"Flights over house: {self.stats['flights_over_house']} | "
                           f"Unique aircraft: {len(self.stats['unique_aircraft_spotted'])}")
    
    def start_two_week_collection(self):
        """Start smart 2-week automated collection"""
        self.logger.info("🚀 Starting 2-Week Intensive Flight Collection")
        self.logger.info(f"📅 Collection period: {self.start_time.strftime('%Y-%m-%d')} to {self.end_time.strftime('%Y-%m-%d')}")
        self.logger.info(f"🏠 Monitoring area around: {self.collection_settings['house_coords']}")
        
        # Dynamic scheduling based on time of day
        def schedule_next_collection():
            interval = self.get_collection_interval()
            schedule.every(interval).minutes.do(self.run_collection_cycle)
            self.logger.debug(f"⏰ Next collection in {interval} minutes")
        
        # Initial collection
        self.run_collection_cycle()
        
        self.running = True
        
        # Smart scheduling loop
        while self.running and datetime.now() < self.end_time:
            # Clear and reschedule based on current time
            schedule.clear()
            schedule_next_collection()
            
            # Run scheduled tasks
            schedule.run_pending()
            time.sleep(30)  # Check every 30 seconds
        
        # Final summary
        self.generate_collection_summary()
        self.logger.info("✅ 2-week collection completed successfully!")
    
    def generate_collection_summary(self):
        """Generate final collection summary"""
        conn = sqlite3.connect(self.db_path)
        
        summary = {
            'collection_period': f"{self.start_time.date()} to {self.end_time.date()}",
            'total_api_calls': self.stats['api_calls_made'],
            'total_flights_recorded': conn.execute('SELECT COUNT(*) FROM flights').fetchone()[0],
            'flights_over_house': self.stats['flights_over_house'],
            'high_noise_events': self.stats['high_noise_events'],
            'unique_aircraft': len(self.stats['unique_aircraft_spotted']),
            'collection_days': (datetime.now() - self.start_time).days
        }
        
        self.logger.info("\n" + "="*60)
        self.logger.info("2-WEEK COLLECTION SUMMARY")
        self.logger.info("="*60)
        for key, value in summary.items():
            self.logger.info(f"{key}: {value}")
        self.logger.info("="*60)
        
        conn.close()
    
    def signal_handler(self, signum, frame):
        """Graceful shutdown"""
        self.logger.info(f"Received shutdown signal, finishing current cycle...")
        self.running = False


def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description="2-Week Flight Pattern Collector")
    parser.add_argument('--start', action='store_true', help='Start 2-week collection')
    parser.add_argument('--status', action='store_true', help='Show collection status')
    parser.add_argument('--test', action='store_true', help='Test single collection')
    
    args = parser.parse_args()
    
    collector = TwoWeekFlightCollector()
    
    if args.status:
        # Show current status
        print("2-Week Collection Status - Check the log file for details")
    elif args.test:
        print("Testing collection cycle...")
        collector.run_collection_cycle()
        print("Test complete!")
    elif args.start:
        print("🚀 Starting 2-week intensive flight collection...")
        print("📊 This will collect flight patterns for 14 days")
        print("⏱️  More frequent collection during peak hours (6 AM - 11 PM)")
        print("🔇 Less frequent during night hours (11 PM - 6 AM)")
        print("Press Ctrl+C to stop gracefully\n")
        
        collector.start_two_week_collection()
    else:
        parser.print_help()


if __name__ == "__main__":
    main()