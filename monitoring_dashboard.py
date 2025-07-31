#!/usr/bin/env python3
"""
Aviation Data Collection Monitoring Dashboard
Real-time monitoring and validation for the 2-week collection period

Author: Giovanni Antoniazzi (Studio Bereikbaar)
Purpose: Maintain data quality and system health during collection
"""

import psycopg2
import psycopg2.extras
import sqlite3
from datetime import datetime, timedelta
import json
import time
from typing import Dict, List
import sys

class AviationMonitor:
    """Real-time monitoring for aviation data collection"""
    
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
    
    def get_collection_status(self) -> Dict:
        """Get current collection status from SQLite"""
        try:
            conn = sqlite3.connect(self.sqlite_path)
            cursor = conn.cursor()
            
            # Basic stats
            cursor.execute("""
                SELECT 
                    COUNT(*) as total_flights,
                    MIN(collection_time) as first_collection,
                    MAX(collection_time) as last_collection,
                    COUNT(DISTINCT DATE(collection_time)) as collection_days
                FROM flights
            """)
            basic_stats = cursor.fetchone()
            
            # Recent collection rate (last 2 hours)
            cursor.execute("""
                SELECT COUNT(*) as recent_flights
                FROM flights 
                WHERE collection_time > datetime('now', '-2 hours')
            """)
            recent_count = cursor.fetchone()[0]
            
            # Data quality indicators
            cursor.execute("""
                SELECT 
                    COUNT(*) as total,
                    SUM(CASE WHEN latitude IS NULL OR longitude IS NULL THEN 1 ELSE 0 END) as missing_coords,
                    SUM(CASE WHEN estimated_noise_db IS NULL THEN 1 ELSE 0 END) as missing_noise,
                    SUM(CASE WHEN is_high_noise = 1 THEN 1 ELSE 0 END) as high_noise_flights,
                    SUM(CASE WHEN is_over_house = 1 THEN 1 ELSE 0 END) as over_house_flights
                FROM flights
                WHERE collection_time > datetime('now', '-24 hours')
            """)
            quality_stats = cursor.fetchone()
            
            conn.close()
            
            return {
                'collection_status': {
                    'total_flights': basic_stats[0],
                    'first_collection': basic_stats[1],
                    'last_collection': basic_stats[2],
                    'collection_days': basic_stats[3],
                    'recent_flights_2h': recent_count,
                    'collection_rate_per_hour': recent_count / 2 if recent_count else 0
                },
                'data_quality': {
                    'total_24h': quality_stats[0],
                    'missing_coordinates_pct': (quality_stats[1] / quality_stats[0] * 100) if quality_stats[0] else 0,
                    'missing_noise_pct': (quality_stats[2] / quality_stats[0] * 100) if quality_stats[0] else 0,
                    'high_noise_flights': quality_stats[3],
                    'over_house_flights': quality_stats[4]
                }
            }
            
        except Exception as e:
            return {'error': f"SQLite connection failed: {e}"}
    
    def get_postgresql_status(self) -> Dict:
        """Get PostgreSQL analysis database status"""
        try:
            conn = psycopg2.connect(**self.pg_params)
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            
            # Check if tables exist and get record counts
            cursor.execute("""
                SELECT 
                    schemaname, 
                    tablename,
                    n_tup_ins as inserts,
                    n_tup_upd as updates,
                    n_tup_del as deletes
                FROM pg_stat_user_tables 
                WHERE schemaname IN ('flight_data', 'analysis', 'monitoring')
                ORDER BY schemaname, tablename
            """)
            table_stats = cursor.fetchall()
            
            # Get flight data count
            try:
                cursor.execute("SELECT COUNT(*) FROM flight_data.flights")
                flight_count = cursor.fetchone()['count']
            except:
                flight_count = 0
                
            # Get latest monitoring stats
            try:
                cursor.execute("""
                    SELECT * FROM monitoring.collection_stats 
                    ORDER BY collection_date DESC 
                    LIMIT 1
                """)
                latest_stats = cursor.fetchone()
            except:
                latest_stats = None
            
            cursor.close()
            conn.close()
            
            return {
                'postgresql_status': {
                    'connection': 'success',
                    'flight_records': flight_count,
                    'table_statistics': [dict(row) for row in table_stats],
                    'latest_monitoring': dict(latest_stats) if latest_stats else None
                }
            }
            
        except Exception as e:
            return {'postgresql_status': {'connection': 'failed', 'error': str(e)}}
    
    def validate_data_pipeline(self) -> Dict:
        """Validate the data pipeline is working correctly"""
        sqlite_status = self.get_collection_status()
        pg_status = self.get_postgresql_status()
        
        # Calculate pipeline health
        health_score = 100
        issues = []
        
        if 'error' in sqlite_status:
            health_score -= 50
            issues.append("SQLite connection failed")
            
        if pg_status['postgresql_status']['connection'] == 'failed':
            health_score -= 30
            issues.append("PostgreSQL connection failed")
        
        # Check collection rate
        if 'collection_status' in sqlite_status:
            rate = sqlite_status['collection_status']['collection_rate_per_hour']
            if rate < 100:  # Expect ~500 flights/hour
                health_score -= 20
                issues.append(f"Low collection rate: {rate:.1f} flights/hour")
        
        # Check data quality
        if 'data_quality' in sqlite_status:
            quality = sqlite_status['data_quality']
            if quality['missing_coordinates_pct'] > 5:
                health_score -= 10
                issues.append(f"High missing coordinates: {quality['missing_coordinates_pct']:.1f}%")
        
        return {
            'pipeline_health': {
                'score': max(0, health_score),
                'status': 'healthy' if health_score >= 80 else 'warning' if health_score >= 50 else 'critical',
                'issues': issues
            },
            'sqlite_status': sqlite_status,
            'postgresql_status': pg_status,
            'timestamp': datetime.now().isoformat()
        }
    
    def generate_daily_report(self) -> Dict:
        """Generate comprehensive daily report"""
        status = self.validate_data_pipeline()
        
        report = {
            'report_date': datetime.now().strftime('%Y-%m-%d'),
            'system_health': status['pipeline_health'],
            'collection_summary': status.get('sqlite_status', {}),
            'database_status': status.get('postgresql_status', {}),
            'recommendations': []
        }
        
        # Generate recommendations based on status
        if status['pipeline_health']['score'] < 80:
            report['recommendations'].append("System health below optimal - investigate issues")
            
        if 'collection_status' in status.get('sqlite_status', {}):
            rate = status['sqlite_status']['collection_status']['collection_rate_per_hour']
            if rate > 0:
                daily_projection = rate * 24
                report['recommendations'].append(f"Daily projection: {daily_projection:.0f} flights")
                
                if daily_projection > 10000:
                    report['recommendations'].append("High volume - monitor database performance")
        
        return report
    
    def print_status_dashboard(self):
        """Print formatted status dashboard"""
        status = self.validate_data_pipeline()
        
        print("\n" + "="*60)
        print("🛩️  AVIATION DATA COLLECTION MONITOR")
        print("="*60)
        print(f"⏰ Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Pipeline Health
        health = status['pipeline_health']
        health_emoji = "✅" if health['score'] >= 80 else "⚠️" if health['score'] >= 50 else "❌"
        print(f"{health_emoji} Pipeline Health: {health['score']}/100 ({health['status'].upper()})")
        
        if health['issues']:
            print("   Issues:")
            for issue in health['issues']:
                print(f"   - {issue}")
        
        # Collection Status
        if 'collection_status' in status.get('sqlite_status', {}):
            collection = status['sqlite_status']['collection_status']
            print(f"\n📊 Collection Status:")
            print(f"   Total Flights: {collection['total_flights']:,}")
            print(f"   Collection Days: {collection['collection_days']}")
            print(f"   Recent Rate: {collection['collection_rate_per_hour']:.1f} flights/hour")
            print(f"   Last Collection: {collection['last_collection']}")
        
        # Data Quality
        if 'data_quality' in status.get('sqlite_status', {}):
            quality = status['sqlite_status']['data_quality']
            print(f"\n🔍 Data Quality (24h):")
            print(f"   Total Records: {quality['total_24h']:,}")
            print(f"   Missing Coordinates: {quality['missing_coordinates_pct']:.1f}%")
            print(f"   High Noise Events: {quality['high_noise_flights']}")
            print(f"   Over House Events: {quality['over_house_flights']}")
        
        # PostgreSQL Status
        pg_status = status.get('postgresql_status', {})
        if 'flight_records' in pg_status.get('postgresql_status', {}):
            pg_flights = pg_status['postgresql_status']['flight_records']
            print(f"\n🗄️  PostgreSQL Analysis DB:")
            print(f"   Flight Records: {pg_flights:,}")
            print(f"   Connection: {pg_status['postgresql_status']['connection']}")
        
        print("="*60)

def main():
    """Command line interface for monitoring"""
    monitor = AviationMonitor()
    
    if len(sys.argv) > 1 and sys.argv[1] == '--watch':
        # Continuous monitoring mode
        print("Starting continuous monitoring (Ctrl+C to stop)...")
        try:
            while True:
                monitor.print_status_dashboard()
                time.sleep(30)  # Update every 30 seconds
        except KeyboardInterrupt:
            print("\nMonitoring stopped.")
            
    elif len(sys.argv) > 1 and sys.argv[1] == '--report':
        # Generate daily report
        report = monitor.generate_daily_report()
        print(json.dumps(report, indent=2))
        
    else:
        # Single status check
        monitor.print_status_dashboard()

if __name__ == "__main__":
    main()