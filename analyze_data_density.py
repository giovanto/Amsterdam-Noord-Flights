#!/usr/bin/env python3
"""
Analyze data point density per aircraft to determine trajectory reconstruction feasibility
"""

import psycopg2
import pandas as pd
from datetime import datetime, timedelta

pg_params = {
    'host': '172.17.0.1',
    'user': 'gio',
    'password': 'alpinism',
    'database': 'aviation_impact_analysis'
}

def analyze_aircraft_coverage():
    conn = psycopg2.connect(**pg_params)
    
    # Query 1: Points per aircraft
    query_coverage = """
    SELECT 
        icao24,
        callsign,
        COUNT(*) as data_points,
        MIN(collection_time) as first_seen,
        MAX(collection_time) as last_seen,
        EXTRACT(EPOCH FROM (MAX(collection_time) - MIN(collection_time)))/60 as duration_minutes,
        COUNT(DISTINCT DATE_TRUNC('minute', collection_time)) as unique_minutes,
        AVG(distance_km) as avg_distance_km,
        MIN(distance_km) as min_distance_km,
        MAX(distance_km) as max_distance_km
    FROM simple_flights
    WHERE latitude IS NOT NULL
    GROUP BY icao24, callsign
    ORDER BY data_points DESC;
    """
    
    df_coverage = pd.read_sql(query_coverage, conn)
    
    # Query 2: Time gaps between observations
    query_gaps = """
    WITH ordered_flights AS (
        SELECT 
            icao24,
            collection_time,
            LAG(collection_time) OVER (PARTITION BY icao24 ORDER BY collection_time) as prev_time
        FROM simple_flights
        WHERE latitude IS NOT NULL
    )
    SELECT 
        icao24,
        AVG(EXTRACT(EPOCH FROM (collection_time - prev_time))) as avg_gap_seconds,
        MIN(EXTRACT(EPOCH FROM (collection_time - prev_time))) as min_gap_seconds,
        MAX(EXTRACT(EPOCH FROM (collection_time - prev_time))) as max_gap_seconds,
        STDDEV(EXTRACT(EPOCH FROM (collection_time - prev_time))) as stddev_gap_seconds,
        COUNT(*) as gap_count
    FROM ordered_flights
    WHERE prev_time IS NOT NULL
    GROUP BY icao24
    HAVING COUNT(*) > 5;
    """
    
    df_gaps = pd.read_sql(query_gaps, conn)
    
    # Query 3: Spatial coverage per aircraft
    query_spatial = """
    SELECT 
        icao24,
        COUNT(DISTINCT ROUND(latitude::numeric, 2)) * COUNT(DISTINCT ROUND(longitude::numeric, 2)) as spatial_cells,
        (MAX(latitude) - MIN(latitude)) * 111 as lat_range_km,
        (MAX(longitude) - MIN(longitude)) * 111 * COS(RADIANS(52.3)) as lon_range_km,
        STDDEV(latitude) * 111 as lat_stddev_km,
        STDDEV(longitude) * 111 * COS(RADIANS(52.3)) as lon_stddev_km
    FROM simple_flights
    WHERE latitude IS NOT NULL
    GROUP BY icao24
    HAVING COUNT(*) > 5;
    """
    
    df_spatial = pd.read_sql(query_spatial, conn)
    
    # Create summary statistics
    print("=== AIRCRAFT DATA DENSITY ANALYSIS ===\n")
    
    print("1. OVERALL STATISTICS:")
    print(f"Total unique aircraft: {len(df_coverage)}")
    print(f"Total data points: {df_coverage['data_points'].sum():,}")
    print(f"Average points per aircraft: {df_coverage['data_points'].mean():.1f}")
    print(f"Median points per aircraft: {df_coverage['data_points'].median():.0f}")
    
    print("\n2. DATA POINT DISTRIBUTION:")
    print("Points per aircraft:")
    print(f"  1 point only: {len(df_coverage[df_coverage['data_points'] == 1])} aircraft")
    print(f"  2-5 points: {len(df_coverage[(df_coverage['data_points'] >= 2) & (df_coverage['data_points'] <= 5)])} aircraft")
    print(f"  6-10 points: {len(df_coverage[(df_coverage['data_points'] >= 6) & (df_coverage['data_points'] <= 10)])} aircraft")
    print(f"  11-20 points: {len(df_coverage[(df_coverage['data_points'] >= 11) & (df_coverage['data_points'] <= 20)])} aircraft")
    print(f"  21-50 points: {len(df_coverage[(df_coverage['data_points'] >= 21) & (df_coverage['data_points'] <= 50)])} aircraft")
    print(f"  50+ points: {len(df_coverage[df_coverage['data_points'] > 50])} aircraft")
    
    print("\n3. TIME COVERAGE (for aircraft with 10+ points):")
    good_coverage = df_coverage[df_coverage['data_points'] >= 10]
    if len(good_coverage) > 0:
        print(f"Aircraft with good coverage: {len(good_coverage)}")
        print(f"Average duration tracked: {good_coverage['duration_minutes'].mean():.1f} minutes")
        print(f"Average points per minute: {(good_coverage['data_points'] / good_coverage['duration_minutes']).mean():.2f}")
    
    print("\n4. OBSERVATION GAPS (for aircraft with multiple points):")
    if len(df_gaps) > 0:
        print(f"Average gap between observations: {df_gaps['avg_gap_seconds'].mean()/60:.1f} minutes")
        print(f"Minimum gap (best case): {df_gaps['min_gap_seconds'].min():.0f} seconds")
        print(f"Typical gap (median): {df_gaps['avg_gap_seconds'].median()/60:.1f} minutes")
    
    print("\n5. TOP 10 BEST-TRACKED AIRCRAFT:")
    print(df_coverage[['icao24', 'callsign', 'data_points', 'duration_minutes', 'min_distance_km']].head(10).to_string(index=False))
    
    # Save full analysis
    df_coverage.to_csv('aircraft_coverage_analysis.csv', index=False)
    print("\n✅ Full analysis saved to: aircraft_coverage_analysis.csv")
    
    # Create Dekart-ready query
    print("\n6. DEKART QUERY FOR TRAJECTORY ANALYSIS:")
    print("""
-- Show aircraft with enough points for trajectory reconstruction
SELECT 
    icao24,
    MAX(callsign) as callsign,
    COUNT(*) as points,
    MIN(collection_time) as start_time,
    MAX(collection_time) as end_time,
    ROUND(EXTRACT(EPOCH FROM (MAX(collection_time) - MIN(collection_time)))/60) as minutes_tracked,
    ROUND(AVG(distance_km), 1) as avg_distance_km
FROM simple_flights
WHERE latitude IS NOT NULL
GROUP BY icao24
HAVING COUNT(*) >= 10
ORDER BY COUNT(*) DESC
LIMIT 50;
    """)
    
    conn.close()
    
    return df_coverage, df_gaps

if __name__ == "__main__":
    analyze_aircraft_coverage()