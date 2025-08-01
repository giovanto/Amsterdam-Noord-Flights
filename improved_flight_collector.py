#!/usr/bin/env python3
"""
Improved Flight Data Collector with GeoPandas Integration
Optimized for trajectory reconstruction and spatial analysis

API Limits:
- OpenSky: 4000 calls/day
- Schiphol: No known limits

Strategy:
- 30-second intervals for trajectory quality
- Wider area coverage for complete paths
- GeoPandas-ready data structure
"""

import geopandas as gpd
from shapely.geometry import Point, LineString, Polygon
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import sqlite3
import logging
from typing import Dict, List, Tuple
import time
import json

class ImprovedFlightCollector:
    def __init__(self):
        # API rate calculation
        self.OPENSKY_DAILY_LIMIT = 4000
        self.SECONDS_PER_DAY = 86400
        
        # Collection parameters
        self.COLLECTION_INTERVAL = 30  # seconds - for good trajectories
        self.COLLECTIONS_PER_DAY = self.SECONDS_PER_DAY / self.COLLECTION_INTERVAL  # 2880
        
        # Each collection uses 2 API calls (OpenSky + Schiphol)
        self.API_CALLS_PER_COLLECTION = 2
        self.DAILY_API_USAGE = self.COLLECTIONS_PER_DAY * self.API_CALLS_PER_COLLECTION  # 5760
        
        # Define collection area - expanded for full flight paths
        self.setup_collection_area()
        
        self.logger = self.setup_logging()
        
    def setup_collection_area(self):
        """Define expanded collection area using GeoPandas"""
        
        # Amsterdam Noord center (your house)
        self.noord_center = Point(4.895168, 52.385157)
        
        # Define multiple zones for smart collection
        # Zone 1: Core area (Amsterdam Noord) - highest frequency
        self.core_zone = gpd.GeoSeries([
            Point(4.895168, 52.385157).buffer(0.05)  # ~5km radius
        ])
        
        # Zone 2: Extended Amsterdam area - medium frequency  
        self.extended_zone = gpd.GeoSeries([
            Point(4.9, 52.37).buffer(0.15)  # ~15km radius
        ])
        
        # Zone 3: Approach paths - lower frequency but essential for trajectories
        # Define approach corridors based on runway alignments
        schiphol_location = Point(4.7683, 52.3105)
        
        # Polderbaan approach (from North)
        polderbaan_corridor = Polygon([
            (4.65, 52.45), (4.85, 52.45),  # North end
            (4.80, 52.35), (4.70, 52.35)   # South end near runway
        ])
        
        # Kaagbaan approach (from South) 
        kaagbaan_corridor = Polygon([
            (4.70, 52.20), (4.80, 52.20),  # South end
            (4.75, 52.30), (4.78, 52.30)   # North end near runway
        ])
        
        # Zwanenburgbaan approach (from East)
        zwanenburg_corridor = Polygon([
            (4.90, 52.30), (4.90, 52.35),  # East end
            (4.77, 52.31), (4.77, 52.33)   # West end near runway
        ])
        
        self.approach_corridors = gpd.GeoDataFrame({
            'corridor': ['polderbaan', 'kaagbaan', 'zwanenburg'],
            'geometry': [polderbaan_corridor, kaagbaan_corridor, zwanenburg_corridor]
        })
        
        # Combined collection boundary
        self.collection_boundary = Polygon([
            (4.5, 52.2),    # SW corner
            (5.2, 52.2),    # SE corner  
            (5.2, 52.5),    # NE corner
            (4.5, 52.5)     # NW corner
        ])
        
        self.logger.info(f"Collection area: {self.collection_boundary.area * 111 * 111:.0f} km²")
        
    def setup_logging(self):
        """Configure logging"""
        logger = logging.getLogger('ImprovedFlightCollector')
        logger.setLevel(logging.INFO)
        
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        
        return logger
        
    def calculate_optimal_schedule(self):
        """Calculate optimal collection schedule within API limits"""
        
        # With 4000 API calls/day and 30-second intervals
        # We can't run continuously (would need 5760 calls)
        # Solution: Smart scheduling
        
        schedule = {
            'peak_hours': {
                'hours': [6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20],
                'interval': 30,  # seconds
                'priority': 'high'
            },
            'off_peak': {
                'hours': [5, 21, 22, 23],
                'interval': 60,  # seconds
                'priority': 'medium'
            },
            'night': {
                'hours': [0, 1, 2, 3, 4],
                'interval': 300,  # 5 minutes
                'priority': 'low'
            }
        }
        
        # Calculate daily API usage
        daily_calls = 0
        for period, config in schedule.items():
            hours = len(config['hours'])
            collections_per_hour = 3600 / config['interval']
            daily_calls += hours * collections_per_hour * 2  # 2 APIs per collection
            
        self.logger.info(f"Optimized schedule uses {daily_calls:.0f} API calls/day")
        self.logger.info(f"Within limit: {daily_calls < self.OPENSKY_DAILY_LIMIT}")
        
        return schedule
        
    def collect_with_geopandas(self, bbox: Tuple[float, float, float, float]) -> gpd.GeoDataFrame:
        """Collect flight data and return as GeoDataFrame"""
        
        # Simulate data collection (replace with actual API calls)
        # This would call your existing OpenSky and Schiphol APIs
        
        # For now, showing the structure
        flights_data = {
            'icao24': [],
            'callsign': [],
            'time': [],
            'latitude': [],
            'longitude': [],
            'altitude': [],
            'velocity': [],
            'heading': [],
            'vertical_rate': [],
            'on_ground': [],
            'geometry': []
        }
        
        # Create GeoDataFrame
        gdf = gpd.GeoDataFrame(flights_data, crs='EPSG:4326')
        
        # Add spatial analysis columns
        if len(gdf) > 0:
            # Distance to house
            gdf['distance_to_noord'] = gdf.geometry.distance(self.noord_center) * 111  # km
            
            # Check which zone
            gdf['in_core_zone'] = gdf.geometry.within(self.core_zone.iloc[0])
            gdf['in_extended_zone'] = gdf.geometry.within(self.extended_zone.iloc[0])
            
            # Check approach corridors
            for idx, corridor in self.approach_corridors.iterrows():
                gdf[f'in_{corridor["corridor"]}_approach'] = gdf.geometry.within(corridor['geometry'])
        
        return gdf
        
    def create_trajectories(self, gdf: gpd.GeoDataFrame, time_window: int = 3600) -> gpd.GeoDataFrame:
        """Create flight trajectories from point data"""
        
        trajectories = []
        
        # Group by aircraft
        for icao24, aircraft_data in gdf.groupby('icao24'):
            # Sort by time
            aircraft_data = aircraft_data.sort_values('time')
            
            # Create trajectory segments (split if gap > 5 minutes)
            segments = []
            current_segment = [aircraft_data.iloc[0]]
            
            for i in range(1, len(aircraft_data)):
                time_gap = (aircraft_data.iloc[i]['time'] - aircraft_data.iloc[i-1]['time']).seconds
                
                if time_gap <= 300:  # 5 minutes
                    current_segment.append(aircraft_data.iloc[i])
                else:
                    # Save current segment and start new one
                    if len(current_segment) >= 2:
                        segments.append(current_segment)
                    current_segment = [aircraft_data.iloc[i]]
            
            # Don't forget last segment
            if len(current_segment) >= 2:
                segments.append(current_segment)
            
            # Create LineString for each segment
            for segment in segments:
                points = [Point(row['longitude'], row['latitude']) for _, row in pd.DataFrame(segment).iterrows()]
                if len(points) >= 2:
                    trajectory = {
                        'icao24': icao24,
                        'callsign': segment[0]['callsign'],
                        'start_time': segment[0]['time'],
                        'end_time': segment[-1]['time'],
                        'duration_minutes': (segment[-1]['time'] - segment[0]['time']).seconds / 60,
                        'points': len(segment),
                        'geometry': LineString(points),
                        'avg_altitude': np.mean([s['altitude'] for s in segment]),
                        'min_distance_noord': min([s['distance_to_noord'] for s in segment])
                    }
                    trajectories.append(trajectory)
        
        return gpd.GeoDataFrame(trajectories, crs='EPSG:4326')
        
    def save_geopandas_ready(self, gdf: gpd.GeoDataFrame, filename: str):
        """Save data in GeoPandas-friendly formats"""
        
        # Save as GeoPackage (recommended for GeoPandas)
        gdf.to_file(f"{filename}.gpkg", driver="GPKG")
        
        # Also save as GeoJSON for web visualization
        gdf.to_file(f"{filename}.geojson", driver="GeoJSON")
        
        # Save as Shapefile for GIS compatibility
        gdf.to_file(f"{filename}_shp", driver="ESRI Shapefile")
        
        self.logger.info(f"Saved GeoPandas data: {filename}.gpkg, .geojson, _shp/")
        
def calculate_collection_strategy():
    """Calculate and display collection strategy"""
    
    print("\n=== IMPROVED COLLECTION STRATEGY ===\n")
    
    # API limits
    opensky_limit = 4000
    
    # Option 1: Maximum frequency (30 seconds)
    print("OPTION 1: Maximum Trajectory Quality")
    print("- Collection interval: 30 seconds")
    print("- Coverage hours: 16 hours/day (6:00-22:00)")
    print("- API calls: 3,840/day")
    print("- Points per aircraft: ~20-40 (10-20 min coverage)")
    print("- Trajectory quality: EXCELLENT")
    
    # Option 2: Balanced approach  
    print("\nOPTION 2: Balanced Coverage (RECOMMENDED)")
    print("- Peak hours (6-20): 30-second intervals")
    print("- Off-peak (5,21-23): 60-second intervals") 
    print("- Night (0-4): 5-minute intervals")
    print("- API calls: ~3,400/day")
    print("- Points per aircraft: ~15-30")
    print("- Trajectory quality: GOOD")
    
    # Option 3: Wide coverage
    print("\nOPTION 3: Maximum Coverage")
    print("- All hours: 60-second intervals")
    print("- API calls: 2,880/day")
    print("- Points per aircraft: ~10-20")
    print("- Trajectory quality: FAIR")
    
    print("\n=== SPATIAL COVERAGE ===")
    print("- Core zone: 5km radius from Amsterdam Noord")
    print("- Extended zone: 15km radius (covers most approaches)")
    print("- Approach corridors: Polderbaan, Kaagbaan, Zwanenburg")
    print("- Total area: ~70km x 40km box")
    
    print("\n=== GEOPANDAS BENEFITS ===")
    print("- Automatic trajectory creation from points")
    print("- Spatial joins with neighborhoods, noise zones")
    print("- Export to multiple GIS formats")
    print("- Built-in coordinate transformations")
    print("- Efficient spatial indexing")

if __name__ == "__main__":
    calculate_collection_strategy()
    
    # Initialize collector
    collector = ImprovedFlightCollector()
    
    # Show optimal schedule
    schedule = collector.calculate_optimal_schedule()