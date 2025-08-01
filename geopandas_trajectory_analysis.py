#!/usr/bin/env python3
"""
GeoPandas Trajectory Analysis for Flight Data
Converts point observations into flight trajectories and performs spatial analysis
"""

import geopandas as gpd
from shapely.geometry import Point, LineString, Polygon
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import sqlite3
import matplotlib.pyplot as plt
from typing import Dict, List, Tuple
import folium
from folium import plugins

class FlightTrajectoryAnalyzer:
    """Analyze flight data using GeoPandas for trajectory reconstruction"""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        
        # Define key locations
        self.locations = {
            'amsterdam_noord': Point(4.895168, 52.385157),
            'schiphol': Point(4.7683, 52.3105),
            'amsterdam_center': Point(4.9, 52.37)
        }
        
        # Define analysis zones
        self.create_analysis_zones()
        
    def create_analysis_zones(self):
        """Create GeoDataFrame with analysis zones"""
        zones = []
        
        # Noise impact zones (circles around Noord)
        for radius, name in [(2, 'high_impact'), (5, 'medium_impact'), (10, 'low_impact')]:
            zone = self.locations['amsterdam_noord'].buffer(radius / 111)  # Convert km to degrees
            zones.append({'name': f'noise_{name}', 'geometry': zone})
            
        # Approach corridors
        corridors = [
            {
                'name': 'polderbaan_approach',
                'geometry': Polygon([
                    (4.65, 52.45), (4.85, 52.45),
                    (4.80, 52.35), (4.70, 52.35)
                ])
            },
            {
                'name': 'kaagbaan_approach',
                'geometry': Polygon([
                    (4.70, 52.20), (4.80, 52.20),
                    (4.75, 52.30), (4.78, 52.30)
                ])
            }
        ]
        zones.extend(corridors)
        
        self.zones_gdf = gpd.GeoDataFrame(zones, crs='EPSG:4326')
        
    def load_flight_points(self, time_window: str = '24 hours') -> gpd.GeoDataFrame:
        """Load flight points from database into GeoDataFrame"""
        conn = sqlite3.connect(self.db_path)
        
        # Query with time window
        query = f"""
            SELECT 
                f.*,
                a.total_points,
                a.trajectory_quality
            FROM flights f
            LEFT JOIN aircraft_tracks a ON f.icao24 = a.icao24
            WHERE f.latitude IS NOT NULL 
                AND f.longitude IS NOT NULL
                AND f.collection_time > datetime('now', '-{time_window}')
            ORDER BY f.icao24, f.collection_time
        """
        
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        # Convert to GeoDataFrame
        geometry = [Point(row.longitude, row.latitude) for _, row in df.iterrows()]
        gdf = gpd.GeoDataFrame(df, geometry=geometry, crs='EPSG:4326')
        
        # Parse timestamps
        gdf['collection_time'] = pd.to_datetime(gdf['collection_time'])
        
        return gdf
        
    def create_trajectories(self, points_gdf: gpd.GeoDataFrame, 
                          min_points: int = 5,
                          max_time_gap: int = 300) -> gpd.GeoDataFrame:
        """Create flight trajectories from point observations"""
        
        trajectories = []
        
        # Group by aircraft
        for icao24, aircraft_points in points_gdf.groupby('icao24'):
            # Sort by time
            aircraft_points = aircraft_points.sort_values('collection_time')
            
            # Split into trajectory segments based on time gaps
            segments = []
            current_segment = [aircraft_points.iloc[0]]
            
            for i in range(1, len(aircraft_points)):
                current = aircraft_points.iloc[i]
                previous = aircraft_points.iloc[i-1]
                
                time_gap = (current['collection_time'] - previous['collection_time']).total_seconds()
                
                if time_gap <= max_time_gap:
                    current_segment.append(current)
                else:
                    # Save segment if it has enough points
                    if len(current_segment) >= min_points:
                        segments.append(pd.DataFrame(current_segment))
                    current_segment = [current]
            
            # Don't forget the last segment
            if len(current_segment) >= min_points:
                segments.append(pd.DataFrame(current_segment))
            
            # Create LineString for each segment
            for segment_df in segments:
                if len(segment_df) >= 2:
                    # Extract trajectory properties
                    points = [(row.longitude, row.latitude) for _, row in segment_df.iterrows()]
                    
                    trajectory = {
                        'icao24': icao24,
                        'callsign': segment_df.iloc[0]['callsign'],
                        'start_time': segment_df.iloc[0]['collection_time'],
                        'end_time': segment_df.iloc[-1]['collection_time'],
                        'duration_minutes': (segment_df.iloc[-1]['collection_time'] - 
                                           segment_df.iloc[0]['collection_time']).total_seconds() / 60,
                        'points': len(segment_df),
                        'geometry': LineString(points),
                        
                        # Flight characteristics
                        'avg_altitude': segment_df['baro_altitude'].mean(),
                        'min_altitude': segment_df['baro_altitude'].min(),
                        'max_altitude': segment_df['baro_altitude'].max(),
                        'avg_velocity': segment_df['velocity'].mean(),
                        
                        # Noise impact
                        'min_distance_noord': segment_df['distance_to_house_km'].min(),
                        'max_noise_db': segment_df['estimated_noise_db'].max(),
                        'avg_noise_db': segment_df['estimated_noise_db'].mean(),
                        
                        # Coverage
                        'entered_core_zone': segment_df['in_core_zone'].any(),
                        'trajectory_quality': segment_df.iloc[0]['trajectory_quality']
                    }
                    
                    trajectories.append(trajectory)
        
        # Create GeoDataFrame
        trajectories_gdf = gpd.GeoDataFrame(trajectories, crs='EPSG:4326')
        
        # Add spatial analysis
        if len(trajectories_gdf) > 0:
            # Check which zones each trajectory passes through
            for _, zone in self.zones_gdf.iterrows():
                trajectories_gdf[f'crosses_{zone["name"]}'] = trajectories_gdf.geometry.intersects(zone['geometry'])
        
        return trajectories_gdf
        
    def analyze_trajectory_patterns(self, trajectories_gdf: gpd.GeoDataFrame) -> Dict:
        """Analyze patterns in flight trajectories"""
        
        analysis = {
            'total_trajectories': len(trajectories_gdf),
            'aircraft_tracked': trajectories_gdf['icao24'].nunique(),
            'avg_points_per_trajectory': trajectories_gdf['points'].mean(),
            'avg_duration_minutes': trajectories_gdf['duration_minutes'].mean(),
        }
        
        # Quality breakdown
        if 'trajectory_quality' in trajectories_gdf.columns:
            quality_counts = trajectories_gdf['trajectory_quality'].value_counts()
            analysis['quality_breakdown'] = quality_counts.to_dict()
        
        # Noise impact analysis
        noise_impact = trajectories_gdf[trajectories_gdf['entered_core_zone'] == True]
        analysis['trajectories_over_noord'] = len(noise_impact)
        
        if len(noise_impact) > 0:
            analysis['avg_noise_over_noord'] = noise_impact['max_noise_db'].mean()
            analysis['loudest_trajectory'] = {
                'icao24': noise_impact.loc[noise_impact['max_noise_db'].idxmax(), 'icao24'],
                'noise_db': noise_impact['max_noise_db'].max(),
                'callsign': noise_impact.loc[noise_impact['max_noise_db'].idxmax(), 'callsign']
            }
        
        # Approach pattern analysis
        for corridor in ['polderbaan_approach', 'kaagbaan_approach']:
            col_name = f'crosses_{corridor}'
            if col_name in trajectories_gdf.columns:
                analysis[f'{corridor}_usage'] = trajectories_gdf[col_name].sum()
        
        return analysis
        
    def create_folium_map(self, trajectories_gdf: gpd.GeoDataFrame, 
                         points_gdf: gpd.GeoDataFrame = None) -> folium.Map:
        """Create interactive map with trajectories"""
        
        # Create base map centered on Amsterdam
        m = folium.Map(
            location=[52.37, 4.9],
            zoom_start=11,
            tiles='OpenStreetMap'
        )
        
        # Add zones
        for _, zone in self.zones_gdf.iterrows():
            folium.GeoJson(
                zone['geometry'],
                name=zone['name'],
                style_function=lambda x: {
                    'fillColor': 'red' if 'noise' in x['properties']['name'] else 'blue',
                    'color': 'black',
                    'weight': 1,
                    'fillOpacity': 0.2
                }
            ).add_to(m)
        
        # Add trajectories with color based on noise level
        for _, traj in trajectories_gdf.iterrows():
            # Color based on max noise
            if traj['max_noise_db'] > 70:
                color = 'red'
            elif traj['max_noise_db'] > 60:
                color = 'orange'
            else:
                color = 'green'
                
            folium.PolyLine(
                locations=[(lat, lon) for lon, lat in traj['geometry'].coords],
                color=color,
                weight=3,
                opacity=0.7,
                popup=f"{traj['callsign']} ({traj['icao24']})<br>"
                      f"Points: {traj['points']}<br>"
                      f"Max noise: {traj['max_noise_db']:.0f} dB<br>"
                      f"Quality: {traj['trajectory_quality']}"
            ).add_to(m)
        
        # Add markers for key locations
        for name, point in self.locations.items():
            folium.Marker(
                [point.y, point.x],
                popup=name.replace('_', ' ').title(),
                icon=folium.Icon(color='red' if name == 'amsterdam_noord' else 'blue')
            ).add_to(m)
        
        # Add layer control
        folium.LayerControl().add_to(m)
        
        return m
        
    def export_for_gis(self, trajectories_gdf: gpd.GeoDataFrame, base_filename: str):
        """Export trajectories in multiple GIS formats"""
        
        # GeoPackage (recommended)
        trajectories_gdf.to_file(f"{base_filename}.gpkg", driver="GPKG", layer='trajectories')
        
        # GeoJSON for web
        trajectories_gdf.to_file(f"{base_filename}.geojson", driver="GeoJSON")
        
        # Shapefile for legacy GIS
        # Note: column names will be truncated to 10 chars
        trajectories_gdf.to_file(f"{base_filename}_shp", driver="ESRI Shapefile")
        
        print(f"✅ Exported trajectories to:")
        print(f"   - {base_filename}.gpkg (GeoPackage)")
        print(f"   - {base_filename}.geojson (GeoJSON)")
        print(f"   - {base_filename}_shp/ (Shapefile)")
        
def main():
    """Run trajectory analysis"""
    
    # Use the enhanced database
    analyzer = FlightTrajectoryAnalyzer('enhanced_flight_data.db')
    
    print("Loading flight points...")
    points_gdf = analyzer.load_flight_points('24 hours')
    print(f"Loaded {len(points_gdf)} points from {points_gdf['icao24'].nunique()} aircraft")
    
    print("\nCreating trajectories...")
    trajectories_gdf = analyzer.create_trajectories(points_gdf, min_points=5)
    print(f"Created {len(trajectories_gdf)} trajectories")
    
    print("\nAnalyzing patterns...")
    analysis = analyzer.analyze_trajectory_patterns(trajectories_gdf)
    
    print("\n=== TRAJECTORY ANALYSIS RESULTS ===")
    for key, value in analysis.items():
        print(f"{key}: {value}")
    
    # Export data
    print("\nExporting GIS files...")
    analyzer.export_for_gis(trajectories_gdf, 'flight_trajectories')
    
    # Create interactive map
    print("\nCreating interactive map...")
    m = analyzer.create_folium_map(trajectories_gdf)
    m.save('flight_trajectories_map.html')
    print("✅ Map saved to: flight_trajectories_map.html")
    
if __name__ == "__main__":
    main()