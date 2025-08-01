#!/usr/bin/env python3
"""
MAQ (Meteorological and Air Quality) API Integration
Integrates Dutch KNMI weather data with flight trajectory analysis
For Amsterdam Noord aviation impact analysis - Dutch Mobility Hackathon 2025
"""

import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import sqlite3
import json
from typing import Dict, List, Tuple, Optional
import geopandas as gpd
from shapely.geometry import Point
import matplotlib.pyplot as plt
import seaborn as sns

class MAQWeatherIntegrator:
    """Integrate KNMI weather data with flight trajectory analysis"""
    
    def __init__(self, api_key: str = "20bc1e7a-1c9c-4ba1-9da6-926bad84e607"):
        self.api_key = api_key
        self.base_url = "https://api.dataplatform.knmi.nl"
        
        # Key weather stations near Schiphol/Amsterdam Noord
        self.stations = {
            'schiphol': {'id': '240', 'name': 'Schiphol', 'lat': 52.318, 'lon': 4.790},
            'amsterdam': {'id': '240', 'name': 'Amsterdam', 'lat': 52.370, 'lon': 4.895},
            'ijmuiden': {'id': '235', 'name': 'IJmuiden', 'lat': 52.462, 'lon': 4.555},
            'cabauw': {'id': '348', 'name': 'Cabauw', 'lat': 51.971, 'lon': 4.926}
        }
        
        # Runway configurations at Schiphol
        self.runway_headings = {
            'polderbaan': {'heading': 180, 'name': '18R/36L'},
            'kaagbaan': {'heading': 90, 'name': '09/27'},
            'buitenveldertbaan': {'heading': 180, 'name': '18L/36R'},
            'zwanenburgbaan': {'heading': 60, 'name': '06/24'},
            'oostbaan': {'heading': 40, 'name': '04/22'},
            'aalsmeerbaan': {'heading': 60, 'name': '06/24'}
        }
        
    def discover_weather_stations(self) -> pd.DataFrame:
        """Discover available KNMI weather stations"""
        try:
            # KNMI Open Data API endpoint for station metadata
            url = f"{self.base_url}/open-data/v1/datasets/Actuele10mindataKNMIstations/versions/2/files"
            
            headers = {
                'Authorization': f'Bearer {self.api_key}',
                'Content-Type': 'application/json'
            }
            
            response = requests.get(url, headers=headers)
            
            if response.status_code == 200:
                data = response.json()
                stations_data = []
                
                # Extract station information
                for station in data.get('files', []):
                    if 'metadata' in station:
                        metadata = station['metadata']
                        stations_data.append({
                            'station_id': metadata.get('station_id'),
                            'name': metadata.get('name'),
                            'latitude': metadata.get('lat'),
                            'longitude': metadata.get('lon'),
                            'elevation': metadata.get('elevation'),
                            'parameters': metadata.get('parameters', [])
                        })
                
                return pd.DataFrame(stations_data)
            else:
                print(f"❌ API request failed: {response.status_code}")
                return self._get_fallback_stations()
                
        except Exception as e:
            print(f"❌ Error discovering stations: {e}")
            return self._get_fallback_stations()
    
    def _get_fallback_stations(self) -> pd.DataFrame:
        """Fallback station data based on known KNMI stations"""
        stations_data = []
        for key, station in self.stations.items():
            stations_data.append({
                'station_id': station['id'],
                'name': station['name'],
                'latitude': station['lat'],
                'longitude': station['lon'],
                'distance_to_schiphol': self._calculate_distance(
                    station['lat'], station['lon'], 52.318, 4.790
                ),
                'distance_to_noord': self._calculate_distance(
                    station['lat'], station['lon'], 52.385, 4.895
                )
            })
        
        return pd.DataFrame(stations_data)
    
    def retrieve_weather_data(self, station_id: str, 
                            start_date: datetime, 
                            end_date: datetime) -> pd.DataFrame:
        """Retrieve meteorological data for a specific station and time period"""
        
        try:
            # Format dates for KNMI API
            start_str = start_date.strftime('%Y%m%d')
            end_str = end_date.strftime('%Y%m%d')
            
            # KNMI API endpoint for 10-minute weather data
            url = f"{self.base_url}/open-data/v1/datasets/10-minute-in-situ-meteorological-observations/versions/1.0/files"
            
            params = {
                'station': station_id,
                'start': start_str,
                'end': end_str,
                'vars': 'DD,FF,FH,T,TD,P,RH,VV,N,Q'  # Key meteorological variables
            }
            
            headers = {
                'Authorization': f'Bearer {self.api_key}',
                'Content-Type': 'application/json'
            }
            
            response = requests.get(url, headers=headers, params=params)
            
            if response.status_code == 200:
                # Parse weather data (typically CSV format from KNMI)
                weather_df = pd.read_csv(response.text, comment='#', skipinitialspace=True)
                
                # Clean and standardize column names
                weather_df.columns = weather_df.columns.str.strip()
                
                # Convert timestamp
                if 'YYYYMMDD' in weather_df.columns and 'HH' in weather_df.columns:
                    weather_df['datetime'] = pd.to_datetime(
                        weather_df['YYYYMMDD'].astype(str) + ' ' + 
                        weather_df['HH'].astype(str).str.zfill(2) + ':' +
                        weather_df.get('MM', 0).astype(str).str.zfill(2),
                        format='%Y%m%d %H:%M'
                    )
                
                # Standardize key columns
                column_mapping = {
                    'DD': 'wind_direction',    # Wind direction (degrees)
                    'FF': 'wind_speed',        # Wind speed (m/s)
                    'FH': 'max_wind_speed',    # Max wind speed (m/s)
                    'T': 'temperature',        # Temperature (0.1 °C)
                    'P': 'pressure',           # Air pressure (0.1 hPa)
                    'RH': 'humidity',          # Relative humidity (%)
                    'VV': 'visibility',        # Visibility (m)
                    'N': 'cloud_cover',        # Cloud cover (octas)
                    'Q': 'solar_radiation'     # Solar radiation (J/cm²)
                }
                
                weather_df = weather_df.rename(columns=column_mapping)
                
                # Convert units
                if 'temperature' in weather_df.columns:
                    weather_df['temperature'] = weather_df['temperature'] / 10  # to °C
                if 'pressure' in weather_df.columns:
                    weather_df['pressure'] = weather_df['pressure'] / 10  # to hPa
                
                weather_df['station_id'] = station_id
                
                return weather_df
            else:
                print(f"❌ Weather data request failed: {response.status_code}")
                return self._generate_sample_weather_data(station_id, start_date, end_date)
                
        except Exception as e:
            print(f"❌ Error retrieving weather data: {e}")
            return self._generate_sample_weather_data(station_id, start_date, end_date)
    
    def _generate_sample_weather_data(self, station_id: str, 
                                    start_date: datetime, 
                                    end_date: datetime) -> pd.DataFrame:
        """Generate realistic sample weather data for testing"""
        
        # Create time series every 10 minutes
        time_range = pd.date_range(start=start_date, end=end_date, freq='10min')
        
        # Generate realistic weather patterns
        np.random.seed(42)  # Reproducible results
        
        weather_data = []
        for timestamp in time_range:
            # Simulate typical Dutch weather patterns
            base_wind_dir = 240  # Prevailing SW winds
            seasonal_var = 30 * np.sin((timestamp.dayofyear / 365) * 2 * np.pi)
            daily_var = 20 * np.sin((timestamp.hour / 24) * 2 * np.pi)
            
            weather_data.append({
                'datetime': timestamp,
                'station_id': station_id,
                'wind_direction': (base_wind_dir + seasonal_var + daily_var + 
                                 np.random.normal(0, 15)) % 360,
                'wind_speed': max(0, np.random.normal(4.5, 2.0)),
                'max_wind_speed': max(0, np.random.normal(6.0, 2.5)),
                'temperature': 12 + 8 * np.sin((timestamp.dayofyear / 365) * 2 * np.pi) + 
                              3 * np.sin((timestamp.hour / 24) * 2 * np.pi) + 
                              np.random.normal(0, 2),
                'pressure': np.random.normal(1013, 8),
                'humidity': np.random.normal(75, 15),
                'visibility': np.random.exponential(15000),
                'cloud_cover': np.random.randint(0, 9)
            })
        
        return pd.DataFrame(weather_data)
    
    def correlate_weather_flight_data(self, flight_db_path: str, 
                                    weather_data: pd.DataFrame) -> pd.DataFrame:
        """Correlate weather data with flight trajectory data"""
        
        # Load flight data
        conn = sqlite3.connect(flight_db_path)
        flight_query = """
            SELECT 
                collection_time,
                icao24,
                callsign,
                latitude,
                longitude,
                baro_altitude,
                velocity,
                true_track,
                distance_to_house_km,
                estimated_noise_db,
                schiphol_operation
            FROM flights
            WHERE latitude IS NOT NULL 
                AND longitude IS NOT NULL
            ORDER BY collection_time
        """
        
        flight_df = pd.read_sql_query(flight_query, conn)
        conn.close()
        
        # Convert timestamps
        flight_df['collection_time'] = pd.to_datetime(flight_df['collection_time'])
        weather_data['datetime'] = pd.to_datetime(weather_data['datetime'])
        
        # Merge on nearest timestamp (10-minute intervals)
        flight_df['weather_timestamp'] = flight_df['collection_time'].dt.round('10min')
        weather_data['weather_timestamp'] = weather_data['datetime'].dt.round('10min')
        
        # Merge datasets
        merged_df = pd.merge_asof(
            flight_df.sort_values('collection_time'),
            weather_data.sort_values('datetime'),
            left_on='collection_time',
            right_on='datetime',
            direction='nearest',
            tolerance=pd.Timedelta('10 minutes')
        )
        
        # Calculate weather-flight correlations
        merged_df['headwind_component'] = self._calculate_headwind(
            merged_df['true_track'], 
            merged_df['wind_direction'], 
            merged_df['wind_speed']
        )
        
        merged_df['crosswind_component'] = self._calculate_crosswind(
            merged_df['true_track'], 
            merged_df['wind_direction'], 
            merged_df['wind_speed']
        )
        
        # Predict runway usage based on wind
        merged_df['predicted_runway'] = merged_df['wind_direction'].apply(
            self._predict_runway_from_wind
        )
        
        # Analyze noise propagation with wind
        merged_df['wind_adjusted_noise'] = self._adjust_noise_for_wind(
            merged_df['estimated_noise_db'],
            merged_df['wind_speed'],
            merged_df['wind_direction']
        )
        
        return merged_df
    
    def _calculate_headwind(self, aircraft_heading: pd.Series, 
                          wind_direction: pd.Series, 
                          wind_speed: pd.Series) -> pd.Series:
        """Calculate headwind component"""
        # Convert to radians
        aircraft_rad = np.radians(aircraft_heading)
        wind_rad = np.radians(wind_direction)
        
        # Headwind is wind blowing opposite to aircraft direction
        angle_diff = wind_rad - aircraft_rad
        headwind = wind_speed * np.cos(angle_diff)
        
        return headwind
    
    def _calculate_crosswind(self, aircraft_heading: pd.Series, 
                           wind_direction: pd.Series, 
                           wind_speed: pd.Series) -> pd.Series:
        """Calculate crosswind component"""
        aircraft_rad = np.radians(aircraft_heading)
        wind_rad = np.radians(wind_direction)
        
        angle_diff = wind_rad - aircraft_rad
        crosswind = wind_speed * np.sin(angle_diff)
        
        return crosswind
    
    def _predict_runway_from_wind(self, wind_direction: float) -> str:
        """Predict likely runway usage based on wind direction"""
        if pd.isna(wind_direction):
            return 'unknown'
        
        # Calculate which runway aligns best with wind
        best_runway = 'unknown'
        min_angle_diff = 180
        
        for runway, config in self.runway_headings.items():
            # Calculate angle difference (aircraft land into wind)
            runway_heading = config['heading']
            landing_heading = (runway_heading + 180) % 360
            
            angle_diff = min(
                abs(wind_direction - landing_heading),
                360 - abs(wind_direction - landing_heading)
            )
            
            if angle_diff < min_angle_diff:
                min_angle_diff = angle_diff
                best_runway = runway
        
        return best_runway
    
    def _adjust_noise_for_wind(self, noise_db: pd.Series, 
                             wind_speed: pd.Series, 
                             wind_direction: pd.Series) -> pd.Series:
        """Adjust noise estimates based on wind conditions"""
        
        # Wind can affect noise propagation:
        # - Downwind: sound travels further (+2-5 dB)
        # - Upwind: sound attenuated (-1-3 dB)
        # - Higher wind speeds: more turbulence affects propagation
        
        wind_adjustment = np.zeros(len(noise_db))
        
        # Simplified model: assume observer is N of Schiphol (Amsterdam Noord)
        observer_direction = 360  # North
        
        for i, (noise, ws, wd) in enumerate(zip(noise_db, wind_speed, wind_direction)):
            if pd.isna(noise) or pd.isna(ws) or pd.isna(wd):
                continue
            
            # Calculate if wind is carrying sound toward or away from observer
            angle_to_observer = abs(wd - observer_direction)
            angle_to_observer = min(angle_to_observer, 360 - angle_to_observer)
            
            if angle_to_observer < 45:  # Downwind
                wind_adjustment[i] = min(3.0, ws * 0.5)  # Sound travels further
            elif angle_to_observer > 135:  # Upwind
                wind_adjustment[i] = -min(2.0, ws * 0.3)  # Sound attenuated
            
            # Wind speed effects
            if ws > 10:  # High wind creates turbulence
                wind_adjustment[i] *= 0.7
        
        return noise_db + wind_adjustment
    
    def analyze_weather_flight_patterns(self, correlated_data: pd.DataFrame) -> Dict:
        """Analyze patterns in weather-flight correlations"""
        
        analysis = {
            'total_observations': len(correlated_data),
            'time_range': {
                'start': correlated_data['collection_time'].min(),
                'end': correlated_data['collection_time'].max()
            }
        }
        
        # Wind pattern analysis
        wind_stats = {
            'avg_wind_speed': correlated_data['wind_speed'].mean(),
            'max_wind_speed': correlated_data['wind_speed'].max(),
            'prevailing_direction': correlated_data['wind_direction'].mode()[0] if len(correlated_data) > 0 else None,
            'wind_direction_std': correlated_data['wind_direction'].std()
        }
        analysis['wind_patterns'] = wind_stats
        
        # Runway usage correlation
        runway_usage = correlated_data['predicted_runway'].value_counts()
        analysis['predicted_runway_usage'] = runway_usage.to_dict()
        
        # Noise impact with weather
        noise_weather = correlated_data[correlated_data['estimated_noise_db'] > 60].copy()
        if len(noise_weather) > 0:
            analysis['high_noise_weather_conditions'] = {
                'avg_wind_speed': noise_weather['wind_speed'].mean(),
                'avg_wind_direction': noise_weather['wind_direction'].mean(),
                'wind_adjusted_noise_avg': noise_weather['wind_adjusted_noise'].mean(),
                'original_noise_avg': noise_weather['estimated_noise_db'].mean()
            }
        
        # Temporal patterns
        correlated_data['hour'] = correlated_data['collection_time'].dt.hour
        hourly_wind = correlated_data.groupby('hour')['wind_speed'].mean()
        analysis['hourly_wind_patterns'] = hourly_wind.to_dict()
        
        return analysis
    
    def create_weather_flight_visualizations(self, correlated_data: pd.DataFrame, 
                                           output_prefix: str = 'weather_flight'):
        """Create comprehensive visualizations of weather-flight correlations"""
        
        fig, axes = plt.subplots(2, 3, figsize=(18, 12))
        fig.suptitle('Weather-Flight Pattern Analysis - Amsterdam Noord', fontsize=16)
        
        # 1. Wind Rose
        ax1 = axes[0, 0]
        wind_dirs = correlated_data['wind_direction'].dropna()
        wind_speeds = correlated_data['wind_speed'].dropna()
        
        # Create wind rose
        bins = np.arange(0, 361, 30)
        hist, bin_edges = np.histogram(wind_dirs, bins=bins)
        
        theta = np.linspace(0, 2*np.pi, len(hist), endpoint=False)
        ax1 = plt.subplot(2, 3, 1, projection='polar')
        ax1.bar(theta, hist, width=2*np.pi/len(hist), alpha=0.7)
        ax1.set_theta_zero_location('N')
        ax1.set_theta_direction(-1)
        ax1.set_title('Wind Direction Distribution')
        
        # 2. Runway Usage vs Wind Direction
        ax2 = axes[0, 1]
        runway_wind = correlated_data.groupby(['predicted_runway', 
                                              pd.cut(correlated_data['wind_direction'], 
                                                   bins=8, labels=False)]).size().unstack(fill_value=0)
        runway_wind.plot(kind='bar', stacked=True, ax=ax2)
        ax2.set_title('Predicted Runway Usage by Wind Direction')
        ax2.set_xlabel('Runway')
        ax2.tick_params(axis='x', rotation=45)
        
        # 3. Noise vs Wind Speed
        ax3 = axes[0, 2]
        scatter = ax3.scatter(correlated_data['wind_speed'], 
                            correlated_data['estimated_noise_db'],
                            c=correlated_data['wind_direction'], 
                            cmap='hsv', alpha=0.6)
        ax3.set_xlabel('Wind Speed (m/s)')
        ax3.set_ylabel('Estimated Noise (dB)')
        ax3.set_title('Noise vs Wind Speed (colored by wind direction)')
        plt.colorbar(scatter, ax=ax3)
        
        # 4. Wind-Adjusted Noise Comparison
        ax4 = axes[1, 0]
        noise_comparison = pd.DataFrame({
            'Original': correlated_data['estimated_noise_db'],
            'Wind-Adjusted': correlated_data['wind_adjusted_noise']
        })
        noise_comparison.plot(kind='hist', alpha=0.7, bins=30, ax=ax4)
        ax4.set_xlabel('Noise Level (dB)')
        ax4.set_ylabel('Frequency')
        ax4.set_title('Original vs Wind-Adjusted Noise Distribution')
        
        # 5. Temporal Wind Patterns
        ax5 = axes[1, 1]
        hourly_data = correlated_data.groupby(correlated_data['collection_time'].dt.hour).agg({
            'wind_speed': 'mean',
            'estimated_noise_db': 'mean'
        })
        
        ax5_twin = ax5.twinx()
        ax5.plot(hourly_data.index, hourly_data['wind_speed'], 'b-', label='Wind Speed')
        ax5_twin.plot(hourly_data.index, hourly_data['estimated_noise_db'], 'r-', label='Noise')
        ax5.set_xlabel('Hour of Day')
        ax5.set_ylabel('Wind Speed (m/s)', color='b')
        ax5_twin.set_ylabel('Noise (dB)', color='r')
        ax5.set_title('Diurnal Wind and Noise Patterns')
        
        # 6. Headwind vs Aircraft Performance
        ax6 = axes[1, 2]
        performance_data = correlated_data[correlated_data['velocity'].notna()]
        scatter = ax6.scatter(performance_data['headwind_component'], 
                            performance_data['velocity'],
                            c=performance_data['baro_altitude'], 
                            cmap='viridis', alpha=0.6)
        ax6.set_xlabel('Headwind Component (m/s)')
        ax6.set_ylabel('Aircraft Velocity (m/s)')
        ax6.set_title('Aircraft Performance vs Headwind')
        plt.colorbar(scatter, ax=ax6)
        
        plt.tight_layout()
        plt.savefig(f'{output_prefix}_analysis.png', dpi=300, bbox_inches='tight')
        plt.show()
        
        print(f"✅ Weather-flight analysis visualizations saved to {output_prefix}_analysis.png")
    
    def _calculate_distance(self, lat1: float, lon1: float, 
                          lat2: float, lon2: float) -> float:
        """Calculate distance between two points in kilometers"""
        from math import radians, cos, sin, asin, sqrt
        
        # Convert to radians
        lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
        
        # Haversine formula
        dlat = lat2 - lat1
        dlon = lon2 - lon1
        a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
        c = 2 * asin(sqrt(a))
        
        # Earth radius in kilometers
        return c * 6371

def main():
    """Demonstrate MAQ weather integration capabilities"""
    
    print("=== MAQ Weather Integration for Aviation Analysis ===")
    
    # Initialize integrator
    integrator = MAQWeatherIntegrator()
    
    print("\n1. Discovering weather stations...")
    stations_df = integrator.discover_weather_stations()
    print(f"Found {len(stations_df)} weather stations")
    print(stations_df.head())
    
    print("\n2. Retrieving weather data (sample)...")
    end_date = datetime.now()
    start_date = end_date - timedelta(days=1)
    
    weather_data = integrator.retrieve_weather_data('240', start_date, end_date)
    print(f"Retrieved {len(weather_data)} weather observations")
    print(weather_data.head())
    
    print("\n3. Correlating with flight data...")
    if os.path.exists('enhanced_flight_data.db'):
        correlated_data = integrator.correlate_weather_flight_data(
            'enhanced_flight_data.db', weather_data
        )
        print(f"Correlated {len(correlated_data)} flight-weather observations")
        
        print("\n4. Analyzing patterns...")
        analysis = integrator.analyze_weather_flight_patterns(correlated_data)
        
        print("\n=== WEATHER-FLIGHT ANALYSIS RESULTS ===")
        for key, value in analysis.items():
            if isinstance(value, dict):
                print(f"\n{key}:")
                for sub_key, sub_value in value.items():
                    print(f"  {sub_key}: {sub_value}")
            else:
                print(f"{key}: {value}")
        
        print("\n5. Creating visualizations...")
        integrator.create_weather_flight_visualizations(correlated_data)
        
    else:
        print("❌ Flight database not found. Using sample weather data only.")

if __name__ == "__main__":
    import os
    main()