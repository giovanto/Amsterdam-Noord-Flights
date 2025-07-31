"""
Enhanced flight analyzer for Schiphol airport traffic and Amsterdam Noord 1032 analysis
"""
import pandas as pd
import numpy as np
import requests
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import json
from geopy.distance import geodesic
import math


class SchipholFlightAnalyzer:
    """Analyze flights specifically for Schiphol airport operations and local impact"""
    
    def __init__(self):
        """Initialize Schiphol flight analyzer"""
        
        # Schiphol airport details
        self.schiphol_coords = (52.3105, 4.7683)  # lat, lon
        self.schiphol_icao = "EHAM"
        self.schiphol_iata = "AMS"
        
        # Schiphol runway configurations (approximate coordinates)
        self.runways = {
            '18L-36R-Aalsmeerbaan': {'start': (52.2928, 4.7544), 'end': (52.3282, 4.7822)},
            '18R-36L-Zwanenburgbaan': {'start': (52.2953, 4.7344), 'end': (52.3307, 4.7622)},
            '06-24-Kaagbaan': {'start': (52.3083, 4.7264), 'end': (52.3127, 4.8103)},
            '09-27-Buitenveldertbaan': {'start': (52.2853, 4.7283), 'end': (52.2895, 4.8122)},
            '04-22-Schiphol-Oostbaan': {'start': (52.3153, 4.7664), 'end': (52.3055, 4.7703)},
        }
        
        # Aircraft noise classification (approximate dB levels)
        self.aircraft_noise_levels = {
            'A380': 85, 'B747': 84, 'B777': 82, 'A350': 78, 'B787': 77,
            'A340': 83, 'B767': 81, 'A330': 80, 'B757': 79, 'A321': 76,
            'B737': 75, 'A320': 74, 'E190': 72, 'CRJ': 70, 'ATR': 68
        }
        
        # Typical approach/departure corridors from/to Schiphol
        self.approach_corridors = {
            'north': {'bearing_range': (315, 45), 'description': 'Northern approach (over Amsterdam Noord)'},
            'northeast': {'bearing_range': (45, 90), 'description': 'Northeastern approach'},  
            'east': {'bearing_range': (90, 135), 'description': 'Eastern approach'},
            'southeast': {'bearing_range': (135, 180), 'description': 'Southeastern approach'},
            'south': {'bearing_range': (180, 225), 'description': 'Southern approach'},
            'southwest': {'bearing_range': (225, 270), 'description': 'Southwestern approach'},
            'west': {'bearing_range': (270, 315), 'description': 'Western approach (over North Sea)'}
        }
    
    def classify_aircraft_by_icao(self, icao24: str, callsign: str = None) -> Dict[str, str]:
        """
        Enhanced aircraft classification using ICAO24 address and callsign
        
        Args:
            icao24: ICAO 24-bit address
            callsign: Aircraft callsign (optional)
            
        Returns:
            Dictionary with aircraft classification details
        """
        if not icao24:
            return {'type': 'Unknown', 'category': 'Unknown', 'noise_level': 'Unknown'}
        
        icao24 = icao24.upper().strip()
        classification = {
            'type': 'Unknown',
            'category': 'Unknown', 
            'noise_level': 'Unknown',
            'likely_commercial': False,
            'likely_private': False
        }
        
        # Basic classification by ICAO24 prefix (country/region codes)
        if icao24.startswith(('4', 'D', 'G', 'F', 'I')):  # Europe
            classification['likely_commercial'] = True
            classification['category'] = 'Commercial'
        elif icao24.startswith(('A', 'C')):  # North America (often private/corporate)
            classification['likely_private'] = True
            classification['category'] = 'Private/Corporate'
        elif icao24.startswith('PH'):  # Netherlands prefix
            classification['category'] = 'Netherlands Registered'
        
        # Enhanced classification using callsign patterns
        if callsign:
            callsign = callsign.strip().upper()
            
            # Major airlines
            airline_patterns = {
                'KLM': {'type': 'Commercial Airline', 'category': 'Major Carrier'},
                'TRA': {'type': 'Transavia', 'category': 'Low Cost Carrier'},
                'EZY': {'type': 'EasyJet', 'category': 'Low Cost Carrier'},
                'RYR': {'type': 'Ryanair', 'category': 'Low Cost Carrier'},
                'BAW': {'type': 'British Airways', 'category': 'Major Carrier'},
                'DLH': {'type': 'Lufthansa', 'category': 'Major Carrier'},
                'AFR': {'type': 'Air France', 'category': 'Major Carrier'},
                'UAE': {'type': 'Emirates', 'category': 'Major Carrier'},
                'QTR': {'type': 'Qatar Airways', 'category': 'Major Carrier'},
            }
            
            for pattern, info in airline_patterns.items():
                if callsign.startswith(pattern):
                    classification.update(info)
                    classification['likely_commercial'] = True
                    break
            
            # Private/corporate patterns
            if any(callsign.startswith(p) for p in ['N', 'G-', 'PH-', 'D-', 'F-']):
                if len(callsign) <= 6:  # Typical private aircraft callsign length
                    classification['likely_private'] = True
                    classification['category'] = 'Private/General Aviation'
        
        return classification
    
    def calculate_noise_impact(self, flight_data: pd.DataFrame, target_coords: Tuple[float, float]) -> pd.DataFrame:
        """
        Calculate estimated noise impact for flights over a specific location
        
        Args:
            flight_data: DataFrame with flight information
            target_coords: (lat, lon) of target location
            
        Returns:
            DataFrame with added noise impact columns
        """
        if flight_data.empty:
            return flight_data
        
        df = flight_data.copy()
        
        # Calculate distance from target location
        def calc_distance(row):
            if pd.isna(row['latitude']) or pd.isna(row['longitude']):
                return float('inf')
            return geodesic((row['latitude'], row['longitude']), target_coords).kilometers
        
        df['distance_km'] = df.apply(calc_distance, axis=1)
        
        # Estimate noise level based on altitude and distance
        def estimate_noise(row):
            if pd.isna(row['baro_altitude']) or row['distance_km'] == float('inf'):
                return 0
            
            altitude_ft = max(row['baro_altitude'], 100)  # Avoid division by zero
            distance_km = max(row['distance_km'], 0.1)
            
            # Basic noise model: higher altitude = less noise, closer distance = more noise
            # This is a simplified model - real noise depends on many factors
            base_noise = 80  # Base noise level for jet aircraft
            altitude_reduction = min(altitude_ft / 1000 * 5, 40)  # Reduce 5dB per 1000ft, max 40dB
            distance_reduction = min(distance_km * 2, 20)  # Reduce 2dB per km, max 20dB
            
            estimated_noise = max(base_noise - altitude_reduction - distance_reduction, 30)
            return round(estimated_noise, 1)
        
        df['estimated_noise_db'] = df.apply(estimate_noise, axis=1)
        
        # Classify noise impact level
        def noise_impact_level(noise_db):
            if noise_db >= 65:
                return 'High Impact'
            elif noise_db >= 55:
                return 'Moderate Impact'  
            elif noise_db >= 45:
                return 'Low Impact'
            else:
                return 'Minimal Impact'
        
        df['noise_impact'] = df['estimated_noise_db'].apply(noise_impact_level)
        
        return df
    
    def identify_schiphol_operations(self, flight_data: pd.DataFrame) -> pd.DataFrame:
        """
        Identify flights that are likely Schiphol arrivals or departures
        
        Args:
            flight_data: DataFrame with flight data
            
        Returns:
            DataFrame with Schiphol operation classifications
        """
        if flight_data.empty:
            return flight_data
        
        df = flight_data.copy()
        
        # Calculate distance from Schiphol
        def distance_to_schiphol(row):
            if pd.isna(row['latitude']) or pd.isna(row['longitude']):
                return float('inf')
            return geodesic((row['latitude'], row['longitude']), self.schiphol_coords).kilometers
        
        df['distance_to_schiphol_km'] = df.apply(distance_to_schiphol, axis=1)
        
        # Calculate bearing from Schiphol (for approach/departure analysis)
        def bearing_from_schiphol(row):
            if pd.isna(row['latitude']) or pd.isna(row['longitude']):
                return None
            
            lat1, lon1 = math.radians(self.schiphol_coords[0]), math.radians(self.schiphol_coords[1])
            lat2, lon2 = math.radians(row['latitude']), math.radians(row['longitude'])
            
            dlon = lon2 - lon1
            y = math.sin(dlon) * math.cos(lat2)
            x = math.cos(lat1) * math.sin(lat2) - math.sin(lat1) * math.cos(lat2) * math.cos(dlon)
            
            bearing = math.atan2(y, x)
            bearing = math.degrees(bearing)
            bearing = (bearing + 360) % 360  # Normalize to 0-360
            
            return round(bearing, 1)
        
        df['bearing_from_schiphol'] = df.apply(bearing_from_schiphol, axis=1)
        
        # Classify operation type based on distance, altitude, and patterns
        def classify_operation(row):
            distance = row['distance_to_schiphol_km']
            altitude = row.get('baro_altitude', 0)
            
            if distance == float('inf'):
                return 'Unknown'
            elif distance < 5:  # Very close to Schiphol
                if altitude < 1000:
                    return 'Landing/Takeoff'
                else:
                    return 'Airport Vicinity'
            elif distance < 15:  # Approach/departure area
                if altitude < 5000:
                    return 'Approach/Departure'
                else:
                    return 'Transit (Low)'
            elif distance < 30:  # Extended approach area
                if altitude < 10000:
                    return 'Extended Approach'
                else:
                    return 'Transit (Medium)'
            else:
                return 'Transit (High)'
        
        df['schiphol_operation'] = df.apply(classify_operation, axis=1)
        
        # Identify approach corridor
        def identify_corridor(bearing):
            if pd.isna(bearing):
                return 'Unknown'
            
            for corridor_name, corridor_info in self.approach_corridors.items():
                start, end = corridor_info['bearing_range']
                if start <= end:  # Normal range
                    if start <= bearing <= end:
                        return corridor_name
                else:  # Range crosses 0 degrees (e.g., 315-45)
                    if bearing >= start or bearing <= end:
                        return corridor_name
            
            return 'Other'
        
        df['approach_corridor'] = df['bearing_from_schiphol'].apply(identify_corridor)
        
        return df
    
    def analyze_residential_impact(self, flight_data: pd.DataFrame, 
                                 target_coords: Tuple[float, float],
                                 postcode: str = "1032") -> Dict:
        """
        Comprehensive analysis of flight impact on residential area
        
        Args:
            flight_data: DataFrame with flight data
            target_coords: (lat, lon) of residential area center
            postcode: Postal code for reference
            
        Returns:
            Dictionary with detailed residential impact analysis
        """
        if flight_data.empty:
            return {"error": "No flight data available"}
        
        # Process the data
        df = flight_data.copy()
        df = self.calculate_noise_impact(df, target_coords)
        df = self.identify_schiphol_operations(df)
        
        # Add enhanced aircraft classification
        if 'icao24' in df.columns:
            classifications = df.apply(
                lambda row: self.classify_aircraft_by_icao(
                    row.get('icao24', ''), 
                    row.get('callsign', '')
                ), axis=1
            )
            
            for key in ['type', 'category', 'likely_commercial', 'likely_private']:
                df[f'aircraft_{key}'] = [c.get(key, 'Unknown') for c in classifications]
        
        # Analysis results
        analysis = {
            "postcode": postcode,
            "analysis_time": datetime.now().isoformat(),
            "target_coordinates": target_coords,
            "total_flights": len(df),
        }
        
        # Noise impact analysis
        noise_impacts = df['noise_impact'].value_counts().to_dict()
        high_noise_flights = df[df['estimated_noise_db'] >= 65]
        
        analysis["noise_analysis"] = {
            "impact_distribution": noise_impacts,
            "high_impact_flights": len(high_noise_flights),
            "average_noise_level": round(df['estimated_noise_db'].mean(), 1),
            "max_noise_level": round(df['estimated_noise_db'].max(), 1),
            "high_noise_aircraft": high_noise_flights['callsign'].tolist() if not high_noise_flights.empty else []
        }
        
        # Schiphol operations analysis
        operations = df['schiphol_operation'].value_counts().to_dict()
        corridors = df['approach_corridor'].value_counts().to_dict()
        
        analysis["schiphol_operations"] = {
            "operation_types": operations,
            "approach_corridors": corridors,
            "likely_schiphol_traffic": len(df[df['distance_to_schiphol_km'] < 30]),
            "direct_overhead": len(df[df['distance_km'] < 2])  # Within 2km
        }
        
        # Aircraft classification
        if 'aircraft_category' in df.columns:
            categories = df['aircraft_category'].value_counts().to_dict()
            commercial_flights = len(df[df['aircraft_likely_commercial'] == True])
            private_flights = len(df[df['aircraft_likely_private'] == True])
            
            analysis["aircraft_analysis"] = {
                "categories": categories,
                "commercial_flights": commercial_flights,
                "private_flights": private_flights,
                "commercial_percentage": round((commercial_flights / len(df)) * 100, 1),
                "private_percentage": round((private_flights / len(df)) * 100, 1)
            }
        
        # Time-based patterns (if time data available)
        if 'data_time' in df.columns:
            df['hour'] = pd.to_datetime(df['data_time']).dt.hour
            hourly_distribution = df['hour'].value_counts().sort_index().to_dict()
            
            # Night flights (23:00 - 07:00)
            night_hours = [23, 0, 1, 2, 3, 4, 5, 6]
            night_flights = df[df['hour'].isin(night_hours)]
            
            analysis["temporal_analysis"] = {
                "hourly_distribution": hourly_distribution,
                "night_flights": len(night_flights),
                "night_percentage": round((len(night_flights) / len(df)) * 100, 1),
                "peak_hour": df['hour'].mode().iloc[0] if not df['hour'].mode().empty else None
            }
        
        return analysis


if __name__ == "__main__":
    # Test the Schiphol analyzer
    analyzer = SchipholFlightAnalyzer()
    
    # Test coordinates for Amsterdam Noord 1032 (approximate)
    test_coords = (52.395, 4.915)
    
    # Create sample flight data for testing
    sample_data = pd.DataFrame({
        'icao24': ['484506', 'A1B2C3', 'PH1234'],
        'callsign': ['KLM123', 'EZY456', 'PHG789'],
        'latitude': [52.40, 52.35, 52.38],
        'longitude': [4.90, 4.78, 4.92],
        'baro_altitude': [2500, 15000, 8000],
        'velocity': [180, 420, 250],
        'data_time': pd.Timestamp.now()
    })
    
    print("Testing Schiphol Flight Analyzer...")
    result = analyzer.analyze_residential_impact(sample_data, test_coords)
    
    print(f"\nResidential Impact Analysis:")
    for section, data in result.items():
        if isinstance(data, dict):
            print(f"\n{section.upper()}:")
            for key, value in data.items():
                print(f"  {key}: {value}")
        else:
            print(f"{section}: {data}")