"""
OpenSky Network API client for Amsterdam Noord flight analysis
"""
import requests
import pandas as pd
import time
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
import json


class OpenSkyFetcher:
    """Client for fetching flight data from OpenSky Network API"""
    
    BASE_URL = "https://opensky-network.org/api"
    TOKEN_URL = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"
    
    # Amsterdam Noord bounding box coordinates
    AMSTERDAM_NOORD_BOUNDS = {
        'lat_min': 52.35,
        'lat_max': 52.45, 
        'lon_min': 4.85,
        'lon_max': 4.95
    }
    
    def __init__(self, client_id: Optional[str] = None, client_secret: Optional[str] = None, 
                 username: Optional[str] = None, password: Optional[str] = None):
        """
        Initialize OpenSky API client
        
        Args:
            client_id: OAuth2 client ID (preferred for new accounts)
            client_secret: OAuth2 client secret (preferred for new accounts)
            username: Legacy username (for older accounts)
            password: Legacy password (for older accounts)
        """
        self.session = requests.Session()
        self.access_token = None
        self.token_expires_at = None
        
        # Prefer OAuth2 credentials
        if client_id and client_secret:
            self.client_id = client_id
            self.client_secret = client_secret
            self.auth_method = 'oauth2'
            self.auth = None
        elif username and password:
            self.auth = (username, password)
            self.auth_method = 'basic'
            self.client_id = None
            self.client_secret = None
        else:
            self.auth = None
            self.auth_method = 'anonymous'
            self.client_id = None
            self.client_secret = None
    
    def _get_oauth2_token(self) -> bool:
        """Get OAuth2 access token"""
        if not self.client_id or not self.client_secret:
            return False
            
        # Check if we have a valid token
        if (self.access_token and self.token_expires_at and 
            datetime.now() < self.token_expires_at):
            return True
            
        try:
            response = self.session.post(
                self.TOKEN_URL,
                headers={'Content-Type': 'application/x-www-form-urlencoded'},
                data={
                    'grant_type': 'client_credentials',
                    'client_id': self.client_id,
                    'client_secret': self.client_secret
                },
                timeout=30
            )
            response.raise_for_status()
            
            token_data = response.json()
            self.access_token = token_data['access_token']
            expires_in = token_data.get('expires_in', 1800)  # Default 30 minutes
            self.token_expires_at = datetime.now() + timedelta(seconds=expires_in - 60)  # Refresh 1 min early
            
            print(f"✅ OAuth2 token obtained, expires in {expires_in} seconds")
            return True
            
        except Exception as e:
            print(f"❌ Failed to get OAuth2 token: {e}")
            return False
    
    def _get_auth_headers(self) -> dict:
        """Get authentication headers for API requests"""
        if self.auth_method == 'oauth2':
            if self._get_oauth2_token():
                return {'Authorization': f'Bearer {self.access_token}'}
            else:
                return {}
        return {}
        
    def get_current_flights(self) -> pd.DataFrame:
        """
        Get current flights over Amsterdam Noord
        
        Returns:
            DataFrame with current flight data
        """
        url = f"{self.BASE_URL}/states/all"
        
        params = {
            'lamin': self.AMSTERDAM_NOORD_BOUNDS['lat_min'],
            'lamax': self.AMSTERDAM_NOORD_BOUNDS['lat_max'],
            'lomin': self.AMSTERDAM_NOORD_BOUNDS['lon_min'],
            'lomax': self.AMSTERDAM_NOORD_BOUNDS['lon_max']
        }
        
        try:
            headers = self._get_auth_headers()
            response = self.session.get(url, params=params, headers=headers, 
                                      auth=self.auth, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            if not data or 'states' not in data or not data['states']:
                print("No flights found in Amsterdam Noord area")
                return pd.DataFrame()
                
            # Convert to DataFrame with proper column names
            columns = [
                'icao24', 'callsign', 'origin_country', 'time_position',
                'last_contact', 'longitude', 'latitude', 'baro_altitude',
                'on_ground', 'velocity', 'true_track', 'vertical_rate',
                'sensors', 'geo_altitude', 'squawk', 'spi', 'position_source'
            ]
            
            df = pd.DataFrame(data['states'], columns=columns)
            df['fetch_time'] = datetime.utcnow()
            df['data_time'] = pd.to_datetime(data['time'], unit='s')
            
            return self._clean_flight_data(df)
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data from OpenSky API: {e}")
            return pd.DataFrame()
        except json.JSONDecodeError as e:
            print(f"Error parsing JSON response: {e}")
            return pd.DataFrame()
    
    def get_historical_flights(self, hours_back: int = 1) -> pd.DataFrame:
        """
        Get historical flights for the past N hours
        
        Args:
            hours_back: Number of hours to look back (max 1 for free accounts)
        
        Returns:
            DataFrame with historical flight data
        """
        if hours_back > 1:
            print("Warning: Free OpenSky accounts limited to 1 hour historical data")
            hours_back = 1
            
        end_time = int(time.time())
        begin_time = end_time - (hours_back * 3600)
        
        url = f"{self.BASE_URL}/states/all"
        
        params = {
            'time': begin_time,
            'lamin': self.AMSTERDAM_NOORD_BOUNDS['lat_min'],
            'lamax': self.AMSTERDAM_NOORD_BOUNDS['lat_max'],
            'lomin': self.AMSTERDAM_NOORD_BOUNDS['lon_min'],
            'lomax': self.AMSTERDAM_NOORD_BOUNDS['lon_max']
        }
        
        try:
            headers = self._get_auth_headers()
            response = self.session.get(url, params=params, headers=headers, 
                                      auth=self.auth, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            if not data or 'states' not in data or not data['states']:
                print(f"No historical flights found for {hours_back} hours back")
                return pd.DataFrame()
                
            columns = [
                'icao24', 'callsign', 'origin_country', 'time_position',
                'last_contact', 'longitude', 'latitude', 'baro_altitude',
                'on_ground', 'velocity', 'true_track', 'vertical_rate',
                'sensors', 'geo_altitude', 'squawk', 'spi', 'position_source'
            ]
            
            df = pd.DataFrame(data['states'], columns=columns)
            df['fetch_time'] = datetime.utcnow()
            df['data_time'] = pd.to_datetime(data['time'], unit='s')
            
            return self._clean_flight_data(df)
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching historical data: {e}")
            return pd.DataFrame()
    
    def _clean_flight_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and process flight data"""
        if df.empty:
            return df
            
        # Clean callsigns
        df['callsign'] = df['callsign'].astype(str).str.strip()
        
        # Convert numeric columns
        numeric_cols = ['longitude', 'latitude', 'baro_altitude', 'velocity', 
                       'true_track', 'vertical_rate', 'geo_altitude']
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            
        # Filter out invalid coordinates
        df = df.dropna(subset=['longitude', 'latitude'])
        df = df[(df['longitude'] != 0) | (df['latitude'] != 0)]
        
        # Add aircraft type classification
        df['aircraft_type'] = df['icao24'].apply(self._classify_aircraft)
        
        return df
    
    def _classify_aircraft(self, icao24: str) -> str:
        """
        Basic aircraft type classification based on ICAO24 prefix
        This is a simplified classification - more sophisticated methods exist
        """
        if not icao24:
            return 'Unknown'
            
        # This is a basic heuristic - you might want to use a proper aircraft database
        icao24 = icao24.upper()
        
        # European aircraft codes (rough classification)
        if icao24.startswith(('4', 'D', 'G', 'F', 'I', 'N')):
            return 'Commercial'
        elif icao24.startswith(('A', 'C')):
            return 'Private/General Aviation'
        else:
            return 'Other'


if __name__ == "__main__":
    # Test the fetcher
    fetcher = OpenSkyFetcher()
    
    print("Fetching current flights over Amsterdam Noord...")
    current_flights = fetcher.get_current_flights()
    
    if not current_flights.empty:
        print(f"Found {len(current_flights)} flights")
        print("\nFlight summary:")
        print(current_flights[['callsign', 'origin_country', 'baro_altitude', 
                              'velocity', 'aircraft_type']].head())
    else:
        print("No flights found")