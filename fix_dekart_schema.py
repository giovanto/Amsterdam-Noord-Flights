#!/usr/bin/env python3
"""
Fix Dekart schema access - create views in public schema
"""

import psycopg2

pg_params = {
    'host': '172.17.0.1',
    'user': 'gio',
    'password': 'alpinism',
    'database': 'aviation_impact_analysis'
}

def fix_schema():
    conn = psycopg2.connect(**pg_params)
    cursor = conn.cursor()
    
    # Create views in public schema that reference flight_data schema
    cursor.execute("""
        CREATE OR REPLACE VIEW public.dekart_map AS
        SELECT * FROM flight_data.dekart_map;
    """)
    
    cursor.execute("""
        CREATE OR REPLACE VIEW public.dekart_timeline AS
        SELECT * FROM flight_data.dekart_timeline;
    """)
    
    cursor.execute("""
        CREATE OR REPLACE VIEW public.dekart_patterns AS
        SELECT * FROM flight_data.dekart_patterns;
    """)
    
    cursor.execute("""
        CREATE OR REPLACE VIEW public.simple_flights AS
        SELECT * FROM flight_data.simple_flights;
    """)
    
    # Also set default search path
    cursor.execute("""
        ALTER DATABASE aviation_impact_analysis SET search_path TO public, flight_data;
    """)
    
    conn.commit()
    print("✅ Views created in public schema")
    print("\n📊 Updated queries for Dekart:")
    print("\n1. SELECT * FROM dekart_map ORDER BY collection_time DESC LIMIT 1000;")
    print("2. SELECT * FROM dekart_timeline;")
    print("3. SELECT * FROM dekart_patterns;")
    print("4. SELECT * FROM simple_flights WHERE noise_db > 65 ORDER BY noise_db DESC;")
    
    cursor.close()
    conn.close()

if __name__ == "__main__":
    fix_schema()