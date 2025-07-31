# Aviation Impact Analysis - Setup Guide

## Quick Start

### 1. Check System Status
```bash
# Monitor collection status
./monitor.sh

# Check pipeline health
python3 monitoring_dashboard.py
```

### 2. Access Databases
```bash
# SQLite collection database (on server)
ssh linux-server "cd /opt/flight-collector && sqlite3 amsterdam_flight_patterns_2week.db"

# PostgreSQL analysis database
ssh linux-server "PGPASSWORD=alpinism psql -h 172.17.0.1 -U gio -d aviation_impact_analysis"
```

### 3. Run Data Pipeline
```bash
# Single ETL run (SQLite → PostgreSQL)
python3 etl_pipeline.py

# View available analysis tables
ssh linux-server "PGPASSWORD=alpinism psql -h 172.17.0.1 -U gio -d aviation_impact_analysis -c '\dt flight_data.*'"
```

## Architecture Overview

```
┌─── Data Collection ───┐    ┌──── ETL Pipeline ────┐    ┌─── Analysis Layer ───┐
│                       │    │                      │    │                      │
│  OpenSky API         │    │                      │    │  PostgreSQL          │
│        ↓              │    │   etl_pipeline.py    │    │  aviation_impact_    │
│  SQLite Database     │───→│                      │───→│  analysis            │
│  (Primary Storage)    │    │   - Data Validation  │    │                      │
│                       │    │   - Quality Checks   │    │  ├─ flight_data.*    │
│  Flight Collector     │    │   - Error Handling   │    │  ├─ analysis.*       │
│  Service              │    │                      │    │  └─ monitoring.*     │
└───────────────────────┘    └──────────────────────┘    └──────────────────────┘
           │                           │                           │
           ↓                           ↓                           ↓
    monitor.sh                monitoring_dashboard.py         Dekart/Analysis
```

## Database Schema

### Primary Schemas
- **`flight_data`**: Core flight records and aircraft metadata
- **`analysis`**: Correlation tables (prepared for demographic data)
- **`monitoring`**: Data quality and collection statistics

### Key Tables
```sql
flight_data.flights              -- Main flight records (optimized for analysis)
flight_data.aircraft_types       -- Aircraft reference data
analysis.demographic_zones       -- Future: demographic correlation zones
analysis.flight_zone_correlations -- Future: correlation results
monitoring.collection_stats      -- Daily collection metrics
monitoring.data_quality_issues   -- Automated issue tracking
```

## Common Operations

### Daily Monitoring
```bash
# Quick status check
./monitor.sh

# Detailed dashboard
python3 monitoring_dashboard.py

# Generate daily report
python3 monitoring_dashboard.py --report > daily_report_$(date +%Y%m%d).json
```

### Data Pipeline Management
```bash
# Transfer new data to PostgreSQL
python3 etl_pipeline.py

# Continuous ETL processing (production)
python3 etl_pipeline.py --continuous

# Check ETL status
python3 monitoring_dashboard.py | grep -A 10 "PostgreSQL"
```

### Analysis Queries
```sql
-- Recent high-impact flights
SELECT * FROM analysis.high_impact_flights LIMIT 10;

-- Daily collection summary
SELECT * FROM analysis.collection_summary ORDER BY collection_date DESC LIMIT 7;

-- Data quality overview
SELECT 
    collection_date,
    flights_collected,
    data_quality_score
FROM monitoring.collection_stats 
ORDER BY collection_date DESC LIMIT 7;
```

## Development Workflow

### 1. Check Collection Health
```bash
./monitor.sh
```

### 2. Transfer Data for Analysis
```bash
python3 etl_pipeline.py
```

### 3. Monitor Data Quality
```bash
python3 monitoring_dashboard.py --watch
```

### 4. Access Analysis Database
```bash
ssh linux-server "PGPASSWORD=alpinism psql -h 172.17.0.1 -U gio -d aviation_impact_analysis"
```

## Troubleshooting

### Collection Issues
```bash
# Check service status
ssh linux-server "systemctl status flight-collector"

# View recent logs
ssh linux-server "journalctl -u flight-collector -n 50"

# Restart if needed
ssh linux-server "systemctl restart flight-collector"
```

### Database Issues
```bash
# Test PostgreSQL connection
ssh linux-server "PGPASSWORD=alpinism psql -h 172.17.0.1 -U gio -d aviation_impact_analysis -c 'SELECT version();'"

# Check disk space
ssh linux-server "df -h"

# View monitoring issues
ssh linux-server "PGPASSWORD=alpinism psql -h 172.17.0.1 -U gio -d aviation_impact_analysis -c 'SELECT * FROM monitoring.data_quality_issues ORDER BY created_at DESC LIMIT 10;'"
```

### Pipeline Issues
```bash
# Test ETL pipeline
python3 etl_pipeline.py --verbose

# Check monitoring dashboard
python3 monitoring_dashboard.py
```

## Next Steps

Once you have 2 weeks of reliable data:

1. **Correlation Analysis**: Use specialized agents to develop demographic correlations
2. **Dekart Visualization**: Set up real-time dashboards using PostgreSQL backend
3. **ODIN Integration**: Connect with Roland's mobility dataset
4. **Environmental Justice Analysis**: Implement bias detection algorithms
5. **Hackathon Preparation**: Build interactive presentation system

---

**System Status**: ✅ Production ready for 2-week collection
**Data Quality**: ✅ Automated validation and monitoring
**Analysis Ready**: ✅ PostgreSQL backend with PostGIS support