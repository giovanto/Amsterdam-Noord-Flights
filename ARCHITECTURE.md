# Aviation Impact Analysis - System Architecture

## Overview
Robust data pipeline and analysis framework for 2-week flight data collection and future multi-dimensional correlation analysis for Dutch Mobility Hackathon 2025.

## System Components

### 1. Data Collection Layer
- **SQLite Database**: `amsterdam_flight_patterns_2week.db`
  - Primary collection database on remote server
  - Real-time flight data from OpenSky API
  - ~500 flights/hour collection rate
  - Cleaned of unreliable pre-fix data (before 2025-07-31 15:42:05)

### 2. Analysis Database Layer
- **PostgreSQL Database**: `aviation_impact_analysis`
  - Host: `172.17.0.1:5432`
  - Credentials: `gio:alpinism`
  - Schemas:
    - `flight_data`: Core flight records and aircraft metadata
    - `analysis`: Correlation tables and demographic zones (ready for future)
    - `monitoring`: Data quality and collection statistics

### 3. Data Pipeline Components

#### ETL Pipeline (`etl_pipeline.py`)
- **Extract**: Read new flights from SQLite (incremental processing)
- **Transform**: Clean and standardize data for PostgreSQL schema
- **Load**: Batch insert with error handling and validation
- **Modes**: Single run or continuous processing

#### Monitoring System (`monitoring_dashboard.py`)
- **Real-time monitoring**: Collection rates, data quality, system health
- **Data validation**: Coordinate validation, noise level checks
- **Health scoring**: 0-100 pipeline health score with issue detection
- **Reporting**: Daily summaries and recommendations

### 4. Database Schema Design

#### Core Tables
```sql
flight_data.flights              -- Main flight records
flight_data.aircraft_types       -- Aircraft reference data
analysis.demographic_zones       -- Future demographic correlation (PostGIS ready)
analysis.flight_zone_correlations -- Future correlation results
monitoring.collection_stats      -- Daily collection metrics
monitoring.data_quality_issues   -- Automated issue tracking
```

#### Key Features
- **Spatial indexing**: PostGIS enabled for geographic analysis
- **Temporal indexing**: Optimized for time-series analysis
- **Data validation**: Automated triggers for quality assurance
- **Views**: Pre-built analysis views for common patterns

## Usage Instructions

### Initial Setup
```bash
# Create PostgreSQL database (already done)
ssh linux-server "PGPASSWORD=alpinism psql -h 172.17.0.1 -U gio -d aviation_impact_analysis -f database_schema.sql"

# Test monitoring
python3 monitoring_dashboard.py

# Run ETL pipeline
python3 etl_pipeline.py
```

### Daily Operations
```bash
# Check collection status
./monitor.sh

# View dashboard
python3 monitoring_dashboard.py

# Continuous monitoring
python3 monitoring_dashboard.py --watch

# Generate daily report
python3 monitoring_dashboard.py --report
```

### Data Pipeline Modes
```bash
# Single ETL run
python3 etl_pipeline.py

# Continuous ETL processing
python3 etl_pipeline.py --continuous
```

## System Health Metrics

### Collection Performance
- **Target**: 400-600 flights/hour
- **Current**: ~500 flights/hour ✅
- **Quality**: <5% missing coordinates
- **Uptime**: 99%+ collection service availability

### Data Quality Indicators
- **Coordinate Validation**: Lat/Lon range checking
- **Altitude Validation**: Reasonable flight altitude ranges
- **Noise Estimation**: Completeness of noise impact calculations
- **Temporal Consistency**: Regular collection intervals

## Future Development Ready

### Prepared Integrations
1. **ODIN Mobility Data**: Schema ready for demographic correlation
2. **Dekart Visualization**: PostgreSQL backend ready for real-time dashboards
3. **Spatial Analysis**: PostGIS enabled for geographic correlations
4. **Wind Data Integration**: Schema extensible for meteorological factors

### Analysis Framework Extensions
- **Correlation Engine**: Modular design for multi-dimensional analysis
- **Environmental Justice**: Demographic bias detection algorithms
- **Policy Recommendations**: Data-driven insight generation
- **Predictive Modeling**: Machine learning pipeline foundation

## Maintenance Guidelines

### Daily Checks
1. Monitor collection rates and system health
2. Review data quality metrics
3. Check PostgreSQL disk space and performance
4. Validate ETL pipeline execution

### Weekly Tasks
1. Generate comprehensive collection reports
2. Analyze data patterns and trends
3. Review and optimize database performance
4. Update documentation based on operational insights

### Data Backup Strategy
- **SQLite**: Automated daily backups on server
- **PostgreSQL**: Point-in-time recovery enabled
- **Configuration**: Version controlled in git repository

## Scalability Considerations

### Current Capacity
- **Storage**: Handles 10K+ flights (2-week collection target)
- **Processing**: Real-time ETL with <5 minute latency
- **Analysis**: Optimized queries for correlation discovery

### Growth Handling
- **Partitioning**: Time-based partitioning ready for large datasets
- **Indexing**: Strategic indexes for common analysis patterns
- **Caching**: Pipeline caching for repeated analysis operations

---

**Status**: Production ready for 2-week collection period
**Next Phase**: Correlation analysis and hackathon preparation after data collection completion