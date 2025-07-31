#!/bin/bash
# Push Amsterdam Noord Flights to GitHub

echo "🚀 Initializing Amsterdam Noord Flights repository..."

# Initialize git if not already done
if [ ! -d ".git" ]; then
    git init
fi

# Add all files
git add .

# Create initial commit
git commit -m "Initial commit: Amsterdam Noord flight analysis system

- Real-time flight data collection (500+ flights/hour)
- PostgreSQL analysis database with PostGIS support
- ETL pipeline with automated validation and monitoring  
- Multi-dimensional correlation analysis framework
- Production-ready on Studio Bereikbaar infrastructure
- Prepared for Dutch Mobility Hackathon 2025

Complete architectural thinking and planning preserved:
- CLAUDE.md: Development session guide and project context
- ARCHITECTURE.md: Technical architecture and design decisions
- HACKATHON_PROJECT_BRIEF.md: Multi-dimensional analysis strategy
- AGENT_BRIEFING.md: Development priorities and research questions
- SETUP_GUIDE.md: Operations guide and workflow documentation"

# Add remote and push
git remote add origin https://github.com/giovanto/Amsterdam-Noord-Flights.git
git branch -M main
git push -u origin main

echo "✅ Repository pushed to GitHub!"
echo "📍 https://github.com/giovanto/Amsterdam-Noord-Flights"