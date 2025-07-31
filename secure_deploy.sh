#!/bin/bash
set -e

# Secure Flight Collector Deployment Script
# This script safely deploys the flight collector to a Linux server

SERVER_HOST="linux-server"
APP_USER="flightcollector"
APP_DIR="/opt/flight-collector"
SERVICE_NAME="flight-collector"

echo "🔒 Starting secure deployment to Linux server..."

# Step 1: Create dedicated user (non-root)
echo "📝 Creating dedicated application user..."
ssh $SERVER_HOST "
    # Create app user if doesn't exist
    if ! id -u $APP_USER > /dev/null 2>&1; then
        useradd -r -s /bin/bash -d $APP_DIR -m $APP_USER
        echo '✅ Created user: $APP_USER'
    else
        echo '✅ User $APP_USER already exists'
    fi
"

# Step 2: Create secure application directory
echo "📁 Setting up application directory..."
ssh $SERVER_HOST "
    mkdir -p $APP_DIR
    chown $APP_USER:$APP_USER $APP_DIR
    chmod 755 $APP_DIR
    echo '✅ Created directory: $APP_DIR'
"

# Step 3: Install Python dependencies
echo "🐍 Installing Python dependencies..."
ssh $SERVER_HOST "
    apt-get update
    apt-get install -y python3 python3-pip python3-venv git
    echo '✅ Python environment ready'
"

# Step 4: Create virtual environment
echo "🏗️ Setting up Python virtual environment..."
ssh $SERVER_HOST "
    sudo -u $APP_USER python3 -m venv $APP_DIR/venv
    sudo -u $APP_USER $APP_DIR/venv/bin/pip install --upgrade pip
    echo '✅ Virtual environment created'
"

echo "🚀 Deployment infrastructure ready!"
echo "Next: Run this script to continue with secure deployment"