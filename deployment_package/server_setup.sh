#!/bin/bash
set -e

APP_USER="flightcollector"
APP_DIR="/opt/flight-collector"

echo "🔧 Installing application files..."

# Install Python dependencies
sudo -u $APP_USER $APP_DIR/venv/bin/pip install -r $APP_DIR/requirements_prod.txt

# Set proper permissions
chmod 644 $APP_DIR/*.py
chmod 600 $APP_DIR/credentials.json
chown $APP_USER:$APP_USER $APP_DIR/* 

# Install systemd service
cp $APP_DIR/flight-collector.service /etc/systemd/system/
systemctl daemon-reload

# Install log rotation
cp $APP_DIR/flight-collector.logrotate /etc/logrotate.d/flight-collector

echo "✅ Application installed successfully!"
echo "📝 Next steps:"
echo "   1. Edit $APP_DIR/credentials.json with your OpenSky API credentials"
echo "   2. Run: systemctl enable flight-collector"
echo "   3. Run: systemctl start flight-collector"
echo "   4. Monitor: journalctl -u flight-collector -f"
