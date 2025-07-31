#!/bin/bash
set -e

# Secure File Deployment Script
SERVER_HOST="linux-server"
APP_USER="flightcollector"
APP_DIR="/opt/flight-collector"

echo "📦 Preparing deployment package..."

# Create deployment package locally
DEPLOY_DIR="./deployment_package"
rm -rf $DEPLOY_DIR
mkdir -p $DEPLOY_DIR

# Copy essential files only (security-focused)
echo "📋 Copying essential files..."
cp opensky_fetcher.py $DEPLOY_DIR/
cp schiphol_analyzer.py $DEPLOY_DIR/
cp cache_manager.py $DEPLOY_DIR/
cp two_week_flight_collector.py $DEPLOY_DIR/
cp requirements.txt $DEPLOY_DIR/

# Create production requirements (minimal)
cat > $DEPLOY_DIR/requirements_prod.txt << 'EOF'
requests>=2.32.3
pandas>=2.3.1
geopy>=2.4.1
schedule>=1.2.2
pyarrow>=21.0.0
EOF

# Create secure credentials template
cat > $DEPLOY_DIR/credentials.json.template << 'EOF'
{
    "clientId": "YOUR_OPENSKY_CLIENT_ID",
    "clientSecret": "YOUR_OPENSKY_CLIENT_SECRET"
}
EOF

# Create systemd service file
cat > $DEPLOY_DIR/flight-collector.service << EOF
[Unit]
Description=Flight Data Collector for Amsterdam Noord
After=network.target
Wants=network-online.target

[Service]
Type=simple
User=$APP_USER
Group=$APP_USER
WorkingDirectory=$APP_DIR
Environment=PATH=$APP_DIR/venv/bin
ExecStart=$APP_DIR/venv/bin/python $APP_DIR/two_week_flight_collector.py --start
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal
SyslogIdentifier=flight-collector

# Security settings
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=$APP_DIR

[Install]
WantedBy=multi-user.target
EOF

# Create log rotation config
cat > $DEPLOY_DIR/flight-collector.logrotate << EOF
$APP_DIR/two_week_collector.log {
    daily
    rotate 7
    compress
    delaycompress
    missingok
    notifempty
    create 644 $APP_USER $APP_USER
    postrotate
        systemctl reload-or-restart flight-collector.service
    endscript
}
EOF

# Create deployment script for server
cat > $DEPLOY_DIR/server_setup.sh << 'EOF'
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
EOF

chmod +x $DEPLOY_DIR/server_setup.sh

echo "📦 Deployment package ready in: $DEPLOY_DIR"
echo "🚀 Uploading to server..."

# Upload files securely
scp -r $DEPLOY_DIR/* $SERVER_HOST:$APP_DIR/

echo "✅ Files uploaded successfully!"
echo "🔧 Running server setup..."

# Run setup on server
ssh $SERVER_HOST "cd $APP_DIR && bash server_setup.sh"

echo "🎉 Deployment complete!"
echo ""
echo "⚠️  IMPORTANT SECURITY STEPS:"
echo "1. Add your OpenSky credentials to: $APP_DIR/credentials.json"
echo "2. Test the service: systemctl start flight-collector"
echo "3. Enable auto-start: systemctl enable flight-collector"
echo "4. Monitor logs: journalctl -u flight-collector -f"