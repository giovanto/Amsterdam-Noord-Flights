#!/bin/bash

# Flight Collector Monitoring Script
SERVER_HOST="linux-server"

echo "🔍 Flight Collector Status Monitor"
echo "=================================="

echo "📊 Service Status:"
ssh $SERVER_HOST "systemctl is-active flight-collector.service"

echo ""
echo "💾 Database Status:"
ssh $SERVER_HOST "
cd /opt/flight-collector
if [ -f amsterdam_flight_patterns_2week.db ]; then
    echo '✅ Database exists'
    echo '📈 Database size:' \$(du -h amsterdam_flight_patterns_2week.db | cut -f1)
    echo '📝 Flight count:' \$(sqlite3 amsterdam_flight_patterns_2week.db 'SELECT COUNT(*) FROM flights;' 2>/dev/null || echo '0')
    echo '🏠 Flights over house:' \$(sqlite3 amsterdam_flight_patterns_2week.db 'SELECT COUNT(*) FROM flights WHERE is_over_house = 1;' 2>/dev/null || echo '0')
else
    echo '❌ Database not found'
fi
"

echo ""
echo "📋 Recent Logs (last 10 lines):"
ssh $SERVER_HOST "tail -10 /opt/flight-collector/two_week_collector.log 2>/dev/null || echo 'No logs yet'"

echo ""
echo "🔧 Service Details:"
ssh $SERVER_HOST "systemctl status flight-collector.service --no-pager -l"