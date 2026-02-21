#!/bin/bash
# macOS: Double-click this file to start Famfolioz
# It activates the venv, starts Flask, and opens the browser.

cd "$(dirname "$0")"

# Check if port 5000 is already in use
if lsof -i :5000 -sTCP:LISTEN -t >/dev/null 2>&1; then
    echo "App is already running on port 5000. Opening browser..."
    open "http://127.0.0.1:5000"
    exit 0
fi

# Check setup
if [ ! -d "venv" ]; then
    echo "First time? Running setup..."
    bash setup_app.sh
fi

# Activate virtual environment
source venv/bin/activate

echo "Starting Famfolioz..."

# Start Flask in background
python3 -m cas_parser.webapp.app &
SERVER_PID=$!

# Wait for the server to be ready
for i in {1..20}; do
    if curl -s http://127.0.0.1:5000/health >/dev/null 2>&1; then
        echo "Server is ready!"
        open "http://127.0.0.1:5000"
        echo ""
        echo "App running at http://127.0.0.1:5000"
        echo "Close this window or press Ctrl+C to stop."
        echo ""
        wait $SERVER_PID
        exit 0
    fi
    sleep 0.5
done

echo "Server failed to start. Check the logs above."
kill $SERVER_PID 2>/dev/null
exit 1
