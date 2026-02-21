#!/bin/bash
# Start Famfolioz (works on macOS and Linux)

cd "$(dirname "$0")"

# Check setup
if [ ! -d "venv" ]; then
    echo "First time? Running setup..."
    bash setup_app.sh
fi

# Activate virtual environment
source venv/bin/activate

echo "Starting Famfolioz..."
echo "Open http://127.0.0.1:5000 in your browser"
echo "Press Ctrl+C to stop."
echo ""

python3 -m cas_parser.webapp.app
