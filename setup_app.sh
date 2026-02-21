#!/bin/bash
#
# One-time setup for Famfolioz
# Run this after cloning/unzipping the project
#
set -e

cd "$(dirname "$0")"

echo "==================================="
echo "  Famfolioz - First Time Setup"
echo "==================================="
echo ""

# Check Python
if ! command -v python3 &>/dev/null; then
    echo "ERROR: Python 3 is not installed."
    echo ""
    echo "Install it from: https://www.python.org/downloads/"
    echo "  - macOS: brew install python3"
    echo "  - Ubuntu: sudo apt install python3 python3-venv python3-pip"
    echo "  - Windows: Download from python.org (check 'Add to PATH')"
    exit 1
fi

PYTHON_VERSION=$(python3 --version 2>&1)
echo "Found $PYTHON_VERSION"

# Create virtual environment if not exists
if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv venv
    echo "Virtual environment created."
else
    echo "Virtual environment already exists."
fi

# Activate
source venv/bin/activate 2>/dev/null || . venv/bin/activate

# Install dependencies
echo "Installing dependencies..."
pip install --upgrade pip --quiet
pip install -r requirements.txt --quiet
pip install -e . --quiet

echo ""
echo "==================================="
echo "  Setup complete!"
echo "==================================="
echo ""
echo "To start the app:"
echo "  macOS:   Double-click start.command"
echo "  Any OS:  ./start.sh  (or: source venv/bin/activate && python3 -m cas_parser.webapp.app)"
echo ""
echo "Then open http://127.0.0.1:5000 in your browser."
echo ""
