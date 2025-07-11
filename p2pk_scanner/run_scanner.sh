#!/bin/bash
# Run script for P2PK Scanner

echo "P2PK Scanner"
echo "============"

# Check if we're in the right directory
if [ ! -f "scanner.py" ]; then
    echo "Error: scanner.py not found. Please run this script from the p2pk_scanner directory."
    exit 1
fi

# Check if virtual environment exists and activate it
if [ -d "../venv" ]; then
    echo "Activating virtual environment..."
    source ../venv/bin/activate
fi

# Check if .env file exists in project root
if [ ! -f "../.env" ]; then
    echo "Warning: .env file not found in project root. Copying from example..."
    cp ../env.example ../.env
    echo "Please edit ../.env file with your configuration before running the scanner."
    exit 1
fi

# Create logs directory if it doesn't exist
mkdir -p logs

echo "Starting P2PK scanner..."
echo "Press Ctrl+C to stop the scanner"

# Run the scanner with all arguments passed through
python scanner.py "$@"

echo ""
echo "Scanner completed." 