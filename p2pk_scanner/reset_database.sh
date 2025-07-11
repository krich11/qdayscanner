#!/bin/bash
# Reset script for P2PK Scanner database

echo "P2PK Scanner Database Reset"
echo "=========================="

# Check if we're in the right directory
if [ ! -f "setup_database.py" ]; then
    echo "Error: setup_database.py not found. Please run this script from the p2pk_scanner directory."
    exit 1
fi

# Check if virtual environment exists and activate it
if [ -d "../venv" ]; then
    echo "Activating virtual environment..."
    source ../venv/bin/activate
fi

echo "Resetting database tables..."
python setup_database.py --reset

if [ $? -eq 0 ]; then
    echo "✓ Database reset completed successfully"
else
    echo "✗ Database reset failed"
    exit 1
fi

echo ""
echo "Database has been reset. You can now run the scanner from the beginning." 