#!/bin/bash
# Setup script for Quantum Vulnerability Analysis

set -e  # Exit on any error

echo "Quantum Vulnerability Analysis Setup"
echo "==================================="

# Check if we're in the right directory
if [ ! -f "setup_database.py" ]; then
    echo "Error: setup_database.py not found. Please run this script from the quantum_analysis directory."
    exit 1
fi

# Check if virtual environment exists and activate it
if [ -d "../venv" ]; then
    echo "Activating virtual environment..."
    source ../venv/bin/activate
    echo "✓ Virtual environment activated"
else
    echo "Warning: Virtual environment not found. Please run setup.sh from the main project directory first."
fi

# Install additional dependencies
echo "Installing quantum analysis dependencies..."
pip install -r requirements.txt
echo "✓ Dependencies installed"

# Setup database
echo "Setting up quantum analysis database..."
python setup_database.py
echo "✓ Database setup completed"

# Verify P2PK data
echo "Verifying P2PK scanner data..."
python setup_database.py --verify
echo "✓ Data verification completed"

echo ""
echo "Quantum Vulnerability Analysis setup completed successfully!"
echo ""
echo "Next steps:"
echo "1. Run basic statistics: python basic_stats.py"
echo "2. Run anomaly detection: python detect_anomalies.py"
echo "3. Run complete analysis: python run_analysis.py"
echo ""
echo "For more information, see the README.md file." 