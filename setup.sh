#!/bin/bash
# Setup script for Bitcoin Quantum Vulnerability Scanner

set -e  # Exit on any error

echo "Bitcoin Quantum Vulnerability Scanner Setup"
echo "=========================================="

# Check if Python 3 is available
if ! command -v python3 &> /dev/null; then
    echo "Error: Python 3 is required but not installed."
    exit 1
fi

echo "✓ Python 3 found: $(python3 --version)"

# Create virtual environment
echo "Creating virtual environment..."
python3 -m venv venv
echo "✓ Virtual environment created"

# Activate virtual environment
echo "Activating virtual environment..."
source venv/bin/activate
echo "✓ Virtual environment activated"

# Upgrade pip
echo "Upgrading pip..."
pip install --upgrade pip
echo "✓ Pip upgraded"

# Install dependencies
echo "Installing Python dependencies..."
pip install -r requirements.txt
echo "✓ Dependencies installed"

# Create .env file if it doesn't exist
if [ ! -f ".env" ]; then
    echo "Creating .env file from template..."
    cp env.example .env
    echo "✓ .env file created"
    echo "⚠️  Please edit .env file with your configuration before running scanners"
else
    echo "✓ .env file already exists"
fi

# Create necessary directories
echo "Creating project directories..."
mkdir -p logs
mkdir -p data
mkdir -p output
echo "✓ Directories created"

# Setup P2PK scanner database
echo "Setting up P2PK scanner database..."
cd p2pk_scanner
python setup_database.py
cd ..
echo "✓ P2PK scanner database setup completed"

echo ""
echo "Setup completed successfully!"
echo ""
echo "Next steps:"
echo "1. Edit .env file with your Bitcoin Core and PostgreSQL credentials"
echo "2. Test the setup: cd p2pk_scanner && python test_scanner.py"
echo "3. Run the P2PK scanner: cd p2pk_scanner && ./run_scanner.sh"
echo ""
echo "For more information, see the README.md file." 