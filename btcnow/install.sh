#!/bin/bash
"""
BTCNow Installation Script
Installs BTCNow Bitcoin price fetcher with proper setup and dependencies.
"""

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

echo -e "${GREEN}Installing BTCNow Bitcoin Price Fetcher...${NC}"

# Check if running as root
if [[ $EUID -eq 0 ]]; then
   echo -e "${RED}This script should not be run as root${NC}" 
   exit 1
fi

# Check if Python 3 is installed
if ! command -v python3 &> /dev/null; then
    echo -e "${RED}Python 3 is required but not installed.${NC}"
    exit 1
fi

# Check if pip is installed
if ! command -v pip3 &> /dev/null; then
    echo -e "${RED}pip3 is required but not installed.${NC}"
    exit 1
fi

echo -e "${YELLOW}Installing Python dependencies...${NC}"

# Install required Python packages
pip3 install requests

echo -e "${YELLOW}Setting up BTCNow script...${NC}"

# Make the script executable
chmod +x "$SCRIPT_DIR/btcnow.py"

# Create symlink to make it available system-wide (optional)
if [[ -w /usr/local/bin ]]; then
    echo -e "${YELLOW}Creating system-wide symlink...${NC}"
    sudo ln -sf "$SCRIPT_DIR/btcnow.py" /usr/local/bin/btcnow
    echo -e "${GREEN}BTCNow is now available as 'btcnow' command${NC}"
else
    echo -e "${YELLOW}To make BTCNow available system-wide, run:${NC}"
    echo -e "sudo ln -sf $SCRIPT_DIR/btcnow.py /usr/local/bin/btcnow"
fi

# Test the script
echo -e "${YELLOW}Testing BTCNow script...${NC}"
if python3 "$SCRIPT_DIR/btcnow.py"; then
    echo -e "${GREEN}✓ BTCNow script test successful${NC}"
    
    # Show the output file
    if [[ -f /tmp/.btcnow ]]; then
        echo -e "${GREEN}✓ Price file created at /tmp/.btcnow${NC}"
        echo -e "${YELLOW}Current price data:${NC}"
        cat /tmp/.btcnow
    else
        echo -e "${RED}✗ Price file not created${NC}"
    fi
else
    echo -e "${RED}✗ BTCNow script test failed${NC}"
    exit 1
fi

echo -e "${GREEN}Installation completed successfully!${NC}"
echo -e "${YELLOW}Next steps:${NC}"
echo -e "1. Run setup.sh to configure cron job"
echo -e "2. Or manually add to crontab: */5 * * * * $SCRIPT_DIR/btcnow.py"
echo -e "3. Check price anytime: cat /tmp/.btcnow" 