#!/bin/bash
"""
BTCNow Uninstall Script
Removes BTCNow installation and cleans up all related files and cron jobs.
"""

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo -e "${GREEN}BTCNow Uninstaller${NC}"
echo -e "${YELLOW}This script will remove BTCNow and clean up all related files.${NC}"
echo

# Function to remove cron jobs
remove_cron_jobs() {
    echo -e "${YELLOW}Removing btcnow cron jobs...${NC}"
    
    # Create temporary file with current crontab
    crontab -l 2>/dev/null > /tmp/btcnow_crontab_temp || true
    
    # Remove btcnow entries
    grep -v "btcnow.py" /tmp/btcnow_crontab_temp > /tmp/btcnow_crontab_clean || true
    
    # Install new crontab
    crontab /tmp/btcnow_crontab_clean
    
    # Clean up
    rm -f /tmp/btcnow_crontab_temp /tmp/btcnow_crontab_clean
    
    echo -e "${GREEN}✓ Cron jobs removed${NC}"
}

# Function to remove system symlink
remove_symlink() {
    if [[ -L /usr/local/bin/btcnow ]]; then
        echo -e "${YELLOW}Removing system symlink...${NC}"
        sudo rm -f /usr/local/bin/btcnow
        echo -e "${GREEN}✓ System symlink removed${NC}"
    else
        echo -e "${YELLOW}No system symlink found${NC}"
    fi
}

# Function to remove price file
remove_price_file() {
    if [[ -f /tmp/.btcnow ]]; then
        echo -e "${YELLOW}Removing price file...${NC}"
        rm -f /tmp/.btcnow
        echo -e "${GREEN}✓ Price file removed${NC}"
    else
        echo -e "${YELLOW}No price file found${NC}"
    fi
}

# Function to remove Python dependencies (optional)
remove_dependencies() {
    echo -e "${YELLOW}Do you want to remove the 'requests' Python package? (y/N)${NC}"
    read -p "This may affect other scripts that use requests: " choice
    
    case $choice in
        [Yy]* )
            echo -e "${YELLOW}Removing requests package...${NC}"
            pip3 uninstall -y requests
            echo -e "${GREEN}✓ Requests package removed${NC}"
            ;;
        * )
            echo -e "${YELLOW}Skipping dependency removal${NC}"
            ;;
    esac
}

# Main uninstall process
echo -e "${BLUE}Starting uninstall process...${NC}"

# Remove cron jobs
remove_cron_jobs

# Remove system symlink
remove_symlink

# Remove price file
remove_price_file

# Ask about dependencies
remove_dependencies

echo -e "${GREEN}Uninstall completed successfully!${NC}"
echo -e "${YELLOW}Note: The btcnow.py script files remain in $SCRIPT_DIR${NC}"
echo -e "${YELLOW}To completely remove, manually delete the btcnow directory.${NC}" 