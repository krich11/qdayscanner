#!/bin/bash
"""
BTCNow Setup Script
Configures cron job for automatic Bitcoin price fetching.
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

echo -e "${GREEN}BTCNow Cron Job Setup${NC}"
echo -e "${YELLOW}This script will configure automatic Bitcoin price fetching.${NC}"
echo

# Check if btcnow.py exists and is executable
if [[ ! -f "$SCRIPT_DIR/btcnow.py" ]]; then
    echo -e "${RED}Error: btcnow.py not found in $SCRIPT_DIR${NC}"
    echo -e "${YELLOW}Please run install.sh first.${NC}"
    exit 1
fi

if [[ ! -x "$SCRIPT_DIR/btcnow.py" ]]; then
    echo -e "${RED}Error: btcnow.py is not executable${NC}"
    echo -e "${YELLOW}Please run install.sh first.${NC}"
    exit 1
fi

# Function to add cron job
add_cron_job() {
    local interval="$1"
    local cron_expression="$2"
    local description="$3"
    
    echo -e "${BLUE}Adding cron job: $description${NC}"
    echo -e "${YELLOW}Cron expression: $cron_expression${NC}"
    
    # Create temporary file with current crontab
    crontab -l 2>/dev/null > /tmp/btcnow_crontab_temp || true
    
    # Remove any existing btcnow entries
    grep -v "btcnow.py" /tmp/btcnow_crontab_temp > /tmp/btcnow_crontab_clean || true
    
    # Add new entry
    echo "$cron_expression $SCRIPT_DIR/btcnow.py >/dev/null 2>&1" >> /tmp/btcnow_crontab_clean
    
    # Install new crontab
    crontab /tmp/btcnow_crontab_clean
    
    # Clean up
    rm -f /tmp/btcnow_crontab_temp /tmp/btcnow_crontab_clean
    
    echo -e "${GREEN}✓ Cron job added successfully!${NC}"
}

# Function to show current cron jobs
show_cron_jobs() {
    echo -e "${BLUE}Current cron jobs:${NC}"
    crontab -l 2>/dev/null | grep btcnow || echo -e "${YELLOW}No btcnow cron jobs found.${NC}"
}

# Function to remove cron jobs
remove_cron_jobs() {
    echo -e "${YELLOW}Removing all btcnow cron jobs...${NC}"
    
    # Create temporary file with current crontab
    crontab -l 2>/dev/null > /tmp/btcnow_crontab_temp || true
    
    # Remove btcnow entries
    grep -v "btcnow.py" /tmp/btcnow_crontab_temp > /tmp/btcnow_crontab_clean || true
    
    # Install new crontab
    crontab /tmp/btcnow_crontab_clean
    
    # Clean up
    rm -f /tmp/btcnow_crontab_temp /tmp/btcnow_crontab_clean
    
    echo -e "${GREEN}✓ All btcnow cron jobs removed!${NC}"
}

# Main menu
while true; do
    echo -e "${BLUE}Choose an option:${NC}"
    echo "1) Add cron job - Every 5 minutes"
    echo "2) Add cron job - Every 15 minutes"
    echo "3) Add cron job - Every hour"
    echo "4) Add cron job - Every 6 hours"
    echo "5) Add cron job - Once daily"
    echo "6) Show current cron jobs"
    echo "7) Remove all btcnow cron jobs"
    echo "8) Test btcnow script"
    echo "9) Exit"
    echo
    read -p "Enter your choice (1-9): " choice
    
    case $choice in
        1)
            add_cron_job "5min" "*/5 * * * *" "Every 5 minutes"
            ;;
        2)
            add_cron_job "15min" "*/15 * * * *" "Every 15 minutes"
            ;;
        3)
            add_cron_job "1hour" "0 * * * *" "Every hour"
            ;;
        4)
            add_cron_job "6hours" "0 */6 * * *" "Every 6 hours"
            ;;
        5)
            add_cron_job "daily" "0 0 * * *" "Once daily at midnight"
            ;;
        6)
            show_cron_jobs
            ;;
        7)
            remove_cron_jobs
            ;;
        8)
            echo -e "${YELLOW}Testing btcnow script...${NC}"
            if python3 "$SCRIPT_DIR/btcnow.py"; then
                echo -e "${GREEN}✓ Test successful!${NC}"
                if [[ -f /tmp/.btcnow ]]; then
                    echo -e "${YELLOW}Current price data:${NC}"
                    cat /tmp/.btcnow
                fi
            else
                echo -e "${RED}✗ Test failed!${NC}"
            fi
            ;;
        9)
            echo -e "${GREEN}Goodbye!${NC}"
            exit 0
            ;;
        *)
            echo -e "${RED}Invalid choice. Please enter 1-9.${NC}"
            ;;
    esac
    
    echo
    read -p "Press Enter to continue..."
    echo
done 