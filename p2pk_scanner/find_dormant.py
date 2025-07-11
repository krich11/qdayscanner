#!/usr/bin/env python3
"""
Script to find dormant P2PK addresses.
"""

import sys
import logging
import json
from datetime import datetime, timedelta
from pathlib import Path

# Add parent directory to path to import utils
sys.path.append(str(Path(__file__).parent.parent))

from utils.config import config
from utils.database import db_manager

# Set up logging
logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_bitcoin_price():
    """Get current Bitcoin price from /tmp/.btcnow file."""
    try:
        btcnow_file = Path("/tmp/.btcnow")
        if btcnow_file.exists():
            with open(btcnow_file, 'r') as f:
                data = json.load(f)
                return data.get('price_usd', 0)
        else:
            logger.warning("BTCNow price file not found. USD values will not be displayed.")
            return None
    except Exception as e:
        logger.warning(f"Could not read Bitcoin price: {e}. USD values will not be displayed.")
        return None


def format_balance_display(balance_btc, btc_price=None):
    """Format balance display with BTC and USD values."""
    if btc_price and btc_price > 0:
        balance_usd = balance_btc * btc_price
        return f"{balance_btc:.8f} BTC (${balance_usd:,.2f})"
    else:
        return f"{balance_btc:.8f} BTC"


def find_dormant_addresses(years_dormant: int = 10, min_balance: float = 0.0, limit: int = 50):
    """Find addresses that have been dormant for at least the specified years."""
    try:
        # Get current Bitcoin price
        btc_price = get_bitcoin_price()
        
        # Calculate the cutoff date
        cutoff_date = datetime.now() - timedelta(days=years_dormant * 365)
        
        # Find addresses with no transactions after the cutoff date
        query = """
        SELECT 
            a.id,
            a.address,
            a.public_key_hex,
            a.current_balance_satoshi,
            a.first_seen_block,
            a.last_seen_block,
            MAX(t.block_time) as last_transaction_date,
            COUNT(t.id) as transaction_count
        FROM p2pk_addresses a
        LEFT JOIN p2pk_transactions t ON a.id = t.address_id
        WHERE a.current_balance_satoshi >= %s
        GROUP BY a.id, a.address, a.public_key_hex, a.current_balance_satoshi, a.first_seen_block, a.last_seen_block
        HAVING MAX(t.block_time) IS NULL OR MAX(t.block_time) < %s
        ORDER BY a.current_balance_satoshi DESC
        LIMIT %s
        """
        
        min_balance_satoshi = int(min_balance * 100000000)  # Convert BTC to satoshis
        
        results = db_manager.execute_query(query, (min_balance_satoshi, cutoff_date, limit))
        
        if not results:
            print(f"No dormant addresses found (dormant for {years_dormant}+ years, min balance {min_balance} BTC).")
            return
        
        # Display Bitcoin price if available
        if btc_price:
            print(f"\nCurrent Bitcoin Price: ${btc_price:,.2f}")
        
        print(f"\nP2PK Addresses Dormant for {years_dormant}+ Years (min balance: {min_balance} BTC)")
        print("=" * 120)
        print(f"{'ID':<4} {'Address':<35} {'Balance':<35} {'Last Tx Date':<12} {'First Block':<12} {'Last Block':<12} {'Tx Count':<8}")
        print("-" * 120)
        
        total_balance = 0
        for row in results:
            balance_btc = row['current_balance_satoshi'] / 100000000
            total_balance += row['current_balance_satoshi']
            
            balance_display = format_balance_display(balance_btc, btc_price)
            
            last_date = row['last_transaction_date']
            if last_date:
                last_date_str = last_date.strftime('%Y-%m-%d')
            else:
                last_date_str = "Unknown"
            
            print(f"{row['id']:<4} {row['address']:<35} {balance_display:<35} {last_date_str:<12} {row['first_seen_block']:<12} {row['last_seen_block']:<12} {row['transaction_count']:<8}")
        
        print("-" * 120)
        total_balance_btc = total_balance / 100000000
        total_balance_display = format_balance_display(total_balance_btc, btc_price)
        print(f"Found {len(results)} dormant addresses with total balance: {total_balance_display} ({total_balance:,} satoshis)")
        
        # Get total dormant addresses count
        count_query = """
        SELECT COUNT(*) as total_dormant
        FROM p2pk_addresses a
        LEFT JOIN p2pk_transactions t ON a.id = t.address_id
        WHERE a.current_balance_satoshi >= %s
        GROUP BY a.id
        HAVING MAX(t.block_time) IS NULL OR MAX(t.block_time) < %s
        """
        count_results = db_manager.execute_query(count_query, (min_balance_satoshi, cutoff_date))
        total_dormant = len(count_results)
        
        print(f"Total dormant addresses in database: {total_dormant:,}")
        
    except Exception as e:
        logger.error(f"Error finding dormant addresses: {e}")


def main():
    """Main function."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Find dormant P2PK addresses')
    parser.add_argument('--years', type=int, default=10, help='Minimum years dormant (default: 10)')
    parser.add_argument('--min-balance', type=float, default=0.0, help='Minimum balance in BTC (default: 0.0)')
    parser.add_argument('--limit', type=int, default=50, help='Maximum number of results to show (default: 50)')
    
    args = parser.parse_args()
    
    find_dormant_addresses(args.years, args.min_balance, args.limit)


if __name__ == "__main__":
    main() 