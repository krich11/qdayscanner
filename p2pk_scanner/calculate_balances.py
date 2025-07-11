#!/usr/bin/env python3
"""
Balance calculation utility for P2PK Scanner.
Calculates and updates current balances for all P2PK addresses.
"""

import sys
import logging
import json
from pathlib import Path

# Add parent directory to path to import utils
sys.path.append(str(Path(__file__).parent.parent))

from utils.config import config
from utils.database import db_manager
from scanner import P2PKScanner

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


def show_balance_summary(top_n: int = 10):
    """Show a summary of top P2PK address balances."""
    try:
        # Get current Bitcoin price
        btc_price = get_bitcoin_price()
        
        # Get top addresses
        query = """
        SELECT 
            a.id,
            a.address,
            a.public_key_hex,
            a.current_balance_satoshi,
            a.first_seen_block,
            a.last_seen_block,
            COUNT(t.id) as transaction_count
        FROM p2pk_addresses a
        LEFT JOIN p2pk_transactions t ON a.id = t.address_id
        GROUP BY a.id, a.address, a.public_key_hex, a.current_balance_satoshi, a.first_seen_block, a.last_seen_block
        ORDER BY a.current_balance_satoshi DESC
        LIMIT %s
        """
        
        results = db_manager.execute_query(query, (top_n,))
        
        if not results:
            print("No P2PK addresses found in database.")
            return
        
        # Get total database statistics
        total_query = """
        SELECT 
            COUNT(*) as total_addresses,
            SUM(current_balance_satoshi) as total_balance_satoshi
        FROM p2pk_addresses
        """
        total_stats = db_manager.execute_query(total_query)
        total_addresses = total_stats[0]['total_addresses']
        total_balance_satoshi = total_stats[0]['total_balance_satoshi'] or 0
        
        # Display Bitcoin price if available
        if btc_price:
            print(f"\nCurrent Bitcoin Price: ${btc_price:,.2f}")
        
        print(f"\nTop {len(results)} P2PK Addresses by Balance")
        print("=" * 100)
        print(f"{'ID':<4} {'Address':<35} {'Balance':<35} {'First Block':<12} {'Last Block':<12} {'Tx Count':<8}")
        print("-" * 100)
        
        top_balance = 0
        for row in results:
            balance_btc = row['current_balance_satoshi'] / 100000000
            top_balance += row['current_balance_satoshi']
            
            balance_display = format_balance_display(balance_btc, btc_price)
            print(f"{row['id']:<4} {row['address']:<35} {balance_display:<35} {row['first_seen_block']:<12} {row['last_seen_block']:<12} {row['transaction_count']:<8}")
        
        print("-" * 100)
        print(f"Showing top {len(results)} addresses out of {total_addresses:,} total P2PK addresses.")
        
        top_balance_btc = top_balance / 100000000
        top_balance_display = format_balance_display(top_balance_btc, btc_price)
        print(f"Top {len(results)} balance: {top_balance_display} ({top_balance:,} satoshis)")
        
        total_balance_btc = total_balance_satoshi / 100000000
        total_balance_display = format_balance_display(total_balance_btc, btc_price)
        print(f"Total database balance: {total_balance_display} ({total_balance_satoshi:,} satoshis)")
        
    except Exception as e:
        logger.error(f"Error showing balance summary: {e}")


def calculate_balance_for_address(address_id: int, up_to_block: int = None):
    """Calculate and display balance for a specific address."""
    try:
        scanner = P2PKScanner()
        
        # Get current Bitcoin price
        btc_price = get_bitcoin_price()
        
        # Get address info
        addr_query = "SELECT address, public_key_hex FROM p2pk_addresses WHERE id = %s"
        addr_result = db_manager.execute_query(addr_query, (address_id,))
        
        if not addr_result:
            print(f"Address ID {address_id} not found.")
            return
        
        address_info = addr_result[0]
        
        # Calculate balance
        if up_to_block:
            balance = scanner.calculate_address_balance(address_id, up_to_block)
            balance_btc = balance / 100000000
            balance_display = format_balance_display(balance_btc, btc_price)
            print(f"Balance for address {address_id} ({address_info['address']}) at block {up_to_block}: {balance_display}")
        else:
            balance = scanner.calculate_address_balance(address_id)
            balance_btc = balance / 100000000
            balance_display = format_balance_display(balance_btc, btc_price)
            print(f"Current balance for address {address_id} ({address_info['address']}): {balance_display}")
        
        # Show transaction history
        tx_query = """
        SELECT block_height, is_input, amount_satoshi, txid, block_time
        FROM p2pk_transactions 
        WHERE address_id = %s
        ORDER BY block_height, is_input
        """
        tx_results = db_manager.execute_query(tx_query, (address_id,))
        
        if tx_results:
            print(f"\nTransaction History:")
            print(f"{'Date':<12} {'Block':<8} {'Type':<8} {'Amount':<35} {'TXID'}")
            print("-" * 90)
            
            for tx in tx_results:
                tx_type = "SPEND" if tx['is_input'] else "RECEIVE"
                amount_btc = tx['amount_satoshi'] / 100000000
                amount_display = format_balance_display(amount_btc, btc_price)
                # Format the date
                if tx['block_time']:
                    date_str = tx['block_time'].strftime('%Y-%m-%d')
                else:
                    date_str = "Unknown"
                print(f"{date_str:<12} {tx['block_height']:<8} {tx_type:<8} {amount_display:<35} {tx['txid']}")
        
    except Exception as e:
        logger.error(f"Error calculating balance for address {address_id}: {e}")


def update_all_balances():
    """Update current balances for all P2PK addresses."""
    try:
        scanner = P2PKScanner()
        scanner.update_all_balances()
        print("✓ All balances updated successfully!")
        
    except Exception as e:
        logger.error(f"Error updating balances: {e}")


def main():
    """Main function."""
    import argparse
    
    parser = argparse.ArgumentParser(description='P2PK Address Balance Calculator')
    parser.add_argument('--summary', action='store_true', help='Show balance summary for top addresses')
    parser.add_argument('--top', type=int, default=10, help='Number of top addresses to show (default: 10)')
    parser.add_argument('--update', action='store_true', help='Update all address balances')
    parser.add_argument('--address', type=int, help='Calculate balance for specific address ID')
    parser.add_argument('--block', type=int, help='Calculate balance up to specific block height (use with --address)')
    
    args = parser.parse_args()
    
    if args.summary:
        show_balance_summary(args.top)
    elif args.update:
        update_all_balances()
    elif args.address:
        calculate_balance_for_address(args.address, args.block)
    else:
        # Default: show summary of top 10
        show_balance_summary(args.top)


if __name__ == "__main__":
    main() 