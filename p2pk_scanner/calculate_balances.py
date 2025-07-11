#!/usr/bin/env python3
"""
Balance calculation utility for P2PK Scanner.
Calculates and updates current balances for all P2PK addresses.
"""

import sys
import logging
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


def show_balance_summary():
    """Show a summary of all P2PK address balances."""
    try:
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
        """
        
        results = db_manager.execute_query(query)
        
        if not results:
            print("No P2PK addresses found in database.")
            return
        
        print(f"\nP2PK Address Balance Summary")
        print("=" * 80)
        print(f"{'ID':<4} {'Address':<35} {'Balance (BTC)':<15} {'First Block':<12} {'Last Block':<12} {'Tx Count':<8}")
        print("-" * 80)
        
        total_balance = 0
        for row in results:
            balance_btc = row['current_balance_satoshi'] / 100000000
            total_balance += row['current_balance_satoshi']
            
            print(f"{row['id']:<4} {row['address']:<35} {balance_btc:<15.8f} {row['first_seen_block']:<12} {row['last_seen_block']:<12} {row['transaction_count']:<8}")
        
        print("-" * 80)
        print(f"Total P2PK addresses: {len(results)}")
        print(f"Total balance: {total_balance / 100000000:.8f} BTC ({total_balance:,} satoshis)")
        
    except Exception as e:
        logger.error(f"Error showing balance summary: {e}")


def calculate_balance_for_address(address_id: int, up_to_block: int = None):
    """Calculate and display balance for a specific address."""
    try:
        scanner = P2PKScanner()
        
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
            print(f"Balance for address {address_id} ({address_info['address']}) at block {up_to_block}: {balance / 100000000:.8f} BTC")
        else:
            balance = scanner.calculate_address_balance(address_id)
            print(f"Current balance for address {address_id} ({address_info['address']}): {balance / 100000000:.8f} BTC")
        
        # Show transaction history
        tx_query = """
        SELECT block_height, is_input, amount_satoshi, txid
        FROM p2pk_address_blocks 
        WHERE address_id = %s
        ORDER BY block_height, is_input
        """
        tx_results = db_manager.execute_query(tx_query, (address_id,))
        
        if tx_results:
            print(f"\nTransaction History:")
            print(f"{'Block':<8} {'Type':<8} {'Amount (BTC)':<15} {'TXID'}")
            print("-" * 50)
            
            for tx in tx_results:
                tx_type = "SPEND" if tx['is_input'] else "RECEIVE"
                amount_btc = tx['amount_satoshi'] / 100000000
                print(f"{tx['block_height']:<8} {tx_type:<8} {amount_btc:<15.8f} {tx['txid']}")
        
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
    parser.add_argument('--summary', action='store_true', help='Show balance summary for all addresses')
    parser.add_argument('--update', action='store_true', help='Update all address balances')
    parser.add_argument('--address', type=int, help='Calculate balance for specific address ID')
    parser.add_argument('--block', type=int, help='Calculate balance up to specific block height (use with --address)')
    
    args = parser.parse_args()
    
    if args.summary:
        show_balance_summary()
    elif args.update:
        update_all_balances()
    elif args.address:
        calculate_balance_for_address(args.address, args.block)
    else:
        # Default: show summary
        show_balance_summary()


if __name__ == "__main__":
    main() 