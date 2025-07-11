#!/usr/bin/env python3
"""
Simple script to find and analyze a specific P2PK address.
"""

import sys
import json
from pathlib import Path

# Add parent directory to path to import utils
sys.path.append(str(Path(__file__).parent.parent))

from utils.database import DatabaseManager

def get_bitcoin_price():
    """Get current Bitcoin price from /tmp/.btcnow file."""
    try:
        btcnow_file = Path("/tmp/.btcnow")
        if btcnow_file.exists():
            with open(btcnow_file, 'r') as f:
                data = json.load(f)
                return data.get('price_usd', 0)
        else:
            print("BTCNow price file not found. USD values will not be displayed.")
            return None
    except Exception as e:
        print(f"Could not read Bitcoin price: {e}. USD values will not be displayed.")
        return None


def format_balance_display(balance_btc, btc_price=None):
    """Format balance display with BTC and USD values."""
    if btc_price and btc_price > 0:
        balance_usd = balance_btc * btc_price
        return f"{balance_btc:.8f} BTC (${balance_usd:,.2f})"
    else:
        return f"{balance_btc:.8f} BTC"


def find_address(address_truncated):
    """Find an address by its truncated string."""
    db = DatabaseManager()
    try:
        # Get current Bitcoin price
        btc_price = get_bitcoin_price()
        
        # Find the address
        query = "SELECT id, address, public_key_hex, current_balance_satoshi, first_seen_block, last_seen_block FROM p2pk_addresses WHERE address = %s"
        result = db.execute_query(query, (address_truncated,))
        
        if not result:
            print(f"Address '{address_truncated}' not found in database.")
            return
        
        addr = result[0]
        print(f"Address found!")
        print(f"ID: {addr['id']}")
        print(f"Address: {addr['address']}")
        print(f"Public Key: {addr['public_key_hex']}")
        
        balance_btc = addr['current_balance_satoshi'] / 100000000
        balance_display = format_balance_display(balance_btc, btc_price)
        print(f"Current Balance: {balance_display}")
        
        print(f"First seen: Block {addr['first_seen_block']}")
        print(f"Last seen: Block {addr['last_seen_block']}")
        
        # Get transaction count
        tx_query = "SELECT COUNT(*) as tx_count FROM p2pk_transactions WHERE address_id = %s"
        tx_result = db.execute_query(tx_query, (addr['id'],))
        print(f"Total transactions: {tx_result[0]['tx_count']}")
        
        # Get recent transactions
        recent_query = """
        SELECT block_height, is_input, amount_satoshi, txid 
        FROM p2pk_address_blocks 
        WHERE address_id = %s 
        ORDER BY block_height DESC 
        LIMIT 10
        """
        recent_txs = db.execute_query(recent_query, (addr['id'],))
        
        if recent_txs:
            print(f"\nRecent transactions (last 10):")
            print(f"{'Block':<8} {'Type':<8} {'Amount':<35} {'TXID'}")
            print("-" * 80)
            for tx in recent_txs:
                tx_type = "SPEND" if tx['is_input'] else "RECEIVE"
                amount_btc = tx['amount_satoshi'] / 100000000
                amount_display = format_balance_display(amount_btc, btc_price)
                print(f"{tx['block_height']:<8} {tx_type:<8} {amount_display:<35} {tx['txid']}")
        
        print(f"\nTo get full transaction history, run:")
        print(f"python calculate_balances.py --address {addr['id']}")
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        db.close()

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python find_address.py <address_truncated>")
        print("Example: python find_address.py 0476b6fb873f5cb9c0e98a9c9ee32b6e07")
        sys.exit(1)
    
    find_address(sys.argv[1]) 