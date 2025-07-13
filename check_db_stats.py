#!/usr/bin/env python3
"""
Quick database statistics checker
"""

import psycopg2
import os
from pathlib import Path

# Database connection parameters
DB_HOST = "localhost"
DB_PORT = 5432
DB_NAME = "bitcoin_scanner"
DB_USER = "scanneruser"
DB_PASSWORD = "abc123"

def check_database_stats():
    """Check database statistics for addresses, transactions, and blocks."""
    try:
        # Connect to database
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        
        cur = conn.cursor()
        
        # Check addresses
        cur.execute("SELECT COUNT(*) FROM p2pk_addresses")
        address_count = cur.fetchone()[0]
        
        # Check transactions
        cur.execute("SELECT COUNT(*) FROM p2pk_transactions")
        transaction_count = cur.fetchone()[0]
        
        # Check blocks
        cur.execute("SELECT COUNT(*) FROM p2pk_address_blocks")
        block_count = cur.fetchone()[0]
        
        # Check scan progress
        cur.execute("SELECT current_block, total_blocks_scanned FROM scan_progress WHERE scanner_name = 'hydra_mode_scanner'")
        progress_result = cur.fetchone()
        if progress_result:
            current_block, total_scanned = progress_result
        else:
            current_block, total_scanned = 0, 0
        
        # Get some sample data
        cur.execute("SELECT address, public_key_hex FROM p2pk_addresses LIMIT 5")
        sample_addresses = cur.fetchall()
        
        print("=== Database Statistics ===")
        print(f"Total P2PK Addresses: {address_count:,}")
        print(f"Total P2PK Transactions: {transaction_count:,}")
        print(f"Total P2PK Block Records: {block_count:,}")
        print(f"Current Block: {current_block:,}")
        print(f"Total Blocks Scanned: {total_scanned:,}")
        
        if sample_addresses:
            print("\n=== Sample Addresses ===")
            for i, (address, pubkey) in enumerate(sample_addresses, 1):
                print(f"{i}. Address: {address}")
                print(f"   Public Key: {pubkey[:50]}...")
                print()
        
        conn.close()
        
    except Exception as e:
        print(f"Error connecting to database: {e}")
        print("Make sure PostgreSQL is running and credentials are correct")

if __name__ == "__main__":
    check_database_stats() 