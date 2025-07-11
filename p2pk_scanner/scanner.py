#!/usr/bin/env python3
"""
P2PK Scanner for Bitcoin Quantum Vulnerability Scanner.
Identifies and tracks P2PK (Pay-to-Public-Key) addresses on the Bitcoin blockchain.
"""

import sys
import os
import logging
import time
from pathlib import Path
from typing import Dict, Any, List, Optional
from datetime import datetime

# Add parent directory to path to import utils
sys.path.append(str(Path(__file__).parent.parent))

from utils.config import config
from utils.database import db_manager
from bitcoin_rpc import bitcoin_rpc

# Set up logging
logs_dir = Path(__file__).parent.parent / 'logs'
logs_dir.mkdir(exist_ok=True)

logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(logs_dir / 'p2pk_scanner.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class P2PKScanner:
    """Scanner for identifying P2PK addresses on the Bitcoin blockchain."""
    
    def __init__(self):
        self.scanner_name = 'p2pk_scanner'
        self.p2pk_addresses_found = 0
        self.transactions_processed = 0
        self.blocks_processed = 0
        
        # Create logs directory if it doesn't exist (in project root)
        logs_dir = Path(__file__).parent.parent / 'logs'
        logs_dir.mkdir(exist_ok=True)
    
    def get_scan_progress(self) -> int:
        """Get the last scanned block height."""
        query = "SELECT last_scanned_block FROM scan_progress WHERE scanner_name = %s"
        result = db_manager.execute_query(query, (self.scanner_name,))
        return result[0]['last_scanned_block'] if result else 0
    
    def update_scan_progress(self, block_height: int, blocks_scanned: int = 1):
        """Update the scan progress."""
        query = """
        UPDATE scan_progress 
        SET last_scanned_block = %s, total_blocks_scanned = total_blocks_scanned + %s, last_updated = CURRENT_TIMESTAMP
        WHERE scanner_name = %s
        """
        db_manager.execute_command(query, (block_height, blocks_scanned, self.scanner_name))
    
    def is_p2pk_script(self, script_pub_key: Dict[str, Any]) -> Optional[str]:
        """Check if a script is a P2PK script and extract the public key."""
        try:
            # P2PK script pattern: OP_PUSHDATA <public_key> OP_CHECKSIG
            if script_pub_key.get('type') == 'pubkey':
                # This is a P2PK script
                asm = script_pub_key.get('asm', '')
                if asm and 'OP_CHECKSIG' in asm:
                    # Extract public key from ASM
                    parts = asm.split()
                    if len(parts) >= 2 and parts[-1] == 'OP_CHECKSIG':
                        public_key = parts[0]
                        if len(public_key) == 130 and public_key.startswith('04'):  # Uncompressed public key
                            return public_key
                        elif len(public_key) == 66 and public_key.startswith(('02', '03')):  # Compressed public key
                            return public_key
            return None
        except Exception as e:
            logger.debug(f"Error parsing script: {e}")
            return None
    
    def process_transaction(self, tx: Dict[str, Any], block_height: int, block_time: int) -> List[Dict[str, Any]]:
        """Process a transaction and extract P2PK addresses."""
        p2pk_transactions = []
        
        try:
            # Process outputs (vout) - P2PK addresses receiving funds
            for vout in tx.get('vout', []):
                script_pub_key = vout.get('scriptPubKey', {})
                public_key = self.is_p2pk_script(script_pub_key)
                
                if public_key:
                    # Found a P2PK output
                    p2pk_transaction = {
                        'txid': tx['txid'],
                        'block_height': block_height,
                        'block_time': datetime.fromtimestamp(block_time),
                        'public_key_hex': public_key,
                        'amount_satoshi': int(vout['value'] * 100000000),  # Convert BTC to satoshis
                        'is_input': False
                    }
                    p2pk_transactions.append(p2pk_transaction)
            
            # Process inputs (vin) - P2PK addresses spending funds
            for vin in tx.get('vin', []):
                if 'txid' in vin and 'vout' in vin:
                    # This is a regular input (not coinbase)
                    # We need to get the previous transaction to see if it was a P2PK output
                    try:
                        prev_tx = bitcoin_rpc.get_raw_transaction(vin['txid'])
                        if prev_tx and 'vout' in prev_tx:
                            prev_vout = prev_tx['vout'][vin['vout']]
                            script_pub_key = prev_vout.get('scriptPubKey', {})
                            public_key = self.is_p2pk_script(script_pub_key)
                            
                            if public_key:
                                # Found a P2PK input (spending)
                                p2pk_transaction = {
                                    'txid': tx['txid'],
                                    'block_height': block_height,
                                    'block_time': datetime.fromtimestamp(block_time),
                                    'public_key_hex': public_key,
                                    'amount_satoshi': int(prev_vout['value'] * 100000000),
                                    'is_input': True
                                }
                                p2pk_transactions.append(p2pk_transaction)
                    except Exception as e:
                        logger.debug(f"Could not process input transaction {vin['txid']}: {e}")
                        # Continue processing other inputs
            
        except Exception as e:
            logger.error(f"Error processing transaction {tx.get('txid', 'unknown')}: {e}")
        
        return p2pk_transactions
    
    def save_p2pk_transaction(self, p2pk_transaction: Dict[str, Any]) -> int:
        """Save a P2PK transaction to the database."""
        try:
            # Check if address already exists
            query = "SELECT id FROM p2pk_addresses WHERE public_key_hex = %s"
            existing = db_manager.execute_query(query, (p2pk_transaction['public_key_hex'],))
            
            if existing:
                # Update existing address
                address_id = existing[0]['id']
                update_query = """
                UPDATE p2pk_addresses 
                SET last_seen_block = %s, 
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = %s
                """
                db_manager.execute_command(update_query, (
                    p2pk_transaction['block_height'],
                    address_id
                ))
            else:
                # Insert new address
                insert_query = """
                INSERT INTO p2pk_addresses 
                (address, public_key_hex, first_seen_block, first_seen_txid, last_seen_block, 
                 total_received_satoshi, current_balance_satoshi)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                RETURNING id
                """
                # For now, use public key as address (we'll derive proper address later)
                address = p2pk_transaction['public_key_hex'][:34]  # Truncated for display
                initial_balance = p2pk_transaction['amount_satoshi'] if not p2pk_transaction['is_input'] else 0
                result = db_manager.execute_query(insert_query, (
                    address,
                    p2pk_transaction['public_key_hex'],
                    p2pk_transaction['block_height'],
                    p2pk_transaction['txid'],
                    p2pk_transaction['block_height'],
                    initial_balance,
                    initial_balance
                ))
                address_id = result[0]['id']
                self.p2pk_addresses_found += 1
            
            # Save transaction record
            tx_query = """
            INSERT INTO p2pk_transactions 
            (txid, block_height, block_time, address_id, is_input, amount_satoshi)
            VALUES (%s, %s, %s, %s, %s, %s)
            """
            db_manager.execute_command(tx_query, (
                p2pk_transaction['txid'],
                p2pk_transaction['block_height'],
                p2pk_transaction['block_time'],
                address_id,
                p2pk_transaction['is_input'],
                p2pk_transaction['amount_satoshi']
            ))
            
            # Save to address_blocks table for efficient balance calculation
            block_query = """
            INSERT INTO p2pk_address_blocks 
            (address_id, block_height, is_input, amount_satoshi, txid)
            VALUES (%s, %s, %s, %s, %s)
            """
            db_manager.execute_command(block_query, (
                address_id,
                p2pk_transaction['block_height'],
                p2pk_transaction['is_input'],
                p2pk_transaction['amount_satoshi'],
                p2pk_transaction['txid']
            ))
            
            return address_id
            
        except Exception as e:
            logger.error(f"Error saving P2PK transaction: {e}")
            return 0
    
    def scan_block(self, block: Dict[str, Any]) -> int:
        """Scan a single block for P2PK addresses."""
        block_height = block['height']
        block_time = block['time']
        p2pk_count = 0
        
        logger.debug(f"Scanning block {block_height} with {len(block.get('tx', []))} transactions")
        
        for tx in block.get('tx', []):
            try:
                p2pk_transactions = self.process_transaction(tx, block_height, block_time)
                
                for p2pk_transaction in p2pk_transactions:
                    self.save_p2pk_transaction(p2pk_transaction)
                    p2pk_count += 1
                
                self.transactions_processed += 1
                
            except Exception as e:
                logger.error(f"Error processing transaction in block {block_height}: {e}")
        
        return p2pk_count
    
    def scan_blocks_range(self, start_height: int, end_height: int) -> int:
        """Scan a range of blocks."""
        total_p2pk_found = 0
        
        logger.info(f"Scanning blocks {start_height} to {end_height}")
        
        for height in range(start_height, end_height + 1):
            try:
                block = bitcoin_rpc.get_block_by_height(height)
                p2pk_count = self.scan_block(block)
                total_p2pk_found += p2pk_count
                self.blocks_processed += 1
                
                # Update progress periodically
                if self.blocks_processed % config.PROGRESS_UPDATE_INTERVAL == 0:
                    self.update_scan_progress(height, config.PROGRESS_UPDATE_INTERVAL)
                    logger.info(f"Progress: {height}/{end_height} blocks ({height/end_height*100:.1f}%) - "
                              f"Found {total_p2pk_found} P2PK addresses so far")
                
                # Small delay to avoid overwhelming the RPC
                time.sleep(0.1)
                
            except Exception as e:
                logger.error(f"Error scanning block {height}: {e}")
                # Continue with next block instead of failing completely
                continue
        
        return total_p2pk_found
    
    def run(self, start_block: Optional[int] = None, end_block: Optional[int] = None):
        """Run the P2PK scanner."""
        logger.info("Starting P2PK scanner...")
        
        # Test connections
        if not bitcoin_rpc.test_connection():
            logger.error("Failed to connect to Bitcoin Core")
            return
        
        if not config.validate_config():
            logger.error("Configuration validation failed")
            return
        
        # Get current blockchain info
        try:
            blockchain_info = bitcoin_rpc.get_blockchain_info()
            current_height = blockchain_info['blocks']
            logger.info(f"Current blockchain height: {current_height}")
        except Exception as e:
            logger.error(f"Failed to get blockchain info: {e}")
            return
        
        # Determine scan range
        if start_block is None:
            start_block = self.get_scan_progress()
            logger.info(f"Resuming from block {start_block}")
        
        if end_block is None:
            end_block = current_height
        
        if start_block >= end_block:
            logger.info("Already up to date")
            return
        
        # Scan in batches
        batch_size = config.SCAN_BATCH_SIZE
        total_p2pk_found = 0
        
        for batch_start in range(start_block, end_block, batch_size):
            batch_end = min(batch_start + batch_size - 1, end_block)
            
            logger.info(f"Processing blocks {batch_start}-{batch_end}")
            start_time = time.time()
            
            p2pk_found = self.scan_blocks_range(batch_start, batch_end)
            total_p2pk_found += p2pk_found
            
            batch_time = time.time() - start_time
            logger.info(f"Batch completed in {batch_time:.1f} seconds - Found {p2pk_found} P2PK addresses")
            
            # Update progress
            self.update_scan_progress(batch_end, batch_end - batch_start + 1)
        
        # Final summary
        logger.info("P2PK scanning completed!")
        logger.info(f"Total blocks processed: {self.blocks_processed}")
        logger.info(f"Total transactions processed: {self.transactions_processed}")
        logger.info(f"Total P2PK addresses found: {total_p2pk_found}")
        
        # Show database statistics
        p2pk_count = db_manager.get_table_count('p2pk_addresses')
        tx_count = db_manager.get_table_count('p2pk_transactions')
        block_count = db_manager.get_table_count('p2pk_address_blocks')
        logger.info(f"Database: {p2pk_count} P2PK addresses, {tx_count} transactions, {block_count} block records")
    
    def calculate_address_balance(self, address_id: int, up_to_block: Optional[int] = None) -> int:
        """Calculate the balance of a P2PK address up to a specific block height."""
        try:
            if up_to_block is None:
                # Calculate current balance
                query = """
                SELECT SUM(CASE WHEN is_input THEN -amount_satoshi ELSE amount_satoshi END) as balance
                FROM p2pk_address_blocks 
                WHERE address_id = %s
                """
                result = db_manager.execute_query(query, (address_id,))
            else:
                # Calculate balance at specific block height
                query = """
                SELECT SUM(CASE WHEN is_input THEN -amount_satoshi ELSE amount_satoshi END) as balance
                FROM p2pk_address_blocks 
                WHERE address_id = %s AND block_height <= %s
                """
                result = db_manager.execute_query(query, (address_id, up_to_block))
            
            balance = result[0]['balance'] if result and result[0]['balance'] is not None else 0
            return balance
            
        except Exception as e:
            logger.error(f"Error calculating balance for address {address_id}: {e}")
            return 0
    
    def update_all_balances(self):
        """Update current balances for all P2PK addresses."""
        try:
            logger.info("Updating current balances for all P2PK addresses...")
            
            # Get all address IDs
            addresses = db_manager.execute_query("SELECT id FROM p2pk_addresses")
            
            updated_count = 0
            for addr in addresses:
                address_id = addr['id']
                current_balance = self.calculate_address_balance(address_id)
                
                # Update the address record
                update_query = """
                UPDATE p2pk_addresses 
                SET current_balance_satoshi = %s, updated_at = CURRENT_TIMESTAMP
                WHERE id = %s
                """
                db_manager.execute_command(update_query, (current_balance, address_id))
                updated_count += 1
            
            logger.info(f"Updated balances for {updated_count} addresses")
            
        except Exception as e:
            logger.error(f"Error updating balances: {e}")


def main():
    """Main function."""
    import argparse
    
    parser = argparse.ArgumentParser(description='P2PK Scanner for Bitcoin Quantum Vulnerability Scanner')
    parser.add_argument('--start-block', type=int, help='Block height to start scanning from')
    parser.add_argument('--end-block', type=int, help='Block height to stop scanning at')
    parser.add_argument('--reset', action='store_true', help='Reset scan progress and start from beginning')
    
    args = parser.parse_args()
    
    scanner = P2PKScanner()
    
    if args.reset:
        logger.info("Resetting scan progress...")
        db_manager.execute_command(
            "UPDATE scan_progress SET last_scanned_block = 0 WHERE scanner_name = %s",
            (scanner.scanner_name,)
        )
    
    scanner.run(start_block=args.start_block, end_block=args.end_block)


if __name__ == "__main__":
    main() 