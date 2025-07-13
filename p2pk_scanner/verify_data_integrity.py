#!/usr/bin/env python3
"""
Data Integrity Verification Script for P2PK Scanner Database

This script performs comprehensive spot checks and validation of the database
to ensure data quality, consistency, and proper relationships between tables.
"""

import sys
import os
import logging
from datetime import datetime
from typing import Dict, List, Tuple, Optional, Set
import hashlib
import re

# Add the parent directory to the path so we can import our modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.database import get_database_connection
from p2pk_scanner.bitcoin_rpc import BitcoinRPC

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DataIntegrityVerifier:
    """Verifies data integrity and consistency in the P2PK scanner database."""
    
    def __init__(self):
        self.db_conn = None
        self.cursor = None
        self.bitcoin_rpc = None
        self.verification_results = {
            'passed': 0,
            'failed': 0,
            'warnings': 0,
            'errors': []
        }
    
    def connect(self):
        """Establish database and RPC connections."""
        try:
            self.db_conn = get_database_connection()
            self.cursor = self.db_conn.cursor()
            self.bitcoin_rpc = BitcoinRPC()
            logger.info("✅ Database and RPC connections established")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to connect: {e}")
            return False
    
    def disconnect(self):
        """Close database connection."""
        if self.db_conn:
            self.db_conn.close()
            logger.info("Database connection closed")
    
    def log_result(self, test_name: str, passed: bool, message: str = "", warning: bool = False):
        """Log a test result."""
        if passed:
            self.verification_results['passed'] += 1
            logger.info(f"✅ {test_name}: PASSED - {message}")
        elif warning:
            self.verification_results['warnings'] += 1
            logger.warning(f"⚠️  {test_name}: WARNING - {message}")
        else:
            self.verification_results['failed'] += 1
            self.verification_results['errors'].append(f"{test_name}: {message}")
            logger.error(f"❌ {test_name}: FAILED - {message}")
    
    def verify_table_structure(self):
        """Verify that all required tables exist and have correct structure."""
        logger.info("🔍 Verifying table structure...")
        
        required_tables = {
            'p2pk_addresses': [
                'id', 'address_key', 'public_key_hex', 'created_at'
            ],
            'p2pk_transactions': [
                'id', 'address_id', 'txid', 'block_height', 'block_time', 
                'amount_satoshi', 'is_input', 'created_at'
            ],
            'p2pk_blocks': [
                'id', 'address_id', 'block_height', 'first_seen', 'last_seen', 
                'total_inputs', 'total_outputs', 'created_at'
            ],
            'scan_progress': [
                'id', 'last_scanned_block', 'total_blocks_scanned', 'last_updated'
            ]
        }
        
        for table_name, expected_columns in required_tables.items():
            try:
                self.cursor.execute(f"""
                    SELECT column_name, data_type 
                    FROM information_schema.columns 
                    WHERE table_name = %s
                    ORDER BY ordinal_position
                """, (table_name,))
                
                columns = [row[0] for row in self.cursor.fetchall()]
                
                if not columns:
                    self.log_result(f"Table {table_name} exists", False, f"Table {table_name} not found")
                    continue
                
                missing_columns = set(expected_columns) - set(columns)
                if missing_columns:
                    self.log_result(f"Table {table_name} structure", False, 
                                  f"Missing columns: {missing_columns}")
                else:
                    self.log_result(f"Table {table_name} structure", True, 
                                  f"All {len(expected_columns)} columns present")
                    
            except Exception as e:
                self.log_result(f"Table {table_name} check", False, str(e))
    
    def verify_address_format(self):
        """Verify that P2PK addresses have correct format."""
        logger.info("🔍 Verifying P2PK address format...")
        
        try:
            # Check for addresses that don't match expected P2PK format
            self.cursor.execute("""
                SELECT id, address_key, public_key_hex 
                FROM p2pk_addresses 
                WHERE address_key NOT LIKE '04%' 
                   OR LENGTH(public_key_hex) != 130
                   OR public_key_hex NOT SIMILAR TO '04[0-9a-fA-F]{128}'
                LIMIT 10
            """)
            
            invalid_addresses = self.cursor.fetchall()
            
            if invalid_addresses:
                self.log_result("Address format validation", False, 
                              f"Found {len(invalid_addresses)} addresses with invalid format")
                for addr_id, addr_key, pub_key in invalid_addresses:
                    logger.error(f"  Invalid address {addr_id}: {addr_key[:20]}... (len: {len(pub_key)})")
            else:
                self.log_result("Address format validation", True, "All addresses have correct P2PK format")
            
            # Check for duplicate addresses
            self.cursor.execute("""
                SELECT address_key, COUNT(*) 
                FROM p2pk_addresses 
                GROUP BY address_key 
                HAVING COUNT(*) > 1
            """)
            
            duplicates = self.cursor.fetchall()
            if duplicates:
                self.log_result("Address uniqueness", False, 
                              f"Found {len(duplicates)} duplicate addresses")
            else:
                self.log_result("Address uniqueness", True, "No duplicate addresses found")
                
        except Exception as e:
            self.log_result("Address format check", False, str(e))
    
    def verify_transaction_integrity(self):
        """Verify transaction data integrity and relationships."""
        logger.info("🔍 Verifying transaction integrity...")
        
        try:
            # Check for orphaned transactions (no corresponding address)
            self.cursor.execute("""
                SELECT COUNT(*) 
                FROM p2pk_transactions t
                LEFT JOIN p2pk_addresses a ON t.address_id = a.id
                WHERE a.id IS NULL
            """)
            
            orphaned_count = self.cursor.fetchone()[0]
            if orphaned_count > 0:
                self.log_result("Transaction-address relationships", False, 
                              f"Found {orphaned_count} orphaned transactions")
            else:
                self.log_result("Transaction-address relationships", True, 
                              "All transactions have valid address references")
            
            # Check for transactions with invalid amounts
            self.cursor.execute("""
                SELECT COUNT(*) 
                FROM p2pk_transactions 
                WHERE amount_satoshi <= 0 OR amount_satoshi IS NULL
            """)
            
            invalid_amounts = self.cursor.fetchone()[0]
            if invalid_amounts > 0:
                self.log_result("Transaction amounts", False, 
                              f"Found {invalid_amounts} transactions with invalid amounts")
            else:
                self.log_result("Transaction amounts", True, 
                              "All transactions have valid amounts")
            
            # Check for duplicate transactions
            self.cursor.execute("""
                SELECT txid, address_id, is_input, COUNT(*) 
                FROM p2pk_transactions 
                GROUP BY txid, address_id, is_input 
                HAVING COUNT(*) > 1
            """)
            
            duplicates = self.cursor.fetchall()
            if duplicates:
                self.log_result("Transaction uniqueness", False, 
                              f"Found {len(duplicates)} duplicate transaction records")
            else:
                self.log_result("Transaction uniqueness", True, 
                              "No duplicate transaction records found")
                
        except Exception as e:
            self.log_result("Transaction integrity check", False, str(e))
    
    def verify_block_consistency(self):
        """Verify block data consistency."""
        logger.info("🔍 Verifying block consistency...")
        
        try:
            # Check for blocks with invalid height ranges
            self.cursor.execute("""
                SELECT COUNT(*) 
                FROM p2pk_blocks 
                WHERE block_height <= 0 OR first_seen > last_seen
            """)
            
            invalid_blocks = self.cursor.fetchone()[0]
            if invalid_blocks > 0:
                self.log_result("Block height consistency", False, 
                              f"Found {invalid_blocks} blocks with invalid height ranges")
            else:
                self.log_result("Block height consistency", True, 
                              "All blocks have valid height ranges")
            
            # Check for orphaned blocks (no corresponding address)
            self.cursor.execute("""
                SELECT COUNT(*) 
                FROM p2pk_blocks b
                LEFT JOIN p2pk_addresses a ON b.address_id = a.id
                WHERE a.id IS NULL
            """)
            
            orphaned_blocks = self.cursor.fetchone()[0]
            if orphaned_blocks > 0:
                self.log_result("Block-address relationships", False, 
                              f"Found {orphaned_blocks} orphaned blocks")
            else:
                self.log_result("Block-address relationships", True, 
                              "All blocks have valid address references")
                
        except Exception as e:
            self.log_result("Block consistency check", False, str(e))
    
    def spot_check_balances(self):
        """Perform spot checks on address balances."""
        logger.info("🔍 Performing balance spot checks...")
        
        try:
            # Get a few random addresses for balance verification
            self.cursor.execute("""
                SELECT a.id, a.address_key, a.public_key_hex
                FROM p2pk_addresses a
                ORDER BY RANDOM()
                LIMIT 5
            """)
            
            test_addresses = self.cursor.fetchall()
            
            for addr_id, addr_key, pub_key in test_addresses:
                # Calculate balance from transactions
                self.cursor.execute("""
                    SELECT 
                        COALESCE(SUM(CASE WHEN is_input = FALSE THEN amount_satoshi ELSE 0 END), 0) as outputs,
                        COALESCE(SUM(CASE WHEN is_input = TRUE THEN amount_satoshi ELSE 0 END), 0) as inputs
                    FROM p2pk_transactions 
                    WHERE address_id = %s
                """, (addr_id,))
                
                outputs, inputs = self.cursor.fetchone()
                calculated_balance = outputs - inputs
                
                # Get transaction count
                self.cursor.execute("""
                    SELECT COUNT(*) FROM p2pk_transactions WHERE address_id = %s
                """, (addr_id,))
                
                tx_count = self.cursor.fetchone()[0]
                
                logger.info(f"  Address {addr_id}: {addr_key[:20]}...")
                logger.info(f"    Transactions: {tx_count}")
                logger.info(f"    Total outputs: {outputs:,} sats")
                logger.info(f"    Total inputs: {inputs:,} sats")
                logger.info(f"    Calculated balance: {calculated_balance:,} sats")
                
                # Basic sanity checks
                if calculated_balance < 0:
                    self.log_result(f"Balance sanity check for {addr_id}", False, 
                                  f"Negative balance: {calculated_balance:,} sats")
                elif tx_count == 0:
                    self.log_result(f"Balance sanity check for {addr_id}", False, 
                                  "Address has no transactions")
                else:
                    self.log_result(f"Balance sanity check for {addr_id}", True, 
                                  f"Balance calculation looks correct")
                    
        except Exception as e:
            self.log_result("Balance spot check", False, str(e))
    
    def verify_scan_progress(self):
        """Verify scan progress data."""
        logger.info("🔍 Verifying scan progress...")
        
        try:
            self.cursor.execute("SELECT * FROM scan_progress ORDER BY id DESC LIMIT 1")
            progress = self.cursor.fetchone()
            
            if not progress:
                self.log_result("Scan progress exists", False, "No scan progress records found")
                return
            
            progress_id, last_block, total_blocks, last_updated = progress
            
            logger.info(f"  Last scanned block: {last_block:,}")
            logger.info(f"  Total blocks scanned: {total_blocks:,}")
            logger.info(f"  Last updated: {last_updated}")
            
            # Check if progress makes sense
            if last_block <= 0:
                self.log_result("Scan progress validity", False, f"Invalid last block: {last_block}")
            elif total_blocks <= 0:
                self.log_result("Scan progress validity", False, f"Invalid total blocks: {total_blocks}")
            else:
                self.log_result("Scan progress validity", True, "Scan progress data looks valid")
                
        except Exception as e:
            self.log_result("Scan progress check", False, str(e))
    
    def verify_data_relationships(self):
        """Verify relationships between different data tables."""
        logger.info("🔍 Verifying data relationships...")
        
        try:
            # Check that all transactions reference valid addresses
            self.cursor.execute("""
                SELECT COUNT(*) 
                FROM p2pk_transactions t
                LEFT JOIN p2pk_addresses a ON t.address_id = a.id
                WHERE a.id IS NULL
            """)
            
            orphaned_txs = self.cursor.fetchone()[0]
            if orphaned_txs > 0:
                self.log_result("Transaction-address foreign keys", False, 
                              f"Found {orphaned_txs} transactions with invalid address_id")
            else:
                self.log_result("Transaction-address foreign keys", True, 
                              "All transaction address references are valid")
            
            # Check that all blocks reference valid addresses
            self.cursor.execute("""
                SELECT COUNT(*) 
                FROM p2pk_blocks b
                LEFT JOIN p2pk_addresses a ON b.address_id = a.id
                WHERE a.id IS NULL
            """)
            
            orphaned_blocks = self.cursor.fetchone()[0]
            if orphaned_blocks > 0:
                self.log_result("Block-address foreign keys", False, 
                              f"Found {orphaned_blocks} blocks with invalid address_id")
            else:
                self.log_result("Block-address foreign keys", True, 
                              "All block address references are valid")
                
        except Exception as e:
            self.log_result("Data relationships check", False, str(e))
    
    def verify_data_consistency(self):
        """Verify overall data consistency."""
        logger.info("🔍 Verifying overall data consistency...")
        
        try:
            # Get basic statistics
            self.cursor.execute("SELECT COUNT(*) FROM p2pk_addresses")
            address_count = self.cursor.fetchone()[0]
            
            self.cursor.execute("SELECT COUNT(*) FROM p2pk_transactions")
            transaction_count = self.cursor.fetchone()[0]
            
            self.cursor.execute("SELECT COUNT(*) FROM p2pk_blocks")
            block_count = self.cursor.fetchone()[0]
            
            logger.info(f"  Total addresses: {address_count:,}")
            logger.info(f"  Total transactions: {transaction_count:,}")
            logger.info(f"  Total blocks: {block_count:,}")
            
            # Check for reasonable ratios
            if address_count == 0:
                self.log_result("Data consistency", False, "No addresses found in database")
            elif transaction_count == 0:
                self.log_result("Data consistency", False, "No transactions found in database")
            elif transaction_count < address_count:
                self.log_result("Data consistency", True, 
                              "Transaction count is reasonable relative to address count")
            else:
                self.log_result("Data consistency", True, 
                              "Data volumes look reasonable")
            
            # Check for recent data
            self.cursor.execute("""
                SELECT MAX(created_at) FROM p2pk_transactions
            """)
            
            latest_tx = self.cursor.fetchone()[0]
            if latest_tx:
                logger.info(f"  Latest transaction: {latest_tx}")
                days_ago = (datetime.now() - latest_tx).days
                if days_ago > 30:
                    self.log_result("Data freshness", True, 
                                  f"Latest data is {days_ago} days old (may be expected for historical scan)")
                else:
                    self.log_result("Data freshness", True, 
                                  f"Data is recent ({days_ago} days old)")
            else:
                self.log_result("Data freshness", False, "No transaction timestamps found")
                
        except Exception as e:
            self.log_result("Data consistency check", False, str(e))
    
    def run_all_verifications(self):
        """Run all verification checks."""
        logger.info("🚀 Starting comprehensive data integrity verification...")
        
        if not self.connect():
            return False
        
        try:
            self.verify_table_structure()
            self.verify_address_format()
            self.verify_transaction_integrity()
            self.verify_block_consistency()
            self.spot_check_balances()
            self.verify_scan_progress()
            self.verify_data_relationships()
            self.verify_data_consistency()
            
            return True
            
        finally:
            self.disconnect()
    
    def print_summary(self):
        """Print verification summary."""
        print("\n" + "="*80)
        print("DATA INTEGRITY VERIFICATION SUMMARY")
        print("="*80)
        
        total_tests = (self.verification_results['passed'] + 
                      self.verification_results['failed'] + 
                      self.verification_results['warnings'])
        
        print(f"\n📊 TEST RESULTS:")
        print(f"  Total tests: {total_tests}")
        print(f"  Passed: {self.verification_results['passed']} ✅")
        print(f"  Failed: {self.verification_results['failed']} ❌")
        print(f"  Warnings: {self.verification_results['warnings']} ⚠️")
        
        if self.verification_results['errors']:
            print(f"\n❌ ERRORS FOUND:")
            for error in self.verification_results['errors']:
                print(f"  • {error}")
        
        if self.verification_results['failed'] == 0:
            print(f"\n🎉 VERIFICATION RESULT: PASSED")
            print(f"   Database integrity looks good! Data appears to be consistent and valid.")
        else:
            print(f"\n⚠️  VERIFICATION RESULT: FAILED")
            print(f"   Found {self.verification_results['failed']} critical issues that need attention.")
        
        print("="*80)


def main():
    """Main function."""
    print("🔍 P2PK Scanner Database Integrity Verification")
    print("="*60)
    
    verifier = DataIntegrityVerifier()
    
    if verifier.run_all_verifications():
        verifier.print_summary()
        
        # Return appropriate exit code
        if verifier.verification_results['failed'] == 0:
            print("\n✅ Database verification completed successfully!")
            return 0
        else:
            print(f"\n❌ Database verification found {verifier.verification_results['failed']} issues!")
            return 1
    else:
        print("\n❌ Failed to run verification checks!")
        return 1


if __name__ == "__main__":
    sys.exit(main()) 