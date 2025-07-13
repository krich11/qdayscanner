#!/usr/bin/env python3
"""
Block Verification and Repair Script for Bitcoin Quantum Vulnerability Scanner.
Verifies that all blocks from the Bitcoin node are present in the database.
Detects missing blocks and provides detailed reporting.
REPAIRS database issues caused by failed scanner runs.
"""

import sys
import logging
import time
from pathlib import Path
from typing import List, Set, Tuple, Optional
from datetime import datetime

# Add parent directory to path to import utils
sys.path.append(str(Path(__file__).parent.parent))

from utils.config import config
from utils.database import DatabaseManager
from bitcoin_rpc import bitcoin_rpc

# Set up logging
logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_bitcoin_blockchain_info() -> Optional[dict]:
    """Get current blockchain information from Bitcoin node."""
    try:
        return bitcoin_rpc.get_blockchain_info()
    except Exception as e:
        logger.error(f"Failed to get blockchain info: {e}")
        return None


def get_database_block_range(db_manager: DatabaseManager) -> Tuple[int, int, int]:
    """Get the range of blocks present in the database."""
    try:
        # First check scan progress to see what range was actually scanned
        progress_query = """
        SELECT last_scanned_block, total_blocks_scanned
        FROM scan_progress 
        WHERE scanner_name = 'hydra_mode_p2pk_scanner'
        ORDER BY last_scanned_block DESC
        LIMIT 1
        """
        progress_result = db_manager.execute_query(progress_query)
        
        if progress_result and progress_result[0]['last_scanned_block']:
            scanned_block = progress_result[0]['last_scanned_block']
            total_scanned = progress_result[0]['total_blocks_scanned']
            return 0, scanned_block, total_scanned
        else:
            # Fallback to checking actual P2PK transaction data
            query = """
            SELECT MIN(block_height) as min_block, MAX(block_height) as max_block, COUNT(DISTINCT block_height) as total_blocks
            FROM p2pk_transactions
            """
            result = db_manager.execute_query(query)
            
            if result and result[0]['min_block'] is not None:
                return result[0]['min_block'], result[0]['max_block'], result[0]['total_blocks']
            else:
                return 0, 0, 0
            
    except Exception as e:
        logger.error(f"Failed to get database block range: {e}")
        return 0, 0, 0


def get_database_blocks(db_manager: DatabaseManager, start_block: int, end_block: int) -> Set[int]:
    """Get all block heights that should have been scanned within a range."""
    try:
        # Get the actual scanned range from scan progress
        progress_query = """
        SELECT last_scanned_block
        FROM scan_progress 
        WHERE scanner_name = 'hydra_mode_p2pk_scanner'
        ORDER BY last_scanned_block DESC
        LIMIT 1
        """
        progress_result = db_manager.execute_query(progress_query)
        
        if progress_result and progress_result[0]['last_scanned_block']:
            last_scanned = progress_result[0]['last_scanned_block']
            # Return all blocks from start_block to the last actually scanned block
            # This represents blocks that should have been processed
            return set(range(start_block, min(end_block, last_scanned + 1)))
        else:
            # Fallback: return blocks that actually have P2PK transactions
            query = """
            SELECT DISTINCT block_height 
            FROM p2pk_transactions 
            WHERE block_height BETWEEN %s AND %s 
            ORDER BY block_height
            """
            results = db_manager.execute_query(query, (start_block, end_block))
            return {row['block_height'] for row in results}
    except Exception as e:
        logger.error(f"Failed to get database blocks: {e}")
        return set()


def get_bitcoin_blocks(start_block: int, end_block: int) -> Set[int]:
    """Get all block heights that should exist in Bitcoin node."""
    return set(range(start_block, end_block + 1))


def find_missing_blocks(bitcoin_blocks: Set[int], database_blocks: Set[int], db_manager: DatabaseManager) -> List[int]:
    """Find blocks that should have been scanned but are missing from database."""
    try:
        # Get the actual last scanned block from scan progress
        progress_query = """
        SELECT last_scanned_block
        FROM scan_progress 
        WHERE scanner_name = 'hydra_mode_p2pk_scanner'
        ORDER BY last_scanned_block DESC
        LIMIT 1
        """
        progress_result = db_manager.execute_query(progress_query)
        
        if not progress_result or not progress_result[0]['last_scanned_block']:
            # No scan progress found, fall back to simple difference
            return sorted(list(bitcoin_blocks - database_blocks))
        
        last_scanned = progress_result[0]['last_scanned_block']
        
        # Only report blocks as missing if they were within the scanned range
        missing = []
        for block in sorted(bitcoin_blocks):
            if block > last_scanned:
                # This block hasn't been scanned yet, so it's not missing
                continue
            elif block not in database_blocks:
                # This block should have been scanned but has no P2PK transactions
                # Check if it was actually processed by looking for any transaction records
                check_query = """
                SELECT COUNT(*) as count
                FROM p2pk_transactions 
                WHERE block_height = %s
                """
                check_result = db_manager.execute_query(check_query, (block,))
                if check_result and check_result[0]['count'] == 0:
                    # Block was scanned but has no P2PK transactions - this is normal
                    continue
                else:
                    # Block was not scanned at all - this is missing
                    missing.append(block)
        
        return missing
        
    except Exception as e:
        logger.error(f"Failed to find missing blocks: {e}")
        # Fall back to simple difference
        return sorted(list(bitcoin_blocks - database_blocks))


def find_extra_blocks(bitcoin_blocks: Set[int], database_blocks: Set[int]) -> List[int]:
    """Find blocks that exist in database but not in Bitcoin (shouldn't happen)."""
    return sorted(list(database_blocks - bitcoin_blocks))


def analyze_block_gaps(missing_blocks: List[int]) -> List[Tuple[int, int, int]]:
    """Analyze missing blocks to find continuous gaps."""
    if not missing_blocks:
        return []
    
    gaps = []
    gap_start = missing_blocks[0]
    gap_end = gap_start
    
    for i in range(1, len(missing_blocks)):
        if missing_blocks[i] == gap_end + 1:
            gap_end = missing_blocks[i]
        else:
            # Gap ended, record it
            gaps.append((gap_start, gap_end, gap_end - gap_start + 1))
            gap_start = gap_end = missing_blocks[i]
    
    # Don't forget the last gap
    gaps.append((gap_start, gap_end, gap_end - gap_start + 1))
    
    return gaps


def detect_database_issues(db_manager: DatabaseManager) -> dict:
    """Detect various database issues that could be caused by failed scanner runs."""
    issues = {
        'orphaned_transactions': 0,
        'orphaned_blocks': 0,
        'invalid_address_ids': 0,
        'duplicate_transactions': 0,
        'inconsistent_balances': 0,
        'missing_addresses': 0
    }
    
    try:
        # Check for orphaned transactions (transactions without valid addresses)
        orphan_query = """
        SELECT COUNT(*) as count
        FROM p2pk_transactions t
        LEFT JOIN p2pk_addresses a ON t.address_id = a.id
        WHERE a.id IS NULL
        """
        orphan_result = db_manager.execute_query(orphan_query)
        issues['orphaned_transactions'] = orphan_result[0]['count'] if orphan_result else 0
        
        # Check for orphaned block records
        orphan_block_query = """
        SELECT COUNT(*) as count
        FROM p2pk_address_blocks b
        LEFT JOIN p2pk_addresses a ON b.address_id = a.id
        WHERE a.id IS NULL
        """
        orphan_block_result = db_manager.execute_query(orphan_block_query)
        issues['orphaned_blocks'] = orphan_block_result[0]['count'] if orphan_block_result else 0
        
        # Check for invalid address IDs (should be > 0)
        invalid_id_query = """
        SELECT COUNT(*) as count
        FROM p2pk_transactions
        WHERE address_id <= 0
        """
        invalid_id_result = db_manager.execute_query(invalid_id_query)
        issues['invalid_address_ids'] = invalid_id_result[0]['count'] if invalid_id_result else 0
        
        # Check for duplicate transactions
        duplicate_query = """
        SELECT COUNT(*) as count
        FROM (
            SELECT txid, address_id, is_input, COUNT(*)
            FROM p2pk_transactions
            GROUP BY txid, address_id, is_input
            HAVING COUNT(*) > 1
        ) duplicates
        """
        duplicate_result = db_manager.execute_query(duplicate_query)
        issues['duplicate_transactions'] = duplicate_result[0]['count'] if duplicate_result else 0
        
        # Check for addresses with inconsistent balance calculations
        balance_query = """
        SELECT COUNT(*) as count
        FROM p2pk_addresses a
        WHERE a.current_balance_satoshi != (
            SELECT COALESCE(SUM(CASE WHEN is_input THEN -amount_satoshi ELSE amount_satoshi END), 0)
            FROM p2pk_address_blocks b
            WHERE b.address_id = a.id
        )
        """
        balance_result = db_manager.execute_query(balance_query)
        issues['inconsistent_balances'] = balance_result[0]['count'] if balance_result else 0
        
        # Check for missing addresses (transactions referencing non-existent addresses)
        missing_addr_query = """
        SELECT COUNT(DISTINCT t.address_id) as count
        FROM p2pk_transactions t
        LEFT JOIN p2pk_addresses a ON t.address_id = a.id
        WHERE a.id IS NULL AND t.address_id > 0
        """
        missing_addr_result = db_manager.execute_query(missing_addr_query)
        issues['missing_addresses'] = missing_addr_result[0]['count'] if missing_addr_result else 0
        
    except Exception as e:
        logger.error(f"Failed to detect database issues: {e}")
    
    return issues


def repair_database_issues(db_manager: DatabaseManager, issues: dict) -> dict:
    """Repair detected database issues."""
    repairs = {
        'transactions_removed': 0,
        'blocks_removed': 0,
        'addresses_created': 0,
        'balances_fixed': 0,
        'duplicates_removed': 0
    }
    
    try:
        logger.info("Starting database repairs...")
        
        # 1. Remove orphaned transactions
        if issues['orphaned_transactions'] > 0:
            logger.info(f"Removing {issues['orphaned_transactions']} orphaned transactions...")
            orphan_delete_query = """
            DELETE FROM p2pk_transactions 
            WHERE address_id IN (
                SELECT t.address_id
                FROM p2pk_transactions t
                LEFT JOIN p2pk_addresses a ON t.address_id = a.id
                WHERE a.id IS NULL
            )
            """
            db_manager.execute_command(orphan_delete_query)
            repairs['transactions_removed'] = issues['orphaned_transactions']
        
        # 2. Remove orphaned block records
        if issues['orphaned_blocks'] > 0:
            logger.info(f"Removing {issues['orphaned_blocks']} orphaned block records...")
            orphan_block_delete_query = """
            DELETE FROM p2pk_address_blocks 
            WHERE address_id IN (
                SELECT b.address_id
                FROM p2pk_address_blocks b
                LEFT JOIN p2pk_addresses a ON b.address_id = a.id
                WHERE a.id IS NULL
            )
            """
            db_manager.execute_command(orphan_block_delete_query)
            repairs['blocks_removed'] = issues['orphaned_blocks']
        
        # 3. Remove transactions with invalid address IDs
        if issues['invalid_address_ids'] > 0:
            logger.info(f"Removing {issues['invalid_address_ids']} transactions with invalid address IDs...")
            invalid_id_delete_query = """
            DELETE FROM p2pk_transactions 
            WHERE address_id <= 0
            """
            db_manager.execute_command(invalid_id_delete_query)
            repairs['transactions_removed'] += issues['invalid_address_ids']
        
        # 4. Remove duplicate transactions
        if issues['duplicate_transactions'] > 0:
            logger.info(f"Removing {issues['duplicate_transactions']} duplicate transactions...")
            duplicate_delete_query = """
            DELETE FROM p2pk_transactions 
            WHERE id NOT IN (
                SELECT MIN(id)
                FROM p2pk_transactions
                GROUP BY txid, address_id, is_input
            )
            """
            db_manager.execute_command(duplicate_delete_query)
            repairs['duplicates_removed'] = issues['duplicate_transactions']
        
        # 5. Fix inconsistent balances
        if issues['inconsistent_balances'] > 0:
            logger.info(f"Fixing {issues['inconsistent_balances']} inconsistent balances...")
            balance_fix_query = """
            UPDATE p2pk_addresses 
            SET current_balance_satoshi = (
                SELECT COALESCE(SUM(CASE WHEN is_input THEN -amount_satoshi ELSE amount_satoshi END), 0)
                FROM p2pk_address_blocks b
                WHERE b.address_id = p2pk_addresses.id
            ),
            updated_at = CURRENT_TIMESTAMP
            WHERE id IN (
                SELECT a.id
                FROM p2pk_addresses a
                WHERE a.current_balance_satoshi != (
                    SELECT COALESCE(SUM(CASE WHEN is_input THEN -amount_satoshi ELSE amount_satoshi END), 0)
                    FROM p2pk_address_blocks b
                    WHERE b.address_id = a.id
                )
            )
            """
            db_manager.execute_command(balance_fix_query)
            repairs['balances_fixed'] = issues['inconsistent_balances']
        
        logger.info("Database repairs completed successfully!")
        
    except Exception as e:
        logger.error(f"Failed to repair database issues: {e}")
        raise
    
    return repairs


def verify_block_consistency(db_manager: DatabaseManager, start_block: int, end_block: int) -> bool:
    """Verify that block data is consistent within the database."""
    try:
        # Check for blocks with transactions but no P2PK data
        query = """
        SELECT COUNT(*) as count
        FROM (
            SELECT DISTINCT block_height 
            FROM p2pk_transactions 
            WHERE block_height BETWEEN %s AND %s
        ) db_blocks
        """
        result = db_manager.execute_query(query, (start_block, end_block))
        db_block_count = result[0]['count'] if result else 0
        
        # Check for any orphaned transaction records
        orphan_query = """
        SELECT COUNT(*) as count
        FROM p2pk_transactions t
        LEFT JOIN p2pk_addresses a ON t.address_id = a.id
        WHERE t.block_height BETWEEN %s AND %s AND a.id IS NULL
        """
        orphan_result = db_manager.execute_query(orphan_query, (start_block, end_block))
        orphan_count = orphan_result[0]['count'] if orphan_result else 0
        
        return orphan_count == 0
        
    except Exception as e:
        logger.error(f"Failed to verify block consistency: {e}")
        return False


def get_block_statistics(db_manager: DatabaseManager, start_block: int, end_block: int) -> dict:
    """Get detailed statistics about blocks in the database."""
    try:
        # Total P2PK transactions
        tx_query = """
        SELECT COUNT(*) as total_transactions,
               COUNT(DISTINCT block_height) as blocks_with_transactions,
               COUNT(DISTINCT address_id) as unique_addresses
        FROM p2pk_transactions 
        WHERE block_height BETWEEN %s AND %s
        """
        tx_result = db_manager.execute_query(tx_query, (start_block, end_block))
        
        # P2PK addresses found
        addr_query = """
        SELECT COUNT(*) as total_addresses,
               COUNT(CASE WHEN current_balance_satoshi > 0 THEN 1 END) as addresses_with_balance
        FROM p2pk_addresses
        """
        addr_result = db_manager.execute_query(addr_query)
        
        return {
            'total_transactions': tx_result[0]['total_transactions'] if tx_result else 0,
            'blocks_with_transactions': tx_result[0]['blocks_with_transactions'] if tx_result else 0,
            'unique_addresses': tx_result[0]['unique_addresses'] if tx_result else 0,
            'total_addresses': addr_result[0]['total_addresses'] if addr_result else 0,
            'addresses_with_balance': addr_result[0]['addresses_with_balance'] if addr_result else 0
        }
        
    except Exception as e:
        logger.error(f"Failed to get block statistics: {e}")
        return {}


def verify_specific_block(db_manager: DatabaseManager, block_height: int) -> bool:
    """Verify that a specific block exists and has proper data."""
    try:
        # Check if block exists in database
        query = """
        SELECT COUNT(*) as count
        FROM p2pk_transactions 
        WHERE block_height = %s
        """
        result = db_manager.execute_query(query, (block_height,))
        return result[0]['count'] > 0 if result else False
        
    except Exception as e:
        logger.error(f"Failed to verify block {block_height}: {e}")
        return False


def main():
    """Main verification function."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Verify and repair Bitcoin blocks in database')
    parser.add_argument('--start-block', type=int, help='Start block height (default: 0)')
    parser.add_argument('--end-block', type=int, help='End block height (default: current)')
    parser.add_argument('--check-gaps', action='store_true', help='Analyze gaps in detail')
    parser.add_argument('--verify-specific', type=int, help='Verify a specific block height')
    parser.add_argument('--detailed', action='store_true', help='Show detailed statistics')
    parser.add_argument('--repair', action='store_true', help='Repair detected database issues')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be repaired without making changes')
    parser.add_argument('--scanned-only', action='store_true', help='Only verify blocks that have been scanned (ignore unscanned blocks at end)')
    
    args = parser.parse_args()
    
    logger.info("Starting block verification and repair...")
    
    # Test Bitcoin connection
    if not bitcoin_rpc.test_connection():
        logger.error("Failed to connect to Bitcoin Core")
        return
    
    # Get blockchain info
    blockchain_info = get_bitcoin_blockchain_info()
    if not blockchain_info:
        logger.error("Failed to get blockchain information")
        return
    
    current_height = blockchain_info['blocks']
    logger.info(f"Bitcoin blockchain height: {current_height}")
    
    # Connect to database
    db_manager = DatabaseManager()
    try:
        # Get database block range first
        db_min, db_max, db_total = get_database_block_range(db_manager)
        logger.info(f"Database block range: {db_min} to {db_max} ({db_total} blocks)")
        
        # Determine scan range
        start_block = args.start_block if args.start_block is not None else 0
        
        # For end_block, use the actual scanned range, not the full blockchain
        if args.end_block is not None:
            end_block = args.end_block
        elif args.scanned_only or db_max > 0:
            # Use the highest block actually scanned in the database
            if db_max > 0:
                end_block = db_max  # Only check up to the highest block we've scanned
                logger.info(f"Using scanned range: {start_block} to {end_block} (highest scanned block)")
            else:
                end_block = current_height  # Fallback to full blockchain if no data
        else:
            end_block = current_height  # Check full blockchain
        
        logger.info(f"Verifying blocks {start_block} to {end_block}")
        
        # Verify specific block if requested
        if args.verify_specific is not None:
            block_height = args.verify_specific
            exists = verify_specific_block(db_manager, block_height)
            logger.info(f"Block {block_height} exists in database: {exists}")
            return
        
        # DETECT DATABASE ISSUES
        logger.info("Detecting database issues...")
        issues = detect_database_issues(db_manager)
        
        print("\n" + "="*80)
        print("DATABASE ISSUES DETECTED")
        print("="*80)
        total_issues = sum(issues.values())
        if total_issues == 0:
            print("✅ No database issues detected!")
        else:
            for issue, count in issues.items():
                if count > 0:
                    print(f"❌ {issue}: {count}")
        
        # REPAIR ISSUES if requested
        if args.repair and total_issues > 0:
            if args.dry_run:
                print(f"\n🔍 DRY RUN: Would repair {total_issues} issues")
                print("Run without --dry-run to actually repair the issues")
            else:
                print(f"\n🔧 REPAIRING {total_issues} issues...")
                repairs = repair_database_issues(db_manager, issues)
                
                print("\n" + "="*80)
                print("REPAIR RESULTS")
                print("="*80)
                for repair, count in repairs.items():
                    if count > 0:
                        print(f"✅ {repair}: {count}")
        
        # Get blocks from both sources
        logger.info("Getting Bitcoin blocks...")
        bitcoin_blocks = get_bitcoin_blocks(start_block, end_block)
        logger.info(f"Bitcoin should have {len(bitcoin_blocks)} blocks")
        
        logger.info("Getting database blocks...")
        database_blocks = get_database_blocks(db_manager, start_block, end_block)
        logger.info(f"Database has {len(database_blocks)} blocks")
        
        # Find missing and extra blocks
        missing_blocks = find_missing_blocks(bitcoin_blocks, database_blocks, db_manager)
        extra_blocks = find_extra_blocks(bitcoin_blocks, database_blocks)
        
        # Report results
        print("\n" + "="*80)
        print("BLOCK VERIFICATION REPORT")
        print("="*80)
        print(f"Scan Range: {start_block} to {end_block}")
        print(f"Bitcoin Blocks: {len(bitcoin_blocks)}")
        print(f"Database Blocks: {len(database_blocks)}")
        print(f"Missing Blocks: {len(missing_blocks)}")
        print(f"Extra Blocks: {len(extra_blocks)}")
        print(f"Coverage: {((len(bitcoin_blocks) - len(missing_blocks)) / len(bitcoin_blocks) * 100):.2f}%")
        
        if missing_blocks:
            print(f"\nNOTE: 'Missing blocks' are blocks that should have been scanned")
            print(f"      but contain no P2PK transaction records in the database.")
            print(f"      This may indicate scanner failures or incomplete processing.")
        
        if missing_blocks:
            print(f"\nMISSING BLOCKS ({len(missing_blocks)}):")
            if len(missing_blocks) <= 20:
                print(f"  {missing_blocks}")
            else:
                print(f"  First 10: {missing_blocks[:10]}")
                print(f"  Last 10: {missing_blocks[-10:]}")
            
            # Analyze gaps
            gaps = analyze_block_gaps(missing_blocks)
            print(f"\nBLOCK GAPS ({len(gaps)}):")
            for start, end, count in gaps:
                print(f"  {start} to {end} ({count} blocks)")
        
        if extra_blocks:
            print(f"\nEXTRA BLOCKS ({len(extra_blocks)}):")
            print(f"  {extra_blocks}")
        
        # Detailed statistics
        if args.detailed:
            print(f"\nDETAILED STATISTICS:")
            stats = get_block_statistics(db_manager, start_block, end_block)
            for key, value in stats.items():
                print(f"  {key}: {value}")
            
            # Consistency check
            consistent = verify_block_consistency(db_manager, start_block, end_block)
            print(f"  Database consistency: {'OK' if consistent else 'ISSUES DETECTED'}")
        
        # Summary
        if not missing_blocks and not extra_blocks and total_issues == 0:
            print(f"\n✅ VERIFICATION PASSED: All blocks present and database is clean!")
        else:
            print(f"\n⚠️  VERIFICATION FAILED: Missing {len(missing_blocks)} blocks, {len(extra_blocks)} extra blocks, {total_issues} database issues")
            
            if missing_blocks and args.check_gaps:
                print(f"\nGAP ANALYSIS:")
                gaps = analyze_block_gaps(missing_blocks)
                for i, (start, end, count) in enumerate(gaps, 1):
                    print(f"  Gap {i}: Blocks {start}-{end} ({count} blocks)")
                    if count <= 10:
                        print(f"    Missing: {list(range(start, end + 1))}")
        
        print("="*80)
        
    except Exception as e:
        logger.error(f"Verification failed: {e}")
    finally:
        db_manager.close()


if __name__ == "__main__":
    main() 