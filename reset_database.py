#!/usr/bin/env python3
"""
Comprehensive Database Reset Script for Bitcoin P2PK Scanner
Handles both p2pk_scanner and hydra_mode_p2pk_scanner with progress management.
"""

import sys
import os
import logging
from pathlib import Path

# Add parent directory to path to import utils
sys.path.append(str(Path(__file__).parent))

from utils.config import config
from utils.database import db_manager

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_current_progress():
    """Get current scan progress for all scanners."""
    try:
        with db_manager.get_cursor() as cursor:
            cursor.execute("SELECT scanner_name, last_scanned_block, total_blocks_scanned FROM scan_progress;")
            results = cursor.fetchall()
            return {row[0]: {'last_block': row[1], 'total_blocks': row[2]} for row in results}
    except Exception as e:
        logger.error(f"Failed to get current progress: {e}")
        return {}


def backup_progress():
    """Backup current scan progress."""
    progress = get_current_progress()
    if progress:
        logger.info("Current scan progress:")
        for scanner, data in progress.items():
            logger.info(f"  {scanner}: Block {data['last_block']} ({data['total_blocks']} total)")
    return progress


def drop_all_tables():
    """Drop all P2PK scanner tables."""
    try:
        logger.info("Dropping all P2PK scanner tables...")
        
        tables = [
            'p2pk_transactions',
            'p2pk_address_blocks', 
            'p2pk_addresses',
            'scan_progress'
        ]
        
        for table in tables:
            if db_manager.table_exists(table):
                db_manager.execute_command(f"DROP TABLE {table} CASCADE;")
                logger.info(f"Dropped table: {table}")
        
        logger.info("All P2PK scanner tables dropped successfully!")
        
    except Exception as e:
        logger.error(f"Failed to drop tables: {e}")
        raise


def create_optimized_schema():
    """Create optimized schema with better performance characteristics."""
    
    # Create optimized p2pk_addresses table
    p2pk_addresses_sql = """
    CREATE TABLE p2pk_addresses (
        id BIGSERIAL PRIMARY KEY,  -- BIGSERIAL for larger range
        address VARCHAR(255) NOT NULL,
        public_key_hex VARCHAR(130) NOT NULL,
        first_seen_block INTEGER NOT NULL,
        first_seen_txid VARCHAR(64) NOT NULL,
        last_seen_block INTEGER NOT NULL,
        total_received_satoshi BIGINT DEFAULT 0,
        current_balance_satoshi BIGINT DEFAULT 0,
        is_spent BOOLEAN DEFAULT FALSE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    ) WITH (fillfactor = 90);  -- Optimize for inserts
    """
    
    # Create optimized p2pk_transactions table
    p2pk_transactions_sql = """
    CREATE TABLE p2pk_transactions (
        id BIGSERIAL PRIMARY KEY,
        txid VARCHAR(64) NOT NULL,
        block_height INTEGER NOT NULL,
        block_time TIMESTAMP NOT NULL,
        address_id BIGINT NOT NULL,  -- Remove foreign key constraint for speed
        is_input BOOLEAN NOT NULL,
        amount_satoshi BIGINT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    ) WITH (fillfactor = 90);
    """
    
    # Create optimized p2pk_address_blocks table
    p2pk_address_blocks_sql = """
    CREATE TABLE p2pk_address_blocks (
        id BIGSERIAL PRIMARY KEY,
        address_id BIGINT NOT NULL,  -- Remove foreign key constraint for speed
        block_height INTEGER NOT NULL,
        is_input BOOLEAN NOT NULL,
        amount_satoshi BIGINT NOT NULL,
        txid VARCHAR(64) NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    ) WITH (fillfactor = 90);
    """
    
    # Create scan_progress table
    scan_progress_sql = """
    CREATE TABLE scan_progress (
        id SERIAL PRIMARY KEY,
        scanner_name VARCHAR(50) NOT NULL UNIQUE,
        last_scanned_block INTEGER NOT NULL,
        total_blocks_scanned INTEGER DEFAULT 0,
        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    
    try:
        logger.info("Creating optimized database schema...")
        
        # Create optimized tables
        db_manager.execute_command(p2pk_addresses_sql)
        logger.info("Created optimized p2pk_addresses table")
        
        db_manager.execute_command(p2pk_transactions_sql)
        logger.info("Created optimized p2pk_transactions table")
        
        db_manager.execute_command(p2pk_address_blocks_sql)
        logger.info("Created optimized p2pk_address_blocks table")
        
        db_manager.execute_command(scan_progress_sql)
        logger.info("Created scan_progress table")
        
        logger.info("Optimized schema created successfully!")
        
    except Exception as e:
        logger.error(f"Failed to create optimized schema: {e}")
        raise


def create_high_performance_indexes():
    """Create optimized indexes for maximum performance."""
    
    # Create minimal, high-performance indexes
    indexes_sql = [
        # Only essential indexes for the scanner (no CONCURRENTLY for speed during bulk load)
        "CREATE UNIQUE INDEX idx_p2pk_addresses_address ON p2pk_addresses(address);",
        "CREATE INDEX idx_p2pk_addresses_public_key ON p2pk_addresses(public_key_hex);",
        "CREATE INDEX idx_p2pk_transactions_address_id ON p2pk_transactions(address_id);",
        "CREATE INDEX idx_p2pk_transactions_block_height ON p2pk_transactions(block_height);",
        "CREATE INDEX idx_p2pk_address_blocks_address_id ON p2pk_address_blocks(address_id);",
        "CREATE INDEX idx_p2pk_address_blocks_block_height ON p2pk_address_blocks(block_height);",
    ]
    
    try:
        logger.info("Creating high-performance indexes...")
        
        for index_sql in indexes_sql:
            db_manager.execute_command(index_sql)
            logger.info(f"Created index: {index_sql.split('ON')[1].strip()}")
        
        logger.info("High-performance indexes created successfully!")
        
    except Exception as e:
        logger.error(f"Failed to create indexes: {e}")
        raise


def create_prepared_statements():
    """Create prepared statements for maximum insert performance."""
    
    prepared_statements = [
        # Prepared statement for address insertion
        """
        PREPARE insert_address (VARCHAR, VARCHAR, INTEGER, VARCHAR, INTEGER) AS
        INSERT INTO p2pk_addresses (address, public_key_hex, first_seen_block, first_seen_txid, last_seen_block)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (address) DO UPDATE SET 
            last_seen_block = EXCLUDED.last_seen_block,
            updated_at = CURRENT_TIMESTAMP
        RETURNING id;
        """,
        
        # Prepared statement for transaction insertion
        """
        PREPARE insert_transaction (VARCHAR, INTEGER, TIMESTAMP, BIGINT, BOOLEAN, BIGINT) AS
        INSERT INTO p2pk_transactions (txid, block_height, block_time, address_id, is_input, amount_satoshi)
        VALUES ($1, $2, $3, $4, $5, $6);
        """,
        
        # Prepared statement for block record insertion
        """
        PREPARE insert_block_record (BIGINT, INTEGER, BOOLEAN, BIGINT, VARCHAR) AS
        INSERT INTO p2pk_address_blocks (address_id, block_height, is_input, amount_satoshi, txid)
        VALUES ($1, $2, $3, $4, $5);
        """,
        
        # Prepared statement for address lookup
        """
        PREPARE get_address_id (VARCHAR) AS
        SELECT id FROM p2pk_addresses WHERE address = $1;
        """,
    ]
    
    try:
        logger.info("Creating prepared statements for maximum performance...")
        
        for stmt in prepared_statements:
            db_manager.execute_command(stmt)
        
        logger.info("Prepared statements created successfully!")
        
    except Exception as e:
        logger.error(f"Failed to create prepared statements: {e}")
        raise


def optimize_table_settings():
    """Apply table-level optimizations."""
    
    table_optimizations = [
        # Set table storage parameters for better performance
        "ALTER TABLE p2pk_addresses SET (fillfactor = 90);",
        "ALTER TABLE p2pk_transactions SET (fillfactor = 90);",
        "ALTER TABLE p2pk_address_blocks SET (fillfactor = 90);",
        
        # Disable autovacuum during bulk loading
        "ALTER TABLE p2pk_addresses SET (autovacuum_enabled = false);",
        "ALTER TABLE p2pk_transactions SET (autovacuum_enabled = false);",
        "ALTER TABLE p2pk_address_blocks SET (autovacuum_enabled = false);",
        
        # Set toast table parameters
        "ALTER TABLE p2pk_addresses SET (toast_tuple_target = 4096);",
        "ALTER TABLE p2pk_transactions SET (toast_tuple_target = 4096);",
        "ALTER TABLE p2pk_address_blocks SET (toast_tuple_target = 4096);",
    ]
    
    try:
        logger.info("Applying table-level optimizations...")
        
        for optimization in table_optimizations:
            db_manager.execute_command(optimization)
        
        logger.info("Table optimizations applied successfully!")
        
    except Exception as e:
        logger.error(f"Failed to apply table optimizations: {e}")
        raise


def restore_progress(progress_backup, preserve_progress=False):
    """Restore scan progress if requested."""
    if not preserve_progress or not progress_backup:
        # Initialize fresh progress entries
        init_sql = """
        INSERT INTO scan_progress (scanner_name, last_scanned_block, total_blocks_scanned)
        VALUES 
            ('p2pk_scanner', 0, 0),
            ('hydra_mode_p2pk_scanner', 0, 0)
        ON CONFLICT (scanner_name) DO NOTHING;
        """
        db_manager.execute_command(init_sql)
        logger.info("Initialized fresh scan progress entries")
    else:
        # Restore previous progress
        for scanner_name, data in progress_backup.items():
            restore_sql = """
            INSERT INTO scan_progress (scanner_name, last_scanned_block, total_blocks_scanned)
            VALUES (%s, %s, %s)
            ON CONFLICT (scanner_name) DO UPDATE SET
                last_scanned_block = EXCLUDED.last_scanned_block,
                total_blocks_scanned = EXCLUDED.total_blocks_scanned,
                last_updated = CURRENT_TIMESTAMP;
            """
            db_manager.execute_command(restore_sql, (scanner_name, data['last_block'], data['total_blocks']))
            logger.info(f"Restored progress for {scanner_name}: Block {data['last_block']}")


def main():
    """Main function to handle command line arguments."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Reset Bitcoin P2PK Scanner Database')
    parser.add_argument('--reset', action='store_true', help='Reset all tables and data')
    parser.add_argument('--preserve-progress', action='store_true', help='Preserve scan progress when resetting')
    parser.add_argument('--optimize-only', action='store_true', help='Only optimize schema, don\'t reset data')
    
    args = parser.parse_args()
    
    try:
        if args.optimize_only:
            logger.info("🔄 Optimizing database schema only...")
            create_optimized_schema()
            create_high_performance_indexes()
            create_prepared_statements()
            optimize_table_settings()
            logger.info("✅ Schema optimization completed!")
            
        elif args.reset:
            logger.info("🔄 Resetting database...")
            
            # Backup current progress
            progress_backup = backup_progress()
            
            # Drop all tables
            drop_all_tables()
            
            # Create optimized schema
            create_optimized_schema()
            create_high_performance_indexes()
            create_prepared_statements()
            optimize_table_settings()
            
            # Restore progress if requested
            restore_progress(progress_backup, args.preserve_progress)
            
            logger.info("✅ Database reset completed!")
            
        else:
            logger.info("🔄 Creating database schema...")
            create_optimized_schema()
            create_high_performance_indexes()
            create_prepared_statements()
            optimize_table_settings()
            restore_progress({}, preserve_progress=False)
            logger.info("✅ Database setup completed!")
        
        # Show final table counts
        tables = ['p2pk_addresses', 'p2pk_transactions', 'p2pk_address_blocks', 'scan_progress']
        for table in tables:
            count = db_manager.get_table_count(table)
            logger.info(f"Table {table}: {count} rows")
            
    except Exception as e:
        logger.error(f"Database operation failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main() 