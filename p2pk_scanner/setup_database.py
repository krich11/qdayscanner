#!/usr/bin/env python3
"""
Database setup script for P2PK Scanner.
Creates all necessary tables and indexes for the P2PK scanner.
"""

import sys
import os
import logging
from pathlib import Path

# Add parent directory to path to import utils
sys.path.append(str(Path(__file__).parent.parent))

from utils.config import config
from utils.database import db_manager

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def create_tables():
    """Create all necessary tables for the P2PK scanner."""
    
    # Create p2pk_addresses table
    p2pk_addresses_sql = """
    CREATE TABLE IF NOT EXISTS p2pk_addresses (
        id SERIAL PRIMARY KEY,
        address VARCHAR(255) NOT NULL UNIQUE,
        public_key_hex VARCHAR(130) NOT NULL,
        first_seen_block INTEGER NOT NULL,
        first_seen_txid VARCHAR(64) NOT NULL,
        last_seen_block INTEGER NOT NULL,
        total_received_satoshi BIGINT DEFAULT 0,
        current_balance_satoshi BIGINT DEFAULT 0,
        is_spent BOOLEAN DEFAULT FALSE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    
    # Create p2pk_transactions table
    p2pk_transactions_sql = """
    CREATE TABLE IF NOT EXISTS p2pk_transactions (
        id SERIAL PRIMARY KEY,
        txid VARCHAR(64) NOT NULL,
        block_height INTEGER NOT NULL,
        block_time TIMESTAMP NOT NULL,
        address_id INTEGER REFERENCES p2pk_addresses(id) ON DELETE CASCADE,
        is_input BOOLEAN NOT NULL,
        amount_satoshi BIGINT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    
    # Create p2pk_address_blocks table for efficient balance calculation
    p2pk_address_blocks_sql = """
    CREATE TABLE IF NOT EXISTS p2pk_address_blocks (
        id SERIAL PRIMARY KEY,
        address_id INTEGER REFERENCES p2pk_addresses(id) ON DELETE CASCADE,
        block_height INTEGER NOT NULL,
        is_input BOOLEAN NOT NULL,
        amount_satoshi BIGINT NOT NULL,
        txid VARCHAR(64) NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    
    # Create scan_progress table
    scan_progress_sql = """
    CREATE TABLE IF NOT EXISTS scan_progress (
        id SERIAL PRIMARY KEY,
        scanner_name VARCHAR(50) NOT NULL UNIQUE,
        last_scanned_block INTEGER NOT NULL,
        total_blocks_scanned INTEGER DEFAULT 0,
        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    
    # Create indexes for better performance
    indexes_sql = [
        "CREATE INDEX IF NOT EXISTS idx_p2pk_addresses_address ON p2pk_addresses(address);",
        "CREATE INDEX IF NOT EXISTS idx_p2pk_addresses_public_key ON p2pk_addresses(public_key_hex);",
        "CREATE INDEX IF NOT EXISTS idx_p2pk_addresses_first_seen ON p2pk_addresses(first_seen_block);",
        "CREATE INDEX IF NOT EXISTS idx_p2pk_addresses_last_seen ON p2pk_addresses(last_seen_block);",
        "CREATE INDEX IF NOT EXISTS idx_p2pk_transactions_txid ON p2pk_transactions(txid);",
        "CREATE INDEX IF NOT EXISTS idx_p2pk_transactions_block_height ON p2pk_transactions(block_height);",
        "CREATE INDEX IF NOT EXISTS idx_p2pk_transactions_address_id ON p2pk_transactions(address_id);",
        "CREATE INDEX IF NOT EXISTS idx_p2pk_transactions_block_time ON p2pk_transactions(block_time);",
        "CREATE INDEX IF NOT EXISTS idx_p2pk_address_blocks_address_id ON p2pk_address_blocks(address_id);",
        "CREATE INDEX IF NOT EXISTS idx_p2pk_address_blocks_block_height ON p2pk_address_blocks(block_height);",
        "CREATE INDEX IF NOT EXISTS idx_p2pk_address_blocks_address_block ON p2pk_address_blocks(address_id, block_height);",
        "CREATE INDEX IF NOT EXISTS idx_scan_progress_scanner_name ON scan_progress(scanner_name);"
    ]
    
    try:
        logger.info("Creating P2PK scanner database tables...")
        
        # Execute table creation
        db_manager.execute_command(p2pk_addresses_sql)
        logger.info("Created p2pk_addresses table")
        
        db_manager.execute_command(p2pk_transactions_sql)
        logger.info("Created p2pk_transactions table")
        
        db_manager.execute_command(p2pk_address_blocks_sql)
        logger.info("Created p2pk_address_blocks table")
        
        db_manager.execute_command(scan_progress_sql)
        logger.info("Created scan_progress table")
        
        # Create indexes
        for index_sql in indexes_sql:
            db_manager.execute_command(index_sql)
        logger.info("Created database indexes")
        
        # Initialize scan progress for P2PK scanner
        init_progress_sql = """
        INSERT INTO scan_progress (scanner_name, last_scanned_block, total_blocks_scanned)
        VALUES ('p2pk_scanner', 0, 0)
        ON CONFLICT (scanner_name) DO NOTHING;
        """
        db_manager.execute_command(init_progress_sql)
        logger.info("Initialized scan progress tracking")
        
        logger.info("Database setup completed successfully!")
        
        # Show table counts
        tables = ['p2pk_addresses', 'p2pk_transactions', 'p2pk_address_blocks', 'scan_progress']
        for table in tables:
            count = db_manager.get_table_count(table)
            logger.info(f"Table {table}: {count} rows")
            
    except Exception as e:
        logger.error(f"Database setup failed: {e}")
        sys.exit(1)


def drop_tables():
    """Drop all P2PK scanner tables (for reset)."""
    try:
        logger.info("Dropping P2PK scanner tables...")
        
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
        sys.exit(1)


def main():
    """Main function to handle command line arguments."""
    if len(sys.argv) > 1 and sys.argv[1] == '--reset':
        logger.info("Resetting database...")
        drop_tables()
        create_tables()
    else:
        create_tables()


if __name__ == "__main__":
    main() 