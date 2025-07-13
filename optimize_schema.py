#!/usr/bin/env python3
"""
Database Schema Optimizer for Bitcoin P2PK Scanner
Optimizes table structure and indexes for maximum write performance.
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


def create_optimized_schema():
    """Create optimized schema with better performance characteristics."""
    
    # Drop existing tables if they exist
    drop_tables_sql = """
    DROP TABLE IF EXISTS p2pk_transactions CASCADE;
    DROP TABLE IF EXISTS p2pk_address_blocks CASCADE;
    DROP TABLE IF EXISTS p2pk_addresses CASCADE;
    DROP TABLE IF EXISTS scan_progress CASCADE;
    """
    
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
        
        # Drop existing tables
        db_manager.execute_command(drop_tables_sql)
        logger.info("Dropped existing tables")
        
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


def main():
    """Main optimization function."""
    try:
        logger.info("🚀 Starting database schema optimization...")
        
        # 1. Create optimized schema
        create_optimized_schema()
        
        # 2. Create high-performance indexes
        create_high_performance_indexes()
        
        # 3. Create prepared statements
        create_prepared_statements()
        
        # 4. Apply table optimizations
        optimize_table_settings()
        
        logger.info("✅ Database schema optimization completed!")
        logger.info("🔄 The scanner should now have much better write performance!")
        
    except Exception as e:
        logger.error(f"Database optimization failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main() 