#!/usr/bin/env python3
"""
High-Performance Database Optimizer for Bitcoin P2PK Scanner
Implements multiple optimization techniques to maximize write throughput.
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


def optimize_postgresql_settings():
    """Optimize PostgreSQL settings for maximum write performance."""
    
    optimizations = [
        # Disable synchronous commits for maximum speed (data can be lost on crash)
        "ALTER SYSTEM SET synchronous_commit = 'off';",
        
        # Increase WAL buffers for better write performance
        "ALTER SYSTEM SET wal_buffers = '256MB';",
        
        # Optimize checkpoint settings for write-heavy workloads
        "ALTER SYSTEM SET checkpoint_completion_target = 0.9;",
        "ALTER SYSTEM SET checkpoint_timeout = '30min';",
        "ALTER SYSTEM SET max_wal_size = '8GB';",
        "ALTER SYSTEM SET min_wal_size = '2GB';",
        
        # Optimize for bulk inserts
        "ALTER SYSTEM SET wal_writer_delay = '200ms';",
        "ALTER SYSTEM SET commit_delay = 1000;",
        "ALTER SYSTEM SET commit_siblings = 5;",
        
        # Increase work memory for better performance
        "ALTER SYSTEM SET work_mem = '4GB';",
        "ALTER SYSTEM SET maintenance_work_mem = '8GB';",
        
        # Optimize for concurrent connections
        "ALTER SYSTEM SET max_connections = 200;",
        "ALTER SYSTEM SET shared_preload_libraries = 'pg_stat_statements';",
        
        # Disable expensive features during bulk load
        "ALTER SYSTEM SET autovacuum = 'off';",
        "ALTER SYSTEM SET fsync = 'off';",  # DANGEROUS - only for bulk loading!
        "ALTER SYSTEM SET full_page_writes = 'off';",  # DANGEROUS - only for bulk loading!
        
        # Optimize for high-throughput inserts
        "ALTER SYSTEM SET random_page_cost = 1.1;",
        "ALTER SYSTEM SET effective_io_concurrency = 200;",
        
        # Increase connection limits
        "ALTER SYSTEM SET max_worker_processes = 32;",
        "ALTER SYSTEM SET max_parallel_workers = 32;",
        "ALTER SYSTEM SET max_parallel_workers_per_gather = 16;",
    ]
    
    try:
        logger.info("Applying PostgreSQL performance optimizations...")
        
        for optimization in optimizations:
            db_manager.execute_command(optimization)
            logger.info(f"Applied: {optimization.strip()}")
        
        logger.info("PostgreSQL optimizations applied successfully!")
        logger.warning("⚠️  WARNING: fsync=off and full_page_writes=off are DANGEROUS for production!")
        logger.warning("⚠️  These settings should only be used during bulk data loading!")
        
    except Exception as e:
        logger.error(f"Failed to apply PostgreSQL optimizations: {e}")
        raise


def create_optimized_schema():
    """Create optimized schema with better performance characteristics."""
    
    # Drop existing tables if they exist
    drop_tables_sql = """
    DROP TABLE IF EXISTS p2pk_transactions CASCADE;
    DROP TABLE IF EXISTS p2pk_address_blocks CASCADE;
    DROP TABLE IF EXISTS p2pk_addresses CASCADE;
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
        # Only essential indexes for the scanner
        "CREATE UNIQUE INDEX CONCURRENTLY idx_p2pk_addresses_address ON p2pk_addresses(address);",
        "CREATE INDEX CONCURRENTLY idx_p2pk_addresses_public_key ON p2pk_addresses(public_key_hex);",
        "CREATE INDEX CONCURRENTLY idx_p2pk_transactions_address_id ON p2pk_transactions(address_id);",
        "CREATE INDEX CONCURRENTLY idx_p2pk_transactions_block_height ON p2pk_transactions(block_height);",
        "CREATE INDEX CONCURRENTLY idx_p2pk_address_blocks_address_id ON p2pk_address_blocks(address_id);",
        "CREATE INDEX CONCURRENTLY idx_p2pk_address_blocks_block_height ON p2pk_address_blocks(block_height);",
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
        
        # Prepared statement for batch address insertion
        """
        PREPARE batch_insert_addresses (VARCHAR[], VARCHAR[], INTEGER[], VARCHAR[], INTEGER[]) AS
        INSERT INTO p2pk_addresses (address, public_key_hex, first_seen_block, first_seen_txid, last_seen_block)
        SELECT unnest($1), unnest($2), unnest($3), unnest($4), unnest($5)
        ON CONFLICT (address) DO UPDATE SET 
            last_seen_block = EXCLUDED.last_seen_block,
            updated_at = CURRENT_TIMESTAMP;
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


def create_copy_functions():
    """Create COPY-based functions for ultra-fast bulk inserts."""
    
    copy_functions = [
        # Function for bulk address insertion using COPY
        """
        CREATE OR REPLACE FUNCTION bulk_insert_addresses(
            address_data TEXT
        ) RETURNS VOID AS $$
        BEGIN
            -- Use COPY for ultra-fast bulk insertion
            EXECUTE format('COPY p2pk_addresses (address, public_key_hex, first_seen_block, first_seen_txid, last_seen_block) FROM STDIN');
            -- The actual COPY data would be passed from Python
        END;
        $$ LANGUAGE plpgsql;
        """,
        
        # Function for bulk transaction insertion
        """
        CREATE OR REPLACE FUNCTION bulk_insert_transactions(
            transaction_data TEXT
        ) RETURNS VOID AS $$
        BEGIN
            EXECUTE format('COPY p2pk_transactions (txid, block_height, block_time, address_id, is_input, amount_satoshi) FROM STDIN');
        END;
        $$ LANGUAGE plpgsql;
        """,
    ]
    
    try:
        logger.info("Creating COPY-based functions for ultra-fast inserts...")
        
        for func in copy_functions:
            db_manager.execute_command(func)
        
        logger.info("COPY functions created successfully!")
        
    except Exception as e:
        logger.error(f"Failed to create COPY functions: {e}")
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


def create_partitioning():
    """Create table partitioning for better performance on large datasets."""
    
    partitioning_sql = [
        # Create partitioned version of transactions table by block height
        """
        CREATE TABLE p2pk_transactions_partitioned (
            id BIGSERIAL,
            txid VARCHAR(64) NOT NULL,
            block_height INTEGER NOT NULL,
            block_time TIMESTAMP NOT NULL,
            address_id BIGINT NOT NULL,
            is_input BOOLEAN NOT NULL,
            amount_satoshi BIGINT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        ) PARTITION BY RANGE (block_height);
        """,
        
        # Create partitions for different block ranges
        "CREATE TABLE p2pk_transactions_0_100k PARTITION OF p2pk_transactions_partitioned FOR VALUES FROM (0) TO (100000);",
        "CREATE TABLE p2pk_transactions_100k_200k PARTITION OF p2pk_transactions_partitioned FOR VALUES FROM (100000) TO (200000);",
        "CREATE TABLE p2pk_transactions_200k_300k PARTITION OF p2pk_transactions_partitioned FOR VALUES FROM (200000) TO (300000);",
        "CREATE TABLE p2pk_transactions_300k_400k PARTITION OF p2pk_transactions_partitioned FOR VALUES FROM (300000) TO (400000);",
        "CREATE TABLE p2pk_transactions_400k_500k PARTITION OF p2pk_transactions_partitioned FOR VALUES FROM (400000) TO (500000);",
        "CREATE TABLE p2pk_transactions_500k_600k PARTITION OF p2pk_transactions_partitioned FOR VALUES FROM (500000) TO (600000);",
        "CREATE TABLE p2pk_transactions_600k_700k PARTITION OF p2pk_transactions_partitioned FOR VALUES FROM (600000) TO (700000);",
        "CREATE TABLE p2pk_transactions_700k_800k PARTITION OF p2pk_transactions_partitioned FOR VALUES FROM (700000) TO (800000);",
        "CREATE TABLE p2pk_transactions_800k_900k PARTITION OF p2pk_transactions_partitioned FOR VALUES FROM (800000) TO (900000);",
        "CREATE TABLE p2pk_transactions_900k_plus PARTITION OF p2pk_transactions_partitioned FOR VALUES FROM (900000) TO (MAXVALUE);",
    ]
    
    try:
        logger.info("Creating table partitioning for better performance...")
        
        for sql in partitioning_sql:
            db_manager.execute_command(sql)
        
        logger.info("Table partitioning created successfully!")
        
    except Exception as e:
        logger.error(f"Failed to create partitioning: {e}")
        # Partitioning is optional, don't fail the whole process
        logger.info("Continuing without partitioning...")


def main():
    """Main optimization function."""
    try:
        logger.info("🚀 Starting high-performance database optimization...")
        
        # 1. Optimize PostgreSQL settings
        optimize_postgresql_settings()
        
        # 2. Create optimized schema
        create_optimized_schema()
        
        # 3. Create high-performance indexes
        create_high_performance_indexes()
        
        # 4. Create prepared statements
        create_prepared_statements()
        
        # 5. Create COPY functions
        create_copy_functions()
        
        # 6. Apply table optimizations
        optimize_table_settings()
        
        # 7. Create partitioning (optional)
        try:
            create_partitioning()
        except Exception as e:
            logger.warning(f"Partitioning failed (optional): {e}")
        
        logger.info("✅ High-performance database optimization completed!")
        logger.info("🔄 Remember to restart PostgreSQL for settings to take effect!")
        logger.warning("⚠️  IMPORTANT: Re-enable fsync and full_page_writes after bulk loading!")
        
    except Exception as e:
        logger.error(f"Database optimization failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main() 