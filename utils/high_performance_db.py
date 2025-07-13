#!/usr/bin/env python3
"""
High-Performance Database Manager for Bitcoin P2PK Scanner
Uses COPY commands, prepared statements, and batch operations for maximum throughput.
"""

import psycopg2
import psycopg2.extras
import logging
from typing import List, Dict, Any, Optional, Tuple
from io import StringIO
import time
from contextlib import contextmanager

from utils.config import config

logger = logging.getLogger(__name__)


class HighPerformanceDBManager:
    """High-performance database manager using COPY and prepared statements."""
    
    def __init__(self):
        self.connection = None
        self.prepared_statements_created = False
        self.batch_size = 10000  # Large batch size for maximum throughput
        
    def connect(self):
        """Connect to the database."""
        try:
            self.connection = psycopg2.connect(
                host=config.DB_HOST,
                port=config.DB_PORT,
                database=config.DB_NAME,
                user=config.DB_USER,
                password=config.DB_PASSWORD,
                # High-performance connection settings
                application_name='hydra_scanner_hp',
                # Disable autocommit for batch operations
                autocommit=False,
                # Connection pooling settings
                keepalives_idle=30,
                keepalives_interval=10,
                keepalives_count=5
            )
            logger.info("Connected to database with high-performance settings")
            
            # Set session-level optimizations
            with self.connection.cursor() as cursor:
                cursor.execute("SET synchronous_commit = 'off';")
                cursor.execute("SET work_mem = '4GB';")
                cursor.execute("SET maintenance_work_mem = '8GB';")
                cursor.execute("SET random_page_cost = 1.1;")
                cursor.execute("SET effective_io_concurrency = 200;")
            
            self.connection.commit()
            logger.info("Applied session-level performance optimizations")
            
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise
    
    def disconnect(self):
        """Disconnect from the database."""
        if self.connection:
            self.connection.close()
            logger.info("Disconnected from database")
    
    def create_prepared_statements(self):
        """Create prepared statements for maximum performance."""
        if self.prepared_statements_created or not self.connection:
            return
            
        try:
            with self.connection.cursor() as cursor:
                # Prepared statement for address insertion with conflict handling
                cursor.execute("""
                    PREPARE insert_address (VARCHAR, VARCHAR, INTEGER, VARCHAR, INTEGER) AS
                    INSERT INTO p2pk_addresses (address, public_key_hex, first_seen_block, first_seen_txid, last_seen_block)
                    VALUES ($1, $2, $3, $4, $5)
                    ON CONFLICT (address) DO UPDATE SET 
                        last_seen_block = EXCLUDED.last_seen_block,
                        updated_at = CURRENT_TIMESTAMP
                    RETURNING id;
                """)
                
                # Prepared statement for transaction insertion
                cursor.execute("""
                    PREPARE insert_transaction (VARCHAR, INTEGER, TIMESTAMP, BIGINT, BOOLEAN, BIGINT) AS
                    INSERT INTO p2pk_transactions (txid, block_height, block_time, address_id, is_input, amount_satoshi)
                    VALUES ($1, $2, $3, $4, $5, $6);
                """)
                
                # Prepared statement for block record insertion
                cursor.execute("""
                    PREPARE insert_block_record (BIGINT, INTEGER, BOOLEAN, BIGINT, VARCHAR) AS
                    INSERT INTO p2pk_address_blocks (address_id, block_height, is_input, amount_satoshi, txid)
                    VALUES ($1, $2, $3, $4, $5);
                """)
                
                # Prepared statement for address lookup
                cursor.execute("""
                    PREPARE get_address_id (VARCHAR) AS
                    SELECT id FROM p2pk_addresses WHERE address = $1;
                """)
                
                # Prepared statement for batch address insertion using COPY
                cursor.execute("""
                    PREPARE batch_insert_addresses (TEXT) AS
                    COPY p2pk_addresses (address, public_key_hex, first_seen_block, first_seen_txid, last_seen_block) 
                    FROM STDIN WITH (FORMAT CSV, DELIMITER ',');
                """)
                
                # Prepared statement for batch transaction insertion using COPY
                cursor.execute("""
                    PREPARE batch_insert_transactions (TEXT) AS
                    COPY p2pk_transactions (txid, block_height, block_time, address_id, is_input, amount_satoshi)
                    FROM STDIN WITH (FORMAT CSV, DELIMITER ',');
                """)
                
                # Prepared statement for batch block record insertion using COPY
                cursor.execute("""
                    PREPARE batch_insert_block_records (TEXT) AS
                    COPY p2pk_address_blocks (address_id, block_height, is_input, amount_satoshi, txid)
                    FROM STDIN WITH (FORMAT CSV, DELIMITER ',');
                """)
            
            self.connection.commit()
            self.prepared_statements_created = True
            logger.info("Created all prepared statements for high-performance operations")
            
        except Exception as e:
            logger.error(f"Failed to create prepared statements: {e}")
            raise
    
    def get_or_create_address_id(self, address: str, public_key_hex: str, 
                                first_seen_block: int, first_seen_txid: str, 
                                last_seen_block: int) -> Optional[int]:
        """Get or create address ID using prepared statement."""
        if not self.connection:
            logger.error("No database connection")
            return None
            
        try:
            with self.connection.cursor() as cursor:
                # Try to get existing address ID
                cursor.execute("EXECUTE get_address_id (%s);", (address,))
                result = cursor.fetchone()
                
                if result:
                    address_id = result[0]
                    # Update last_seen_block if needed
                    if last_seen_block > first_seen_block:
                        cursor.execute("""
                            UPDATE p2pk_addresses 
                            SET last_seen_block = %s, updated_at = CURRENT_TIMESTAMP 
                            WHERE id = %s
                        """, (last_seen_block, address_id))
                    return address_id
                
                # Insert new address
                cursor.execute("EXECUTE insert_address (%s, %s, %s, %s, %s);", 
                             (address, public_key_hex, first_seen_block, first_seen_txid, last_seen_block))
                result = cursor.fetchone()
                
                if result:
                    return result[0]
                else:
                    logger.error(f"Failed to insert address {address}")
                    return None
                    
        except Exception as e:
            logger.error(f"Error in get_or_create_address_id for {address}: {e}")
            return None
    
    def batch_insert_addresses_copy(self, addresses: List[Dict[str, Any]]) -> Dict[str, int]:
        """Insert addresses using COPY command for maximum performance."""
        if not addresses or not self.connection:
            return {}
        
        try:
            # Prepare CSV data for COPY
            csv_data = StringIO()
            for addr in addresses:
                csv_data.write(f"{addr['address']},{addr['public_key_hex']},{addr['first_seen_block']},"
                             f"{addr['first_seen_txid']},{addr['last_seen_block']}\n")
            csv_data.seek(0)
            
            # Use COPY for ultra-fast insertion
            with self.connection.cursor() as cursor:
                cursor.copy_expert("""
                    COPY p2pk_addresses (address, public_key_hex, first_seen_block, first_seen_txid, last_seen_block) 
                    FROM STDIN WITH (FORMAT CSV, DELIMITER ',')
                """, csv_data)
            
            # Get the inserted IDs
            address_id_map = {}
            with self.connection.cursor() as cursor:
                for addr in addresses:
                    cursor.execute("SELECT id FROM p2pk_addresses WHERE address = %s", (addr['address'],))
                    result = cursor.fetchone()
                    if result:
                        address_id_map[addr['address']] = result[0]
            
            logger.info(f"Batch inserted {len(addresses)} addresses using COPY")
            return address_id_map
            
        except Exception as e:
            logger.error(f"Failed to batch insert addresses: {e}")
            # Fallback to individual inserts
            return self._fallback_address_inserts(addresses)
    
    def _fallback_address_inserts(self, addresses: List[Dict[str, Any]]) -> Dict[str, int]:
        """Fallback to individual address inserts if COPY fails."""
        address_id_map = {}
        
        for addr in addresses:
            address_id = self.get_or_create_address_id(
                addr['address'], addr['public_key_hex'], 
                addr['first_seen_block'], addr['first_seen_txid'], 
                addr['last_seen_block']
            )
            if address_id:
                address_id_map[addr['address']] = address_id
        
        return address_id_map
    
    def batch_insert_transactions_copy(self, transactions: List[Dict[str, Any]]) -> bool:
        """Insert transactions using COPY command for maximum performance."""
        if not transactions or not self.connection:
            return True
        
        try:
            # Prepare CSV data for COPY
            csv_data = StringIO()
            for tx in transactions:
                csv_data.write(f"{tx['txid']},{tx['block_height']},{tx['block_time']},"
                             f"{tx['address_id']},{tx['is_input']},{tx['amount_satoshi']}\n")
            csv_data.seek(0)
            
            # Use COPY for ultra-fast insertion
            with self.connection.cursor() as cursor:
                cursor.copy_expert("""
                    COPY p2pk_transactions (txid, block_height, block_time, address_id, is_input, amount_satoshi)
                    FROM STDIN WITH (FORMAT CSV, DELIMITER ',')
                """, csv_data)
            
            logger.info(f"Batch inserted {len(transactions)} transactions using COPY")
            return True
            
        except Exception as e:
            logger.error(f"Failed to batch insert transactions: {e}")
            # Fallback to individual inserts
            return self._fallback_transaction_inserts(transactions)
    
    def _fallback_transaction_inserts(self, transactions: List[Dict[str, Any]]) -> bool:
        """Fallback to individual transaction inserts if COPY fails."""
        try:
            with self.connection.cursor() as cursor:
                for tx in transactions:
                    cursor.execute("EXECUTE insert_transaction (%s, %s, %s, %s, %s, %s);",
                                 (tx['txid'], tx['block_height'], tx['block_time'], 
                                  tx['address_id'], tx['is_input'], tx['amount_satoshi']))
            return True
        except Exception as e:
            logger.error(f"Failed to fallback insert transactions: {e}")
            return False
    
    def batch_insert_block_records_copy(self, block_records: List[Dict[str, Any]]) -> bool:
        """Insert block records using COPY command for maximum performance."""
        if not block_records:
            return True
        
        try:
            # Prepare CSV data for COPY
            csv_data = StringIO()
            for record in block_records:
                csv_data.write(f"{record['address_id']},{record['block_height']},"
                             f"{record['is_input']},{record['amount_satoshi']},{record['txid']}\n")
            csv_data.seek(0)
            
            # Use COPY for ultra-fast insertion
            with self.connection.cursor() as cursor:
                cursor.copy_expert("""
                    COPY p2pk_address_blocks (address_id, block_height, is_input, amount_satoshi, txid)
                    FROM STDIN WITH (FORMAT CSV, DELIMITER ',')
                """, csv_data)
            
            logger.info(f"Batch inserted {len(block_records)} block records using COPY")
            return True
            
        except Exception as e:
            logger.error(f"Failed to batch insert block records: {e}")
            # Fallback to individual inserts
            return self._fallback_block_record_inserts(block_records)
    
    def _fallback_block_record_inserts(self, block_records: List[Dict[str, Any]]) -> bool:
        """Fallback to individual block record inserts if COPY fails."""
        try:
            with self.connection.cursor() as cursor:
                for record in block_records:
                    cursor.execute("EXECUTE insert_block_record (%s, %s, %s, %s, %s);",
                                 (record['address_id'], record['block_height'], 
                                  record['is_input'], record['amount_satoshi'], record['txid']))
            return True
        except Exception as e:
            logger.error(f"Failed to fallback insert block records: {e}")
            return False
    
    def update_scan_progress(self, scanner_name: str, last_block: int, total_blocks: int):
        """Update scan progress using prepared statement."""
        try:
            with self.connection.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO scan_progress (scanner_name, last_scanned_block, total_blocks_scanned, last_updated)
                    VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
                    ON CONFLICT (scanner_name) DO UPDATE SET
                        last_scanned_block = EXCLUDED.last_scanned_block,
                        total_blocks_scanned = EXCLUDED.total_blocks_scanned,
                        last_updated = CURRENT_TIMESTAMP;
                """, (scanner_name, last_block, total_blocks))
            
            self.connection.commit()
            
        except Exception as e:
            logger.error(f"Failed to update scan progress: {e}")
            self.connection.rollback()
    
    def commit(self):
        """Commit the current transaction."""
        if self.connection:
            self.connection.commit()
    
    def rollback(self):
        """Rollback the current transaction."""
        if self.connection:
            self.connection.rollback()
    
    @contextmanager
    def transaction(self):
        """Context manager for database transactions."""
        try:
            yield self
            self.commit()
        except Exception:
            self.rollback()
            raise
    
    def get_table_count(self, table_name: str) -> int:
        """Get the row count of a table."""
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
                result = cursor.fetchone()
                return result[0] if result else 0
        except Exception as e:
            logger.error(f"Failed to get count for table {table_name}: {e}")
            return 0


# Global instance
hp_db_manager = HighPerformanceDBManager() 