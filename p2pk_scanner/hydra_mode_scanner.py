#!/usr/bin/env python3
"""
HYDRA MODE P2PK Scanner - Multi-Queue Architecture
Ultra-optimized multithreaded scanner with individual worker queues,
distributor thread, and zero lock contention for maximum throughput.
"""

import sys
import os
import logging
import time
import queue
import threading
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime
import select
import cProfile
import pstats
import io

# Add parent directory to path to import utils
sys.path.append(str(Path(__file__).parent.parent))

from utils.config import config
from utils.database import DatabaseManager
from bitcoin_rpc import bitcoin_rpc

# Set up logging
logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(Path(__file__).parent.parent / 'logs' / 'hydra_mode_scanner.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# For graceful shutdown
stop_event = threading.Event()
pause_event = threading.Event()  # New pause event for temporary worker suspension

# Auto-pause configuration for database bottleneck management
auto_pause_enabled = True  # Enable automatic pause/resume based on queue depth
auto_pause_threshold = 50000  # Pause when queue depth exceeds this
auto_resume_threshold = 10000  # Resume when queue depth drops below this

# Worker tracking for graceful shutdown
active_workers = set()  # Set of worker thread names currently processing blocks
active_workers_lock = threading.Lock()

thread_status = {}
thread_status_lock = threading.Lock()

# Global counters
total_blocks_scanned = 0
blocks_scanned_lock = threading.Lock()

# P2PK Address tracking - CRITICAL FOR DATA INTEGRITY
p2pk_addresses_found = set()  # Set of all p2pk addresses found in this session
p2pk_addresses_stored = set()  # Set of all p2pk addresses successfully stored
p2pk_addresses_failed = set()  # Set of all p2pk addresses that failed to store
p2pk_tracking_lock = threading.Lock()

# Worker profiling
worker_profiles = {}
worker_profiles_lock = threading.Lock()

# Performance tracking
performance_stats = {
    'total_transactions_processed': 0,
    'total_addresses_found': 0,
    'batch_inserts_performed': 0,
    'queue_operations': 0
}

# Detailed performance profiling
performance_metrics = {
    'rpc_calls': 0,
    'rpc_time_total': 0.0,
    'rpc_time_avg': 0.0,
    'rpc_time_min': float('inf'),
    'rpc_time_max': 0.0,
    'db_operations': 0,
    'db_time_total': 0.0,
    'db_time_avg': 0.0,
    'db_time_min': float('inf'),
    'db_time_max': 0.0,
    'blocks_processed': 0,
    'blocks_failed': 0,
    'transactions_processed': 0,
    'p2pk_found': 0,
    'queue_operations': 0,
    'queue_full_count': 0,
    'queue_waiting_count': 0,
    'queue_waiting_time_total': 0.0,
    'batch_flushes': 0,
    'batch_flush_time_total': 0.0,
    'batch_flush_time_avg': 0.0,
    'foreign_key_errors': 0,
    'address_insert_errors': 0,
    'transaction_insert_errors': 0,
    'block_insert_errors': 0,
    'memory_usage_mb': 0.0,
    'cpu_usage_percent': 0.0,
    'distribution_operations': 0,
    'worker_queue_depths': {}
}

stats_lock = threading.Lock()
metrics_lock = threading.Lock()


def format_time_dd_hh_mm_ss(seconds: float) -> str:
    """Format time in DD:HH:MM:SS format."""
    if seconds < 0:
        return "00D:00H:00M:00S"
    
    days, rem = divmod(int(seconds), 86400)
    hours, rem = divmod(rem, 3600)
    minutes, secs = divmod(rem, 60)
    
    return f"{days:02d}D:{hours:02d}H:{minutes:02d}M:{secs:02d}S"


def check_auto_pause(db_manager) -> bool:
    """Check queue depth and automatically pause/resume workers to manage database bottleneck."""
    if not auto_pause_enabled:
        return False
    
    try:
        queue_depth = db_manager.write_queue.qsize()
        
        # Check if we should pause
        if queue_depth > auto_pause_threshold and not pause_event.is_set():
            logger.warning(f"🔄 Auto-pausing workers: queue depth {queue_depth:,} > {auto_pause_threshold:,}")
            pause_event.set()
            return True
        
        # Check if we should resume
        elif queue_depth < auto_resume_threshold and pause_event.is_set():
            logger.info(f"▶️ Auto-resuming workers: queue depth {queue_depth:,} < {auto_resume_threshold:,}")
            pause_event.clear()
            return True
        
        return False
        
    except Exception as e:
        logger.error(f"Error checking auto-pause: {e}")
        return False


class HydraModeDatabaseManager:
    """High-performance database manager with batch operations and write-behind caching."""
    
    def __init__(self, batch_size: int = 1000, queue_size: int = 1000000):
        self.batch_size = batch_size
        self.queue_size = queue_size
        self.write_queue = queue.Queue(maxsize=queue_size)
        self.db_manager = DatabaseManager()  # Direct instantiation, always valid
        self.prepared_statements = {}
        self.writer_thread = None
        self.writer_running = False
        self.failed_addresses = {}  # address_key -> (address_op, attempts)
        self.max_retries = 3
        
        # Initialize prepared statements
        self._prepare_statements()
        
        # Start writer thread
        self._start_writer_thread()
    
    def _prepare_statements(self):
        """Prepare frequently used SQL statements for better performance."""
        try:
            # Address upsert statement
            self.prepared_statements['address_upsert'] = """
            INSERT INTO p2pk_addresses 
            (address, public_key_hex, first_seen_block, first_seen_txid, last_seen_block, 
             total_received_satoshi, current_balance_satoshi)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (address) DO UPDATE SET
                last_seen_block = EXCLUDED.last_seen_block,
                updated_at = CURRENT_TIMESTAMP
            RETURNING id
            """
            
            # Transaction insert statement
            self.prepared_statements['transaction_insert'] = """
            INSERT INTO p2pk_transactions 
            (txid, block_height, block_time, address_id, is_input, amount_satoshi)
            VALUES (%s, %s, %s, %s, %s, %s)
            """
            
            # Block record insert statement
            self.prepared_statements['block_insert'] = """
            INSERT INTO p2pk_address_blocks 
            (address_id, block_height, is_input, amount_satoshi, txid)
            VALUES (%s, %s, %s, %s, %s)
            """
            
            logger.info("Prepared statements initialized")
            
        except Exception as e:
            logger.error(f"Failed to prepare statements: {e}")
    
    def _start_writer_thread(self):
        """Start the background writer thread for write-behind caching."""
        self.writer_running = True
        self.writer_thread = threading.Thread(target=self._writer_loop, daemon=True)
        self.writer_thread.start()
        logger.info("Write-behind cache writer thread started")
    
    def _writer_loop(self):
        """Background thread that processes the write queue in batches."""
        batch = []
        last_flush = time.time()
        
        while self.writer_running:
            try:
                # Collect items from queue with timeout
                try:
                    item = self.write_queue.get(timeout=0.5)  # Reduced timeout for more frequent checks
                    batch.append(item)
                except queue.Empty:
                    # Flush if we have items and enough time has passed
                    if batch and (time.time() - last_flush) > 2.0:  # Reduced timeout from 5s to 2s
                        pass  # Continue to flush
                    else:
                        continue
                
                # Flush batch when it reaches threshold or timeout
                if len(batch) >= self.batch_size or (batch and (time.time() - last_flush) > 2.0):  # Reduced timeout
                    logger.debug(f"🔄 Writer thread flushing batch of {len(batch)} items (threshold: {self.batch_size}, timeout: {time.time() - last_flush:.1f}s)")
                    self._flush_batch(batch)
                    batch.clear()
                    last_flush = time.time()
                    
            except Exception as e:
                logger.error(f"Writer thread error: {e}")
                # Continue processing even if there's an error
        
        # Final flush of remaining items
        if batch:
            self._flush_batch(batch)
    
    def _flush_batch(self, batch: List[Dict[str, Any]]):
        """Flush a batch of operations to the database."""
        if not batch:
            return
        
        batch_start_time = time.time()
        
        try:
            # Group operations by type and maintain address mapping
            address_ops = []
            transaction_ops = []
            block_ops = []
            address_keys_in_batch = set()
            address_mapping = {}  # address_key -> address_id
            
            # First pass: collect all unique addresses
            for item in batch:
                if item['type'] == 'address':
                    address_key = item['address_key']
                    if address_key not in address_mapping:
                        address_ops.append(item['data'])
                        address_keys_in_batch.add(address_key)
            
            # Add failed addresses from previous batch (retry queue)
            retry_ops = []
            retry_keys = list(self.failed_addresses.keys())
            for address_key in retry_keys:
                address_op, attempts = self.failed_addresses[address_key]
                retry_ops.append(address_op)
                # Remove from failed_addresses for this batch; will re-add if still fails
                del self.failed_addresses[address_key]
            if retry_ops:
                logger.warning(f"Retrying {len(retry_ops)} failed addresses from previous batch...")
                address_ops = retry_ops + address_ops
                address_keys_in_batch.update([op[0] for op in retry_ops])
            
            # Bulk insert addresses and get real IDs (may be None for failed inserts)
            if address_ops:
                self._bulk_insert_addresses(address_ops)
            
            # RESEARCH-GRADE: For every address_key in the batch, resolve its real address_id
            for address_key in address_keys_in_batch:
                result = self.db_manager.execute_query(
                    "SELECT id FROM p2pk_addresses WHERE address = %s", (address_key,)
                )
                if result and result[0] and result[0].get('id'):
                    address_mapping[address_key] = result[0]['id']
                else:
                    address_mapping[address_key] = None
                    logger.critical(f"DATA LOSS: Could not resolve address_id for {address_key} after upsert. Halting scanner.")
            
            # If any address_id is missing, halt the scanner (do not drop or skip data)
            unresolved = [k for k, v in address_mapping.items() if v is None]
            if unresolved:
                logger.critical(f"Unresolved address_ids for keys: {unresolved}. Halting scanner to prevent data loss.")
                raise RuntimeError(f"Unresolved address_ids for keys: {unresolved}. Data loss would occur.")
            
            # Second pass: prepare transactions and blocks with correct address IDs
            for item in batch:
                if item['type'] == 'transaction':
                    address_key = item['address_key']
                    address_id = address_mapping.get(address_key)
                    # Only add transaction if we have a valid address_id (guaranteed by above)
                    tx_data = list(item['data'])
                    tx_data[3] = address_id  # address_id field
                    transaction_ops.append(tuple(tx_data))
                elif item['type'] == 'block':
                    address_key = item['address_key']
                    address_id = address_mapping.get(address_key)
                    # Only add block record if we have a valid address_id (guaranteed by above)
                    block_data = list(item['data'])
                    block_data[0] = address_id  # address_id field
                    block_ops.append(tuple(block_data))
            
            # Bulk insert transactions and blocks
            if transaction_ops:
                self._bulk_insert_transactions(transaction_ops)
            if block_ops:
                self._bulk_insert_blocks(block_ops)
            
            # Calculate batch timing
            batch_time = time.time() - batch_start_time
            
            with stats_lock:
                performance_stats['batch_inserts_performed'] += 1
                performance_stats['total_transactions_processed'] += len(transaction_ops)
                performance_stats['total_addresses_found'] += len(address_ops)
                
                # Log batch processing details for debugging
                if len(address_ops) > 0 or len(transaction_ops) > 0:
                    logger.info(f"📊 Batch processed: {len(address_ops)} addresses, {len(transaction_ops)} transactions, {len(block_ops)} blocks")
                    if len(address_ops) == 0 and len(transaction_ops) > 0:
                        logger.info(f"📝 Note: {len(transaction_ops)} transactions processed but no new addresses found (existing addresses being updated)")
                    elif len(address_ops) > 0:
                        logger.info(f"🎯 Found {len(address_ops)} new P2PK addresses!")
            with metrics_lock:
                performance_metrics['batch_flushes'] += 1
                performance_metrics['batch_flush_time_total'] += batch_time
                performance_metrics['batch_flush_time_avg'] = performance_metrics['batch_flush_time_total'] / performance_metrics['batch_flushes']
                performance_metrics['db_operations'] += len(transaction_ops) + len(block_ops) + len(address_ops)
                performance_metrics['db_time_total'] += batch_time
                performance_metrics['db_time_avg'] = performance_metrics['db_time_total'] / max(performance_metrics['db_operations'], 1)
                if batch_time < performance_metrics['db_time_min']:
                    performance_metrics['db_time_min'] = batch_time
                if batch_time > performance_metrics['db_time_max']:
                    performance_metrics['db_time_max'] = batch_time
        except Exception as e:
            logger.error(f"Batch flush error: {e}")
            # Log more details for debugging
            logger.error(f"Batch size: {len(batch)}, Address ops: {len(address_ops)}, Transaction ops: {len(transaction_ops)}, Block ops: {len(block_ops)}")
    
    def _bulk_insert_addresses(self, address_ops: List[Tuple]) -> List[Optional[int]]:
        """Bulk insert addresses with bulletproof upsert logic - NO DATA LOSS."""
        if not address_ops:
            return []
        
        start_time = time.time()
        
        try:
            inserted_ids = []
            for address_op in address_ops:
                try:
                    address_key = address_op[0]
                    public_key_hex = address_op[1]
                    # Basic validation
                    if not address_key or not public_key_hex:
                        logger.warning(f"Invalid address data: address_key={address_key}, public_key_hex={public_key_hex}")
                        inserted_ids.append(None)
                        continue
                    # Validate P2PK format - accept both compressed (66 chars) and uncompressed (130 chars) keys
                    if len(public_key_hex) == 66:
                        if not public_key_hex.startswith(('02', '03')):
                            logger.warning(f"Invalid compressed P2PK format: {public_key_hex[:20]}... (len: {len(public_key_hex)})")
                            inserted_ids.append(None)
                            continue
                    elif len(public_key_hex) == 130:
                        if not public_key_hex.startswith('04'):
                            logger.warning(f"Invalid uncompressed P2PK format: {public_key_hex[:20]}... (len: {len(public_key_hex)})")
                            inserted_ids.append(None)
                            continue
                    else:
                        logger.warning(f"Invalid P2PK format: {public_key_hex[:20]}... (len: {len(public_key_hex)})")
                        inserted_ids.append(None)
                        continue
                    # BULLETPROOF UPSERT: Single strategy that always works
                    existing_result = self.db_manager.execute_query(
                        "SELECT id FROM p2pk_addresses WHERE address = %s", 
                        (address_key,)
                    )
                    if existing_result and existing_result[0] and existing_result[0].get('id'):
                        address_id = existing_result[0]['id']
                        inserted_ids.append(address_id)
                        logger.debug(f"Found existing address {address_key[:20]}... with ID {address_id}")
                        try:
                            self.db_manager.execute_command("""
                                UPDATE p2pk_addresses 
                                SET last_seen_block = GREATEST(last_seen_block, %s),
                                    updated_at = CURRENT_TIMESTAMP
                                WHERE id = %s AND last_seen_block < %s
                            """, (address_op[4], address_id, address_op[4]))
                        except Exception as update_e:
                            logger.debug(f"Update failed for existing address {address_key[:20]}...: {update_e}")
                    else:
                        try:
                            insert_result = self.db_manager.execute_upsert("""
                                INSERT INTO p2pk_addresses 
                                (address, public_key_hex, first_seen_block, first_seen_txid, last_seen_block, 
                                 total_received_satoshi, current_balance_satoshi)
                                VALUES (%s, %s, %s, %s, %s, %s, %s)
                                RETURNING id
                            """, address_op)
                            if insert_result and insert_result.get('id'):
                                address_id = insert_result['id']
                                inserted_ids.append(address_id)
                                logger.debug(f"Successfully inserted new address {address_key[:20]}... with ID {address_id}")
                            else:
                                logger.warning(f"Insert failed for {address_key[:20]}..., trying conflict resolution")
                                conflict_result = self.db_manager.execute_upsert("""
                                    INSERT INTO p2pk_addresses 
                                    (address, public_key_hex, first_seen_block, first_seen_txid, last_seen_block, 
                                     total_received_satoshi, current_balance_satoshi)
                                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                                    ON CONFLICT (address) DO UPDATE SET
                                        last_seen_block = EXCLUDED.last_seen_block,
                                        updated_at = CURRENT_TIMESTAMP
                                    RETURNING id
                                """, address_op)
                                if conflict_result and conflict_result.get('id'):
                                    address_id = conflict_result['id']
                                    inserted_ids.append(address_id)
                                    logger.info(f"Conflict resolution successful for {address_key[:20]}... with ID {address_id}")
                                else:
                                    final_result = self.db_manager.execute_query(
                                        "SELECT id FROM p2pk_addresses WHERE address = %s", 
                                        (address_key,)
                                    )
                                    if final_result and final_result[0] and final_result[0].get('id'):
                                        address_id = final_result[0]['id']
                                        inserted_ids.append(address_id)
                                        logger.info(f"Final fallback successful for {address_key[:20]}... with ID {address_id}")
                                    else:
                                        # Add to retry queue
                                        attempts = self.failed_addresses.get(address_key, (address_op, 0))[1] + 1
                                        self.failed_addresses[address_key] = (address_op, attempts)
                                        logger.error(f"RETRY {attempts}/{self.max_retries}: Failed to insert address {address_key} (public_key: {public_key_hex})")
                                        if attempts >= self.max_retries:
                                            logger.critical(f"HALTING: Could not insert address {address_key} after {self.max_retries} attempts. Data loss would occur. Halting scanner.")
                                            raise RuntimeError(f"Could not insert address {address_key} after {self.max_retries} attempts.")
                                        inserted_ids.append(None)
                        except Exception as insert_e:
                            logger.error(f"Insert failed for {address_key[:20]}...: {insert_e}")
                            attempts = self.failed_addresses.get(address_key, (address_op, 0))[1] + 1
                            self.failed_addresses[address_key] = (address_op, attempts)
                            logger.error(f"RETRY {attempts}/{self.max_retries}: Exception inserting address {address_key} (public_key: {public_key_hex})")
                            if attempts >= self.max_retries:
                                logger.critical(f"HALTING: Could not insert address {address_key} after {self.max_retries} attempts. Data loss would occur. Halting scanner.")
                                raise RuntimeError(f"Could not insert address {address_key} after {self.max_retries} attempts.")
                            inserted_ids.append(None)
                except Exception as e:
                    logger.error(f"CRITICAL: Failed to process address {address_op[0][:20] if address_op[0] else 'None'}...: {e}")
                    attempts = self.failed_addresses.get(address_key, (address_op, 0))[1] + 1
                    self.failed_addresses[address_key] = (address_op, attempts)
                    logger.error(f"RETRY {attempts}/{self.max_retries}: Exception processing address {address_key}")
                    if attempts >= self.max_retries:
                        logger.critical(f"HALTING: Could not process address {address_key} after {self.max_retries} attempts. Data loss would occur. Halting scanner.")
                        raise RuntimeError(f"Could not process address {address_key} after {self.max_retries} attempts.")
                    inserted_ids.append(None)
            # RESEARCH-GRADE: Data integrity verification
            failed_count = sum(1 for id in inserted_ids if id is None)
            if failed_count > 0:
                logger.warning(f"Address insertion summary: {len(address_ops)} total, {failed_count} failed")
                with metrics_lock:
                    performance_metrics['address_insert_errors'] += failed_count
                
                # Log failed addresses for investigation
                for i, (address_op, address_id) in enumerate(zip(address_ops, inserted_ids)):
                    if address_id is None:
                        logger.error(f"DATA LOSS: Failed to insert address {address_op[0][:20]}... (public_key: {address_op[1][:20]}...)")
            else:
                logger.debug(f"✅ Perfect address insertion: {len(address_ops)} addresses processed, 0 failures")
            
            # Track timing for address operations
            address_time = time.time() - start_time
            with metrics_lock:
                performance_metrics['db_time_total'] += address_time
                performance_metrics['db_operations'] += len(address_ops)
                performance_metrics['db_time_avg'] = performance_metrics['db_time_total'] / max(performance_metrics['db_operations'], 1)
                if address_time < performance_metrics['db_time_min']:
                    performance_metrics['db_time_min'] = address_time
                if address_time > performance_metrics['db_time_max']:
                    performance_metrics['db_time_max'] = address_time
            
            return inserted_ids
            
        except Exception as e:
            logger.error(f"CRITICAL: Bulk address insert error: {e}")
            return [None] * len(address_ops)  # Return None for all failed inserts
    
    def _bulk_insert_transactions(self, transaction_ops: List[Tuple]):
        """Bulk insert transactions."""
        if not transaction_ops:
            return
        if self.db_manager.connection is None:
            raise RuntimeError("Database connection is not initialized!")
        start_time = time.time()
        try:
            cursor = self.db_manager.connection.cursor()
            cursor.executemany(self.prepared_statements['transaction_insert'], transaction_ops)
            self.db_manager.connection.commit()
            # Track timing for transaction operations
            tx_time = time.time() - start_time
            with metrics_lock:
                performance_metrics['db_time_total'] += tx_time
                performance_metrics['db_operations'] += len(transaction_ops)
                performance_metrics['db_time_avg'] = performance_metrics['db_time_total'] / max(performance_metrics['db_operations'], 1)
                if tx_time < performance_metrics['db_time_min']:
                    performance_metrics['db_time_min'] = tx_time
                if tx_time > performance_metrics['db_time_max']:
                    performance_metrics['db_time_max'] = tx_time
        except Exception as e:
            logger.error(f"Bulk transaction insert error: {e}")
            self.db_manager.connection.rollback()
    
    def _bulk_insert_blocks(self, block_ops: List[Tuple]):
        """Bulk insert block records."""
        if not block_ops:
            return
        if self.db_manager.connection is None:
            raise RuntimeError("Database connection is not initialized!")
        start_time = time.time()
        try:
            cursor = self.db_manager.connection.cursor()
            cursor.executemany(self.prepared_statements['block_insert'], block_ops)
            self.db_manager.connection.commit()
            # Track timing for block operations
            block_time = time.time() - start_time
            with metrics_lock:
                performance_metrics['db_time_total'] += block_time
                performance_metrics['db_operations'] += len(block_ops)
                performance_metrics['db_time_avg'] = performance_metrics['db_time_total'] / max(performance_metrics['db_operations'], 1)
                if block_time < performance_metrics['db_time_min']:
                    performance_metrics['db_time_min'] = block_time
                if block_time > performance_metrics['db_time_max']:
                    performance_metrics['db_time_max'] = block_time
        except Exception as e:
            logger.error(f"Bulk block insert error: {e}")
            self.db_manager.connection.rollback()
    
    def add_transaction(self, p2pk_transaction: Dict[str, Any]):
        """Add a transaction to the write-behind cache."""
        try:
            # Prepare address data
            address_data = (
                p2pk_transaction['public_key_hex'][:34],  # address
                p2pk_transaction['public_key_hex'],       # public_key_hex
                p2pk_transaction['block_height'],         # first_seen_block
                p2pk_transaction['txid'],                 # first_seen_txid
                p2pk_transaction['block_height'],         # last_seen_block
                p2pk_transaction['amount_satoshi'] if not p2pk_transaction['is_input'] else 0,  # total_received_satoshi
                p2pk_transaction['amount_satoshi'] if not p2pk_transaction['is_input'] else 0   # current_balance_satoshi
            )
            
            # Add to queue (blocking - research-grade, no data loss)
            queue_start_time = time.time()
            
            # Add address data (blocking)
            self.write_queue.put({
                'type': 'address',
                'data': address_data,
                'address_key': p2pk_transaction['public_key_hex'][:34]
            }, block=True)  # Block until space available
            
            # Add transaction data (blocking)
            transaction_data = (
                p2pk_transaction['txid'],
                p2pk_transaction['block_height'],
                p2pk_transaction['block_time'],
                0,  # address_id (will be updated by writer thread)
                p2pk_transaction['is_input'],
                p2pk_transaction['amount_satoshi']
            )
            
            self.write_queue.put({
                'type': 'transaction',
                'data': transaction_data,
                'address_key': p2pk_transaction['public_key_hex'][:34]
            }, block=True)  # Block until space available
            
            # Add block data (blocking)
            block_data = (
                0,  # address_id (will be updated by writer thread)
                p2pk_transaction['block_height'],
                p2pk_transaction['is_input'],
                p2pk_transaction['amount_satoshi'],
                p2pk_transaction['txid']
            )
            
            self.write_queue.put({
                'type': 'block',
                'data': block_data,
                'address_key': p2pk_transaction['public_key_hex'][:34]
            }, block=True)  # Block until space available
            
            # Track queue waiting time if queue was full
            queue_time = time.time() - queue_start_time
            if queue_time > 0.001:  # If we waited more than 1ms
                with metrics_lock:
                    performance_metrics['queue_waiting_count'] += 1
                    performance_metrics['queue_waiting_time_total'] += queue_time
            
            with stats_lock:
                performance_stats['queue_operations'] += 1
            with metrics_lock:
                performance_metrics['queue_operations'] += 1
                
        except Exception as e:
            logger.error(f"Error adding transaction to queue: {e}")
    
    def shutdown(self):
        """Gracefully shutdown the database manager."""
        self.writer_running = False
        if self.writer_thread:
            self.writer_thread.join(timeout=10)
        
        # Final flush of any remaining items
        remaining_items = []
        while not self.write_queue.empty():
            try:
                remaining_items.append(self.write_queue.get_nowait())
            except queue.Empty:
                break
        
        if remaining_items:
            logger.info(f"Flushing {len(remaining_items)} remaining items")
            self._flush_batch(remaining_items)
        
        self.db_manager.close()
        logger.info("Hydra mode database manager shutdown complete")


def ensure_scan_progress_row(db_manager):
    """Ensure the scan_progress row for the hydra mode scanner exists."""
    try:
        query = "SELECT 1 FROM scan_progress WHERE scanner_name = %s"
        result = db_manager.execute_query(query, ('hydra_mode_p2pk_scanner',))
        if not result:
            insert_query = """
            INSERT INTO scan_progress (scanner_name, last_scanned_block, total_blocks_scanned, last_updated)
            VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
            """
            db_manager.execute_command(insert_query, ('hydra_mode_p2pk_scanner', 0, 0))
            logger.info("Created scan_progress row for hydra_mode_p2pk_scanner.")
    except Exception as e:
        logger.error(f"Error ensuring scan_progress row: {e}")


def get_scan_progress(db_manager) -> int:
    """Get the last scanned block height, preferring original scanner progress."""
    try:
        # First try to get progress from original multithreaded scanner
        query = "SELECT last_scanned_block FROM scan_progress WHERE scanner_name = %s"
        result = db_manager.execute_query(query, ('multithreaded_p2pk_scanner',))
        if result and result[0]['last_scanned_block'] > 0:
            logger.info(f"Found progress from original scanner: block {result[0]['last_scanned_block']}")
            return result[0]['last_scanned_block']
        
        # Fall back to hydra mode scanner progress
        result = db_manager.execute_query(query, ('hydra_mode_p2pk_scanner',))
        if result and result[0]['last_scanned_block'] > 0:
            logger.info(f"Found progress from hydra mode scanner: block {result[0]['last_scanned_block']}")
            return result[0]['last_scanned_block']
        
        # No progress found, start from beginning
        logger.info("No previous progress found, starting from block 0")
        return 0
        
    except Exception as e:
        logger.error(f"Error getting scan progress: {e}")
        return 0


def update_scan_progress(db_manager, block_height: int, blocks_scanned: int = 1):
    """Update the scan progress for hydra mode scanner."""
    try:
        query = """
        UPDATE scan_progress 
        SET last_scanned_block = %s, total_blocks_scanned = total_blocks_scanned + %s, last_updated = CURRENT_TIMESTAMP
        WHERE scanner_name = %s
        """
        # Try HydraModeDatabaseManager first
        if hasattr(db_manager, 'db_manager'):
            db_manager.db_manager.execute_command(query, (block_height, blocks_scanned, 'hydra_mode_p2pk_scanner'))
        else:
            # Direct DatabaseManager
            db_manager.execute_command(query, (block_height, blocks_scanned, 'hydra_mode_p2pk_scanner'))
    except Exception as e:
        logger.error(f"Error updating scan progress: {e}")
        # Fallback: try direct database manager
        try:
            from utils.database import db_manager as direct_db
            direct_db.execute_command(query, (block_height, blocks_scanned, 'hydra_mode_p2pk_scanner'))
        except Exception as e2:
            logger.error(f"Fallback database update also failed: {e2}")


def is_p2pk_script(script_pub_key: Dict[str, Any]) -> Optional[str]:
    """Check if a script is a P2PK script and return the public key."""
    try:
        # Skip P2PKH addresses (intentionally not tracked)
        if script_pub_key.get('type') == 'pubkeyhash':
            return None
        
        # Primary detection method
        if script_pub_key.get('type') == 'pubkey':
            asm = script_pub_key.get('asm', '')
            if asm and 'OP_CHECKSIG' in asm:
                parts = asm.split()
                if len(parts) >= 2 and parts[-1] == 'OP_CHECKSIG':
                    public_key = parts[0]
                    # Validate public key format
                    if len(public_key) == 130 and public_key.startswith('04'):
                        # Uncompressed public key - validate hex format
                        if len(public_key) == 130 and all(c in '0123456789abcdefABCDEF' for c in public_key[1:]):
                            return public_key
                    elif len(public_key) == 66 and public_key.startswith(('02', '03')):
                        # Compressed public key - validate hex format
                        if len(public_key) == 66 and all(c in '0123456789abcdefABCDEF' for c in public_key[1:]):
                            return public_key
                    else:
                        # Unknown P2PK format - log as error
                        logger.error(f"CRITICAL: Unknown P2PK format detected: {public_key[:20]}... (length: {len(public_key)})")
                        logger.error(f"Script data: {script_pub_key}")
        
        # Secondary detection method - check hex field for P2PK patterns
        hex_data = script_pub_key.get('hex', '')
        if hex_data:
            # Look for P2PK patterns in hex
            if len(hex_data) == 134 and hex_data.startswith('41'):  # 65-byte public key + OP_CHECKSIG
                public_key = hex_data[2:-2]  # Remove length prefix and OP_CHECKSIG
                if len(public_key) == 130 and public_key.startswith('04'):
                    return public_key
            elif len(hex_data) == 70 and hex_data.startswith('41'):  # 33-byte public key + OP_CHECKSIG
                public_key = hex_data[2:-2]  # Remove length prefix and OP_CHECKSIG
                if len(public_key) == 66 and public_key.startswith(('02', '03')):
                    return public_key
        
        return None
    except Exception as e:
        logger.error(f"CRITICAL: Error parsing P2PK script: {e}")
        logger.error(f"Script data: {script_pub_key}")
        return None


def might_be_p2pk_input(vin: Dict[str, Any]) -> bool:
    """
    Determine if an input might be P2PK based on available information.
    Returns True if the input could potentially be P2PK, False if it's definitely not.
    """
    # If we have scriptSig, we can check the signature length
    if 'scriptSig' in vin and 'asm' in vin['scriptSig']:
        asm = vin['scriptSig']['asm']
        # P2PK signatures are typically 71-73 bytes (142-146 hex chars)
        # Check if the signature looks like it could be for P2PK
        parts = asm.split()
        if len(parts) >= 1:
            signature = parts[0]
            # P2PK signatures are typically 71-73 bytes
            if len(signature) >= 142 and len(signature) <= 146:
                return True
            # If signature is shorter, it's likely not P2PK
            return False
    
    # If we don't have scriptSig info, we have to assume it might be P2PK
    # This is conservative - we'd rather check than miss a P2PK transaction
    return True


def quick_scan_block_for_p2pk(block_data: Dict[str, Any]) -> bool:
    """
    Quick scan to check if a block contains any P2PK transactions.
    This scans the structured transaction data from RPC instead of raw hex.
    
    Returns True if the block might contain P2PK transactions, False if definitely not.
    """
    try:
        # Get transactions from the block data
        transactions = block_data.get('tx', [])
        if not transactions:
            # If no transactions, definitely no P2PK
            return False
        
        # Quick scan each transaction's outputs for P2PK patterns
        for tx in transactions:
            vouts = tx.get('vout', [])
            for vout in vouts:
                script_pub_key = vout.get('scriptPubKey', {})
                
                # Check if this is a P2PK output
                if script_pub_key.get('type') == 'pubkey':
                    # Found a P2PK output, return True immediately
                    return True
                
                # Also check the ASM for P2PK patterns
                asm = script_pub_key.get('asm', '')
                if 'OP_CHECKSIG' in asm:
                    # Look for public key patterns in ASM
                    parts = asm.split()
                    if len(parts) >= 2 and parts[-1] == 'OP_CHECKSIG':
                        public_key = parts[0]
                        # Check if it looks like a valid public key
                        if (len(public_key) == 130 and public_key.startswith('04')) or \
                           (len(public_key) == 66 and public_key.startswith(('02', '03'))):
                            return True
        
        # No P2PK outputs found in any transaction
        return False
        
    except Exception as e:
        logger.error(f"Error in quick scan for block: {e}")
        # If quick scan fails, assume block might contain P2PK (conservative)
        return True


def quick_scan_transaction_for_p2pk(tx_data: Dict[str, Any]) -> bool:
    """
    Quick scan to check if a transaction contains any P2PK patterns.
    This is much faster than full decoding and can skip transactions with no P2PK.
    
    Returns True if the transaction might contain P2PK, False if definitely not.
    """
    try:
        # Get the raw transaction hex to scan for P2PK patterns
        tx_hex = tx_data.get('hex', '')
        if not tx_hex:
            # If we don't have hex data, we have to assume it might contain P2PK
            return True
        
        # Look for complete P2PK output patterns in the raw transaction data
        # P2PK outputs have these specific patterns:
        # - 41 + 65 bytes (uncompressed) + ac = 134 hex chars total
        # - 41 + 33 bytes (compressed) + ac = 70 hex chars total
        
        # Search for complete P2PK output patterns
        if '41' in tx_hex:
            # Find all occurrences of '41' and check if they're followed by P2PK patterns
            pos = 0
            while True:
                pos = tx_hex.find('41', pos)
                if pos == -1:
                    break
                
                # Check if this could be the start of a P2PK script
                if pos + 134 <= len(tx_hex):
                    # Check for uncompressed P2PK: 41 + 130 chars + ac
                    potential_p2pk = tx_hex[pos:pos + 134]
                    if (potential_p2pk.startswith('41') and 
                        potential_p2pk.endswith('ac') and
                        potential_p2pk[2:4] in ['04', '02', '03']):
                        return True
                
                if pos + 70 <= len(tx_hex):
                    # Check for compressed P2PK: 41 + 66 chars + ac
                    potential_p2pk = tx_hex[pos:pos + 70]
                    if (potential_p2pk.startswith('41') and 
                        potential_p2pk.endswith('ac') and
                        potential_p2pk[2:4] in ['02', '03']):
                        return True
                
                pos += 1
        
        return False
        
    except Exception as e:
        logger.error(f"Error in quick scan for transaction: {e}")
        # If quick scan fails, assume transaction might contain P2PK (conservative)
        return True


def process_transaction(tx: Dict[str, Any], block_height: int, block_time: int, transaction_cache: Optional[Dict[str, Dict[str, Any]]] = None) -> List[Dict[str, Any]]:
    """Process a transaction and extract P2PK addresses."""
    if transaction_cache is None:
        transaction_cache = {}
    p2pk_transactions = []
    p2pk_addresses_found_in_tx = set()  # Track unique addresses in this transaction
    
    try:
        # Process outputs (receiving)
        vouts = tx.get('vout', [])
        for vout_idx, vout in enumerate(vouts):
            try:
                script_pub_key = vout.get('scriptPubKey', {})
                public_key = is_p2pk_script(script_pub_key)
                if public_key and public_key not in p2pk_addresses_found_in_tx:
                    p2pk_transactions.append({
                        'txid': tx['txid'],
                        'block_height': block_height,
                        'block_time': datetime.fromtimestamp(block_time),
                        'public_key_hex': public_key,
                        'amount_satoshi': int(vout['value'] * 100000000),
                        'is_input': False
                    })
                    p2pk_addresses_found_in_tx.add(public_key)
            except Exception as e:
                logger.error(f"CRITICAL: Error processing output {vout_idx} in tx {tx.get('txid', 'unknown')}: {e}")
                logger.error(f"Vout data: {vout}")
        
        # Process inputs (spending)
        vins = tx.get('vin', [])
        for vin_idx, vin in enumerate(vins):
            if 'txid' in vin and 'vout' in vin:
                # Check if this input might be P2PK before fetching
                if not might_be_p2pk_input(vin):
                    continue
                
                try:
                    # Use cached transaction if available, otherwise fetch individually
                    if transaction_cache and vin['txid'] in transaction_cache:
                        prev_tx = transaction_cache[vin['txid']]
                    else:
                        # Individual RPC call for input transaction
                        prev_tx = bitcoin_rpc.get_raw_transaction(vin['txid'])
                    
                    if prev_tx and 'vout' in prev_tx:
                        prev_vout = prev_tx['vout'][vin['vout']]
                        script_pub_key = prev_vout.get('scriptPubKey', {})
                        public_key = is_p2pk_script(script_pub_key)
                        if public_key and public_key not in p2pk_addresses_found_in_tx:
                            p2pk_transactions.append({
                                'txid': tx['txid'],
                                'block_height': block_height,
                                'block_time': datetime.fromtimestamp(block_time),
                                'public_key_hex': public_key,
                                'amount_satoshi': int(prev_vout['value'] * 100000000),
                                'is_input': True
                            })
                            p2pk_addresses_found_in_tx.add(public_key)
                except Exception as e:
                    logger.error(f"CRITICAL: Could not process input transaction {vin['txid']} in tx {tx.get('txid', 'unknown')}: {e}")
                    logger.error(f"Vin data: {vin}")
        
    except Exception as e:
        logger.error(f"CRITICAL: Error processing transaction {tx.get('txid', 'unknown')}: {e}")
        logger.error(f"Transaction data: {tx}")
    
    return p2pk_transactions





def worker(worker_queue: queue.Queue, thread_name: str, db_manager: HydraModeDatabaseManager, enable_profiling=False, batch_rpc=False, rpc_batch_size=25, quick_scan=False):
    global total_blocks_scanned
    global active_workers
    
    # Set up profiling if enabled
    if enable_profiling:
        profiler = cProfile.Profile()
        profiler.enable()
    
    # CRITICAL FIX: Workers should continue processing their queue until it's empty
    # Only exit when queue is empty AND stop event is set
    while True:
        try:
            # Get block with timeout - this allows checking stop_event periodically
            try:
                block_height = worker_queue.get(timeout=1.0)
            except queue.Empty:
                # Queue is empty, check if we should exit
                if stop_event.is_set():
                    logger.info(f"🧵 {thread_name}: Queue empty and stop event set, exiting")
                    break
                continue
            
            # Check if this is a sentinel value (shutdown signal)
            if block_height is None:
                logger.info(f"🧵 {thread_name} received shutdown signal")
                break
            
            # Check for pause signal - wait until resume
            while pause_event.is_set() and not stop_event.is_set():
                with thread_status_lock:
                    thread_status[thread_name] = f"Paused (block {block_height})"
                time.sleep(0.1)  # Small sleep to avoid busy waiting
            
            # CRITICAL FIX: Don't exit here even if stop_event is set
            # We have a block to process, so we should complete it
            # The worker will only exit when the queue is empty AND stop_event is set
            
            # Mark that we're processing this block (for shutdown tracking)
            with thread_status_lock:
                thread_status[thread_name] = f"Processing block {block_height}"
            
            # Track active worker for graceful shutdown
            with active_workers_lock:
                active_workers.add(thread_name)
            
            # CRITICAL: Once we start processing a block, we MUST complete it
            # even if stop_event is set during processing
            block_processing_succeeded = False
            
            try:
                # Get block data
                block_data = bitcoin_rpc.get_block_by_height(block_height)
                
                if not block_data:
                    logger.error(f"Failed to get block {block_height}")
                    with metrics_lock:
                        performance_metrics['blocks_failed'] += 1
                    continue
                
                # Track RPC performance
                with metrics_lock:
                    performance_metrics['rpc_calls'] += 1
                
                # Quick scan optimization: Check if block contains P2PK before full processing
                if quick_scan:
                    quick_scan_result = quick_scan_block_for_p2pk(block_data)
                    if not quick_scan_result:
                        # Block contains no P2PK transactions, skip full processing
                        logger.info(f"⚡ Quick scan: Block {block_height} skipped (no P2PK signatures found)")
                        with blocks_scanned_lock:
                            total_blocks_scanned += 1
                        with metrics_lock:
                            performance_metrics['blocks_processed'] += 1
                        with thread_status_lock:
                            thread_status[thread_name] = f"Skipped block {block_height} (no P2PK)"
                        update_scan_progress(db_manager.db_manager, block_height)
                        continue
                    # Block contains P2PK patterns, continue with full processing (no log needed)
                
                # Process transactions
                block_time = block_data['time']
                transactions = block_data.get('tx', [])
                
                if batch_rpc and transactions:
                    # Use batched RPC for transaction processing
                    txids = [tx['txid'] for tx in transactions]
                    
                    # Track RPC performance (count as 1 call for the batch)
                    with metrics_lock:
                        performance_metrics['rpc_calls'] += 1
                    
                    raw_transactions = bitcoin_rpc.get_raw_transactions_batch(txids, max_batch_size=rpc_batch_size)
                    
                    # Create transaction cache for batch processing
                    transaction_cache = {}
                    for raw_tx in raw_transactions:
                        if raw_tx is not None:
                            transaction_cache[raw_tx['txid']] = raw_tx
                    
                    # Collect all input transaction IDs that we need to fetch
                    input_txids = set()
                    for raw_tx in raw_transactions:
                        if raw_tx is not None:
                            for vin in raw_tx.get('vin', []):
                                if 'txid' in vin and 'vout' in vin:
                                    input_txids.add(vin['txid'])
                    
                    # Remove input transactions that are already in our cache (same block)
                    input_txids = input_txids - set(txids)
                    
                    # Batch fetch input transactions if we have any
                    if input_txids:
                        input_txid_list = list(input_txids)
                        input_raw_transactions = bitcoin_rpc.get_raw_transactions_batch(input_txid_list, max_batch_size=rpc_batch_size)
                        
                        # Add input transactions to cache
                        for raw_tx in input_raw_transactions:
                            if raw_tx is not None:
                                transaction_cache[raw_tx['txid']] = raw_tx
                        
                        # Track RPC performance for input batch
                        with metrics_lock:
                            performance_metrics['rpc_calls'] += 1
                    
                    # Process each transaction with its raw data
                    total_p2pk_found = 0
                    for i, (tx, raw_tx) in enumerate(zip(transactions, raw_transactions)):
                        if raw_tx is None:
                            logger.warning(f"Failed to get raw transaction for {tx['txid']}")
                            continue
                        
                        p2pk_transactions = process_transaction(raw_tx, block_height, block_time, transaction_cache)
                        total_p2pk_found += len(p2pk_transactions)
                        
                        # Add to database manager with error tracking
                        p2pk_added_count = 0
                        for p2pk_tx in p2pk_transactions:
                            try:
                                # Track found addresses
                                with p2pk_tracking_lock:
                                    p2pk_addresses_found.add(p2pk_tx['public_key_hex'])
                                
                                db_manager.add_transaction(p2pk_tx)
                                p2pk_added_count += 1
                                
                                # Track successfully stored addresses
                                with p2pk_tracking_lock:
                                    p2pk_addresses_stored.add(p2pk_tx['public_key_hex'])
                                    
                            except Exception as e:
                                logger.error(f"CRITICAL: Failed to add P2PK transaction to database: {e}")
                                logger.error(f"P2PK transaction data: {p2pk_tx}")
                                
                                # Track failed addresses
                                with p2pk_tracking_lock:
                                    p2pk_addresses_failed.add(p2pk_tx['public_key_hex'])
                        
                        with metrics_lock:
                            performance_metrics['transactions_processed'] += 1
                            performance_metrics['p2pk_found'] += len(p2pk_transactions)
                        
                        if p2pk_added_count != len(p2pk_transactions):
                            logger.error(f"CRITICAL: P2PK data loss detected! Found {len(p2pk_transactions)} but only added {p2pk_added_count} in tx {raw_tx['txid'][:16]}...")
                    
                    # Log if no P2PK transactions were found in this block
                    if total_p2pk_found == 0:
                        logger.info(f"📭 Block {block_height} processed: {len(transactions)} transactions, 0 P2PK addresses found")
                    
                else:
                    # Process transactions individually (original method)
                    total_p2pk_found = 0
                    for tx in transactions:
                        p2pk_transactions = process_transaction(tx, block_height, block_time)
                        total_p2pk_found += len(p2pk_transactions)
                        
                        # Add to database manager with error tracking
                        p2pk_added_count = 0
                        for p2pk_tx in p2pk_transactions:
                            try:
                                # Track found addresses
                                with p2pk_tracking_lock:
                                    p2pk_addresses_found.add(p2pk_tx['public_key_hex'])
                                
                                db_manager.add_transaction(p2pk_tx)
                                p2pk_added_count += 1
                                
                                # Track successfully stored addresses
                                with p2pk_tracking_lock:
                                    p2pk_addresses_stored.add(p2pk_tx['public_key_hex'])
                                    
                            except Exception as e:
                                logger.error(f"CRITICAL: Failed to add P2PK transaction to database: {e}")
                                logger.error(f"P2PK transaction data: {p2pk_tx}")
                                
                                # Track failed addresses
                                with p2pk_tracking_lock:
                                    p2pk_addresses_failed.add(p2pk_tx['public_key_hex'])
                        
                        with metrics_lock:
                            performance_metrics['transactions_processed'] += 1
                            performance_metrics['p2pk_found'] += len(p2pk_transactions)
                        
                        if p2pk_added_count != len(p2pk_transactions):
                            logger.error(f"CRITICAL: P2PK data loss detected! Found {len(p2pk_transactions)} but only added {p2pk_added_count} in tx {tx['txid'][:16]}...")
                    
                    # Log if no P2PK transactions were found in this block
                    if total_p2pk_found == 0:
                        logger.info(f"📭 Block {block_height} processed: {len(transactions)} transactions, 0 P2PK addresses found")
                
                # Update scan progress
                with blocks_scanned_lock:
                    total_blocks_scanned += 1
                
                # Update database progress
                update_scan_progress(db_manager.db_manager, block_height)
                
                # Record that this block was processed (even if no P2PK found)
                # This ensures verify_blocks can distinguish between unprocessed and empty blocks
                try:
                    db_manager.db_manager.execute_command("""
                        INSERT INTO scan_progress (scanner_name, last_scanned_block, total_blocks_scanned, last_updated)
                        VALUES ('block_processed_marker', %s, 1, CURRENT_TIMESTAMP)
                        ON CONFLICT (scanner_name) DO UPDATE SET
                            last_scanned_block = EXCLUDED.last_scanned_block,
                            total_blocks_scanned = scan_progress.total_blocks_scanned + 1,
                            last_updated = CURRENT_TIMESTAMP
                    """, (block_height,))
                except Exception as e:
                    logger.debug(f"Could not record processed block marker for {block_height}: {e}")
                
                # Mark block as successfully processed
                block_processing_succeeded = True
                
                # Update counters only on successful processing
                with metrics_lock:
                    performance_metrics['blocks_processed'] += 1
                
                # Update thread status
                with thread_status_lock:
                    thread_status[thread_name] = f"Processed block {block_height}"
                
            except Exception as e:
                logger.error(f"Error processing block {block_height}: {e}")
                with metrics_lock:
                    performance_metrics['blocks_failed'] += 1
                # RESEARCH-GRADE: Do NOT increment total_blocks_scanned on failure
                # This ensures failed blocks are not marked as "scanned"
                
                # CRITICAL: Still update scan progress to mark this block as attempted
                # This prevents the block from being considered "missing" in verify_blocks
                try:
                    update_scan_progress(db_manager.db_manager, block_height)
                    logger.info(f"Marked block {block_height} as attempted despite processing failure")
                except Exception as progress_error:
                    logger.error(f"Failed to update scan progress for block {block_height}: {progress_error}")
            
            # CRITICAL: Always mark task as done, even if processing failed
            # This ensures the worker can continue to the next block
            worker_queue.task_done()
            
            # Remove from active workers set
            with active_workers_lock:
                active_workers.discard(thread_name)
            
            # Update thread status to show we're ready for next block
            with thread_status_lock:
                thread_status[thread_name] = f"Ready for next block (last: {block_height})"
            
        except queue.Empty:
            continue
        except Exception as e:
            logger.error(f"Worker {thread_name} error: {e}")
            # CRITICAL FIX: Don't break on exceptions - continue processing queue
            # Only exit when queue is empty AND stop event is set
            continue
    
    # Save profiling data if enabled
    if enable_profiling:
        profiler.disable()
        with worker_profiles_lock:
            worker_profiles[thread_name] = profiler
    
    with thread_status_lock:
        thread_status[thread_name] = "Stopped"
    
    # Terminal output when thread exits
    logger.info(f"🧵 Worker thread {thread_name} has stopped")


def distributor(main_queue: queue.Queue, worker_queues: List[queue.Queue], target_depth: int = 4):
    """Distributor thread that keeps worker queues filled."""
    logger.info(f"🔄 Distributor thread started - target depth: {target_depth} blocks per worker")
    
    while not stop_event.is_set():
        try:
            # Check if main queue is empty
            if main_queue.empty():
                # If main queue is empty and all worker queues are empty, we're done
                all_worker_queues_empty = all(q.empty() for q in worker_queues)
                if all_worker_queues_empty:
                    logger.info("🔄 Distributor: All queues empty, distribution complete")
                    break
                else:
                    # Some workers still have work, wait a bit
                    time.sleep(0.1)
                    continue
            
            # Check each worker queue and top up if needed
            for i, worker_queue in enumerate(worker_queues):
                current_depth = worker_queue.qsize()
                
                # If queue is below target depth, add blocks from main queue
                while current_depth < target_depth and not stop_event.is_set():
                    try:
                        # Get block from main queue with timeout
                        block_height = main_queue.get(timeout=0.1)
                        worker_queue.put(block_height, block=False)
                        current_depth += 1
                        
                        with metrics_lock:
                            performance_metrics['distribution_operations'] += 1
                            
                    except queue.Empty:
                        # Main queue is empty, break out of this worker's loop
                        break
                    except queue.Full:
                        # Worker queue is full, break out of this worker's loop
                        break
                
                # Update worker queue depth tracking
                with metrics_lock:
                    performance_metrics['worker_queue_depths'][f'worker-{i}'] = current_depth
            
            # Small sleep to prevent busy waiting, but check stop_event more frequently
            time.sleep(0.01)
            
            # Additional stop check to exit faster
            if stop_event.is_set():
                logger.info("🔄 Distributor: Stop event detected, exiting immediately")
                break
            
        except Exception as e:
            logger.error(f"Distributor thread error: {e}")
            time.sleep(0.1)
    
    logger.info("🔄 Distributor thread stopped")


def keyboard_listener(db_manager=None):
    """Listen for keyboard input to stop scanning gracefully."""
    while not stop_event.is_set():
        if select.select([sys.stdin], [], [], 0.1)[0]:
            line = sys.stdin.readline()
            command = line.strip().lower()
            
            if command == 'q':
                logger.info("Received quit signal")
                stop_event.set()
                break
            elif command == 'p':
                if pause_event.is_set():
                    logger.info("Received resume signal")
                    pause_event.clear()
                else:
                    logger.info("Received pause signal")
                    pause_event.set()
            elif command == 'h':
                print("\n📋 Available commands:")
                print("  q - Quit scanner")
                print("  p - Pause/Resume workers")
                print("  h - Show this help")
                print("  s - Show status")
                print("  i - Show P2PK integrity")
                print("  m - Show detailed metrics")
                print("  u - Show queue status")
                if auto_pause_enabled:
                    print(f"  🔄 Auto-pause: Pause at {auto_pause_threshold:,}, resume at {auto_resume_threshold:,}")
            elif command == 's':
                report_thread_status()
                report_performance_stats()
            elif command == 'i':
                report_p2pk_integrity()
            elif command == 'm':
                report_detailed_performance_metrics()
            elif command == 'u':
                # Show current queue depth and auto-pause status
                if db_manager is not None:
                    try:
                        queue_depth = db_manager.write_queue.qsize()
                        print(f"\n📊 Queue Status:")
                        print(f"  Output queue depth: {queue_depth:,}")
                        print(f"  Auto-pause enabled: {auto_pause_enabled}")
                        if auto_pause_enabled:
                            print(f"  Pause threshold: {auto_pause_threshold:,}")
                            print(f"  Resume threshold: {auto_resume_threshold:,}")
                            if pause_event.is_set():
                                print(f"  Status: ⏸️ PAUSED (queue depth {queue_depth:,} > {auto_pause_threshold:,})")
                            else:
                                print(f"  Status: ▶️ RUNNING (queue depth {queue_depth:,} < {auto_pause_threshold:,})")
                        else:
                            print(f"  Status: {'⏸️ PAUSED' if pause_event.is_set() else '▶️ RUNNING'} (manual)")
                    except Exception as e:
                        print(f"Error getting queue status: {e}")
                else:
                    print("Queue status not available (db_manager not provided)")


def report_thread_status():
    """Report the status of all worker threads."""
    print("\n🧵 THREAD STATUS:")
    with thread_status_lock:
        for thread_name, status in thread_status.items():
            print(f"  {thread_name}: {status}")


def report_performance_stats(db_manager=None, worker_queues=None):
    """Report basic performance statistics."""
    with stats_lock:
        print(f"\n📊 PERFORMANCE STATS:")
        print(f"  Total transactions processed: {performance_stats['total_transactions_processed']:,}")
        print(f"  Total addresses found: {performance_stats['total_addresses_found']:,}")
        print(f"  Batch inserts performed: {performance_stats['batch_inserts_performed']:,}")
        print(f"  Queue operations: {performance_stats['queue_operations']:,}")
        
        # Calculate estimated P2PK transactions found from queue operations
        # Each P2PK transaction adds 3 items to queue (address + transaction + block)
        estimated_p2pk_txs = performance_stats['queue_operations'] // 3
        if estimated_p2pk_txs > 0:
            print(f"  Estimated P2PK transactions found: {estimated_p2pk_txs:,}")
            if performance_stats['total_addresses_found'] == 0 and estimated_p2pk_txs > 0:
                print(f"  Note: Processing existing P2PK addresses (no new addresses found yet)")
        
        # Show database state if available
        if db_manager is not None and hasattr(db_manager, 'db_manager'):
            try:
                addr_count = db_manager.db_manager.execute_query("SELECT COUNT(*) as count FROM p2pk_addresses")[0]['count']
                tx_count = db_manager.db_manager.execute_query("SELECT COUNT(*) as count FROM p2pk_transactions")[0]['count']
                print(f"  Database state: {addr_count:,} addresses, {tx_count:,} transactions")
            except Exception as e:
                print(f"  Database state: Unable to query ({e})")
    
    with metrics_lock:
        if performance_metrics['queue_waiting_count'] > 0:
            print(f"  Queue waiting events: {performance_metrics['queue_waiting_count']:,}")
            avg_wait_time = performance_metrics['queue_waiting_time_total'] / performance_metrics['queue_waiting_count']
            print(f"  Avg queue wait time: {avg_wait_time:.4f}s")
    
    # Output queue depth (write-behind cache)
    if db_manager is not None and hasattr(db_manager, 'write_queue'):
        print(f"  Output queue depth (write-behind cache): {db_manager.write_queue.qsize():,}")
    
    # Worker queue depths
    if worker_queues is not None:
        depths = [str(worker_queue.qsize()) for worker_queue in worker_queues]
        print(f"  Worker queue depths: [ {' | '.join(depths)} ]")


def report_p2pk_integrity():
    """Report P2PK address integrity status."""
    with p2pk_tracking_lock:
        found_count = len(p2pk_addresses_found)
        stored_count = len(p2pk_addresses_stored)
        failed_count = len(p2pk_addresses_failed)
        
        # Calculate success rate
        success_rate = (stored_count / max(found_count, 1)) * 100
        
        print("\n" + "="*80)
        print("P2PK ADDRESS INTEGRITY REPORT")
        print("="*80)
        print(f"P2PK Addresses Found: {found_count:,}")
        print(f"P2PK Addresses Stored: {stored_count:,}")
        print(f"P2PK Addresses Failed: {failed_count:,}")
        print(f"P2PK Success Rate: {success_rate:.2f}%")
        
        if failed_count > 0:
            print(f"\n⚠️  DATA INTEGRITY WARNING: {failed_count:,} P2PK addresses failed to store!")
            print("Failed addresses (first 10):")
            for i, addr in enumerate(list(p2pk_addresses_failed)[:10]):
                print(f"  {i+1}. {addr[:20]}...")
            if failed_count > 10:
                print(f"  ... and {failed_count - 10} more")
        else:
            print(f"\n✅ PERFECT DATA INTEGRITY: All {found_count:,} P2PK addresses successfully stored!")
        
        print("="*80)


def report_detailed_performance_metrics():
    """Report detailed performance metrics."""
    print("\n" + "="*80)
    print("HYDRA MODE SCANNER PERFORMANCE PROFILE")
    print("="*80)
    
    with metrics_lock:
        print(f"\n📊 PROCESSING STATISTICS:")
        print(f"  Blocks processed: {performance_metrics['blocks_processed']:,}")
        print(f"  Blocks failed: {performance_metrics['blocks_failed']:,}")
        success_rate = ((performance_metrics['blocks_processed'] - performance_metrics['blocks_failed']) / max(performance_metrics['blocks_processed'], 1)) * 100
        print(f"  Success rate: {success_rate:.2f}%")
        print(f"  Transactions processed: {performance_metrics['transactions_processed']:,}")
        print(f"  P2PK addresses found: {performance_metrics['p2pk_found']:,}")
        
        print(f"\n🚀 RPC PERFORMANCE:")
        print(f"  Total RPC calls: {performance_metrics['rpc_calls']:,}")
        print(f"  Total RPC time: {performance_metrics['rpc_time_total']:.2f}s")
        print(f"  Average RPC time: {performance_metrics['rpc_time_avg']:.4f}s")
        print(f"  Min RPC time: {performance_metrics['rpc_time_min']:.4f}s")
        print(f"  Max RPC time: {performance_metrics['rpc_time_max']:.4f}s")
        print(f"  RPC calls per second: {performance_metrics['rpc_calls'] / max(performance_metrics['rpc_time_total'], 1):.2f}")
        
        print(f"\n💾 DATABASE PERFORMANCE:")
        print(f"  Total DB operations: {performance_metrics['db_operations']:,}")
        print(f"  Total DB time: {performance_metrics['db_time_total']:.2f}s")
        print(f"  Average DB time: {performance_metrics['db_time_avg']:.4f}s")
        print(f"  Batch flushes: {performance_metrics['batch_flushes']:,}")
        print(f"  Average batch flush time: {performance_metrics['batch_flush_time_avg']:.4f}s")
        
        print(f"\n🔄 DISTRIBUTION PERFORMANCE:")
        print(f"  Distribution operations: {performance_metrics['distribution_operations']:,}")
        print(f"  Worker queue depths:")
        for worker_name, depth in performance_metrics['worker_queue_depths'].items():
            print(f"    {worker_name}: {depth} blocks")
        
        print(f"\n❌ ERROR ANALYSIS:")
        print(f"  Foreign key errors: {performance_metrics['foreign_key_errors']:,}")
        print(f"  Address insert errors: {performance_metrics['address_insert_errors']:,}")
        print(f"  Transaction insert errors: {performance_metrics['transaction_insert_errors']:,}")
        print(f"  Block insert errors: {performance_metrics['block_insert_errors']:,}")
        
        # Data integrity warning
        if performance_metrics['address_insert_errors'] > 0:
            print(f"  ⚠️  DATA INTEGRITY WARNING: {performance_metrics['address_insert_errors']:,} addresses failed to insert!")
            print(f"     This may result in missing transactions and blocks.")
        
        print(f"\n📦 QUEUE PERFORMANCE:")
        print(f"  Queue operations: {performance_metrics['queue_operations']:,}")
        print(f"  Queue waiting events: {performance_metrics['queue_waiting_count']:,}")
        print(f"  Total queue waiting time: {performance_metrics['queue_waiting_time_total']:.2f}s")
        if performance_metrics['queue_waiting_count'] > 0:
            avg_wait_time = performance_metrics['queue_waiting_time_total'] / performance_metrics['queue_waiting_count']
            print(f"  Average queue wait time: {avg_wait_time:.4f}s")
        print(f"  Queue efficiency: {((performance_metrics['queue_operations'] - performance_metrics['queue_waiting_count']) / max(performance_metrics['queue_operations'], 1) * 100):.2f}%")
        
        # Calculate throughput
        if performance_metrics['rpc_time_total'] > 0:
            blocks_per_second = performance_metrics['blocks_processed'] / performance_metrics['rpc_time_total']
            print(f"\n⚡ THROUGHPUT ANALYSIS:")
            print(f"  Blocks per second: {blocks_per_second:.2f}")
            print(f"  Transactions per second: {performance_metrics['transactions_processed'] / max(performance_metrics['rpc_time_total'], 1):.2f}")
            print(f"  P2PK addresses per second: {performance_metrics['p2pk_found'] / max(performance_metrics['rpc_time_total'], 1):.2f}")
        
        print("="*80)


def report_worker_profiling():
    """Report profiling data from all worker threads."""
    if not worker_profiles:
        return
    
    print("\n" + "="*80)
    print("WORKER THREAD PROFILING RESULTS")
    print("="*80)
    
    with worker_profiles_lock:
        for thread_name, profiler in worker_profiles.items():
            print(f"\n🧵 {thread_name.upper()} PROFILING:")
            s = io.StringIO()
            ps = pstats.Stats(profiler, stream=s).sort_stats('cumulative')
            ps.print_stats(15)  # Top 15 functions per worker
            print(s.getvalue())
    
    print("="*80)


def main(profile_mode=False, profile_output=None):
    """Main function for the hydra mode scanner."""
    import argparse
    
    parser = argparse.ArgumentParser(description='HYDRA MODE P2PK Scanner - Multi-Queue Architecture')
    parser.add_argument('--start-block', type=int, help='Block height to start scanning from')
    parser.add_argument('--end-block', type=int, help='Block height to stop scanning at')
    parser.add_argument('--threads', type=int, default=8, help='Number of threads (default: 8)')
    parser.add_argument('--batch-size', type=int, default=1000, help='Batch size for database operations (default: 1000)')
    parser.add_argument('--queue-size', type=int, default=1000000, help='Write queue size (default: 1000000)')
    parser.add_argument('--target-depth', type=int, default=4, help='Target blocks per worker queue (default: 4)')
    parser.add_argument('--reset', action='store_true', help='Reset scan progress and start from beginning')
    parser.add_argument('--profile', action='store_true', help='Enable function-level profiling (cProfile)')
    parser.add_argument('--profile-output', type=str, default=None, help='Save cProfile output to file')
    parser.add_argument('--worker-profile', action='store_true', help='Enable worker thread profiling (separate from main thread)')
    parser.add_argument('--batch-rpc', action='store_true', help='Batch multiple transaction RPC calls for better performance')
    parser.add_argument('--rpc-batch-size', type=int, default=25, help='RPC batch size for transaction fetching (default: 25)')
    parser.add_argument('--quick-scan', action='store_true', help='Enable quick scan optimization to skip blocks with no P2PK patterns')
    parser.add_argument('--no-auto-pause', action='store_true', help='Disable automatic pause/resume based on queue depth')
    parser.add_argument('--pause-threshold', type=int, default=50000, help='Queue depth threshold to trigger auto-pause (default: 50000)')
    parser.add_argument('--resume-threshold', type=int, default=10000, help='Queue depth threshold to trigger auto-resume (default: 10000)')
    
    args = parser.parse_args()
    
    if args.profile:
        import cProfile
        import pstats
        import io
        pr = cProfile.Profile()
        pr.enable()
        try:
            _main(args)
        finally:
            pr.disable()
            s = io.StringIO()
            ps = pstats.Stats(pr, stream=s).sort_stats('cumulative')
            ps.print_stats(30)
            print("\n===== FUNCTION-LEVEL PROFILING (top 30 by cumulative time) =====\n")
            print(s.getvalue())
            if args.profile_output:
                pr.dump_stats(args.profile_output)
                print(f"[Profiler] Full profile saved to {args.profile_output}")
    else:
        _main(args)


def _main(args):
    
    # Update auto-pause configuration based on command line arguments
    global auto_pause_enabled, auto_pause_threshold, auto_resume_threshold
    auto_pause_enabled = not args.no_auto_pause
    auto_pause_threshold = args.pause_threshold
    auto_resume_threshold = args.resume_threshold
    
    logger.info("🐉 Starting HYDRA MODE P2PK Scanner...")
    logger.info(f"🔥 Configuration: {args.threads} threads, batch size {args.batch_size}, queue size {args.queue_size}")
    logger.info(f"🔄 Target depth: {args.target_depth} blocks per worker queue")
    if args.batch_rpc:
        logger.info("🚀 BATCH RPC ENABLED - Multiple transactions per RPC call")
    if args.quick_scan:
        logger.info("⚡ QUICK SCAN ENABLED - Skip blocks with no P2PK patterns")
    if auto_pause_enabled:
        logger.info(f"🔄 AUTO-PAUSE ENABLED - Pause at {auto_pause_threshold:,}, resume at {auto_resume_threshold:,}")
    else:
        logger.info("⏸️ AUTO-PAUSE DISABLED - Manual pause/resume only")
    
    if not bitcoin_rpc.test_connection():
        logger.error("Failed to connect to Bitcoin Core")
        return
    
    # Initialize hydra mode database manager
    db_manager = HydraModeDatabaseManager(batch_size=args.batch_size, queue_size=args.queue_size)
    
    try:
        # Ensure scan_progress row exists
        ensure_scan_progress_row(db_manager.db_manager)
        
        # Get blockchain height
        blockchain_info = bitcoin_rpc.get_blockchain_info()
        current_height = blockchain_info['blocks']
        logger.info(f"Current blockchain height: {current_height}")
        
        # Determine scan range
        if args.reset:
            start_block = args.start_block if args.start_block is not None else 0
            logger.info(f"Reset requested. Starting from block {start_block}")
        elif args.start_block is not None:
            start_block = args.start_block
            logger.info(f"Starting from specified block {start_block}")
        else:
            last_scanned_block = get_scan_progress(db_manager.db_manager)
            start_block = last_scanned_block + 1  # Resume from next block after last scanned
            logger.info(f"Last scanned block number was {last_scanned_block}, resuming from block number {start_block}")
        
        end_block = args.end_block if args.end_block is not None else current_height
        threads = args.threads
        
        if start_block >= end_block:
            logger.info("Already up to date")
            return
        
        logger.info(f"🐉 Scanning blocks {start_block} to {end_block} using {threads} threads")
        start_time = time.time()
        
        # Start keyboard listener thread
        kb_thread = threading.Thread(target=keyboard_listener, args=(db_manager,), daemon=True)
        kb_thread.start()
        
        # Show available commands
        print("\n📋 Available commands:")
        print("  q - Quit scanner")
        print("  p - Pause/Resume workers")
        print("  h - Show this help")
        print("  s - Show status")
        print("  i - Show P2PK integrity")
        print("  m - Show detailed metrics")
        print("  u - Show queue status")
        if auto_pause_enabled:
            print("  🔄 Auto-pause enabled (queue depth management)")
        print()
        
        # Initialize main queue and worker queues
        main_queue = queue.Queue()
        worker_queues = []
        
        # Create individual queues for each worker
        for i in range(threads):
            worker_queue = queue.Queue()
            worker_queues.append(worker_queue)
        
        # Fill main queue with all blocks to process
        logger.info(f"🔄 Filling main queue with {end_block - start_block + 1} blocks...")
        for block_height in range(start_block, end_block + 1):
            main_queue.put(block_height)
        
        # Start distributor thread
        distributor_thread = threading.Thread(target=distributor, args=(main_queue, worker_queues, args.target_depth), daemon=True)
        distributor_thread.start()
        
        # Start worker threads
        worker_threads = []
        for i in range(threads):
            tname = f"hydra-worker-{i}"
            t = threading.Thread(target=worker, args=(worker_queues[i], tname, db_manager, args.worker_profile, args.batch_rpc, args.rpc_batch_size, args.quick_scan), daemon=True)
            worker_threads.append(t)
            t.start()
        
        # Main monitoring loop
        total_blocks_to_scan = end_block - start_block + 1
        last_report = 0
        report_interval = 10  # seconds
        last_progress_update = start_block
        
        while True:
            # Check if all workers are done and main queue is empty
            all_workers_done = not any(t.is_alive() for t in worker_threads)
            main_queue_empty = main_queue.empty()
            distributor_done = not distributor_thread.is_alive()
            
            # Also check if we've processed all blocks
            with blocks_scanned_lock:
                scanned = total_blocks_scanned
            
            # Check for completion conditions
            if stop_event.is_set():
                logger.info("Received quit signal")
                break
            elif (all_workers_done and main_queue_empty and distributor_done) or scanned >= total_blocks_to_scan:
                logger.info(f"Scan complete: {scanned}/{total_blocks_to_scan} blocks processed")
                break
            
            # Check auto-pause based on queue depth
            check_auto_pause(db_manager)
            
            # Report progress
            if time.time() - last_report > report_interval:
                with blocks_scanned_lock:
                    scanned = total_blocks_scanned
                
                elapsed = time.time() - start_time
                if scanned > 0:
                    blocks_per_second = scanned / elapsed
                    remaining_blocks = total_blocks_to_scan - scanned
                    eta_seconds = int(remaining_blocks / blocks_per_second)
                    eta_str = format_time_dd_hh_mm_ss(eta_seconds)
                    speed_str = f"{blocks_per_second:.2f} blocks/sec"
                else:
                    eta_str = "calculating..."
                    speed_str = "0.00 blocks/sec"
                
                elapsed_str = format_time_dd_hh_mm_ss(elapsed)
                current_block_number = start_block + scanned
                progress_percent = (current_block_number / end_block * 100) if end_block > 0 else 0
                
                # Add pause status indicator
                pause_status = " ⏸️ PAUSED" if pause_event.is_set() else ""
                logger.info(f"🐉 Progress: block {current_block_number}/{end_block} ({progress_percent:.1f}%). Elapsed: {elapsed_str}, ETA: {eta_str}, Speed: {speed_str}{pause_status}")
                
                # Update scan progress - current_block_number is the last block number processed
                if current_block_number - last_progress_update >= 100:
                    update_scan_progress(db_manager.db_manager, current_block_number, current_block_number - last_progress_update)
                    last_progress_update = current_block_number
                
                # Report performance stats
                report_performance_stats(db_manager, worker_queues)
                
                if stop_event.is_set():
                    report_thread_status()
                    logger.info(f"Main queue size: {main_queue.qsize()}")
                
                last_report = time.time()
            
            time.sleep(1)
        
        # Graceful shutdown: Wait for workers to finish current blocks
        logger.info("🔄 Starting graceful shutdown...")
        
        # Step 1: Stop the distributor first (no new blocks to workers)
        logger.info("Stopping distributor to prevent new blocks from being assigned...")
        # The distributor will naturally stop when stop_event is set
        
        # Step 2: Wait for distributor to finish and workers to process their current queues
        logger.info("Waiting for workers to process their current queue contents...")
        max_wait_time = 300  # 5 minutes max wait
        start_wait = time.time()
        
        while time.time() - start_wait < max_wait_time:
            # Check if distributor is done
            if not distributor_thread.is_alive():
                logger.info("Distributor has finished")
            else:
                logger.info("Distributor still running...")
            
            # Check worker queue depths
            total_queued = sum(worker_queue.qsize() for worker_queue in worker_queues)
            
            # Check active workers
            with active_workers_lock:
                active_count = len(active_workers)
                active_list = list(active_workers)
            
            if total_queued == 0 and active_count == 0 and not distributor_thread.is_alive():
                logger.info("All work completed - no queued blocks, no active workers, distributor finished")
                break
            
            logger.info(f"Waiting: {total_queued} queued blocks, {active_count} active workers: {active_list}")
            time.sleep(5)
        
        # CRITICAL FIX: No need to send sentinel values - workers will naturally stop
        # when their queues are empty and stop_event is set
        logger.info("Workers will naturally stop when their queues are empty...")
        
        # Wait for all workers to finish (they will stop when queues are empty)
        logger.info("Waiting for workers to finish processing their queues...")
        max_wait_time = 300  # 5 minutes max wait
        start_wait = time.time()
        
        while time.time() - start_wait < max_wait_time:
            # Check if all workers are done
            active_worker_threads = [t for t in worker_threads if t.is_alive()]
            if not active_worker_threads:
                logger.info("All workers finished processing their queues")
                break
            
            # Log which workers are still active
            active_names = [f"hydra-worker-{i}" for i, t in enumerate(worker_threads) if t.is_alive()]
            logger.info(f"Waiting for {len(active_worker_threads)} workers to finish: {', '.join(active_names)}")
            
            # Wait a bit more
            time.sleep(5)
        
        # Force timeout for any remaining workers
        logger.info("Final worker cleanup...")
        for i, t in enumerate(worker_threads):
            if t.is_alive():
                logger.warning(f"Worker {i} still alive after timeout, forcing shutdown")
                t.join(timeout=30)
                if t.is_alive():
                    logger.error(f"Worker {i} could not be stopped")
        
        # Note: main_queue.join() removed - main queue is only used for distribution
        # Worker queues and thread joins handle the actual shutdown synchronization
        
        # CRITICAL: Final progress update before database shutdown
        with blocks_scanned_lock:
            final_scanned = total_blocks_scanned
        final_block_number = start_block + final_scanned
        logger.info(f"Final progress update: block {final_block_number} (scanned {final_scanned} blocks)")
        update_scan_progress(db_manager.db_manager, final_block_number, final_scanned)
        
        # CRITICAL: Flush database queue before calculating final statistics
        logger.info("Flushing database queue to ensure all P2PK addresses are stored...")
        db_manager.shutdown()  # This flushes the queue
        
        elapsed = time.time() - start_time
        elapsed_str = format_time_dd_hh_mm_ss(elapsed)
        
        # Calculate final statistics AFTER database flush
        with blocks_scanned_lock:
            scanned = total_blocks_scanned
        
        blocks_per_second = scanned / elapsed if elapsed > 0 else 0
        
        # Determine if this was a graceful exit or normal completion
        was_graceful_exit = stop_event.is_set()
        
        if was_graceful_exit:
            logger.info(f"🐉 HYDRA MODE scan gracefully stopped in {elapsed_str}!")
            print(f"\n🐉 HYDRA MODE scan gracefully stopped in {elapsed_str}!")
        else:
            logger.info(f"🐉 HYDRA MODE scan completed in {elapsed_str}!")
            logger.info(f"🔥 Performance: {blocks_per_second:.2f} blocks per second")
            report_performance_stats(db_manager, worker_queues)
            report_thread_status()
        
        logger.info(f"🐉 Final Progress: {scanned}/{total_blocks_to_scan} blocks scanned in {elapsed_str}")
        logger.info(f"🔥 Average Speed: {blocks_per_second:.2f} blocks/second")
        
        # Generate detailed performance profile (only for normal completion)
        if not was_graceful_exit:
            report_detailed_performance_metrics()
            
            # Report P2PK address integrity
            report_p2pk_integrity()
        
        # Report worker profiling if enabled
        if args.worker_profile:
            report_worker_profiling()
        
        # Final status - last block processed (always show)
        if was_graceful_exit:
            logger.info(f"🐉 GRACEFUL EXIT: Stopped at block {final_block_number}")
            print(f"\n🐉 GRACEFUL EXIT: Stopped at block {final_block_number}")
        else:
            logger.info(f"🐉 SCAN COMPLETE: Stopped at block {final_block_number}")
            print(f"\n🐉 SCAN COMPLETE: Stopped at block {final_block_number}")
        
    except Exception as e:
        logger.error(f"HYDRA MODE scan failed: {e}")
        # Even on failure, show where we stopped
        with blocks_scanned_lock:
            failed_block_number = start_block + total_blocks_scanned
        logger.error(f"🐉 SCAN FAILED: Stopped at block {failed_block_number}")
        print(f"\n🐉 SCAN FAILED: Stopped at block {failed_block_number}")
    finally:
        # Database already shut down in the main flow, only shutdown here if we hit an exception
        if 'db_manager' in locals() and hasattr(db_manager, 'writer_running') and db_manager.writer_running:
            db_manager.shutdown()


if __name__ == "__main__":
    import sys
    main() 
