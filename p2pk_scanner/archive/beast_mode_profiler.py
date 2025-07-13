#!/usr/bin/env python3
"""
BEAST MODE Profiler - Function-Level Performance Analysis
Uses Python's cProfile to provide detailed function call analysis
showing exactly where time is being spent in the scanner.
"""

import sys
import os
import cProfile
import pstats
import io
import time
import logging
import queue
import threading
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime
import select

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
        logging.FileHandler(Path(__file__).parent.parent / 'logs' / 'beast_mode_profiler.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# For graceful shutdown
stop_event = threading.Event()
thread_status = {}
thread_status_lock = threading.Lock()

# Global counters
total_blocks_scanned = 0
blocks_scanned_lock = threading.Lock()

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
    'batch_flushes': 0,
    'batch_flush_time_total': 0.0,
    'batch_flush_time_avg': 0.0,
    'foreign_key_errors': 0,
    'address_insert_errors': 0,
    'transaction_insert_errors': 0,
    'block_insert_errors': 0,
    'memory_usage_mb': 0.0,
    'cpu_usage_percent': 0.0
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


class BeastModeDatabaseManager:
    """High-performance database manager with batch operations and write-behind caching."""
    
    def __init__(self, batch_size: int = 1000, queue_size: int = 50000):
        self.batch_size = batch_size
        self.queue_size = queue_size
        self.write_queue = queue.Queue(maxsize=queue_size)
        self.db_manager = DatabaseManager()
        self.prepared_statements = {}
        self.writer_thread = None
        self.writer_running = False
        
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
                    item = self.write_queue.get(timeout=1.0)
                    batch.append(item)
                except queue.Empty:
                    # Flush if we have items and enough time has passed
                    if batch and (time.time() - last_flush) > 5.0:
                        pass  # Continue to flush
                    else:
                        continue
                
                # Flush batch when it reaches threshold or timeout
                if len(batch) >= self.batch_size or (batch and (time.time() - last_flush) > 5.0):
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
            address_mapping = {}  # address_key -> address_id
            
            # First pass: collect all unique addresses
            for item in batch:
                if item['type'] == 'address':
                    address_key = item['address_key']
                    if address_key not in address_mapping:
                        address_ops.append(item['data'])
                        address_mapping[address_key] = len(address_ops)  # Temporary ID for now
            
            # Bulk insert addresses and get real IDs
            if address_ops:
                real_address_ids = self._bulk_insert_addresses(address_ops)
                # Update mapping with real IDs
                address_keys = list(address_mapping.keys())
                for i, address_key in enumerate(address_keys):
                    if i < len(real_address_ids) and real_address_ids[i] is not None and real_address_ids[i] > 0:
                        address_mapping[address_key] = real_address_ids[i]
                    else:
                        logger.warning(f"Failed to get valid address_id for {address_key}")
                        # Remove from mapping to skip related transactions/blocks
                        address_mapping.pop(address_key, None)
            
            # Second pass: prepare transactions and blocks with correct address IDs
            for item in batch:
                if item['type'] == 'transaction':
                    address_key = item['address_key']
                    address_id = address_mapping.get(address_key)
                    # Only add transaction if we have a valid address_id
                    if address_id and address_id > 0:
                        tx_data = list(item['data'])
                        tx_data[3] = address_id  # address_id field
                        transaction_ops.append(tuple(tx_data))
                    else:
                        logger.warning(f"Skipping transaction for address_key {address_key} - no valid address_id")
                    
                elif item['type'] == 'block':
                    address_key = item['address_key']
                    address_id = address_mapping.get(address_key)
                    # Only add block record if we have a valid address_id
                    if address_id and address_id > 0:
                        block_data = list(item['data'])
                        block_data[0] = address_id  # address_id field
                        block_ops.append(tuple(block_data))
                    else:
                        logger.warning(f"Skipping block record for address_key {address_key} - no valid address_id")
            
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
        """Bulk insert addresses and return list of inserted IDs."""
        if not address_ops:
            return []
        
        start_time = time.time()
        
        try:
            # Insert addresses one by one to get proper IDs
            inserted_ids = []
            for address_op in address_ops:
                try:
                    result = self.db_manager.execute_query(self.prepared_statements['address_upsert'], address_op)
                    if result and result[0]:
                        inserted_ids.append(result[0]['id'])  # Get the actual inserted ID
                    else:
                        # If no result (upsert case), get the existing ID
                        existing_result = self.db_manager.execute_query("SELECT id FROM p2pk_addresses WHERE address = %s", (address_op[0],))
                        if existing_result and existing_result[0]:
                            inserted_ids.append(existing_result[0]['id'])
                        else:
                            logger.warning(f"Failed to get address_id for {address_op[0]}")
                            inserted_ids.append(None)  # Mark as failed
                except Exception as e:
                    logger.error(f"Failed to insert address {address_op[0]}: {e}")
                    inserted_ids.append(None)  # Mark as failed
            
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
            logger.error(f"Bulk address insert error: {e}")
            return [None] * len(address_ops)  # Return None for all failed inserts
    
    def _bulk_insert_transactions(self, transaction_ops: List[Tuple]):
        """Bulk insert transactions."""
        if not transaction_ops:
            return
        
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
            
            # Add to queue (non-blocking)
            try:
                self.write_queue.put({
                    'type': 'address',
                    'data': address_data,
                    'address_key': p2pk_transaction['public_key_hex'][:34]
                }, block=False)
                
                # Add transaction data
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
                }, block=False)
                
                # Add block data
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
                }, block=False)
                
                with stats_lock:
                    performance_stats['queue_operations'] += 1
                with metrics_lock:
                    performance_metrics['queue_operations'] += 1
                    
            except queue.Full:
                logger.warning("Write queue full, dropping transaction")
                
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
        logger.info("Beast mode database manager shutdown complete")


def ensure_scan_progress_row(db_manager):
    """Ensure the scan_progress row for the beast mode scanner exists."""
    try:
        query = "SELECT 1 FROM scan_progress WHERE scanner_name = %s"
        result = db_manager.execute_query(query, ('beast_mode_p2pk_scanner',))
        if not result:
            insert_query = """
            INSERT INTO scan_progress (scanner_name, last_scanned_block, total_blocks_scanned, last_updated)
            VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
            """
            db_manager.execute_command(insert_query, ('beast_mode_p2pk_scanner', 0, 0))
            logger.info("Created scan_progress row for beast_mode_p2pk_scanner.")
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
        
        # Fall back to beast mode scanner progress
        result = db_manager.execute_query(query, ('beast_mode_p2pk_scanner',))
        if result and result[0]['last_scanned_block'] > 0:
            logger.info(f"Found progress from beast mode scanner: block {result[0]['last_scanned_block']}")
            return result[0]['last_scanned_block']
        
        # No progress found, start from beginning
        logger.info("No previous progress found, starting from block 0")
        return 0
        
    except Exception as e:
        logger.error(f"Error getting scan progress: {e}")
        return 0


def update_scan_progress(db_manager, block_height: int, blocks_scanned: int = 1):
    """Update the scan progress for beast mode scanner."""
    try:
        query = """
        UPDATE scan_progress 
        SET last_scanned_block = %s, total_blocks_scanned = total_blocks_scanned + %s, last_updated = CURRENT_TIMESTAMP
        WHERE scanner_name = %s
        """
        db_manager.execute_command(query, (block_height, blocks_scanned, 'beast_mode_p2pk_scanner'))
    except Exception as e:
        logger.error(f"Error updating scan progress: {e}")


def is_p2pk_script(script_pub_key: Dict[str, Any]) -> Optional[str]:
    """Check if a script is a P2PK script and return the public key."""
    try:
        if script_pub_key.get('type') == 'pubkey':
            asm = script_pub_key.get('asm', '')
            if asm and 'OP_CHECKSIG' in asm:
                parts = asm.split()
                if len(parts) >= 2 and parts[-1] == 'OP_CHECKSIG':
                    public_key = parts[0]
                    if len(public_key) == 130 and public_key.startswith('04'):
                        return public_key
                    elif len(public_key) == 66 and public_key.startswith(('02', '03')):
                        return public_key
        return None
    except Exception as e:
        logger.debug(f"Error parsing script: {e}")
        return None


def process_transaction(tx: Dict[str, Any], block_height: int, block_time: int) -> List[Dict[str, Any]]:
    """Process a transaction and extract P2PK addresses."""
    p2pk_transactions = []
    try:
        # Process outputs (receiving)
        for vout in tx.get('vout', []):
            script_pub_key = vout.get('scriptPubKey', {})
            public_key = is_p2pk_script(script_pub_key)
            if public_key:
                p2pk_transactions.append({
                    'txid': tx['txid'],
                    'block_height': block_height,
                    'block_time': datetime.fromtimestamp(block_time),
                    'public_key_hex': public_key,
                    'amount_satoshi': int(vout['value'] * 100000000),
                    'is_input': False
                })
        
        # Process inputs (spending)
        for vin in tx.get('vin', []):
            if 'txid' in vin and 'vout' in vin:
                try:
                    prev_tx = bitcoin_rpc.get_raw_transaction(vin['txid'])
                    if prev_tx and 'vout' in prev_tx:
                        prev_vout = prev_tx['vout'][vin['vout']]
                        script_pub_key = prev_vout.get('scriptPubKey', {})
                        public_key = is_p2pk_script(script_pub_key)
                        if public_key:
                            p2pk_transactions.append({
                                'txid': tx['txid'],
                                'block_height': block_height,
                                'block_time': datetime.fromtimestamp(block_time),
                                'public_key_hex': public_key,
                                'amount_satoshi': int(prev_vout['value'] * 100000000),
                                'is_input': True
                            })
                except Exception as e:
                    logger.debug(f"Could not process input transaction {vin['txid']}: {e}")
    except Exception as e:
        logger.error(f"Error processing transaction {tx.get('txid', 'unknown')}: {e}")
    return p2pk_transactions


def worker(block_queue: queue.Queue, thread_name: str, db_manager: BeastModeDatabaseManager):
    """Worker thread that processes blocks."""
    global total_blocks_scanned
    
    while not stop_event.is_set():
        try:
            block_height = block_queue.get(timeout=1.0)
            if block_height is None:  # Sentinel value
                break
            
            # Track RPC timing
            rpc_start = time.time()
            
            try:
                # Get block data
                block_data = bitcoin_rpc.get_block_by_height(block_height)
                if not block_data:
                    logger.error(f"Failed to get block {block_height}")
                    with metrics_lock:
                        performance_metrics['blocks_failed'] += 1
                    continue
                
                # Track RPC performance
                rpc_time = time.time() - rpc_start
                with metrics_lock:
                    performance_metrics['rpc_calls'] += 1
                    performance_metrics['rpc_time_total'] += rpc_time
                    performance_metrics['rpc_time_avg'] = performance_metrics['rpc_time_total'] / performance_metrics['rpc_calls']
                    if rpc_time < performance_metrics['rpc_time_min']:
                        performance_metrics['rpc_time_min'] = rpc_time
                    if rpc_time > performance_metrics['rpc_time_max']:
                        performance_metrics['rpc_time_max'] = rpc_time
                
                # Process transactions
                block_time = block_data['time']
                transactions = block_data.get('tx', [])
                
                for tx in transactions:
                    p2pk_transactions = process_transaction(tx, block_height, block_time)
                    
                    # Add to database manager
                    for p2pk_tx in p2pk_transactions:
                        db_manager.add_transaction(p2pk_tx)
                    
                    with metrics_lock:
                        performance_metrics['transactions_processed'] += 1
                        performance_metrics['p2pk_found'] += len(p2pk_transactions)
                
                # Update counters
                with blocks_scanned_lock:
                    total_blocks_scanned += 1
                with metrics_lock:
                    performance_metrics['blocks_processed'] += 1
                
                # Update thread status
                with thread_status_lock:
                    thread_status[thread_name] = f"Processed block {block_height}"
                
            except Exception as e:
                logger.error(f"Error processing block {block_height}: {e}")
                with metrics_lock:
                    performance_metrics['blocks_failed'] += 1
            
            block_queue.task_done()
            
        except queue.Empty:
            continue
        except Exception as e:
            logger.error(f"Worker {thread_name} error: {e}")
            break
    
    with thread_status_lock:
        thread_status[thread_name] = "Stopped"


def keyboard_listener():
    """Listen for keyboard input to stop scanning gracefully."""
    while not stop_event.is_set():
        if select.select([sys.stdin], [], [], 0.1)[0]:
            line = sys.stdin.readline()
            if line.strip().lower() == 'q':
                logger.info("Received quit signal")
                stop_event.set()
                break


def report_thread_status():
    """Report the status of all worker threads."""
    print("\n🧵 THREAD STATUS:")
    with thread_status_lock:
        for thread_name, status in thread_status.items():
            print(f"  {thread_name}: {status}")


def report_performance_stats():
    """Report basic performance statistics."""
    with stats_lock:
        print(f"\n📊 PERFORMANCE STATS:")
        print(f"  Total transactions processed: {performance_stats['total_transactions_processed']:,}")
        print(f"  Total addresses found: {performance_stats['total_addresses_found']:,}")
        print(f"  Batch inserts performed: {performance_stats['batch_inserts_performed']:,}")
        print(f"  Queue operations: {performance_stats['queue_operations']:,}")


def report_detailed_performance_metrics():
    """Report detailed performance metrics."""
    print("\n" + "="*80)
    print("BEAST MODE SCANNER PERFORMANCE PROFILE")
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
        
        print(f"\n❌ ERROR ANALYSIS:")
        print(f"  Foreign key errors: {performance_metrics['foreign_key_errors']:,}")
        print(f"  Address insert errors: {performance_metrics['address_insert_errors']:,}")
        print(f"  Transaction insert errors: {performance_metrics['transaction_insert_errors']:,}")
        print(f"  Block insert errors: {performance_metrics['block_insert_errors']:,}")
        
        print(f"\n📦 QUEUE PERFORMANCE:")
        print(f"  Queue operations: {performance_metrics['queue_operations']:,}")
        print(f"  Queue full count: {performance_metrics['queue_full_count']:,}")
        print(f"  Queue efficiency: {((performance_metrics['queue_operations'] - performance_metrics['queue_full_count']) / max(performance_metrics['queue_operations'], 1) * 100):.2f}%")
        
        # Calculate throughput
        if performance_metrics['rpc_time_total'] > 0:
            blocks_per_second = performance_metrics['blocks_processed'] / performance_metrics['rpc_time_total']
            print(f"\n⚡ THROUGHPUT ANALYSIS:")
            print(f"  Blocks per second: {blocks_per_second:.2f}")
            print(f"  Transactions per second: {performance_metrics['transactions_processed'] / max(performance_metrics['rpc_time_total'], 1):.2f}")
            print(f"  P2PK addresses per second: {performance_metrics['p2pk_found'] / max(performance_metrics['rpc_time_total'], 1):.2f}")
        
        print("="*80)


def profiled_main():
    """Main function that will be profiled."""
    import argparse
    
    parser = argparse.ArgumentParser(description='BEAST MODE P2PK Scanner - Function Profiler')
    parser.add_argument('--start-block', type=int, help='Block height to start scanning from')
    parser.add_argument('--end-block', type=int, help='Block height to stop scanning at')
    parser.add_argument('--threads', type=int, default=8, help='Number of threads (default: 8)')
    parser.add_argument('--batch-size', type=int, default=1000, help='Batch size for database operations (default: 1000)')
    parser.add_argument('--queue-size', type=int, default=50000, help='Write queue size (default: 50000)')
    parser.add_argument('--reset', action='store_true', help='Reset scan progress and start from beginning')
    
    args = parser.parse_args()
    
    logger.info("🚀 Starting BEAST MODE P2PK Scanner with Function Profiling...")
    logger.info(f"🔥 Configuration: {args.threads} threads, batch size {args.batch_size}, queue size {args.queue_size}")
    
    if not bitcoin_rpc.test_connection():
        logger.error("Failed to connect to Bitcoin Core")
        return
    
    # Initialize beast mode database manager
    db_manager = BeastModeDatabaseManager(batch_size=args.batch_size, queue_size=args.queue_size)
    
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
            start_block = get_scan_progress(db_manager.db_manager)
            logger.info(f"Resuming from block {start_block}")
        
        end_block = args.end_block if args.end_block is not None else current_height
        threads = args.threads
        
        if start_block >= end_block:
            logger.info("Already up to date")
            return
        
        logger.info(f"🔥 Scanning blocks {start_block} to {end_block} using {threads} threads")
        start_time = time.time()
        
        # Start keyboard listener thread
        kb_thread = threading.Thread(target=keyboard_listener, daemon=True)
        kb_thread.start()
        
        # Initialize queue and workers
        block_queue = queue.Queue()
        next_block = start_block
        max_queue_depth = threads * 2
        
        # Fill the queue initially
        for _ in range(min(max_queue_depth, end_block - start_block + 1)):
            block_queue.put(next_block)
            next_block += 1
        
        # Start worker threads
        worker_threads = []
        for i in range(threads):
            tname = f"beast-worker-{i}"
            t = threading.Thread(target=worker, args=(block_queue, tname, db_manager), daemon=True)
            worker_threads.append(t)
            t.start()
        
        # Main monitoring loop
        total_blocks_to_scan = end_block - start_block + 1
        last_report = 0
        report_interval = 10  # seconds
        last_progress_update = start_block
        
        while True:
            # Refill the queue if needed
            while not stop_event.is_set() and block_queue.qsize() < max_queue_depth and next_block <= end_block:
                block_queue.put(next_block)
                next_block += 1
            
            # Check if all workers are done and queue is empty
            all_workers_done = not any(t.is_alive() for t in worker_threads)
            queue_empty = block_queue.empty()
            
            # Also check if we've processed all blocks
            with blocks_scanned_lock:
                scanned = total_blocks_scanned
            
            if (all_workers_done and queue_empty) or scanned >= total_blocks_to_scan:
                logger.info(f"Scan complete: {scanned}/{total_blocks_to_scan} blocks processed")
                break
            
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
                logger.info(f"🔥 Progress: {scanned}/{total_blocks_to_scan} blocks. Elapsed: {elapsed_str}, ETA: {eta_str}, Speed: {speed_str}")
                
                # Update scan progress
                current_progress = start_block + scanned
                if current_progress - last_progress_update >= 100:
                    update_scan_progress(db_manager.db_manager, current_progress, current_progress - last_progress_update)
                    last_progress_update = current_progress
                
                # Report performance stats
                report_performance_stats()
                
                if stop_event.is_set():
                    report_thread_status()
                    logger.info(f"Queue size: {block_queue.qsize()}")
                
                last_report = time.time()
            
            time.sleep(1)
        
        # Final processing
        block_queue.join()
        elapsed = time.time() - start_time
        elapsed_str = format_time_dd_hh_mm_ss(elapsed)
        
        # Calculate final statistics
        with blocks_scanned_lock:
            scanned = total_blocks_scanned
        
        blocks_per_second = scanned / elapsed if elapsed > 0 else 0
        
        logger.info(f"🚀 BEAST MODE scan completed in {elapsed_str}!")
        logger.info(f"🔥 Performance: {blocks_per_second:.2f} blocks per second")
        report_performance_stats()
        report_thread_status()
        
        logger.info(f"🔥 Final Progress: {scanned}/{total_blocks_to_scan} blocks scanned in {elapsed_str}")
        logger.info(f"🔥 Average Speed: {blocks_per_second:.2f} blocks/second")
        
        # Final progress update
        final_progress = start_block + scanned
        update_scan_progress(db_manager.db_manager, final_progress, final_progress - last_progress_update)
        
        # Generate detailed performance profile
        report_detailed_performance_metrics()
        
    except Exception as e:
        logger.error(f"BEAST MODE scan failed: {e}")
    finally:
        db_manager.shutdown()


def main():
    """Main function that sets up profiling and runs the scanner."""
    # Create a profiler
    profiler = cProfile.Profile()
    
    # Start profiling
    profiler.enable()
    
    try:
        # Run the profiled function
        profiled_main()
    finally:
        # Stop profiling
        profiler.disable()
        
        # Create stats object
        stats = pstats.Stats(profiler)
        
        # Sort by cumulative time (most time-consuming functions first)
        stats.sort_stats('cumulative')
        
        # Print the top 50 functions by time
        print("\n" + "="*80)
        print("FUNCTION-LEVEL PROFILING RESULTS")
        print("="*80)
        print("Top 50 functions by cumulative time:")
        print("-" * 80)
        
        # Capture the output
        output = io.StringIO()
        stats.stream = output
        stats.print_stats(50)
        
        # Print the results
        print(output.getvalue())
        
        # Also print callers and callees for the top functions
        print("\n" + "="*80)
        print("TOP FUNCTION CALLERS (what calls the slowest functions)")
        print("="*80)
        
        output2 = io.StringIO()
        stats.stream = output2
        stats.print_callers(10)
        print(output2.getvalue())
        
        print("\n" + "="*80)
        print("TOP FUNCTION CALLEES (what the slowest functions call)")
        print("="*80)
        
        output3 = io.StringIO()
        stats.stream = output3
        stats.print_callees(10)
        print(output3.getvalue())


if __name__ == "__main__":
    main() 