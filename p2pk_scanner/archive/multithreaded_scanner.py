#!/usr/bin/env python3
"""
Multithreaded P2PK Scanner for Bitcoin Quantum Vulnerability Scanner.
Scans the blockchain in parallel using multiple threads for faster performance.
Works with the existing database schema and strategy.
Supports graceful shutdown with 'q' and reports thread statuses.
"""

import sys
import os
import logging
import time
from pathlib import Path
from typing import Dict, Any, List, Optional
from datetime import datetime
import threading
import queue
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
        logging.FileHandler(Path(__file__).parent.parent / 'logs' / 'p2pk_multithreaded_scanner.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# For graceful shutdown
stop_event = threading.Event()
thread_status = {}
thread_status_lock = threading.Lock()

# Add at the top, after thread_status and thread_status_lock
total_blocks_scanned = 0
blocks_scanned_lock = threading.Lock()


def format_time_dd_hh_mm_ss(seconds: float) -> str:
    """Format time in DD:HH:MM:SS format."""
    if seconds < 0:
        return "00D:00H:00M:00S"
    
    days, rem = divmod(int(seconds), 86400)
    hours, rem = divmod(rem, 3600)
    minutes, secs = divmod(rem, 60)
    
    return f"{days:02d}D:{hours:02d}H:{minutes:02d}M:{secs:02d}S"


def ensure_scan_progress_row(db_manager):
    """Ensure the scan_progress row for the multithreaded scanner exists."""
    try:
        query = "SELECT 1 FROM scan_progress WHERE scanner_name = %s"
        result = db_manager.execute_query(query, ('multithreaded_p2pk_scanner',))
        if not result:
            # Insert initial row
            insert_query = """
            INSERT INTO scan_progress (scanner_name, last_scanned_block, total_blocks_scanned, last_updated)
            VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
            """
            db_manager.execute_command(insert_query, ('multithreaded_p2pk_scanner', 0, 0))
            logger.info("Created scan_progress row for multithreaded_p2pk_scanner.")
    except Exception as e:
        logger.error(f"Error ensuring scan_progress row: {e}")


def get_scan_progress(db_manager) -> int:
    """Get the last scanned block height for multithreaded scanner."""
    try:
        query = "SELECT last_scanned_block FROM scan_progress WHERE scanner_name = %s"
        result = db_manager.execute_query(query, ('multithreaded_p2pk_scanner',))
        return result[0]['last_scanned_block'] if result else 0
    except Exception as e:
        logger.error(f"Error getting scan progress: {e}")
        return 0


def update_scan_progress(db_manager, block_height: int, blocks_scanned: int = 1):
    """Update the scan progress for multithreaded scanner."""
    try:
        query = """
        UPDATE scan_progress 
        SET last_scanned_block = %s, total_blocks_scanned = total_blocks_scanned + %s, last_updated = CURRENT_TIMESTAMP
        WHERE scanner_name = %s
        """
        db_manager.execute_command(query, (block_height, blocks_scanned, 'multithreaded_p2pk_scanner'))
    except Exception as e:
        logger.error(f"Error updating scan progress: {e}")


def is_p2pk_script(script_pub_key: Dict[str, Any]) -> Optional[str]:
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
    p2pk_transactions = []
    try:
        # Outputs (receiving)
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
        # Inputs (spending)
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


def save_p2pk_transaction(p2pk_transaction: Dict[str, Any], db_manager: DatabaseManager):
    try:
        # Upsert address (ON CONFLICT DO UPDATE)
        upsert_query = """
        INSERT INTO p2pk_addresses 
        (address, public_key_hex, first_seen_block, first_seen_txid, last_seen_block, 
         total_received_satoshi, current_balance_satoshi)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (address) DO UPDATE SET
            last_seen_block = EXCLUDED.last_seen_block,
            updated_at = CURRENT_TIMESTAMP
        RETURNING id
        """
        address = p2pk_transaction['public_key_hex'][:34]
        initial_balance = p2pk_transaction['amount_satoshi'] if not p2pk_transaction['is_input'] else 0
        result = db_manager.execute_query(upsert_query, (
            address,
            p2pk_transaction['public_key_hex'],
            p2pk_transaction['block_height'],
            p2pk_transaction['txid'],
            p2pk_transaction['block_height'],
            initial_balance,
            initial_balance
        ))
        address_id = result[0]['id']
        # Save transaction
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
        # Save to address_blocks
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
    except Exception as e:
        logger.error(f"Error saving P2PK transaction: {e}")


def worker(block_queue: queue.Queue, thread_name: str):
    db_manager = DatabaseManager()
    with thread_status_lock:
        thread_status[thread_name] = 'running'
    total_found = 0
    try:
        while True:
            try:
                height = block_queue.get(timeout=0.5)
            except queue.Empty:
                if stop_event.is_set():
                    break
                else:
                    continue
            # If stop_event is set, continue draining the queue until empty
            try:
                block = bitcoin_rpc.get_block_by_height(height)
                block_time = block['time']
                for tx in block.get('tx', []):
                    p2pk_transactions = process_transaction(tx, height, block_time)
                    for p2pk_transaction in p2pk_transactions:
                        save_p2pk_transaction(p2pk_transaction, db_manager)
                        total_found += 1
            except Exception as e:
                logger.error(f"Error processing block {height}: {e}")
            finally:
                block_queue.task_done()
                # Increment global blocks scanned counter
                global total_blocks_scanned
                with blocks_scanned_lock:
                    total_blocks_scanned += 1
            # Exit if stop_event is set and queue is empty
            if stop_event.is_set() and block_queue.empty():
                break
        with thread_status_lock:
            thread_status[thread_name] = 'finished'
    except Exception as e:
        logger.error(f"Worker {thread_name} error: {e}")
        with thread_status_lock:
            thread_status[thread_name] = f'error: {e}'
    finally:
        db_manager.close()


def keyboard_listener():
    print("Press 'q' then Enter at any time to stop scanning gracefully.")
    while not stop_event.is_set():
        try:
            if sys.stdin in select.select([sys.stdin], [], [], 1)[0]:
                line = sys.stdin.readline()
                if line.strip().lower() == 'q':
                    print("Graceful stop requested. Waiting for threads to finish...")
                    stop_event.set()
                    break
        except Exception:
            break


def report_thread_status():
    with thread_status_lock:
        logger.info("Thread status report:")
        for name, status in thread_status.items():
            logger.info(f"  {name}: {status}")


def main():
    import argparse
    parser = argparse.ArgumentParser(description='Multithreaded P2PK Scanner')
    parser.add_argument('--start-block', type=int, help='Block height to start scanning from')
    parser.add_argument('--end-block', type=int, help='Block height to stop scanning at')
    parser.add_argument('--threads', type=int, default=8, help='Number of threads (default: 8)')
    parser.add_argument('--batch-size', type=int, default=100, help='Batch size for queue fill (default: 100)')
    parser.add_argument('--reset', action='store_true', help='Reset scan progress and start from beginning')
    args = parser.parse_args()

    logger.info("Starting Multithreaded P2PK scanner...")
    if not bitcoin_rpc.test_connection():
        logger.error("Failed to connect to Bitcoin Core")
        return

    # Main thread keeps its DB connection open
    db_manager = DatabaseManager()
    try:
        # Ensure scan_progress row exists
        ensure_scan_progress_row(db_manager)

        # Get blockchain height
        blockchain_info = bitcoin_rpc.get_blockchain_info()
        current_height = blockchain_info['blocks']
        logger.info(f"Current blockchain height: {current_height}")

        # Determine scan range with resume functionality
        if args.reset:
            start_block = args.start_block if args.start_block is not None else 0
            logger.info(f"Reset requested. Starting from block {start_block}")
        elif args.start_block is not None:
            start_block = args.start_block
            logger.info(f"Starting from specified block {start_block}")
        else:
            start_block = get_scan_progress(db_manager)
            logger.info(f"Resuming from block {start_block}")

        end_block = args.end_block if args.end_block is not None else current_height
        threads = args.threads
        batch_size = args.batch_size

        if start_block >= end_block:
            logger.info("Already up to date")
            return

        logger.info(f"Scanning blocks {start_block} to {end_block} using {threads} threads (queue fill batch size: {batch_size})")
        total_found = 0
        start_time = time.time()

        # Start keyboard listener thread
        kb_thread = threading.Thread(target=keyboard_listener, daemon=True)
        kb_thread.start()

        block_queue = queue.Queue()
        next_block = start_block
        max_queue_depth = threads * 2

        # Fill the queue initially
        for _ in range(min(max_queue_depth, end_block - start_block + 1)):
            block_queue.put(next_block)
            next_block += 1

        worker_threads = []
        for i in range(threads):
            tname = f"worker-{i}"
            t = threading.Thread(target=worker, args=(block_queue, tname), daemon=True)
            worker_threads.append(t)
            t.start()

        # --- Main loop: refill queue and handle shutdown ---
        total_blocks_to_scan = end_block - start_block + 1
        last_report = 0
        report_interval = 10  # seconds
        status_reporting = False  # Initialize status_reporting variable
        start_time = time.time()  # Move start_time here for progress calculations
        last_progress_update = start_block  # Track last progress update
        
        while True:
            # Refill the queue if needed (unless stop_event is set or all blocks queued)
            while not stop_event.is_set() and block_queue.qsize() < max_queue_depth and next_block <= end_block:
                block_queue.put(next_block)
                next_block += 1
            # Check if all workers are done and queue is empty
            all_workers_done = not any(t.is_alive() for t in worker_threads)
            queue_empty = block_queue.empty()
            if stop_event.is_set():
                status_reporting = True
            if all_workers_done and queue_empty:
                break
            # Always report progress every report_interval seconds
            if time.time() - last_report > report_interval:
                with blocks_scanned_lock:
                    scanned = total_blocks_scanned
                elapsed = time.time() - start_time
                if scanned > 0:
                    blocks_per_second = scanned / elapsed
                    remaining_blocks = total_blocks_to_scan - scanned
                    eta_seconds = int(remaining_blocks / blocks_per_second)
                    eta_str = format_time_dd_hh_mm_ss(eta_seconds)
                else:
                    eta_str = "calculating..."
                elapsed_str = format_time_dd_hh_mm_ss(elapsed)
                blocks_per_second = scanned / elapsed if elapsed > 0 else 0
                logger.info(f"Progress: {scanned}/{total_blocks_to_scan} blocks scanned. Elapsed: {elapsed_str}, ETA: {eta_str}, Speed: {blocks_per_second:.2f} blocks/sec")
                
                # Update scan progress periodically (every 100 blocks or so)
                current_progress = start_block + scanned
                if current_progress - last_progress_update >= 100:
                    update_scan_progress(db_manager, current_progress, current_progress - last_progress_update)
                    last_progress_update = current_progress
                
                if status_reporting:
                    report_thread_status()
                    logger.info(f"Queue size: {block_queue.qsize()}")
                last_report = time.time()
            time.sleep(1)

        block_queue.join()
        elapsed = time.time() - start_time
        elapsed_str = format_time_dd_hh_mm_ss(elapsed)
        
        # Calculate final statistics
        with blocks_scanned_lock:
            scanned = total_blocks_scanned
        
        blocks_per_second = scanned / elapsed if elapsed > 0 else 0
        
        logger.info(f"Multithreaded scan completed in {elapsed_str}.")
        logger.info(f"Performance: {blocks_per_second:.2f} blocks per second")
        report_thread_status()
        
        # Final block scan status report
        logger.info(f"Final Progress: {scanned}/{total_blocks_to_scan} blocks scanned in {elapsed_str}.")
        logger.info(f"Average Speed: {blocks_per_second:.2f} blocks/second")
        
        # Final progress update
        final_progress = start_block + scanned
        update_scan_progress(db_manager, final_progress, final_progress - last_progress_update)
    finally:
        db_manager.close()

if __name__ == "__main__":
    main() 