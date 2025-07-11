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
        while not stop_event.is_set():
            try:
                height = block_queue.get(timeout=0.5)
            except queue.Empty:
                break
            if stop_event.is_set():
                break
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
    args = parser.parse_args()

    logger.info("Starting Multithreaded P2PK scanner...")
    if not bitcoin_rpc.test_connection():
        logger.error("Failed to connect to Bitcoin Core")
        return

    # Get blockchain height
    blockchain_info = bitcoin_rpc.get_blockchain_info()
    current_height = blockchain_info['blocks']
    logger.info(f"Current blockchain height: {current_height}")

    start_block = args.start_block if args.start_block is not None else 0
    end_block = args.end_block if args.end_block is not None else current_height
    threads = args.threads
    batch_size = args.batch_size

    logger.info(f"Scanning blocks {start_block} to {end_block} using {threads} threads (queue fill batch size: {batch_size})")
    total_found = 0
    start_time = time.time()

    # Start keyboard listener thread
    kb_thread = threading.Thread(target=keyboard_listener, daemon=True)
    kb_thread.start()

    block_queue = queue.Queue()
    for h in range(start_block, end_block + 1):
        block_queue.put(h)

    worker_threads = []
    for i in range(threads):
        tname = f"worker-{i}"
        t = threading.Thread(target=worker, args=(block_queue, tname), daemon=True)
        worker_threads.append(t)
        t.start()

    last_report = 0
    while any(t.is_alive() for t in worker_threads):
        time.sleep(1)
        # Optionally, report progress every 5 seconds
        if time.time() - last_report > 5:
            report_thread_status()
            last_report = time.time()

    block_queue.join()
    elapsed = time.time() - start_time
    logger.info(f"Multithreaded scan completed in {elapsed:.1f} seconds.")
    report_thread_status()

if __name__ == "__main__":
    main() 