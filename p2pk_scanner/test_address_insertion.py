#!/usr/bin/env python3
"""
Test script to debug address insertion issues in the HYDRA scanner.
"""

import sys
import os
from pathlib import Path

# Add parent directory to path to import utils
sys.path.append(str(Path(__file__).parent.parent))

from utils.database import db_manager
import logging

# Set up logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

def test_address_insertion():
    """Test the exact address insertion scenario that's failing."""
    
    # Test data from the error logs
    test_address_key = "028a42df8c1dee98d4dc113201aec7eec0"
    test_public_key = "028a42df8c1dee98d4dc113201aec7eec0"  # This is the same as address_key in this case
    test_block = 272000
    test_txid = "c709969e62d6cdf2582c98c114566d16fd20e2279631ad92134640c522f6d9bb"
    
    print(f"Testing address insertion for: {test_address_key}")
    print(f"Public key: {test_public_key}")
    print(f"Block: {test_block}")
    print(f"TXID: {test_txid}")
    
    # Check if address already exists
    existing_result = db_manager.execute_query(
        "SELECT id FROM p2pk_addresses WHERE address = %s", 
        (test_address_key,)
    )
    
    if existing_result and existing_result[0] and existing_result[0].get('id'):
        address_id = existing_result[0]['id']
        print(f"✅ Address exists with ID: {address_id}")
        
        # Test the update logic
        try:
            update_result = db_manager.execute_command("""
                UPDATE p2pk_addresses 
                SET last_seen_block = GREATEST(last_seen_block, %s),
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = %s AND last_seen_block < %s
            """, (test_block, address_id, test_block))
            print(f"✅ Update successful, rows affected: {update_result}")
        except Exception as e:
            print(f"❌ Update failed: {e}")
        
        # Test the upsert logic
        try:
            address_op = (test_address_key, test_public_key, test_block, test_txid, test_block, 0, 0)
            upsert_result = db_manager.execute_upsert("""
                INSERT INTO p2pk_addresses 
                (address, public_key_hex, first_seen_block, first_seen_txid, last_seen_block, 
                 total_received_satoshi, current_balance_satoshi)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (address) DO UPDATE SET
                    last_seen_block = EXCLUDED.last_seen_block,
                    updated_at = CURRENT_TIMESTAMP
                RETURNING id
            """, address_op)
            
            if upsert_result and upsert_result.get('id'):
                returned_id = upsert_result['id']
                print(f"✅ Upsert successful, returned ID: {returned_id}")
            else:
                print(f"❌ Upsert failed - no ID returned")
                print(f"Upsert result: {upsert_result}")
        except Exception as e:
            print(f"❌ Upsert failed: {e}")
        
        # Verify the address still exists and has correct data
        final_result = db_manager.execute_query(
            "SELECT id, address, last_seen_block FROM p2pk_addresses WHERE address = %s", 
            (test_address_key,)
        )
        if final_result:
            final_data = final_result[0]
            print(f"✅ Final verification - ID: {final_data['id']}, Last seen: {final_data['last_seen_block']}")
        else:
            print(f"❌ Final verification failed - address not found")
    
    else:
        print(f"❌ Address not found in database")
        
        # Test insertion of new address
        try:
            address_op = (test_address_key, test_public_key, test_block, test_txid, test_block, 0, 0)
            insert_result = db_manager.execute_upsert("""
                INSERT INTO p2pk_addresses 
                (address, public_key_hex, first_seen_block, first_seen_txid, last_seen_block, 
                 total_received_satoshi, current_balance_satoshi)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, address_op)
            
            if insert_result and insert_result.get('id'):
                address_id = insert_result['id']
                print(f"✅ Insert successful, new ID: {address_id}")
            else:
                print(f"❌ Insert failed - no ID returned")
                print(f"Insert result: {insert_result}")
        except Exception as e:
            print(f"❌ Insert failed: {e}")

if __name__ == "__main__":
    test_address_insertion() 