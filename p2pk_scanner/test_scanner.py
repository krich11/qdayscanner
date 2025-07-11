#!/usr/bin/env python3
"""
Test script for P2PK Scanner.
Verifies setup and tests basic functionality.
"""

import sys
import os
from pathlib import Path

# Add parent directory to path to import utils
sys.path.append(str(Path(__file__).parent.parent))

from utils.config import config
from utils.database import db_manager
from bitcoin_rpc import bitcoin_rpc


def test_configuration():
    """Test configuration loading."""
    print("Testing configuration...")
    
    print(f"Bitcoin RPC Host: {config.BITCOIN_RPC_HOST}")
    print(f"Bitcoin RPC Port: {config.BITCOIN_RPC_PORT}")
    print(f"Database Host: {config.DB_HOST}")
    print(f"Database Name: {config.DB_NAME}")
    print(f"Scan Batch Size: {config.SCAN_BATCH_SIZE}")
    
    if config.validate_config():
        print("✓ Configuration validation passed")
        return True
    else:
        print("✗ Configuration validation failed")
        return False


def test_database_connection():
    """Test database connection."""
    print("\nTesting database connection...")
    
    try:
        # Test basic connection
        result = db_manager.execute_query("SELECT version()")
        print(f"✓ Database connected: {result[0]['version']}")
        
        # Test table existence
        tables = ['p2pk_addresses', 'p2pk_transactions', 'p2pk_address_blocks', 'scan_progress']
        for table in tables:
            exists = db_manager.table_exists(table)
            count = db_manager.get_table_count(table)
            print(f"  Table {table}: {'✓' if exists else '✗'} ({count} rows)")
        
        return True
    except Exception as e:
        print(f"✗ Database connection failed: {e}")
        return False


def test_bitcoin_rpc():
    """Test Bitcoin RPC connection."""
    print("\nTesting Bitcoin RPC connection...")
    
    try:
        if bitcoin_rpc.test_connection():
            print("✓ Bitcoin RPC connection successful")
            
            # Get some basic info
            info = bitcoin_rpc.get_blockchain_info()
            print(f"  Chain: {info.get('chain', 'unknown')}")
            print(f"  Blocks: {info.get('blocks', 0)}")
            print(f"  Headers: {info.get('headers', 0)}")
            
            return True
        else:
            print("✗ Bitcoin RPC connection failed")
            return False
    except Exception as e:
        print(f"✗ Bitcoin RPC test failed: {e}")
        return False


def test_sample_scan():
    """Test scanning a small sample of blocks."""
    print("\nTesting sample scan...")
    
    try:
        # Get current height
        current_height = bitcoin_rpc.get_block_count()
        print(f"Current blockchain height: {current_height}")
        
        # Scan last 10 blocks as a test
        start_block = max(0, current_height - 10)
        end_block = current_height
        
        print(f"Scanning blocks {start_block} to {end_block}")
        
        # Import scanner here to avoid circular imports
        from scanner import P2PKScanner
        
        scanner = P2PKScanner()
        p2pk_found = scanner.scan_blocks_range(start_block, end_block)
        
        print(f"✓ Sample scan completed - Found {p2pk_found} P2PK addresses")
        return True
        
    except Exception as e:
        print(f"✗ Sample scan failed: {e}")
        return False


def main():
    """Run all tests."""
    print("P2PK Scanner Test Suite")
    print("=" * 50)
    
    tests = [
        ("Configuration", test_configuration),
        ("Database Connection", test_database_connection),
        ("Bitcoin RPC", test_bitcoin_rpc),
        ("Sample Scan", test_sample_scan)
    ]
    
    passed = 0
    total = len(tests)
    
    for test_name, test_func in tests:
        try:
            if test_func():
                passed += 1
        except Exception as e:
            print(f"✗ {test_name} test failed with exception: {e}")
    
    print("\n" + "=" * 50)
    print(f"Test Results: {passed}/{total} tests passed")
    
    if passed == total:
        print("✓ All tests passed! P2PK scanner is ready to use.")
        return 0
    else:
        print("✗ Some tests failed. Please check the configuration and setup.")
        return 1


if __name__ == "__main__":
    sys.exit(main()) 