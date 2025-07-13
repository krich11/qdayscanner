#!/usr/bin/env python3
"""
Verify database is clean after reset.
"""

from utils.database import get_cursor

def verify_clean_database():
    """Verify that the database is clean after reset."""
    cursor = get_cursor()
    
    try:
        # Check P2PK tables
        cursor.execute('SELECT COUNT(*) FROM p2pk_addresses')
        address_count = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(*) FROM p2pk_transactions')
        transaction_count = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(*) FROM p2pk_address_blocks')
        block_count = cursor.fetchone()[0]
        
        # Check scan progress
        cursor.execute('SELECT scanner_name, last_block_scanned FROM scan_progress')
        progress_rows = cursor.fetchall()
        
        print("🔍 Database Cleanliness Verification:")
        print("=" * 40)
        print(f"P2PK addresses: {address_count}")
        print(f"P2PK transactions: {transaction_count}")
        print(f"P2PK address blocks: {block_count}")
        print("\nScan progress:")
        for row in progress_rows:
            print(f"  {row[0]}: block {row[1]}")
        
        # Verify clean state
        is_clean = (address_count == 0 and 
                   transaction_count == 0 and 
                   block_count == 0 and
                   len(progress_rows) == 1)
        
        if is_clean:
            print("\n✅ Database is CLEAN and ready for full scan!")
            return True
        else:
            print("\n❌ Database is NOT clean - contains existing data!")
            return False
            
    except Exception as e:
        print(f"❌ Error verifying database: {e}")
        return False
    finally:
        cursor.close()

if __name__ == "__main__":
    verify_clean_database() 