#!/usr/bin/env python3
"""
Test script to verify auto-pause functionality in hydra_mode_scanner.py
"""

import sys
import threading
import time
from pathlib import Path

# Add parent directory to path to import utils
sys.path.append(str(Path(__file__).parent.parent))

# Import the global events and functions from hydra_mode_scanner
from hydra_mode_scanner import (
    stop_event, pause_event, auto_pause_enabled, 
    auto_pause_threshold, auto_resume_threshold, check_auto_pause
)

class MockDatabaseManager:
    """Mock database manager for testing auto-pause functionality."""
    
    def __init__(self, queue_size=1000000):
        self.write_queue = type('MockQueue', (), {
            'qsize': lambda: queue_size
        })()

def test_auto_pause_functionality():
    """Test the auto-pause functionality with different queue depths."""
    print("🧪 Testing auto-pause functionality...")
    
    # Test initial state
    print(f"Initial state - stop_event: {stop_event.is_set()}, pause_event: {pause_event.is_set()}")
    print(f"Auto-pause enabled: {auto_pause_enabled}")
    print(f"Pause threshold: {auto_pause_threshold:,}")
    print(f"Resume threshold: {auto_resume_threshold:,}")
    
    # Test with low queue depth (should not pause)
    print("\n📊 Test 1: Low queue depth (should not pause)")
    mock_db_low = MockDatabaseManager(5000)
    pause_event.clear()  # Ensure not paused
    result = check_auto_pause(mock_db_low)
    print(f"Queue depth: 5,000, Result: {result}, Pause event: {pause_event.is_set()}")
    
    # Test with high queue depth (should pause)
    print("\n📊 Test 2: High queue depth (should pause)")
    mock_db_high = MockDatabaseManager(60000)
    pause_event.clear()  # Ensure not paused
    result = check_auto_pause(mock_db_high)
    print(f"Queue depth: 60,000, Result: {result}, Pause event: {pause_event.is_set()}")
    
    # Test with medium queue depth while paused (should resume)
    print("\n📊 Test 3: Medium queue depth while paused (should resume)")
    mock_db_medium = MockDatabaseManager(5000)
    pause_event.set()  # Ensure paused
    result = check_auto_pause(mock_db_medium)
    print(f"Queue depth: 5,000, Result: {result}, Pause event: {pause_event.is_set()}")
    
    # Test with high queue depth while paused (should stay paused)
    print("\n📊 Test 4: High queue depth while paused (should stay paused)")
    mock_db_high_again = MockDatabaseManager(60000)
    pause_event.set()  # Ensure paused
    result = check_auto_pause(mock_db_high_again)
    print(f"Queue depth: 60,000, Result: {result}, Pause event: {pause_event.is_set()}")
    
    # Reset for next test
    pause_event.clear()
    print(f"\n✅ Reset - pause_event: {pause_event.is_set()}")
    
    print("\n✅ Auto-pause functionality test completed successfully!")

if __name__ == "__main__":
    test_auto_pause_functionality() 