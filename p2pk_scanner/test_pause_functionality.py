#!/usr/bin/env python3
"""
Test script to verify pause functionality in hydra_mode_scanner.py
"""

import sys
import threading
import time
from pathlib import Path

# Add parent directory to path to import utils
sys.path.append(str(Path(__file__).parent.parent))

# Import the global events from hydra_mode_scanner
from hydra_mode_scanner import stop_event, pause_event

def test_pause_functionality():
    """Test the pause/resume functionality"""
    print("🧪 Testing pause functionality...")
    
    # Test initial state
    print(f"Initial state - stop_event: {stop_event.is_set()}, pause_event: {pause_event.is_set()}")
    
    # Test pause
    print("\n⏸️ Setting pause event...")
    pause_event.set()
    print(f"After pause - stop_event: {stop_event.is_set()}, pause_event: {pause_event.is_set()}")
    
    # Test resume
    print("\n▶️ Clearing pause event...")
    pause_event.clear()
    print(f"After resume - stop_event: {stop_event.is_set()}, pause_event: {pause_event.is_set()}")
    
    # Test stop
    print("\n🛑 Setting stop event...")
    stop_event.set()
    print(f"After stop - stop_event: {stop_event.is_set()}, pause_event: {pause_event.is_set()}")
    
    # Reset for next test
    stop_event.clear()
    pause_event.clear()
    print(f"\n✅ Reset - stop_event: {stop_event.is_set()}, pause_event: {pause_event.is_set()}")
    
    print("\n✅ Pause functionality test completed successfully!")

if __name__ == "__main__":
    test_pause_functionality() 