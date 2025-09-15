#!/usr/bin/env python3

# Quick test script to verify date parsing
import sys
import os
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))

from src.utils.date_utils import parse_fecha

# Test the exact date string from your 32-bit system
test_dates = [
    '7/6/2025 12:00:00 AM',      # 32-bit format
    '06/07/2025 12:00:00 a. m.', # 64-bit format
    '7/6/2025 1:30:00 PM',       # Another 32-bit format
    '06/07/2025 13:30:00',       # 24-hour format
]

print("Testing date parsing...")
for date_str in test_dates:
    print(f"\n--- Testing: '{date_str}' ---")
    result = parse_fecha(date_str)
    print(f"Result: {result}")
    print("-" * 50)
