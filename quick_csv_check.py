#!/usr/bin/env python3
"""
Quick check of the saved CSV file
"""
import pandas as pd
import os

# Check the exact file path from your logs
job_id = "560b43be-0fc8-4c97-81bf-2b634bb7e3aa"
file_path = f"data/input/{job_id}_test_entities.csv"

print(f"🔍 Checking file: {file_path}")

if os.path.exists(file_path):
    print("✅ File exists!")
    
    df = pd.read_csv(file_path)
    print(f"📊 Shape: {df.shape}")
    print(f"📋 Columns: {list(df.columns)}")
    print(f"📄 First 3 rows:")
    print(df.head(3))
    
    if 'creator_name' in df.columns:
        print(f"\n🎯 Creator name values:")
        for i, name in enumerate(df['creator_name'].head()):
            print(f"   {i+1}. '{name}'")
    else:
        print(f"❌ No 'creator_name' column found!")
        
else:
    print(f"❌ File not found!")
    print(f"Checking alternative locations...")
    
    alternatives = [
        "test_entities.csv",
        f"data\\input\\{job_id}_test_entities.csv"
    ]
    
    for alt in alternatives:
        if os.path.exists(alt):
            print(f"✅ Found at: {alt}")
            break