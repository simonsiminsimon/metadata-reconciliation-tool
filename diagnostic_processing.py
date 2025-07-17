#!/usr/bin/env python3
"""
Processing Diagnostic Script
Run this to test CSV processing without going through the web interface.

Usage: python diagnostic_processing.py [csv_file_path] [entity_column]
"""

import os
import sys
import pandas as pd
import time
from pathlib import Path

# Add the app directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__)))

def test_csv_processing_step_by_step(csv_path, entity_column):
    """Test each step of CSV processing to identify where it hangs"""
    print(f"\n🔍 DIAGNOSTIC: Testing CSV processing step by step")
    print("=" * 70)
    
    # Step 1: File existence and basic info
    print("Step 1: File validation...")
    if not os.path.exists(csv_path):
        print(f"❌ File not found: {csv_path}")
        return False
    
    file_size = os.path.getsize(csv_path)
    print(f"✅ File exists: {csv_path}")
    print(f"📊 File size: {file_size:,} bytes ({file_size/1024/1024:.2f} MB)")
    
    # Step 2: Basic CSV reading
    print("\nStep 2: Basic CSV reading...")
    try:
        start_time = time.time()
        df = pd.read_csv(csv_path)
        read_time = time.time() - start_time
        print(f"✅ CSV loaded in {read_time:.2f} seconds")
        print(f"📊 Shape: {df.shape}")
        print(f"📋 Columns: {list(df.columns)}")
        
        if read_time > 10:
            print("⚠️ CSV reading took over 10 seconds - file may be very large")
        
    except Exception as e:
        print(f"❌ CSV reading failed: {e}")
        return False
    
    # Step 3: Column validation
    print(f"\nStep 3: Entity column validation...")
    if entity_column not in df.columns:
        print(f"❌ Column '{entity_column}' not found")
        
        # Try case-insensitive matching
        column_map = {col.lower(): col for col in df.columns}
        if entity_column.lower() in column_map:
            actual_column = column_map[entity_column.lower()]
            print(f"💡 Found similar column: '{actual_column}'")
            entity_column = actual_column
        else:
            print(f"Available columns: {list(df.columns)}")
            return False
    else:
        print(f"✅ Entity column '{entity_column}' found")
    
    # Step 4: Sample data inspection
    print(f"\nStep 4: Sample data inspection...")
    sample_values = df[entity_column].head(10).tolist()
    print(f"📝 First 10 values in '{entity_column}':")
    for i, val in enumerate(sample_values):
        print(f"  {i+1:2d}. {repr(val)} (type: {type(val).__name__})")
    
    # Check for common issues
    null_count = df[entity_column].isnull().sum()
    empty_count = (df[entity_column] == '').sum()
    print(f"📊 Null values: {null_count}")
    print(f"📊 Empty strings: {empty_count}")
    print(f"📊 Total valid values: {len(df) - null_count - empty_count}")
    
    # Step 5: Entity extraction test
    print(f"\nStep 5: Entity extraction test...")
    try:
        from app.services.enhanced_reconciliation_engine import EnhancedReconciliationEngine
        
        start_time = time.time()
        engine = EnhancedReconciliationEngine()
        init_time = time.time() - start_time
        print(f"✅ Engine initialized in {init_time:.2f} seconds")
        
        # Test with just first 5 rows to avoid hanging
        test_df = df.head(5)
        print(f"🧪 Testing entity extraction with first 5 rows...")
        
        start_time = time.time()
        entities = engine.create_entities_from_dataframe(
            test_df,
            entity_column=entity_column
        )
        extract_time = time.time() - start_time
        
        print(f"✅ Entity extraction completed in {extract_time:.2f} seconds")
        print(f"🎯 Extracted {len(entities)} entities from 5 test rows")
        
        if entities:
            print("📝 Sample entities:")
            for i, entity in enumerate(entities):
                print(f"  {i+1}. '{entity.name}' (type: {entity.entity_type.value})")
        else:
            print("⚠️ No entities extracted from test data")
            
        if extract_time > 5:
            print("⚠️ Entity extraction took over 5 seconds for 5 rows - may be slow for full dataset")
        
    except Exception as e:
        print(f"❌ Entity extraction failed: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    # Step 6: Full dataset estimation
    print(f"\nStep 6: Full dataset processing estimation...")
    if len(entities) > 0:
        avg_time_per_row = extract_time / len(test_df)
        estimated_time = avg_time_per_row * len(df)
        print(f"📊 Estimated time for full dataset: {estimated_time:.1f} seconds")
        
        if estimated_time > 300:  # 5 minutes
            print("⚠️ Full processing may take a very long time")
            print("💡 Consider processing in smaller batches or optimizing data")
    
    print(f"\n🎉 DIAGNOSTIC COMPLETE - All steps passed!")
    return True


def test_background_job_system():
    """Test if background job system is working"""
    print(f"\n🔧 DIAGNOSTIC: Testing background job system")
    print("=" * 70)
    
    try:
        from app.background_jobs import celery_app, BACKGROUND_JOBS_AVAILABLE
        print(f"Background jobs available: {BACKGROUND_JOBS_AVAILABLE}")
        
        if BACKGROUND_JOBS_AVAILABLE:
            print("✅ Background job system is available")
            # Test Redis connection
            try:
                from app.background_jobs import test_redis_connection
                success, message = test_redis_connection()
                print(f"Redis connection: {message}")
            except:
                print("⚠️ Could not test Redis connection")
        else:
            print("📝 Background jobs not available - will use threaded processing")
            
    except Exception as e:
        print(f"❌ Background job system test failed: {e}")


def main():
    """Main diagnostic runner"""
    print("🔍 Metadata Reconciliation System - Processing Diagnostic")
    print("=" * 70)
    
    # Get CSV file and entity column
    if len(sys.argv) < 2:
        print("Usage: python diagnostic_processing.py [csv_file] [entity_column]")
        print("\nCreating test CSV for diagnosis...")
        
        # Create a simple test CSV
        test_data = {
            'Name': ['John Smith', 'Jane Doe', 'New York', 'Art History', 'Robert Johnson'],
            'Type': ['Person', 'Person', 'Place', 'Subject', 'Person'],
            'Notes': ['Author', 'Scientist', 'City', 'Academic field', 'Musician']
        }
        df = pd.DataFrame(test_data)
        csv_path = 'diagnostic_test.csv'
        df.to_csv(csv_path, index=False)
        print(f"📁 Created test CSV: {csv_path}")
        entity_column = 'Name'
    else:
        csv_path = sys.argv[1]
        entity_column = sys.argv[2] if len(sys.argv) > 2 else 'Name'
    
    print(f"📄 Testing file: {csv_path}")
    print(f"🎯 Entity column: {entity_column}")
    
    # Run tests
    success = test_csv_processing_step_by_step(csv_path, entity_column)
    test_background_job_system()
    
    # Summary
    print(f"\n🎯 DIAGNOSTIC SUMMARY")
    print("=" * 70)
    if success:
        print("✅ CSV processing should work correctly")
        print("💡 If you're still experiencing infinite loops:")
        print("   1. Check Flask application logs for specific error messages")
        print("   2. Verify the entity column name matches exactly")
        print("   3. Try with a smaller CSV file first")
        print("   4. Restart your Flask application after applying fixes")
    else:
        print("❌ Issues found that need to be fixed before processing will work")
    
    # Clean up test file
    if csv_path == 'diagnostic_test.csv' and os.path.exists(csv_path):
        os.remove(csv_path)
        print(f"\n🧹 Cleaned up test file: {csv_path}")


if __name__ == "__main__":
    main()