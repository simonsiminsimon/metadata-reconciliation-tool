#!/usr/bin/env python3
"""
Debug script to test the exact entity extraction process that your threaded processor uses
This will help identify why you're getting 0 entities detected.
"""

import sys
import os
import pandas as pd

# Add the app directory to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'app'))

def debug_entity_extraction():
    """Debug the exact entity extraction process used by your app"""
    print("🔧 DEBUGGING ENTITY EXTRACTION")
    print("=" * 60)
    
    # Step 1: Simulate the exact file path your app uses
    job_id = "560b43be-0fc8-4c97-81bf-2b634bb7e3aa"  # From your logs
    file_path = f"data/input/{job_id}_test_entities.csv"
    
    print(f"Step 1: Testing file path from your logs...")
    print(f"Looking for: {file_path}")
    
    if not os.path.exists(file_path):
        print(f"❌ File not found at: {file_path}")
        print(f"Let's try alternative paths...")
        
        alternative_paths = [
            "test_entities.csv",
            "data/input/test_entities.csv",
            f"data\\input\\{job_id}_test_entities.csv",  # Windows path
            f"data/input\\{job_id}_test_entities.csv"    # Mixed separators
        ]
        
        found_file = None
        for alt_path in alternative_paths:
            if os.path.exists(alt_path):
                found_file = alt_path
                print(f"✅ Found file at: {alt_path}")
                break
        
        if not found_file:
            print("❌ Could not find test_entities.csv in any expected location")
            print("Please make sure the file exists in one of these locations:")
            for path in alternative_paths:
                print(f"   - {path}")
            return
        
        file_path = found_file
    else:
        print(f"✅ File found at: {file_path}")
    
    # Step 2: Read the CSV exactly like your app does
    print(f"\nStep 2: Reading CSV file...")
    try:
        df = pd.read_csv(file_path)
        print(f"✅ CSV loaded successfully!")
        print(f"   Shape: {df.shape}")
        print(f"   Columns: {list(df.columns)}")
        
        # Check if creator_name column exists
        if 'creator_name' not in df.columns:
            print(f"❌ Column 'creator_name' not found!")
            print(f"   Available columns: {list(df.columns)}")
            print(f"   This is likely why you're getting 0 entities!")
            return
        else:
            print(f"✅ Column 'creator_name' found")
            
        # Check the data in creator_name column
        print(f"\n📊 Creator name column analysis:")
        print(f"   Total rows: {len(df)}")
        print(f"   Non-null values: {df['creator_name'].notna().sum()}")
        print(f"   Null values: {df['creator_name'].isna().sum()}")
        print(f"   Empty strings: {(df['creator_name'] == '').sum()}")
        
        # Show sample values
        print(f"\n📋 Sample values from creator_name:")
        for i, value in enumerate(df['creator_name'].head()):
            if pd.notna(value):
                print(f"   {i+1}. '{value}' (type: {type(value)})")
            else:
                print(f"   {i+1}. NULL/NaN")
                
    except Exception as e:
        print(f"❌ Error reading CSV: {e}")
        return
    
    # Step 3: Test entity creation exactly like your app
    print(f"\nStep 3: Testing entity creation with your exact parameters...")
    
    job_config = {
        'entity_column': 'creator_name',
        'type_column': 'entity_type',
        'context_columns': ['location', 'date_created', 'subject_area']
    }
    
    print(f"Using job config: {job_config}")
    
    try:
        from services.enhanced_reconciliation_engine import EnhancedReconciliationEngine
        
        engine = EnhancedReconciliationEngine()
        print(f"✅ Reconciliation engine initialized")
        
        # This is the EXACT call your threaded processor makes
        entities = engine.create_entities_from_dataframe(
            df, 
            entity_column=job_config['entity_column'],
            type_column=job_config.get('type_column'),
            context_columns=job_config.get('context_columns', [])
        )
        
        print(f"\n🎯 RESULT: {len(entities)} entities created")
        
        if len(entities) == 0:
            print(f"❌ FOUND THE PROBLEM: Entity creation is returning 0 entities!")
            print(f"\nDebugging entity creation logic...")
            
            # Debug the entity creation step by step
            print(f"\nStep 3a: Checking entity_column parameter...")
            entity_column = job_config['entity_column']
            if entity_column not in df.columns:
                print(f"❌ Column '{entity_column}' not in DataFrame!")
                return
            
            print(f"Step 3b: Checking for valid entity names...")
            valid_names = []
            for idx, row in df.iterrows():
                entity_name = str(row[entity_column]).strip()
                if entity_name and entity_name.lower() not in ['nan', 'none', '']:
                    valid_names.append(entity_name)
                else:
                    print(f"   Row {idx}: Skipped '{entity_name}' (invalid)")
            
            print(f"   Found {len(valid_names)} valid entity names")
            if len(valid_names) > 0:
                print(f"   Sample valid names: {valid_names[:3]}")
            
            if len(valid_names) == 0:
                print(f"❌ All entity names are being filtered out!")
                print(f"   Check the entity filtering logic in create_entities_from_dataframe")
            else:
                print(f"❌ Entity creation logic has a bug - valid names exist but entities aren't created")
        
        else:
            print(f"✅ SUCCESS: Found {len(entities)} entities!")
            print(f"\n📋 Sample entities:")
            for i, entity in enumerate(entities[:5]):
                print(f"   {i+1}. {entity.name} ({entity.entity_type.value})")
                if entity.context:
                    print(f"      Context: {entity.context}")
                    
    except Exception as e:
        print(f"❌ Error in entity creation: {e}")
        import traceback
        traceback.print_exc()
        return
    
    # Step 4: If we got here with entities, test one reconciliation
    if len(entities) > 0:
        print(f"\nStep 4: Testing reconciliation on first entity...")
        test_entity = entities[0]
        
        try:
            results = engine.process_entities([test_entity])
            if results:
                result = results[0]
                print(f"✅ Reconciliation test successful:")
                print(f"   Entity: {result.entity.name}")
                print(f"   Matches: {len(result.matches)}")
                print(f"   Best match: {result.best_match}")
            else:
                print(f"⚠️  Reconciliation returned no results")
        except Exception as e:
            print(f"❌ Reconciliation failed: {e}")

def main():
    """Main function"""
    print("🔧 ENTITY EXTRACTION DEBUGGER")
    print("This script tests the exact same entity extraction process")
    print("that your threaded processor uses.\n")
    
    debug_entity_extraction()
    
    print("\n" + "=" * 60)
    print("✅ DEBUG COMPLETED")
    print("\nNext steps based on results:")
    print("1. If file not found → check file path in your app")
    print("2. If column not found → check column name spelling")
    print("3. If 0 entities created → check entity filtering logic")
    print("4. If entities created → problem is elsewhere in your app")

if __name__ == "__main__":
    main()