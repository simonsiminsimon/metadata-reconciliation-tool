# File: test_csv_processing.py
# Test your CSV file processing to identify the issue

import pandas as pd
import os
from app.services.metadata_parser import MetadataParser
from app.services.reconciliation_engine import ReconciliationEngine

def test_csv_file():
    """Test processing your CSV file step by step"""
    print("🔍 Testing CSV file processing...")
    print("=" * 50)
    
    # Find your CSV file
    csv_files = []
    for filename in os.listdir('data/input'):
        if 'page1test.csv' in filename:
            csv_files.append(os.path.join('data/input', filename))
    
    if not csv_files:
        print("❌ No page1test.csv file found in data/input/")
        return False
    
    csv_file = csv_files[0]  # Use the first one found
    print(f"📄 Testing file: {csv_file}")
    
    try:
        # Step 1: Basic pandas read
        print("\n1️⃣ Testing basic CSV read...")
        df = pd.read_csv(csv_file)
        print(f"   ✅ Successfully read CSV: {df.shape[0]} rows, {df.shape[1]} columns")
        print(f"   📋 Columns: {list(df.columns)}")
        
        # Check for creator column
        creator_columns = [col for col in df.columns if 'creator' in col.lower()]
        print(f"   🎯 Creator-related columns: {creator_columns}")
        
        # Show sample data
        print(f"\n📊 Sample data (first 3 rows):")
        for col in df.columns:
            print(f"   {col}:")
            for i in range(min(3, len(df))):
                value = df.iloc[i][col]
                print(f"     Row {i+1}: {repr(value)}")
        
    except Exception as e:
        print(f"   ❌ Error reading CSV: {e}")
        return False
    
    try:
        # Step 2: Test metadata parser
        print(f"\n2️⃣ Testing metadata parser...")
        parser = MetadataParser()
        
        # Try parsing the file
        metadata = parser.parse_csv_metadata(csv_file)
        print(f"   ✅ Metadata parser successful")
        print(f"   📊 Found entities: {metadata['summary']}")
        
    except Exception as e:
        print(f"   ❌ Metadata parser error: {e}")
        print(f"   🔧 This might be the issue!")
        return False
    
    try:
        # Step 3: Test reconciliation engine entity creation
        print(f"\n3️⃣ Testing reconciliation engine...")
        engine = ReconciliationEngine()
        
        # Test with different possible column names
        possible_columns = ['creator', 'Creator', 'CREATOR']
        working_column = None
        
        for col_name in possible_columns:
            if col_name in df.columns:
                print(f"   🔍 Testing with column: '{col_name}'")
                try:
                    entities = engine.create_entities_from_dataframe(
                        df, 
                        entity_column=col_name,
                        type_column=None,
                        context_columns=[]
                    )
                    print(f"   ✅ Successfully created {len(entities)} entities")
                    working_column = col_name
                    
                    # Show sample entities
                    for i, entity in enumerate(entities[:3]):
                        print(f"     Entity {i+1}: {entity.name} ({entity.entity_type.value})")
                    
                    break
                    
                except Exception as e:
                    print(f"   ❌ Failed with column '{col_name}': {e}")
        
        if working_column:
            print(f"\n   ✅ Column '{working_column}' works correctly!")
        else:
            print(f"\n   ❌ No creator column works - this is the problem!")
        
    except Exception as e:
        print(f"   ❌ Reconciliation engine error: {e}")
        return False
    
    return True

def suggest_csv_fixes():
    """Suggest fixes for CSV issues"""
    print(f"\n💡 Suggested fixes:")
    print("=" * 50)
    print("1. Check your CSV column names:")
    print("   - Make sure 'creator' column exists")
    print("   - Check for extra spaces in column names")
    print("   - Verify the column contains actual data (not all empty)")
    
    print("\n2. Common CSV issues:")
    print("   - Column name has spaces: 'Creator ' vs 'Creator'")
    print("   - Different capitalization: 'creator' vs 'Creator'")
    print("   - Empty cells in the creator column")
    print("   - Special characters in data")
    
    print("\n3. Quick test:")
    print("   - Open your CSV in Excel/text editor")
    print("   - Verify the column name is exactly 'creator' or 'Creator'")
    print("   - Check that it contains actual creator names")

if __name__ == "__main__":
    success = test_csv_file()
    suggest_csv_fixes()
    
    if not success:
        print(f"\n🚨 CSV processing failed!")
        print("   This explains why your reconciliation jobs are failing.")
        print("   Fix the CSV issues above, then try uploading again.")