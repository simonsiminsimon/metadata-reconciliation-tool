#!/usr/bin/env python3
"""
Complete test script to verify entity detection from CSV
Run this from your project root directory
"""

import sys
import os
import pandas as pd

# Add the app directory to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'app'))

def test_csv_and_entity_creation():
    """Test CSV loading and entity creation"""
    print("🧪 TESTING CSV LOADING AND ENTITY CREATION")
    print("=" * 60)
    
    # Step 1: Load and examine the CSV
    print("Step 1: Loading test_entities.csv...")
    try:
        df = pd.read_csv('test_entities.csv')
        print(f"✅ CSV loaded successfully!")
        print(f"   Rows: {len(df)}")
        print(f"   Columns: {list(df.columns)}")
        
        # Show first few rows
        print(f"\n📊 First 3 rows:")
        for i, row in df.head(3).iterrows():
            print(f"   {i+1}. {row['creator_name']} ({row.get('entity_type', 'unknown')})")
            
        # Check for missing values in key column
        missing_names = df['creator_name'].isna().sum()
        print(f"\n📋 Missing values in creator_name: {missing_names}")
        
    except FileNotFoundError:
        print("❌ test_entities.csv not found!")
        print("   Make sure the file is in your current directory")
        return
    except Exception as e:
        print(f"❌ Error loading CSV: {e}")
        return
    
    # Step 2: Test entity creation
    print("\n" + "=" * 60)
    print("Step 2: Testing entity creation...")
    
    try:
        from services.enhanced_reconciliation_engine import EnhancedReconciliationEngine
        
        engine = EnhancedReconciliationEngine()
        print("✅ Enhanced reconciliation engine initialized")
        
        # Create entities with the exact parameters from your CSV
        entities = engine.create_entities_from_dataframe(
            df, 
            entity_column='creator_name',
            type_column='entity_type', 
            context_columns=['location', 'date_created', 'subject_area']
        )
        
        print(f"✅ Entities created: {len(entities)}")
        
        if len(entities) == 0:
            print("❌ NO ENTITIES CREATED - This explains your 0 entities detected!")
            print("   Possible issues:")
            print("   - Column name mismatch")
            print("   - All values in creator_name column are empty/NaN")
            print("   - Entity filtering is too strict")
            
            # Debug the DataFrame
            print(f"\n🔍 Debugging creator_name column:")
            print(f"   Column exists: {'creator_name' in df.columns}")
            print(f"   Sample values: {df['creator_name'].head().tolist()}")
            print(f"   Non-null values: {df['creator_name'].notna().sum()}")
            
        else:
            print(f"\n📋 First 5 entities:")
            for i, entity in enumerate(entities[:5]):
                print(f"   {i+1}. {entity.name} ({entity.entity_type.value})")
                if entity.context:
                    print(f"      Context: {entity.context}")
            
            print(f"\n📊 Entity type breakdown:")
            type_counts = {}
            for entity in entities:
                entity_type = entity.entity_type.value
                type_counts[entity_type] = type_counts.get(entity_type, 0) + 1
            
            for entity_type, count in type_counts.items():
                print(f"   {entity_type}: {count}")
                
    except ImportError as e:
        print(f"❌ Import error: {e}")
        print("   Make sure you're running from the project root directory")
        return
    except Exception as e:
        print(f"❌ Error creating entities: {e}")
        import traceback
        traceback.print_exc()
        return
    
    # Step 3: Test a single entity reconciliation (if entities were created)
    if len(entities) > 0:
        print("\n" + "=" * 60)
        print("Step 3: Testing single entity reconciliation...")
        
        # Test with William Shakespeare (should be first entity)
        test_entity = entities[0]
        print(f"Testing reconciliation for: {test_entity.name}")
        
        try:
            results = engine.process_entities([test_entity])
            
            if results:
                result = results[0]
                print(f"✅ Reconciliation completed:")
                print(f"   Matches found: {len(result.matches)}")
                print(f"   Best match: {result.best_match}")
                print(f"   Confidence: {result.confidence}")
                print(f"   Sources queried: {result.sources_queried}")
                
                if result.matches:
                    print(f"\n🎯 Top matches:")
                    for i, match in enumerate(result.matches[:3]):
                        print(f"   {i+1}. {match.name} (Score: {match.score:.2f})")
                else:
                    print("⚠️  No matches found - check authority source configuration")
            else:
                print("❌ No reconciliation results returned")
                
        except Exception as e:
            print(f"❌ Error during reconciliation: {e}")
            import traceback
            traceback.print_exc()

def main():
    """Main function"""
    print("🔧 COMPLETE ENTITY DETECTION TEST")
    print("This script will:")
    print("1. Load your test_entities.csv file")
    print("2. Create entities using your exact column names")
    print("3. Test reconciliation on one entity")
    print("\nThis should help identify why you're getting '0 entities detected'\n")
    
    test_csv_and_entity_creation()
    
    print("\n" + "=" * 60)
    print("✅ TEST COMPLETED")
    print("\nIf you got 0 entities created, check:")
    print("1. CSV file exists and has correct column names")
    print("2. creator_name column has non-empty values")
    print("3. Column names match exactly (case-sensitive)")

if __name__ == "__main__":
    main()