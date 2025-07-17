#!/usr/bin/env python3
"""
Debug and Test Script for Metadata Reconciliation System
Run this script to test the fixes and diagnose issues.

Usage: python debug_fixes.py [csv_file_path]
"""

import os
import sys
import pandas as pd
from pathlib import Path

# Add the app directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__)))

def test_csv_parsing(csv_path):
    """Test CSV parsing and entity extraction"""
    print(f"\n🧪 Testing CSV: {csv_path}")
    print("=" * 60)
    
    if not os.path.exists(csv_path):
        print(f"❌ File not found: {csv_path}")
        return False
    
    try:
        # Test 1: Basic pandas reading
        print("1. Testing basic CSV reading...")
        df = pd.read_csv(csv_path)
        print(f"   ✅ Shape: {df.shape}")
        print(f"   ✅ Columns: {list(df.columns)}")
        
        # Test 2: Show sample data
        print("\n2. Sample data (first 3 rows):")
        for i, row in df.head(3).iterrows():
            print(f"   Row {i}: {dict(row)}")
        
        # Test 3: Test entity extraction
        print("\n3. Testing entity extraction...")
        from app.services.enhanced_reconciliation_engine import EnhancedReconciliationEngine
        
        engine = EnhancedReconciliationEngine()
        
        # Try with different potential entity columns
        entity_columns_to_try = []
        for col in df.columns:
            col_lower = col.lower()
            if any(keyword in col_lower for keyword in ['name', 'title', 'author', 'creator', 'person', 'entity']):
                entity_columns_to_try.append(col)
        
        if not entity_columns_to_try:
            entity_columns_to_try = [df.columns[0]]  # Use first column as fallback
        
        print(f"   Trying entity columns: {entity_columns_to_try}")
        
        for entity_col in entity_columns_to_try:
            print(f"\n   Testing column: '{entity_col}'")
            try:
                entities = engine.create_entities_from_dataframe(
                    df, 
                    entity_column=entity_col
                )
                print(f"   ✅ Created {len(entities)} entities")
                
                if entities:
                    print("   Sample entities:")
                    for i, entity in enumerate(entities[:5]):
                        print(f"     {i+1}. '{entity.name}' (type: {entity.entity_type.value})")
                    break
                else:
                    print("   ⚠️  No entities found")
                    
            except Exception as e:
                print(f"   ❌ Error: {e}")
        
        return True
        
    except Exception as e:
        print(f"❌ CSV parsing failed: {e}")
        return False


def test_database():
    """Test database functionality"""
    print("\n🗄️ Testing Database...")
    print("=" * 60)
    
    try:
        from app.database import JobManager, init_database
        
        # Initialize database
        init_database()
        print("✅ Database initialized")
        
        # Test job creation
        test_job = {
            'id': 'test_debug_123',
            'filename': 'test.csv',
            'filepath': '/tmp/test.csv',
            'entity_column': 'name',
            'data_sources': ['wikidata']
        }
        
        job_id = JobManager.create_job(test_job)
        print(f"✅ Created test job: {job_id}")
        
        # Test job retrieval
        job = JobManager.get_job(job_id)
        if job:
            print(f"✅ Retrieved job: {job['filename']}")
        else:
            print("❌ Failed to retrieve job")
            return False
        
        # Test job update
        JobManager.update_job(job_id, {'status': 'completed', 'progress': 100})
        print("✅ Updated job status")
        
        # Test job deletion
        success = JobManager.delete_job(job_id)
        if success:
            print("✅ Deleted test job")
        else:
            print("❌ Failed to delete test job")
        
        return True
        
    except Exception as e:
        print(f"❌ Database test failed: {e}")
        return False


def test_api_routes():
    """Test API route availability"""
    print("\n🌐 Testing API Routes...")
    print("=" * 60)
    
    try:
        # Test import of API routes
        from app.routes.api import register_api_routes
        print("✅ API routes module imports successfully")
        
        # Test Flask app creation (mock)
        class MockApp:
            def route(self, *args, **kwargs):
                def decorator(func):
                    print(f"   📍 Route registered: {args[0]} - {func.__name__}")
                    return func
                return decorator
        
        mock_app = MockApp()
        register_api_routes(mock_app)
        print("✅ All API routes registered successfully")
        
        return True
        
    except Exception as e:
        print(f"❌ API routes test failed: {e}")
        return False


def create_test_csv():
    """Create a test CSV file for testing"""
    test_data = {
        'Name': ['John Smith', 'Jane Doe', 'Robert Johnson', 'Mary Williams', 'David Brown'],
        'Type': ['Person', 'Person', 'Person', 'Person', 'Person'],
        'Location': ['New York', 'California', 'Texas', 'Florida', 'Illinois'],
        'Subject': ['History', 'Science', 'Literature', 'Art', 'Music']
    }
    
    df = pd.DataFrame(test_data)
    test_file = 'test_sample.csv'
    df.to_csv(test_file, index=False)
    print(f"✅ Created test CSV: {test_file}")
    return test_file


def main():
    """Main test runner"""
    print("🔧 Metadata Reconciliation System - Debug & Test")
    print("=" * 60)
    
    # Check if CSV file provided
    csv_file = None
    if len(sys.argv) > 1:
        csv_file = sys.argv[1]
    
    if not csv_file or not os.path.exists(csv_file):
        print("📁 No CSV file provided or file not found. Creating test CSV...")
        csv_file = create_test_csv()
    
    # Run tests
    tests_passed = 0
    total_tests = 3
    
    if test_database():
        tests_passed += 1
    
    if test_api_routes():
        tests_passed += 1
        
    if test_csv_parsing(csv_file):
        tests_passed += 1
    
    # Summary
    print(f"\n🎯 Test Summary")
    print("=" * 60)
    print(f"Tests passed: {tests_passed}/{total_tests}")
    
    if tests_passed == total_tests:
        print("🎉 All tests passed! Your system should be working correctly.")
        print("\nNext steps:")
        print("1. Start your Flask app: python run.py")
        print("2. Upload a CSV file through the web interface")
        print("3. Monitor the job processing in the jobs page")
    else:
        print("⚠️  Some tests failed. Check the error messages above.")
        print("\nCommon fixes:")
        print("1. Install missing dependencies: pip install -r requirements.txt")
        print("2. Check database permissions")
        print("3. Verify CSV file format and column names")
    
    # Clean up test file if we created it
    if csv_file == 'test_sample.csv' and os.path.exists(csv_file):
        os.remove(csv_file)
        print(f"\n🧹 Cleaned up test file: {csv_file}")


if __name__ == "__main__":
    main()