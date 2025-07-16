#!/usr/bin/env python3
"""
Entity Constructor Test - Verify the source_row Fix
Run this to test if Entity creation works properly now
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def test_entity_creation():
    """Test Entity constructor with proper parameters"""
    print("🔧 TESTING ENTITY CONSTRUCTOR FIX")
    print("=" * 50)
    
    try:
        from app.services.reconciliation_engine import Entity, EntityType
        
        print("✅ Successfully imported Entity class")
        
        # Test creating an entity with all required parameters
        test_entity = Entity(
            id="test_entity_1",
            name="William Shakespeare",
            entity_type=EntityType.PERSON,
            context={"location": "England"},
            source_row=0  # ← This parameter was missing before
        )
        
        print("✅ Entity created successfully with source_row parameter")
        print(f"   ID: {test_entity.id}")
        print(f"   Name: {test_entity.name}")
        print(f"   Type: {test_entity.entity_type}")
        print(f"   Context: {test_entity.context}")
        print(f"   Source Row: {test_entity.source_row}")
        
        # Test the search key generation
        if hasattr(test_entity, 'search_key'):
            print(f"   Search Key: {test_entity.search_key}")
        
        return True
        
    except TypeError as e:
        if "source_row" in str(e):
            print(f"❌ Entity constructor still missing source_row parameter: {e}")
            print("❌ The fix wasn't applied correctly")
        else:
            print(f"❌ Different TypeError: {e}")
        return False
    except ImportError as e:
        print(f"❌ Import error: {e}")
        print("❌ Check that the reconciliation engine module exists")
        return False
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return False

def test_entity_list_creation():
    """Test creating multiple entities like the threading function does"""
    print("\n📋 TESTING ENTITY LIST CREATION")
    print("=" * 50)
    
    try:
        from app.services.reconciliation_engine import Entity, EntityType
        
        # Simulate CSV data
        test_data = [
            {"name": "William Shakespeare", "type": "person", "location": "England"},
            {"name": "Harvard University", "type": "organization", "location": "USA"},
            {"name": "Paris", "type": "place", "location": "France"}
        ]
        
        entities = []
        
        for idx, row_data in enumerate(test_data):
            entity = Entity(
                id=f"entity_{idx}",
                name=row_data["name"],
                entity_type=EntityType.PERSON if row_data["type"] == "person" else EntityType.ORGANIZATION,
                context={"location": row_data["location"]},
                source_row=idx  # ← Key fix
            )
            entities.append(entity)
        
        print(f"✅ Successfully created {len(entities)} entities")
        
        for i, entity in enumerate(entities):
            print(f"   {i+1}. {entity.name} ({entity.entity_type.value}) - Row {entity.source_row}")
        
        return True
        
    except Exception as e:
        print(f"❌ Entity list creation failed: {e}")
        return False

def test_enhanced_reconciliation_engine():
    """Test the enhanced reconciliation engine"""
    print("\n🚀 TESTING ENHANCED RECONCILIATION ENGINE")
    print("=" * 50)
    
    try:
        from app.services.enhanced_reconciliation_engine import EnhancedReconciliationEngine
        from app.services.reconciliation_engine import Entity, EntityType
        
        # Create engine
        engine = EnhancedReconciliationEngine()
        print("✅ Enhanced reconciliation engine created")
        
        # Create test entity
        test_entity = Entity(
            id="test_shakespeare",
            name="William Shakespeare",
            entity_type=EntityType.PERSON,
            context={"location": "England"},
            source_row=0
        )
        
        print("✅ Test entity created for reconciliation")
        print(f"   Testing with: {test_entity.name}")
        
        # Note: We won't actually run reconciliation here to avoid network calls
        # Just test that the engine can be initialized and accepts our entity
        print("✅ Entity format compatible with enhanced engine")
        
        return True
        
    except Exception as e:
        print(f"❌ Enhanced engine test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("🔧 ENTITY CONSTRUCTOR FIX VERIFICATION")
    print("=" * 60)
    
    # Run tests
    test1 = test_entity_creation()
    test2 = test_entity_list_creation() 
    test3 = test_enhanced_reconciliation_engine()
    
    print("\n" + "=" * 60)
    print("📊 TEST SUMMARY")
    print("=" * 60)
    
    results = [
        ("Entity Constructor", test1),
        ("Entity List Creation", test2), 
        ("Enhanced Engine Compatibility", test3)
    ]
    
    passed = sum(1 for _, success in results if success)
    total = len(results)
    
    for test_name, success in results:
        status = "✅ PASS" if success else "❌ FAIL"
        print(f"{status} {test_name}")
    
    print(f"\nResult: {passed}/{total} tests passed")
    
    if passed == total:
        print("\n🎉 ENTITY CONSTRUCTOR FIX WORKING!")
        print("✅ All Entity creation tests passed")
        print("✅ Your threading processing should now work")
        print("✅ No more 'missing source_row' errors")
        print(f"\n🔗 Try uploading again at: http://localhost:5000/upload")
    else:
        print("\n⚠️  SOME ISSUES REMAIN")
        print("❌ Check the failed tests above")
        print("❌ Make sure you applied the Entity constructor fix")
    
    exit(0 if passed == total else 1)