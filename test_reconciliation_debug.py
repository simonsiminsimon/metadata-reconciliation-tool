#!/usr/bin/env python3
"""
Debug script to test reconciliation engine components
Run this from your project root directory
"""

import sys
import os
import logging

# Add the app directory to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'app'))

def test_basic_api():
    """Test basic API connectivity"""
    print("=" * 60)
    print("🌐 TESTING BASIC API CONNECTIVITY")
    print("=" * 60)
    
    import requests
    
    # Test Wikidata API
    try:
        print("Testing Wikidata API...")
        response = requests.get(
            "https://www.wikidata.org/w/api.php",
            params={
                'action': 'wbsearchentities',
                'search': 'William Shakespeare',
                'language': 'en',
                'format': 'json',
                'limit': 3
            },
            timeout=10
        )
        
        if response.status_code == 200:
            data = response.json()
            results = data.get('search', [])
            print(f"✅ Wikidata API: {len(results)} results for 'William Shakespeare'")
            if results:
                print(f"   First result: {results[0].get('label')} ({results[0].get('id')})")
        else:
            print(f"❌ Wikidata API: HTTP {response.status_code}")
    
    except Exception as e:
        print(f"❌ Wikidata API: {e}")
    
    # Test VIAF (if available)
    try:
        print("\nTesting VIAF API...")
        response = requests.get(
            "http://viaf.org/viaf/search",
            params={
                'query': 'local.personalNames all "William Shakespeare"',
                'maximumRecords': 3,
                'httpAccept': 'application/json'
            },
            timeout=10
        )
        
        if response.status_code == 200:
            print("✅ VIAF API: Accessible")
        else:
            print(f"❌ VIAF API: HTTP {response.status_code}")
    
    except Exception as e:
        print(f"❌ VIAF API: {e}")


def test_reconciliation_engines():
    """Test the reconciliation engines"""
    print("\n" + "=" * 60)
    print("🔧 TESTING RECONCILIATION ENGINES")
    print("=" * 60)
    
    # Enable debug logging
    logging.basicConfig(level=logging.DEBUG)
    
    try:
        # Test basic engine
        print("Testing Basic ReconciliationEngine...")
        from services.reconciliation_engine import ReconciliationEngine, Entity, EntityType
        
        basic_engine = ReconciliationEngine()
        print(f"✅ Basic engine initialized: {type(basic_engine).__name__}")
        
        # Create test entity
        test_entity = Entity(
            id="test_shakespeare",
            name="William Shakespeare",
            entity_type=EntityType.PERSON,
            context={},
            source_row=0
        )
        
        print(f"Testing with entity: {test_entity.name}")
        results = basic_engine.process_entities([test_entity])
        
        print(f"📊 Basic engine results: {len(results)} processed")
        if results:
            result = results[0]
            print(f"   Matches found: {len(result.matches)}")
            print(f"   Best match: {result.best_match}")
            print(f"   Confidence: {result.confidence}")
            print(f"   Sources queried: {result.sources_queried}")
    
    except Exception as e:
        print(f"❌ Basic engine error: {e}")
        import traceback
        traceback.print_exc()
    
    try:
        # Test enhanced engine
        print("\nTesting Enhanced ReconciliationEngine...")
        from services.enhanced_reconciliation_engine import EnhancedReconciliationEngine
        
        enhanced_engine = EnhancedReconciliationEngine()
        print(f"✅ Enhanced engine initialized: {type(enhanced_engine).__name__}")
        
        # Create test entity
        test_entity = Entity(
            id="test_shakespeare_enhanced",
            name="William Shakespeare",
            entity_type=EntityType.PERSON,
            context={'date_created': '1564-1616', 'location': 'England'},
            source_row=0
        )
        
        print(f"Testing with entity: {test_entity.name}")
        results = enhanced_engine.process_entities([test_entity])
        
        print(f"📊 Enhanced engine results: {len(results)} processed")
        if results:
            result = results[0]
            print(f"   Matches found: {len(result.matches)}")
            print(f"   Best match: {result.best_match}")
            print(f"   Confidence: {result.confidence}")
            print(f"   Sources queried: {result.sources_queried}")
        
        # Check stats
        stats = enhanced_engine.get_statistics()
        print(f"📈 Enhanced engine stats: {stats}")
    
    except Exception as e:
        print(f"❌ Enhanced engine error: {e}")
        import traceback
        traceback.print_exc()


def test_wikidata_clients():
    """Test the Wikidata clients individually"""
    print("\n" + "=" * 60)
    print("🔍 TESTING WIKIDATA CLIENTS")
    print("=" * 60)
    
    try:
        # Test failsafe client
        print("Testing FailsafeWikidataClient...")
        from services.failsafe_wikidata_client import FailsafeWikidataClient
        
        client = FailsafeWikidataClient(rate_limit=2.0, timeout=15)
        print("✅ Failsafe client initialized")
        
        # Test person search
        print("Searching for 'William Shakespeare'...")
        results = client.search_persons("William Shakespeare")
        
        print(f"📊 Person search results: {len(results)}")
        for i, result in enumerate(results[:3]):
            print(f"   {i+1}. {result.label} (Q{result.wikidata_id})")
            print(f"      Confidence: {result.confidence_level.value} ({result.confidence_score:.2f})")
        
        # Check client stats
        stats = client.get_statistics()
        print(f"📈 Client stats: {stats}")
    
    except Exception as e:
        print(f"❌ Wikidata client error: {e}")
        import traceback
        traceback.print_exc()


def test_csv_processing():
    """Test CSV processing with sample data"""
    print("\n" + "=" * 60)
    print("📄 TESTING CSV PROCESSING")
    print("=" * 60)
    
    try:
        import pandas as pd
        from io import StringIO
        
        # Sample CSV data
        csv_data = """creator_name,entity_type,location,date_created
William Shakespeare,person,England,1564-1616
Leonardo da Vinci,person,Italy,1452-1519
Metropolitan Museum of Art,organization,New York,1870
Paris,place,France,"""
        
        df = pd.read_csv(StringIO(csv_data))
        print(f"✅ CSV parsed: {len(df)} rows")
        print(f"   Columns: {list(df.columns)}")
        
        # Test entity creation
        from services.reconciliation_engine import EntityType
        from services.enhanced_reconciliation_engine import EnhancedReconciliationEngine
        
        engine = EnhancedReconciliationEngine()
        entities = engine.create_entities_from_dataframe(
            df, 
            entity_column='creator_name',
            type_column='entity_type',
            context_columns=['location', 'date_created']
        )
        
        print(f"📊 Created {len(entities)} entities:")
        for entity in entities:
            print(f"   - {entity.name} ({entity.entity_type.value})")
        
        # Process entities
        print("\nProcessing entities...")
        results = engine.process_entities(entities)
        
        for result in results:
            print(f"\n🔍 {result.entity.name}:")
            print(f"   Matches: {len(result.matches)}")
            if result.best_match:
                print(f"   Best: {result.best_match.name} ({result.best_match.score:.2f})")
    
    except Exception as e:
        print(f"❌ CSV processing error: {e}")
        import traceback
        traceback.print_exc()


def main():
    """Run all tests"""
    print("🧪 RECONCILIATION ENGINE DEBUG TESTS")
    print("Testing reconciliation system components...\n")
    
    # Run tests
    test_basic_api()
    test_wikidata_clients()
    test_reconciliation_engines()
    test_csv_processing()
    
    print("\n" + "=" * 60)
    print("✅ DEBUG TESTS COMPLETED")
    print("=" * 60)
    print("\nNext steps:")
    print("1. Review any error messages above")
    print("2. If APIs are working but no matches found, check entity type detection")
    print("3. If timeouts occur, increase timeout values in failsafe client")
    print("4. Upload the test_entities.csv file through your web interface")


if __name__ == "__main__":
    main()