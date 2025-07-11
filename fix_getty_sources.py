# File: fix_getty_sources.py
# Add Getty sources to your reconciliation engine

from app.services.reconciliation_engine import ReconciliationEngine, EntityType
from app.services.data_sources import GettyClient

def show_current_mapping():
    """Show current source mapping"""
    engine = ReconciliationEngine()
    
    print("🔍 Current Source Mapping:")
    print("=" * 50)
    
    for entity_type, sources in engine.source_mapping.items():
        print(f"\n{entity_type.value.upper()}:")
        for source_name, source_func in sources:
            print(f"  - {source_name}")
    
    print(f"\n❌ Problem: Organizations only use Wikidata")
    print(f"❌ Problem: No entity type uses Getty TGN (places) or ULAN (cultural agents)")

def test_getty_manually():
    """Test Getty sources manually to see what they return"""
    print(f"\n🧪 Testing Getty sources manually...")
    print("=" * 50)
    
    client = GettyClient()
    
    # Test entities from your CSV
    test_entities = [
        ("Minneapolis Institute of Art", "ULAN", "Cultural institution"),
        ("Bijou Opera House", "ULAN", "Performance venue"), 
        ("Minnesota", "TGN", "Geographic place"),
        ("Carleton College", "ULAN", "Educational institution")
    ]
    
    for entity_name, vocab, description in test_entities:
        print(f"\n🔍 Testing: {entity_name} in Getty {vocab}")
        
        try:
            if vocab == "ULAN":
                results = client.search_ulan_agents(entity_name, limit=3)
            elif vocab == "TGN":
                results = client.search_tgn_places(entity_name, limit=3)
            elif vocab == "AAT":
                results = client.search_aat_terms(entity_name, limit=3)
            
            if results:
                print(f"   ✅ Found {len(results)} results:")
                for result in results:
                    print(f"     - {result.name} (Score: {result.score:.2f}, Confidence: {result.confidence.value})")
            else:
                print(f"   ❌ No results found")
                
        except Exception as e:
            print(f"   ❌ Error: {e}")

def create_enhanced_source_mapping():
    """Show how to enhance the source mapping"""
    print(f"\n🛠️  Enhanced Source Mapping:")
    print("=" * 50)
    
    enhanced_mapping = """
# Enhanced mapping that includes Getty sources:

self.source_mapping = {
    EntityType.PERSON: [
        ('wikidata_persons', self.wikidata_client.search_persons),
        ('viaf_authors', self.viaf_client.search_authors),
        ('getty_ulan', self.getty_client.search_ulan_agents)  # Artists/cultural figures
    ],
    EntityType.ORGANIZATION: [
        ('wikidata_entities', self.wikidata_client.search_entities),
        ('getty_ulan', self.getty_client.search_ulan_agents)  # Cultural institutions
    ],
    EntityType.PLACE: [
        ('wikidata_places', self.wikidata_client.search_places),
        ('getty_tgn', self.getty_client.search_tgn_places)   # Geographic names
    ],
    EntityType.SUBJECT: [
        ('getty_aat', self.getty_client.search_aat_terms),   # Art & architecture terms
        ('wikidata_entities', self.wikidata_client.search_entities)
    ],
    EntityType.AUTHOR: [
        ('viaf_authors', self.viaf_client.search_authors),
        ('wikidata_persons', self.wikidata_client.search_persons),
        ('getty_ulan', self.getty_client.search_ulan_agents)  # Author/artist overlap
    ]
}
"""
    print(enhanced_mapping)

def patch_reconciliation_engine():
    """Create a patched version of the reconciliation engine"""
    print(f"\n🔧 Creating patched reconciliation engine...")
    
    patch_code = '''
# File: app/services/reconciliation_engine_enhanced.py
# Copy your existing reconciliation_engine.py and modify the source_mapping

# In the __init__ method, replace the source_mapping with:

self.source_mapping = {
    EntityType.PERSON: [
        ('wikidata', self.wikidata_client.search_persons),
        ('viaf', self.viaf_client.search_authors),
        ('getty_ulan', self.getty_client.search_ulan_agents)
    ],
    EntityType.PLACE: [
        ('wikidata', self.wikidata_client.search_places),
        ('getty_tgn', self.getty_client.search_tgn_places)
    ],
    EntityType.ORGANIZATION: [
        ('wikidata', self.wikidata_client.search_entities),
        ('getty_ulan', self.getty_client.search_ulan_agents)  # Many orgs are in ULAN
    ],
    EntityType.SUBJECT: [
        ('getty_aat', self.getty_client.search_aat_terms),
        ('wikidata', self.wikidata_client.search_entities)
    ],
    EntityType.AUTHOR: [
        ('viaf', self.viaf_client.search_authors),
        ('wikidata', self.wikidata_client.search_persons),
        ('getty_ulan', self.getty_client.search_ulan_agents)
    ]
}
'''
    
    print("Save this to app/services/reconciliation_engine_enhanced.py:")
    print(patch_code)

def quick_fix_instructions():
    """Show quick fix instructions"""
    print(f"\n⚡ Quick Fix Instructions:")
    print("=" * 50)
    print("1. Open: app/services/reconciliation_engine.py")
    print("2. Find the source_mapping dictionary (around line 180)")
    print("3. Modify the ORGANIZATION entry to include Getty ULAN:")
    print("")
    print("   EntityType.ORGANIZATION: [")
    print("       ('wikidata', self.wikidata_client.search_entities),")
    print("       ('getty_ulan', self.getty_client.search_ulan_agents)  # ADD THIS LINE")
    print("   ],")
    print("")
    print("4. Restart Flask and reprocess your job")
    print("")
    print("Expected result:")
    print("  - Organizations will query both Wikidata AND Getty ULAN")
    print("  - Should find matches for cultural institutions")
    print("  - Minneapolis Institute of Art → should find Getty ULAN record")

if __name__ == "__main__":
    show_current_mapping()
    test_getty_manually()
    create_enhanced_source_mapping()
    quick_fix_instructions()