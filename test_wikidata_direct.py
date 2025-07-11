# File: test_wikidata_direct.py
from app.services.wikidata_cultural_client import CulturalHeritageWikidataClient

# Test the client directly
client = CulturalHeritageWikidataClient(timeout=30, max_results=5)

test_searches = [
    ("Minnesota Territorial Legislature", "organization"),
    ("Minneapolis Institute of Art", "organization"),
    ("Hodge, Emma B.", "person"),
    ("Bijou Opera House", "place")
]

for search_term, search_type in test_searches:
    print(f"\n🔍 Testing: {search_term} ({search_type})")
    
    try:
        if search_type == "person":
            results = client.search_persons(search_term)
        elif search_type == "organization":
            results = client.search_organizations(search_term)
        elif search_type == "place":
            results = client.search_places(search_term)
        
        print(f"✅ Found {len(results)} results")
        for r in results[:2]:
            print(f"  - {r.label} (Q{r.wikidata_id}): {r.description}")
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

# Check statistics
print(f"\n📊 Statistics: {client.get_statistics()}")