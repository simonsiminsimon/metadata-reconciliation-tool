# File: enhanced_wikidata_client.py
# Enhanced Wikidata client with specialized queries for cultural heritage entities

import requests
import time
import logging
from typing import List, Dict, Any
from app.services.data_sources import MatchResult, ConfidenceLevel

logger = logging.getLogger(__name__)

class EnhancedWikidataClient:
    """Enhanced Wikidata client with specialized queries for cultural heritage metadata"""
    
    def __init__(self, rate_limit: float = 1.0):
        self.endpoint = "https://query.wikidata.org/sparql"
        self.rate_limit = rate_limit
        self.last_request = 0
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'MetadataReconciliation/1.0 (educational; contact@example.com)'
        })
    
    def _wait_for_rate_limit(self):
        """Rate limiting for Wikidata"""
        current_time = time.time()
        time_since_last = current_time - self.last_request
        if time_since_last < self.rate_limit:
            time.sleep(self.rate_limit - time_since_last)
        self.last_request = time.time()
    
    def _execute_query(self, query: str) -> List[Dict]:
        """Execute SPARQL query with error handling"""
        self._wait_for_rate_limit()
        
        try:
            response = self.session.get(
                self.endpoint,
                params={'query': query, 'format': 'json'},
                timeout=30
            )
            response.raise_for_status()
            
            data = response.json()
            return data.get('results', {}).get('bindings', [])
            
        except Exception as e:
            logger.error(f"Wikidata query failed: {e}")
            return []
    
    def _calculate_confidence(self, search_term: str, result_name: str, 
                            has_description: bool = False, entity_type: str = '') -> tuple:
        """Enhanced confidence calculation"""
        search_lower = search_term.lower().strip()
        result_lower = result_name.lower().strip()
        
        # Exact match
        if search_lower == result_lower:
            return ConfidenceLevel.HIGH, 0.95
        
        # Handle institutional name variations
        search_clean = self._clean_institutional_name(search_lower)
        result_clean = self._clean_institutional_name(result_lower)
        
        if search_clean == result_clean:
            return ConfidenceLevel.HIGH, 0.90
        
        # Starts with or contains
        if result_lower.startswith(search_lower) or search_lower.startswith(result_lower):
            score = 0.85 if has_description else 0.75
            return ConfidenceLevel.HIGH, score
        
        if search_lower in result_lower or result_lower in search_lower:
            score = 0.70 if has_description else 0.60
            return ConfidenceLevel.MEDIUM, score
        
        # Word overlap for institutions
        search_words = set(search_clean.split())
        result_words = set(result_clean.split())
        
        if search_words and result_words:
            overlap = len(search_words & result_words)
            total = len(search_words | result_words)
            similarity = overlap / total if total > 0 else 0
            
            if similarity > 0.6:
                score = 0.50 + similarity * 0.3
                confidence = ConfidenceLevel.MEDIUM if score > 0.6 else ConfidenceLevel.LOW
                return confidence, score
        
        return ConfidenceLevel.LOW, 0.3
    
    def _clean_institutional_name(self, name: str) -> str:
        """Clean institutional names for better matching"""
        # Remove common institutional suffixes/prefixes that might vary
        replacements = {
            ' institute of ': ' ',
            ' institute': '',
            ' college': '',
            ' university': '',
            ' museum': '',
            ' library': '',
            ' opera house': ' theater',
            ' opera': ' theater',
            ' house': '',
            ' society': '',
            ' association': '',
            ' organization': '',
            ' the ': ' ',
            "'s office": '',
            ' office': ''
        }
        
        cleaned = name
        for old, new in replacements.items():
            cleaned = cleaned.replace(old, new)
        
        return ' '.join(cleaned.split())  # Remove extra spaces
    
    def search_cultural_institutions(self, search_term: str, limit: int = 10) -> List[MatchResult]:
        """Specialized search for cultural institutions (museums, libraries, theaters)"""
        
        # Enhanced SPARQL query for cultural institutions
        query = f"""
        SELECT DISTINCT ?org ?orgLabel ?orgDescription ?typeLabel ?locationLabel ?foundedDate ?websiteLabel WHERE {{
          {{
            # Museums
            ?org wdt:P31/wdt:P279* wd:Q33506 .
          }} UNION {{
            # Libraries  
            ?org wdt:P31/wdt:P279* wd:Q7075 .
          }} UNION {{
            # Theaters and opera houses
            ?org wdt:P31/wdt:P279* wd:Q24354 .
          }} UNION {{
            # Cultural centers
            ?org wdt:P31/wdt:P279* wd:Q1021645 .
          }} UNION {{
            # Archives
            ?org wdt:P31/wdt:P279* wd:Q166118 .
          }} UNION {{
            # Historic buildings that might be cultural institutions
            ?org wdt:P31/wdt:P279* wd:Q41176 .
            ?org wdt:P31 ?type .
            FILTER(CONTAINS(LCASE(STR(?type)), "cultural") || CONTAINS(LCASE(STR(?type)), "museum") || CONTAINS(LCASE(STR(?type)), "theater"))
          }}
          
          ?org rdfs:label ?orgLabel .
          OPTIONAL {{ ?org schema:description ?orgDescription . }}
          OPTIONAL {{ ?org wdt:P31 ?type . ?type rdfs:label ?typeLabel . }}
          OPTIONAL {{ ?org wdt:P131 ?location . ?location rdfs:label ?locationLabel . }}
          OPTIONAL {{ ?org wdt:P571 ?foundedDate . }}
          OPTIONAL {{ ?org wdt:P856 ?website . }}
          
          FILTER(LANG(?orgLabel) = "en")
          FILTER(LANG(?orgDescription) = "en") 
          FILTER(LANG(?typeLabel) = "en")
          FILTER(LANG(?locationLabel) = "en")
          
          # Flexible name matching
          FILTER(
            CONTAINS(LCASE(?orgLabel), LCASE("{search_term}")) ||
            CONTAINS(LCASE("{search_term}"), LCASE(?orgLabel))
          )
        }}
        ORDER BY DESC(STRLEN(?orgLabel))
        LIMIT {limit}
        """
        
        bindings = self._execute_query(query)
        return self._process_organization_results(bindings, search_term, 'cultural_institution')
    
    def search_educational_institutions(self, search_term: str, limit: int = 10) -> List[MatchResult]:
        """Specialized search for educational institutions"""
        
        query = f"""
        SELECT DISTINCT ?org ?orgLabel ?orgDescription ?typeLabel ?locationLabel ?foundedDate ?studentCount WHERE {{
          {{
            # Universities
            ?org wdt:P31/wdt:P279* wd:Q3918 .
          }} UNION {{
            # Colleges
            ?org wdt:P31/wdt:P279* wd:Q189004 .
          }} UNION {{
            # Schools
            ?org wdt:P31/wdt:P279* wd:Q3914 .
          }} UNION {{
            # Educational institutions (broad)
            ?org wdt:P31/wdt:P279* wd:Q2385804 .
          }}
          
          ?org rdfs:label ?orgLabel .
          OPTIONAL {{ ?org schema:description ?orgDescription . }}
          OPTIONAL {{ ?org wdt:P31 ?type . ?type rdfs:label ?typeLabel . }}
          OPTIONAL {{ ?org wdt:P131 ?location . ?location rdfs:label ?locationLabel . }}
          OPTIONAL {{ ?org wdt:P571 ?foundedDate . }}
          OPTIONAL {{ ?org wdt:P2196 ?studentCount . }}
          
          FILTER(LANG(?orgLabel) = "en")
          FILTER(LANG(?orgDescription) = "en")
          FILTER(LANG(?typeLabel) = "en") 
          FILTER(LANG(?locationLabel) = "en")
          
          FILTER(
            CONTAINS(LCASE(?orgLabel), LCASE("{search_term}")) ||
            CONTAINS(LCASE("{search_term}"), LCASE(?orgLabel))
          )
        }}
        ORDER BY DESC(?studentCount)
        LIMIT {limit}
        """
        
        bindings = self._execute_query(query)
        return self._process_organization_results(bindings, search_term, 'educational_institution')
    
    def search_government_organizations(self, search_term: str, limit: int = 10) -> List[MatchResult]:
        """Specialized search for government organizations and agencies"""
        
        query = f"""
        SELECT DISTINCT ?org ?orgLabel ?orgDescription ?typeLabel ?locationLabel ?jurisdictionLabel WHERE {{
          {{
            # Government agencies
            ?org wdt:P31/wdt:P279* wd:Q327333 .
          }} UNION {{
            # Legislative bodies
            ?org wdt:P31/wdt:P279* wd:Q11204 .
          }} UNION {{
            # Government departments
            ?org wdt:P31/wdt:P279* wd:Q2659904 .
          }} UNION {{
            # Government offices
            ?org wdt:P31/wdt:P279* wd:Q4830453 .
            ?org wdt:P17 ?country .
            FILTER(?country = wd:Q30)  # United States (since your data appears to be US-focused)
          }}
          
          ?org rdfs:label ?orgLabel .
          OPTIONAL {{ ?org schema:description ?orgDescription . }}
          OPTIONAL {{ ?org wdt:P31 ?type . ?type rdfs:label ?typeLabel . }}
          OPTIONAL {{ ?org wdt:P131 ?location . ?location rdfs:label ?locationLabel . }}
          OPTIONAL {{ ?org wdt:P1001 ?jurisdiction . ?jurisdiction rdfs:label ?jurisdictionLabel . }}
          
          FILTER(LANG(?orgLabel) = "en")
          FILTER(LANG(?orgDescription) = "en")
          FILTER(LANG(?typeLabel) = "en")
          FILTER(LANG(?locationLabel) = "en")
          FILTER(LANG(?jurisdictionLabel) = "en")
          
          FILTER(
            CONTAINS(LCASE(?orgLabel), LCASE("{search_term}")) ||
            CONTAINS(LCASE("{search_term}"), LCASE(?orgLabel))
          )
        }}
        LIMIT {limit}
        """
        
        bindings = self._execute_query(query)
        return self._process_organization_results(bindings, search_term, 'government_organization')
    
    def search_historical_organizations(self, search_term: str, limit: int = 10) -> List[MatchResult]:
        """Search for historical organizations, including defunct entities"""
        
        query = f"""
        SELECT DISTINCT ?org ?orgLabel ?orgDescription ?typeLabel ?locationLabel ?startDate ?endDate WHERE {{
          {{
            # Organizations (broad category)
            ?org wdt:P31/wdt:P279* wd:Q43229 .
          }} UNION {{
            # Historical societies
            ?org wdt:P31/wdt:P279* wd:Q5774129 .
          }} UNION {{
            # Historical entities
            ?org wdt:P31/wdt:P279* wd:Q28640 .
          }}
          
          ?org rdfs:label ?orgLabel .
          OPTIONAL {{ ?org schema:description ?orgDescription . }}
          OPTIONAL {{ ?org wdt:P31 ?type . ?type rdfs:label ?typeLabel . }}
          OPTIONAL {{ ?org wdt:P131 ?location . ?location rdfs:label ?locationLabel . }}
          OPTIONAL {{ ?org wdt:P571 ?startDate . }}
          OPTIONAL {{ ?org wdt:P576 ?endDate . }}
          
          FILTER(LANG(?orgLabel) = "en")
          FILTER(LANG(?orgDescription) = "en")
          FILTER(LANG(?typeLabel) = "en")
          FILTER(LANG(?locationLabel) = "en")
          
          FILTER(
            CONTAINS(LCASE(?orgLabel), LCASE("{search_term}")) ||
            CONTAINS(LCASE("{search_term}"), LCASE(?orgLabel))
          )
          
          # Prefer entities with historical context
          FILTER(BOUND(?startDate) || BOUND(?endDate) || CONTAINS(LCASE(?orgDescription), "historical"))
        }}
        LIMIT {limit}
        """
        
        bindings = self._execute_query(query)
        return self._process_organization_results(bindings, search_term, 'historical_organization')
    
    def smart_organization_search(self, search_term: str, limit: int = 10) -> List[MatchResult]:
        """Intelligent organization search that tries multiple specialized queries"""
        
        all_results = []
        
        # Determine search strategy based on keywords
        term_lower = search_term.lower()
        
        if any(word in term_lower for word in ['museum', 'institute', 'library', 'theater', 'opera', 'cultural']):
            results = self.search_cultural_institutions(search_term, limit//2)
            all_results.extend(results)
        
        if any(word in term_lower for word in ['college', 'university', 'school', 'academy']):
            results = self.search_educational_institutions(search_term, limit//2)
            all_results.extend(results)
        
        if any(word in term_lower for word in ['legislature', 'government', 'department', 'office', 'agency']):
            results = self.search_government_organizations(search_term, limit//2)
            all_results.extend(results)
        
        if any(word in term_lower for word in ['society', 'league', 'association', 'historical']):
            results = self.search_historical_organizations(search_term, limit//2)
            all_results.extend(results)
        
        # If no specialized search was triggered, use cultural institutions as default
        if not all_results:
            all_results = self.search_cultural_institutions(search_term, limit)
        
        # Remove duplicates and sort by score
        seen_ids = set()
        unique_results = []
        
        for result in all_results:
            if result.id not in seen_ids:
                seen_ids.add(result.id)
                unique_results.append(result)
        
        unique_results.sort(key=lambda x: x.score, reverse=True)
        return unique_results[:limit]
    
    def _process_organization_results(self, bindings: List[Dict], search_term: str, org_type: str) -> List[MatchResult]:
        """Process organization query results into MatchResult objects"""
        
        results = []
        
        for binding in bindings:
            org_id = binding.get('org', {}).get('value', '').split('/')[-1]
            name = binding.get('orgLabel', {}).get('value', '')
            description = binding.get('orgDescription', {}).get('value', '')
            
            if not org_id or not name:
                continue
            
            # Build additional info
            additional_info = {
                'organization_type': org_type,
                'wikidata_url': f"https://www.wikidata.org/entity/{org_id}"
            }
            
            if 'typeLabel' in binding:
                additional_info['type'] = binding['typeLabel']['value']
            if 'locationLabel' in binding:
                additional_info['location'] = binding['locationLabel']['value']
            if 'foundedDate' in binding:
                additional_info['founded'] = binding['foundedDate']['value']
            if 'websiteLabel' in binding:
                additional_info['website'] = binding['websiteLabel']['value']
            if 'studentCount' in binding:
                additional_info['student_count'] = binding['studentCount']['value']
            if 'jurisdictionLabel' in binding:
                additional_info['jurisdiction'] = binding['jurisdictionLabel']['value']
            if 'startDate' in binding:
                additional_info['start_date'] = binding['startDate']['value']
            if 'endDate' in binding:
                additional_info['end_date'] = binding['endDate']['value']
            
            # Calculate confidence
            confidence, score = self._calculate_confidence(
                search_term, name, bool(description), org_type
            )
            
            result = MatchResult(
                id=org_id,
                name=name,
                description=description,
                confidence=confidence,
                score=score,
                source='wikidata',
                additional_info=additional_info
            )
            
            results.append(result)
        
        return results


def test_enhanced_wikidata():
    """Test the enhanced Wikidata client with your entities"""
    print("🧪 Testing Enhanced Wikidata Queries...")
    print("=" * 60)
    
    client = EnhancedWikidataClient()
    
    # Test entities from your reconciliation report
    test_entities = [
        "Minneapolis Institute of Art",
        "Carleton College", 
        "Minnesota Anti-Saloon League",
        "Bijou Opera House",
        "Minnesota Territorial Legislature"
    ]
    
    for entity in test_entities:
        print(f"\n🔍 Testing: {entity}")
        print("-" * 40)
        
        # Test smart search
        results = client.smart_organization_search(entity, limit=5)
        
        if results:
            print(f"✅ Found {len(results)} results:")
            for i, result in enumerate(results, 1):
                print(f"  {i}. {result.name}")
                print(f"     ID: {result.id}")
                print(f"     Score: {result.score:.2f}")
                print(f"     Confidence: {result.confidence.value}")
                print(f"     Type: {result.additional_info.get('organization_type', 'N/A')}")
                if result.additional_info.get('location'):
                    print(f"     Location: {result.additional_info['location']}")
                if result.description:
                    print(f"     Description: {result.description[:100]}...")
                print()
        else:
            print("❌ No results found")


def integrate_enhanced_wikidata():
    """Show how to integrate enhanced Wikidata into your reconciliation engine"""
    print(f"\n🔧 Integration Instructions:")
    print("=" * 60)
    
    integration_code = '''
# 1. Save the enhanced client as: app/services/enhanced_wikidata_client.py

# 2. Modify your reconciliation_engine.py:

from app.services.enhanced_wikidata_client import EnhancedWikidataClient

# In __init__, replace:
self.wikidata_client = WikidataClient(rate_limit=1.0)
# With:
self.enhanced_wikidata_client = EnhancedWikidataClient(rate_limit=1.0)

# Update source_mapping:
self.source_mapping = {
    EntityType.PERSON: [
        ('wikidata_persons', self.wikidata_client.search_persons),
        ('viaf_authors', self.viaf_client.search_authors)
    ],
    EntityType.ORGANIZATION: [
        ('wikidata_smart', self.enhanced_wikidata_client.smart_organization_search),
        ('viaf_authors', self.viaf_client.search_authors)  # Some orgs might be in VIAF too
    ],
    EntityType.PLACE: [
        ('wikidata_places', self.wikidata_client.search_places)
    ],
    EntityType.SUBJECT: [
        ('wikidata_entities', self.wikidata_client.search_entities)
    ]
}
'''
    
    print(integration_code)
    
    print("\n📈 Expected Improvements:")
    print("- Minneapolis Institute of Art: Should find exact match")
    print("- Carleton College: Should find exact match") 
    print("- Minnesota Anti-Saloon League: Should find historical organization")
    print("- Bijou Opera House: Should find theater/venue matches")
    print("- Better confidence scores with institutional name normalization")
    print("- Richer metadata (founding dates, locations, types)")


if __name__ == "__main__":
    test_enhanced_wikidata()
    integrate_enhanced_wikidata()