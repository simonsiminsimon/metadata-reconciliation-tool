# File: app/services/wikidata_cultural_client.py
"""
Enhanced Wikidata Client for Cultural Heritage Metadata Reconciliation

This client is specifically designed for cultural heritage institutions to reconcile
metadata against Wikidata. It includes specialized queries for:
- People (authors, artists, historical figures)
- Places (geographic locations, institutions)
- Organizations (libraries, museums, universities)
- Cultural objects (artworks, manuscripts, publications)
- Subjects/topics (historical events, concepts)

IMPROVEMENTS:
- API-first approach with SPARQL fallback
- Simplified queries to avoid timeouts
- Retry logic with exponential backoff
- Better error handling and resilience
"""

import requests
import time
import logging
import json
from typing import Dict, List, Optional, Tuple, Union
from dataclasses import dataclass
from enum import Enum
from urllib.parse import quote
import re
from datetime import datetime
import hashlib

# Configure logging
logger = logging.getLogger(__name__)


class EntityType(Enum):
    """Types of entities commonly found in cultural heritage metadata"""
    PERSON = "person"
    PLACE = "place"
    ORGANIZATION = "organization"
    ARTWORK = "artwork"
    PUBLICATION = "publication"
    MANUSCRIPT = "manuscript"
    SUBJECT_TOPIC = "subject"
    EVENT = "event"
    CONCEPT = "concept"
    BUILDING = "building"


class ConfidenceLevel(Enum):
    """Confidence levels for matches"""
    VERY_HIGH = "very_high"  # 0.9+
    HIGH = "high"           # 0.7-0.9
    MEDIUM = "medium"       # 0.5-0.7
    LOW = "low"            # 0.3-0.5
    VERY_LOW = "very_low"  # 0.0-0.3


@dataclass
class WikidataMatch:
    """Enhanced match result with cultural heritage specific fields"""
    wikidata_id: str
    label: str
    description: str
    confidence_level: ConfidenceLevel
    confidence_score: float
    entity_type: EntityType
    aliases: List[str]
    
    # Cultural heritage specific fields
    birth_date: Optional[str] = None
    death_date: Optional[str] = None
    coordinates: Optional[str] = None
    country: Optional[str] = None
    inception_date: Optional[str] = None
    website: Optional[str] = None
    viaf_id: Optional[str] = None
    library_of_congress_id: Optional[str] = None
    
    # Additional metadata
    image_url: Optional[str] = None
    commons_category: Optional[str] = None
    external_ids: Dict[str, str] = None
    
    def __post_init__(self):
        if self.external_ids is None:
            self.external_ids = {}


class CulturalHeritageWikidataClient:
    """
    Enhanced Wikidata client for cultural heritage metadata reconciliation
    
    Features:
    - API-first approach for better reliability
    - Simplified SPARQL queries to avoid timeouts
    - Intelligent fallback strategies
    - Built-in caching and rate limiting
    - Cultural heritage authority linking (VIAF, LC, etc.)
    """
    
    def __init__(self, rate_limit: float = 1.0, timeout: int = 60, 
                 cache_enabled: bool = True, max_results: int = 10):
        """
        Initialize the Wikidata client
        
        Args:
            rate_limit: Requests per second (default: 1.0)
            timeout: Request timeout in seconds (increased to 60)
            cache_enabled: Enable in-memory caching (default: True)
            max_results: Maximum results per query (default: 10)
        """
        self.sparql_endpoint = "https://query.wikidata.org/sparql"
        self.wikidata_api = "https://www.wikidata.org/w/api.php"
        self.rate_limit = rate_limit
        self.timeout = timeout
        self.timeout_short = 15  # For API calls
        self.max_results = max_results
        
        # Rate limiting
        self.last_request_time = 0
        
        # Simple in-memory cache
        self.cache_enabled = cache_enabled
        self.cache = {} if cache_enabled else None
        self.cache_ttl = 3600  # 1 hour
        
        # Session for connection pooling
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'CulturalHeritageReconciliation/2.0 (https://github.com/yourinstitution/metadata-reconciliation)'
        })
        
        # Statistics
        self.stats = {
            'queries_made': 0,
            'api_calls': 0,
            'cache_hits': 0,
            'successful_matches': 0,
            'timeouts': 0,
            'retries': 0
        }
    
    def _respect_rate_limit(self):
        """Ensure we don't exceed the rate limit"""
        if self.rate_limit <= 0:
            return
            
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        min_interval = 1.0 / self.rate_limit
        
        if time_since_last < min_interval:
            sleep_time = min_interval - time_since_last
            time.sleep(sleep_time)
        
        self.last_request_time = time.time()
    
    def _get_from_cache(self, cache_key: str) -> Optional[List[WikidataMatch]]:
        """Get results from cache if available and not expired"""
        if not self.cache_enabled or cache_key not in self.cache:
            return None
        
        cached_data = self.cache[cache_key]
        if time.time() - cached_data['timestamp'] > self.cache_ttl:
            del self.cache[cache_key]
            return None
        
        self.stats['cache_hits'] += 1
        return cached_data['results']
    
    def _store_in_cache(self, cache_key: str, results: List[WikidataMatch]):
        """Store results in cache"""
        if not self.cache_enabled:
            return
        
        self.cache[cache_key] = {
            'results': results,
            'timestamp': time.time()
        }
    
    def _make_cache_key(self, prefix: str, search_term: str, context: Optional[Dict] = None) -> str:
        """Create a cache key from search parameters"""
        key_parts = [prefix, search_term.lower()]
        if context:
            key_parts.append(str(sorted(context.items())))
        key_string = "|".join(key_parts)
        return hashlib.md5(key_string.encode()).hexdigest()
    
    def _api_search(self, search_term: str, limit: int = None) -> List[Dict]:
        """Use Wikidata API for fast, reliable searches"""
        if limit is None:
            limit = self.max_results
            
        self._respect_rate_limit()
        self.stats['api_calls'] += 1
        
        try:
            response = self.session.get(
                self.wikidata_api,
                params={
                    'action': 'wbsearchentities',
                    'search': search_term,
                    'language': 'en',
                    'format': 'json',
                    'limit': limit,
                    'type': 'item'
                },
                timeout=self.timeout_short
            )
            response.raise_for_status()
            
            data = response.json()
            return data.get('search', [])
            
        except requests.exceptions.Timeout:
            self.stats['timeouts'] += 1
            logger.warning(f"API search timeout for '{search_term}'")
            return []
        except Exception as e:
            logger.error(f"API search failed for '{search_term}': {e}")
            return []
    
    def _simple_sparql_query(self, query: str, max_retries: int = 2) -> List[Dict]:
        """Execute a simple SPARQL query with retry logic"""
        for attempt in range(max_retries):
            try:
                self._respect_rate_limit()
                self.stats['queries_made'] += 1
                
                # Use shorter timeout for first attempt
                timeout = 30 if attempt == 0 else self.timeout
                
                response = self.session.get(
                    self.sparql_endpoint,
                    params={
                        'query': query,
                        'format': 'json'
                    },
                    timeout=timeout
                )
                response.raise_for_status()
                
                data = response.json()
                return data.get('results', {}).get('bindings', [])
                
            except requests.exceptions.Timeout:
                self.stats['timeouts'] += 1
                if attempt < max_retries - 1:
                    self.stats['retries'] += 1
                    wait_time = (attempt + 1) * 2
                    logger.warning(f"SPARQL timeout (attempt {attempt + 1}/{max_retries}), retrying in {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    logger.error("SPARQL query failed after all retries")
                    return []
            except Exception as e:
                logger.error(f"SPARQL query failed: {e}")
                return []
        
        return []
    
    def _calculate_confidence(self, search_term: str, result_label: str, 
                            result_description: str = "", aliases: List[str] = None,
                            context_hints: Dict[str, str] = None) -> Tuple[ConfidenceLevel, float]:
        """Calculate confidence score using multiple factors"""
        if aliases is None:
            aliases = []
        if context_hints is None:
            context_hints = {}
        
        # Normalize strings for comparison
        search_lower = search_term.lower().strip()
        label_lower = result_label.lower().strip()
        description_lower = result_description.lower().strip() if result_description else ""
        
        score = 0.0
        
        # Exact label match
        if search_lower == label_lower:
            score = 0.95
        # Very close match
        elif search_lower in label_lower or label_lower in search_lower:
            # Calculate word overlap
            search_words = set(search_lower.split())
            label_words = set(label_lower.split())
            if search_words and label_words:
                overlap = len(search_words & label_words)
                total = len(search_words | label_words)
                score = 0.5 + (overlap / total) * 0.4
            else:
                score = 0.7
        else:
            # Partial word matching
            search_words = set(search_lower.split())
            label_words = set(label_lower.split())
            if search_words and label_words:
                overlap = len(search_words & label_words)
                if overlap > 0:
                    score = 0.3 + (overlap / len(search_words)) * 0.4
                else:
                    score = 0.2
            else:
                score = 0.1
        
        # Boost for description matches
        if description_lower and search_lower in description_lower:
            score = min(score + 0.1, 1.0)
        
        # Determine confidence level
        if score >= 0.9:
            confidence_level = ConfidenceLevel.VERY_HIGH
        elif score >= 0.7:
            confidence_level = ConfidenceLevel.HIGH
        elif score >= 0.5:
            confidence_level = ConfidenceLevel.MEDIUM
        elif score >= 0.3:
            confidence_level = ConfidenceLevel.LOW
        else:
            confidence_level = ConfidenceLevel.VERY_LOW
        
        return confidence_level, score
    
    def search_persons(self, name: str, context_hints: Dict[str, str] = None) -> List[WikidataMatch]:
        """Search for persons using API-first approach with SPARQL fallback"""
        cache_key = self._make_cache_key("person", name, context_hints)
        cached_results = self._get_from_cache(cache_key)
        if cached_results is not None:
            return cached_results
        
        matches = []
        
        # Step 1: Try API search first (fast and reliable)
        api_results = self._api_search(name, limit=self.max_results * 2)
        
        for item in api_results:
            description = item.get('description', '').lower()
            # Filter for persons based on description
            person_indicators = ['born', 'died', 'author', 'writer', 'artist', 'politician', 
                               'actor', 'musician', 'scientist', 'person', 'activist', 
                               'scholar', 'poet', 'philosopher', 'historian']
            
            if any(indicator in description for indicator in person_indicators):
                confidence_level, confidence_score = self._calculate_confidence(
                    name, item.get('label', ''), description
                )
                
                match = WikidataMatch(
                    wikidata_id=item['id'],
                    label=item.get('label', ''),
                    description=item.get('description', ''),
                    confidence_level=confidence_level,
                    confidence_score=confidence_score,
                    entity_type=EntityType.PERSON,
                    aliases=[]
                )
                matches.append(match)
        
        # Step 2: If API didn't return enough results, try simple SPARQL
        if len(matches) < 3:
            query = f"""
            SELECT DISTINCT ?person ?personLabel ?personDescription 
            WHERE {{
              ?person wdt:P31 wd:Q5 .
              ?person rdfs:label ?personLabel .
              FILTER(LANG(?personLabel) = "en")
              FILTER(CONTAINS(LCASE(?personLabel), LCASE("{name}")))
              OPTIONAL {{ ?person schema:description ?personDescription . FILTER(LANG(?personDescription) = "en") }}
            }}
            LIMIT {self.max_results}
            """
            
            sparql_results = self._simple_sparql_query(query)
            
            for binding in sparql_results:
                wikidata_id = binding.get('person', {}).get('value', '').split('/')[-1]
                label = binding.get('personLabel', {}).get('value', '')
                description = binding.get('personDescription', {}).get('value', '')
                
                if wikidata_id and label:
                    # Check if we already have this from API
                    if not any(m.wikidata_id == wikidata_id for m in matches):
                        confidence_level, confidence_score = self._calculate_confidence(
                            name, label, description
                        )
                        
                        match = WikidataMatch(
                            wikidata_id=wikidata_id,
                            label=label,
                            description=description,
                            confidence_level=confidence_level,
                            confidence_score=confidence_score,
                            entity_type=EntityType.PERSON,
                            aliases=[]
                        )
                        matches.append(match)
        
        # Sort by confidence score
        matches.sort(key=lambda x: x.confidence_score, reverse=True)
        matches = matches[:self.max_results]
        
        if matches:
            self.stats['successful_matches'] += 1
        
        self._store_in_cache(cache_key, matches)
        return matches
    
    def search_places(self, place_name: str, context_hints: Dict[str, str] = None) -> List[WikidataMatch]:
        """Search for places using API-first approach"""
        cache_key = self._make_cache_key("place", place_name, context_hints)
        cached_results = self._get_from_cache(cache_key)
        if cached_results is not None:
            return cached_results
        
        matches = []
        
        # Step 1: API search
        api_results = self._api_search(place_name, limit=self.max_results * 2)
        
        for item in api_results:
            description = item.get('description', '').lower()
            # Filter for places
            place_indicators = ['city', 'town', 'village', 'country', 'state', 'province', 
                              'municipality', 'capital', 'island', 'mountain', 'river', 
                              'lake', 'building', 'museum', 'library', 'university']
            
            if any(indicator in description for indicator in place_indicators):
                confidence_level, confidence_score = self._calculate_confidence(
                    place_name, item.get('label', ''), description
                )
                
                match = WikidataMatch(
                    wikidata_id=item['id'],
                    label=item.get('label', ''),
                    description=item.get('description', ''),
                    confidence_level=confidence_level,
                    confidence_score=confidence_score,
                    entity_type=EntityType.PLACE,
                    aliases=[]
                )
                matches.append(match)
        
        # Step 2: Simple SPARQL fallback if needed
        if len(matches) < 3:
            query = f"""
            SELECT DISTINCT ?place ?placeLabel ?placeDescription 
            WHERE {{
              {{ ?place wdt:P31/wdt:P279* wd:Q618123 . }}  # Geographic location
              UNION
              {{ ?place wdt:P31/wdt:P279* wd:Q41176 . }}   # Building
              ?place rdfs:label ?placeLabel .
              FILTER(LANG(?placeLabel) = "en")
              FILTER(CONTAINS(LCASE(?placeLabel), LCASE("{place_name}")))
              OPTIONAL {{ ?place schema:description ?placeDescription . FILTER(LANG(?placeDescription) = "en") }}
            }}
            LIMIT {self.max_results}
            """
            
            sparql_results = self._simple_sparql_query(query)
            
            for binding in sparql_results:
                wikidata_id = binding.get('place', {}).get('value', '').split('/')[-1]
                label = binding.get('placeLabel', {}).get('value', '')
                description = binding.get('placeDescription', {}).get('value', '')
                
                if wikidata_id and label:
                    if not any(m.wikidata_id == wikidata_id for m in matches):
                        confidence_level, confidence_score = self._calculate_confidence(
                            place_name, label, description
                        )
                        
                        match = WikidataMatch(
                            wikidata_id=wikidata_id,
                            label=label,
                            description=description,
                            confidence_level=confidence_level,
                            confidence_score=confidence_score,
                            entity_type=EntityType.PLACE,
                            aliases=[]
                        )
                        matches.append(match)
        
        matches.sort(key=lambda x: x.confidence_score, reverse=True)
        matches = matches[:self.max_results]
        
        if matches:
            self.stats['successful_matches'] += 1
        
        self._store_in_cache(cache_key, matches)
        return matches
    
    def search_organizations(self, org_name: str, context_hints: Dict[str, str] = None) -> List[WikidataMatch]:
        """Search for organizations using API-first approach"""
        cache_key = self._make_cache_key("organization", org_name, context_hints)
        cached_results = self._get_from_cache(cache_key)
        if cached_results is not None:
            return cached_results
        
        matches = []
        
        # Step 1: API search
        api_results = self._api_search(org_name, limit=self.max_results * 2)
        
        for item in api_results:
            description = item.get('description', '').lower()
            label = item.get('label', '').lower()
            
            # Filter for organizations
            org_indicators = ['organization', 'institution', 'company', 'corporation', 
                            'museum', 'library', 'university', 'college', 'school', 
                            'society', 'association', 'foundation', 'institute', 
                            'department', 'agency', 'office', 'legislature', 'council']
            
            if any(indicator in description or indicator in label for indicator in org_indicators):
                confidence_level, confidence_score = self._calculate_confidence(
                    org_name, item.get('label', ''), description
                )
                
                match = WikidataMatch(
                    wikidata_id=item['id'],
                    label=item.get('label', ''),
                    description=item.get('description', ''),
                    confidence_level=confidence_level,
                    confidence_score=confidence_score,
                    entity_type=EntityType.ORGANIZATION,
                    aliases=[]
                )
                matches.append(match)
        
        # Step 2: Simple SPARQL fallback
        if len(matches) < 3:
            query = f"""
            SELECT DISTINCT ?org ?orgLabel ?orgDescription 
            WHERE {{
              {{ ?org wdt:P31/wdt:P279* wd:Q43229 . }}     # Organization
              UNION
              {{ ?org wdt:P31/wdt:P279* wd:Q33506 . }}     # Museum
              UNION
              {{ ?org wdt:P31/wdt:P279* wd:Q7075 . }}      # Library
              UNION
              {{ ?org wdt:P31/wdt:P279* wd:Q3918 . }}      # University
              ?org rdfs:label ?orgLabel .
              FILTER(LANG(?orgLabel) = "en")
              FILTER(CONTAINS(LCASE(?orgLabel), LCASE("{org_name}")))
              OPTIONAL {{ ?org schema:description ?orgDescription . FILTER(LANG(?orgDescription) = "en") }}
            }}
            LIMIT {self.max_results}
            """
            
            sparql_results = self._simple_sparql_query(query)
            
            for binding in sparql_results:
                wikidata_id = binding.get('org', {}).get('value', '').split('/')[-1]
                label = binding.get('orgLabel', {}).get('value', '')
                description = binding.get('orgDescription', {}).get('value', '')
                
                if wikidata_id and label:
                    if not any(m.wikidata_id == wikidata_id for m in matches):
                        confidence_level, confidence_score = self._calculate_confidence(
                            org_name, label, description
                        )
                        
                        match = WikidataMatch(
                            wikidata_id=wikidata_id,
                            label=label,
                            description=description,
                            confidence_level=confidence_level,
                            confidence_score=confidence_score,
                            entity_type=EntityType.ORGANIZATION,
                            aliases=[]
                        )
                        matches.append(match)
        
        matches.sort(key=lambda x: x.confidence_score, reverse=True)
        matches = matches[:self.max_results]
        
        if matches:
            self.stats['successful_matches'] += 1
        
        self._store_in_cache(cache_key, matches)
        return matches
    
    def search_subjects(self, subject_term: str, context_hints: Dict[str, str] = None) -> List[WikidataMatch]:
        """Search for subjects/topics using API-first approach"""
        cache_key = self._make_cache_key("subject", subject_term, context_hints)
        cached_results = self._get_from_cache(cache_key)
        if cached_results is not None:
            return cached_results
        
        matches = []
        
        # For subjects, we can use the API more broadly
        api_results = self._api_search(subject_term, limit=self.max_results)
        
        for item in api_results:
            # For subjects, we're less restrictive
            confidence_level, confidence_score = self._calculate_confidence(
                subject_term, item.get('label', ''), item.get('description', '')
            )
            
            match = WikidataMatch(
                wikidata_id=item['id'],
                label=item.get('label', ''),
                description=item.get('description', ''),
                confidence_level=confidence_level,
                confidence_score=confidence_score,
                entity_type=EntityType.SUBJECT_TOPIC,
                aliases=[]
            )
            matches.append(match)
        
        matches.sort(key=lambda x: x.confidence_score, reverse=True)
        matches = matches[:self.max_results]
        
        if matches:
            self.stats['successful_matches'] += 1
        
        self._store_in_cache(cache_key, matches)
        return matches
    
    def get_entity_details(self, wikidata_id: str) -> Optional[WikidataMatch]:
        """Get detailed information about a specific entity using API"""
        cache_key = f"details:{wikidata_id}"
        cached_results = self._get_from_cache(cache_key)
        if cached_results is not None and cached_results:
            return cached_results[0]
        
        try:
            # Use wbgetentities API for specific entity details
            response = self.session.get(
                self.wikidata_api,
                params={
                    'action': 'wbgetentities',
                    'ids': wikidata_id,
                    'languages': 'en',
                    'format': 'json'
                },
                timeout=self.timeout_short
            )
            response.raise_for_status()
            
            data = response.json()
            entity_data = data.get('entities', {}).get(wikidata_id, {})
            
            if not entity_data:
                return None
            
            # Extract basic information
            labels = entity_data.get('labels', {})
            descriptions = entity_data.get('descriptions', {})
            aliases = entity_data.get('aliases', {})
            
            label = labels.get('en', {}).get('value', '')
            description = descriptions.get('en', {}).get('value', '')
            alias_list = [alias['value'] for alias in aliases.get('en', [])]
            
            # Determine entity type from claims (P31 - instance of)
            entity_type = EntityType.CONCEPT
            claims = entity_data.get('claims', {})
            
            if 'P31' in claims:  # instance of
                for claim in claims['P31']:
                    try:
                        instance_id = claim['mainsnak']['datavalue']['value']['id']
                        if instance_id == 'Q5':  # human
                            entity_type = EntityType.PERSON
                            break
                        elif instance_id in ['Q43229', 'Q33506', 'Q7075']:  # organization types
                            entity_type = EntityType.ORGANIZATION
                            break
                    except:
                        continue
            
            match = WikidataMatch(
                wikidata_id=wikidata_id,
                label=label,
                description=description,
                confidence_level=ConfidenceLevel.VERY_HIGH,
                confidence_score=1.0,
                entity_type=entity_type,
                aliases=alias_list
            )
            
            # Extract additional properties if available
            if 'P214' in claims:  # VIAF ID
                try:
                    match.viaf_id = claims['P214'][0]['mainsnak']['datavalue']['value']
                except:
                    pass
            
            if 'P244' in claims:  # Library of Congress ID
                try:
                    match.library_of_congress_id = claims['P244'][0]['mainsnak']['datavalue']['value']
                except:
                    pass
            
            self._store_in_cache(cache_key, [match])
            return match
            
        except Exception as e:
            logger.error(f"Failed to get entity details for {wikidata_id}: {e}")
            return None
    
    def get_statistics(self) -> Dict[str, Union[int, float]]:
        """Get client usage statistics"""
        stats = self.stats.copy()
        total_calls = stats['queries_made'] + stats['api_calls']
        
        if total_calls > 0:
            stats['cache_hit_rate'] = stats['cache_hits'] / (total_calls + stats['cache_hits'])
            stats['success_rate'] = stats['successful_matches'] / total_calls
            stats['timeout_rate'] = stats['timeouts'] / total_calls
        else:
            stats['cache_hit_rate'] = 0.0
            stats['success_rate'] = 0.0
            stats['timeout_rate'] = 0.0
        
        if self.cache_enabled:
            stats['cache_size'] = len(self.cache)
        
        return stats
    
    def clear_cache(self):
        """Clear the internal cache"""
        if self.cache_enabled:
            self.cache.clear()
            logger.info("Cache cleared")


# Example usage and testing
if __name__ == "__main__":
    # Initialize the client
    client = CulturalHeritageWikidataClient(rate_limit=1.0, max_results=5)
    
    print("Testing Enhanced Cultural Heritage Wikidata Client...")
    print("=" * 50)
    
    # Test entities
    test_entities = [
        ("Emma B. Hodge", EntityType.PERSON),
        ("Minneapolis Institute of Art", EntityType.ORGANIZATION),
        ("Carleton College", EntityType.ORGANIZATION),
        ("Minnesota", EntityType.PLACE)
    ]
    
    for entity_name, entity_type in test_entities:
        print(f"\n🔍 Testing: {entity_name} ({entity_type.value})")
        print("-" * 40)
        
        if entity_type == EntityType.PERSON:
            results = client.search_persons(entity_name)
        elif entity_type == EntityType.ORGANIZATION:
            results = client.search_organizations(entity_name)
        elif entity_type == EntityType.PLACE:
            results = client.search_places(entity_name)
        else:
            results = []
        
        if results:
            print(f"✅ Found {len(results)} results:")
            for i, result in enumerate(results[:3], 1):
                print(f"  {i}. {result.label} (Q{result.wikidata_id})")
                print(f"    Confidence: {result.confidence_level.value} ({result.confidence_score:.2f})")
                print(f"    Description: {result.description}")
        else:
            print("❌ No results found")
    
    # Show statistics
    print("\n📊 Client Statistics:")
    stats = client.get_statistics()
    for key, value in stats.items():
        if isinstance(value, float):
            print(f"  {key}: {value:.2%}")
        else:
            print(f"  {key}: {value}")