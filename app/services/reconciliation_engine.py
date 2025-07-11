# File: app/services/reconciliation_engine.py
"""
Simplified Entity Reconciliation Engine for processing CSV entities.
This version uses basic functionality without complex dependencies.
"""

import hashlib
import json
import time
from typing import Dict, List, Optional, Set, Tuple, Any, Union
from dataclasses import dataclass
from enum import Enum
import logging
import threading

# Third-party imports (basic ones only)
import pandas as pd
import requests
from urllib.parse import quote

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class EntityType(Enum):
    """Types of entities that can be reconciled"""
    PERSON = "person"
    PLACE = "place"
    ORGANIZATION = "organization"
    SUBJECT = "subject"
    AUTHOR = "author"
    ARTWORK = "artwork"
    UNKNOWN = "unknown"


class ConfidenceLevel(Enum):
    """Confidence levels for matches"""
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


@dataclass
class MatchResult:
    """Result from an external authority source"""
    id: str
    name: str
    description: str
    confidence: ConfidenceLevel
    score: float
    source: str
    additional_info: Dict[str, Any]


@dataclass
class Entity:
    """Entity to be reconciled"""
    id: str
    name: str
    entity_type: EntityType
    context: Dict[str, Any]
    source_row: int
    
    def __post_init__(self):
        """Normalize name for consistent processing"""
        self.normalized_name = self.name.strip().lower()
        self.search_key = self._generate_search_key()
    
    def _generate_search_key(self) -> str:
        """Generate a unique key for caching"""
        context_str = json.dumps(self.context, sort_keys=True, default=str)
        key_data = f"{self.normalized_name}:{self.entity_type.value}:{context_str}"
        return hashlib.md5(key_data.encode()).hexdigest()


@dataclass
class ReconciliationResult:
    """Result of reconciliation process"""
    entity: Entity
    matches: List[MatchResult]
    best_match: Optional[MatchResult]
    confidence: ConfidenceLevel
    reconciliation_time: float
    sources_queried: List[str]
    cached: bool = False


class SimpleCache:
    """Simple in-memory cache"""
    
    def __init__(self, max_size: int = 1000):
        self.cache = {}
        self.max_size = max_size
        self._lock = threading.Lock()
    
    def get(self, key: str) -> Optional[Any]:
        with self._lock:
            return self.cache.get(key)
    
    def set(self, key: str, value: Any) -> bool:
        with self._lock:
            if len(self.cache) >= self.max_size:
                # Remove oldest item (simple LRU)
                oldest_key = next(iter(self.cache))
                del self.cache[oldest_key]
            self.cache[key] = value
            return True
    
    def get_many(self, keys: List[str]) -> Dict[str, Any]:
        with self._lock:
            return {key: self.cache[key] for key in keys if key in self.cache}


class WikidataClient:
    """Simple Wikidata client"""
    
    def __init__(self, rate_limit: float = 1.0):
        self.rate_limit = rate_limit
        self.last_request = 0
        self.base_url = "https://www.wikidata.org/w/api.php"
    
    def _wait_for_rate_limit(self):
        """Simple rate limiting"""
        current_time = time.time()
        time_since_last = current_time - self.last_request
        if time_since_last < self.rate_limit:
            time.sleep(self.rate_limit - time_since_last)
        self.last_request = time.time()
    
    def search_entities(self, query: str, entity_type: str = None, limit: int = 10) -> List[MatchResult]:
        """Search Wikidata entities"""
        self._wait_for_rate_limit()
        
        try:
            params = {
                'action': 'wbsearchentities',
                'search': query,
                'language': 'en',
                'format': 'json',
                'limit': limit
            }
            
            if entity_type:
                params['type'] = entity_type
            
            response = requests.get(self.base_url, params=params, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            matches = []
            
            for item in data.get('search', []):
                # Calculate simple score based on label similarity
                score = self._calculate_simple_score(query, item.get('label', ''))
                
                match = MatchResult(
                    id=item.get('id', ''),
                    name=item.get('label', ''),
                    description=item.get('description', ''),
                    confidence=self._score_to_confidence(score),
                    score=score,
                    source='wikidata',
                    additional_info={
                        'url': f"https://www.wikidata.org/entity/{item.get('id', '')}",
                        'concepturi': item.get('concepturi', ''),
                        'aliases': item.get('aliases', [])
                    }
                )
                matches.append(match)
            
            return matches
            
        except Exception as e:
            logger.error(f"Wikidata search error for '{query}': {e}")
            return []
    
    def _calculate_simple_score(self, query: str, label: str) -> float:
        """Simple scoring based on string similarity"""
        if not query or not label:
            return 0.0
        
        query_lower = query.lower().strip()
        label_lower = label.lower().strip()
        
        if query_lower == label_lower:
            return 1.0
        
        if query_lower in label_lower or label_lower in query_lower:
            return 0.8
        
        # Simple word overlap
        query_words = set(query_lower.split())
        label_words = set(label_lower.split())
        
        if query_words and label_words:
            overlap = len(query_words.intersection(label_words))
            total = len(query_words.union(label_words))
            return overlap / total
        
        return 0.0
    
    def _score_to_confidence(self, score: float) -> ConfidenceLevel:
        """Convert score to confidence level"""
        if score >= 0.8:
            return ConfidenceLevel.HIGH
        elif score >= 0.6:
            return ConfidenceLevel.MEDIUM
        else:
            return ConfidenceLevel.LOW


class VIAFClient:
    """Simple VIAF client"""
    
    def __init__(self, rate_limit: float = 2.0):
        self.rate_limit = rate_limit
        self.last_request = 0
        self.base_url = "https://viaf.org/viaf/search"
    
    def _wait_for_rate_limit(self):
        """Simple rate limiting"""
        current_time = time.time()
        time_since_last = current_time - self.last_request
        if time_since_last < self.rate_limit:
            time.sleep(self.rate_limit - time_since_last)
        self.last_request = time.time()
    
    def search_authors(self, query: str, limit: int = 10) -> List[MatchResult]:
        """Search VIAF for authors"""
        self._wait_for_rate_limit()
        
        try:
            params = {
                'query': f'local.personalNames all "{query}"',
                'sortKeys': 'holdingscount',
                'maximumRecords': limit,
                'httpAccept': 'application/json'
            }
            
            response = requests.get(self.base_url, params=params, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            matches = []
            
            # VIAF response parsing (simplified)
            records = data.get('searchRetrieveResponse', {}).get('records', [])
            
            for record in records:
                record_data = record.get('record', {}).get('recordData', {})
                
                # Extract basic info (this is simplified - real VIAF parsing is more complex)
                viaf_id = record_data.get('viafID', '')
                name = record_data.get('nameHeading', {}).get('text', query)
                
                score = self._calculate_simple_score(query, name)
                
                match = MatchResult(
                    id=viaf_id,
                    name=name,
                    description=f"VIAF authority record for {name}",
                    confidence=self._score_to_confidence(score),
                    score=score,
                    source='viaf',
                    additional_info={
                        'url': f"https://viaf.org/viaf/{viaf_id}",
                        'viaf_id': viaf_id
                    }
                )
                matches.append(match)
            
            return matches
            
        except Exception as e:
            logger.error(f"VIAF search error for '{query}': {e}")
            return []
    
    def _calculate_simple_score(self, query: str, label: str) -> float:
        """Simple scoring based on string similarity"""
        if not query or not label:
            return 0.0
        
        query_lower = query.lower().strip()
        label_lower = label.lower().strip()
        
        if query_lower == label_lower:
            return 1.0
        
        if query_lower in label_lower or label_lower in query_lower:
            return 0.8
        
        return 0.5  # Default for VIAF matches
    
    def _score_to_confidence(self, score: float) -> ConfidenceLevel:
        """Convert score to confidence level"""
        if score >= 0.8:
            return ConfidenceLevel.HIGH
        elif score >= 0.6:
            return ConfidenceLevel.MEDIUM
        else:
            return ConfidenceLevel.LOW


class ReconciliationEngine:
    """Main reconciliation engine for processing entities"""
    
    def __init__(self, cache_size: int = 1000):
        self.cache = SimpleCache(max_size=cache_size)
        
        # Initialize API clients
        self.wikidata_client = WikidataClient(rate_limit=1.0)
        self.viaf_client = VIAFClient(rate_limit=2.0)
        
        # Thread-safe counters
        self._lock = threading.Lock()
        self._stats = {
            'total_processed': 0,
            'cache_hits': 0,
            'api_calls': 0,
            'successful_matches': 0
        }
        
        # Source mapping for entity types
        self.source_mapping = {
            EntityType.PERSON: [
                ('wikidata', self.wikidata_client.search_entities),
                ('viaf', self.viaf_client.search_authors)
            ],
            EntityType.AUTHOR: [
                ('viaf', self.viaf_client.search_authors),
                ('wikidata', self.wikidata_client.search_entities)
            ],
            EntityType.PLACE: [
                ('wikidata', self.wikidata_client.search_entities)
            ],
            EntityType.SUBJECT: [
                ('wikidata', self.wikidata_client.search_entities)
            ]
        }
    
    def process_entities(self, entities: List[Entity]) -> List[ReconciliationResult]:
        """Process a list of entities"""
        results = []
        
        for entity in entities:
            # Check cache first
            cached_result = self.cache.get(entity.search_key)
            if cached_result:
                cached_result.entity = entity  # Update entity reference
                cached_result.cached = True
                results.append(cached_result)
                
                with self._lock:
                    self._stats['cache_hits'] += 1
            else:
                # Process entity
                result = self._reconcile_entity(entity)
                results.append(result)
                
                # Cache result
                self.cache.set(entity.search_key, result)
                
                with self._lock:
                    self._stats['api_calls'] += 1
            
            with self._lock:
                self._stats['total_processed'] += 1
                if results[-1].best_match:
                    self._stats['successful_matches'] += 1
        
        return results
    
    def _reconcile_entity(self, entity: Entity) -> ReconciliationResult:
        """Reconcile a single entity against external sources"""
        start_time = time.time()
        all_matches = []
        sources_queried = []
        
        # Get appropriate sources for entity type
        sources = self.source_mapping.get(entity.entity_type, [])
        
        # Query each source
        for source_name, source_func in sources:
            try:
                if source_name == 'wikidata':
                    matches = source_func(entity.name, limit=5)
                else:
                    matches = source_func(entity.name, limit=5)
                
                all_matches.extend(matches)
                sources_queried.append(source_name)
                
            except Exception as e:
                logger.error(f"Error querying {source_name} for {entity.name}: {e}")
                continue
        
        # Remove duplicates and rank matches
        unique_matches = self._deduplicate_matches(all_matches)
        ranked_matches = sorted(unique_matches, key=lambda x: x.score, reverse=True)
        
        # Determine best match and overall confidence
        best_match = ranked_matches[0] if ranked_matches else None
        overall_confidence = self._calculate_overall_confidence(ranked_matches)
        
        result = ReconciliationResult(
            entity=entity,
            matches=ranked_matches[:10],  # Keep top 10 matches
            best_match=best_match,
            confidence=overall_confidence,
            reconciliation_time=time.time() - start_time,
            sources_queried=sources_queried,
            cached=False
        )
        
        return result
    
    def _deduplicate_matches(self, matches: List[MatchResult]) -> List[MatchResult]:
        """Remove duplicate matches based on name and source"""
        seen = set()
        unique_matches = []
        
        for match in matches:
            # Create a key based on normalized name and source
            key = (match.name.lower().strip(), match.source)
            if key not in seen:
                seen.add(key)
                unique_matches.append(match)
        
        return unique_matches
    
    def _calculate_overall_confidence(self, matches: List[MatchResult]) -> ConfidenceLevel:
        """Calculate overall confidence based on top matches"""
        if not matches:
            return ConfidenceLevel.LOW
        
        best_match = matches[0]
        
        if best_match.score >= 0.8:
            return ConfidenceLevel.HIGH
        elif best_match.score >= 0.6:
            return ConfidenceLevel.MEDIUM
        else:
            return ConfidenceLevel.LOW
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get processing statistics"""
        with self._lock:
            stats = self._stats.copy()
        
        # Calculate derived statistics
        if stats['total_processed'] > 0:
            stats['cache_hit_rate'] = stats['cache_hits'] / stats['total_processed']
            stats['match_rate'] = stats['successful_matches'] / stats['total_processed']
        else:
            stats['cache_hit_rate'] = 0.0
            stats['match_rate'] = 0.0
        
        return stats
    
    def create_entities_from_dataframe(self, df: pd.DataFrame, entity_column: str,
                                     type_column: str = None, 
                                     context_columns: List[str] = None) -> List[Entity]:
        """Create entities from DataFrame"""
        entities = []
        context_columns = context_columns or []
        
        for idx, row in df.iterrows():
            entity_name = str(row[entity_column]).strip()
            if not entity_name or entity_name.lower() in ['nan', 'none', '']:
                continue
            
            # Determine entity type
            if type_column and type_column in row:
                entity_type = self._parse_entity_type(str(row[type_column]))
            else:
                entity_type = self._infer_entity_type(entity_name)
            
            # Extract context
            context = {}
            for col in context_columns:
                if col in row and pd.notna(row[col]):
                    context[col] = row[col]
            
            entity = Entity(
                id=f"entity_{idx}",
                name=entity_name,
                entity_type=entity_type,
                context=context,
                source_row=idx
            )
            entities.append(entity)
        
        return entities
    
    def _parse_entity_type(self, type_str: str) -> EntityType:
        """Parse entity type from string"""
        type_str = type_str.lower().strip()
        
        type_mapping = {
            'person': EntityType.PERSON,
            'people': EntityType.PERSON,
            'author': EntityType.AUTHOR,
            'place': EntityType.PLACE,
            'location': EntityType.PLACE,
            'geography': EntityType.PLACE,
            'organization': EntityType.ORGANIZATION,
            'org': EntityType.ORGANIZATION,
            'subject': EntityType.SUBJECT,
            'topic': EntityType.SUBJECT,
            'artwork': EntityType.ARTWORK,
            'art': EntityType.ARTWORK
        }
        
        return type_mapping.get(type_str, EntityType.UNKNOWN)
    
    def _infer_entity_type(self, entity_name: str) -> EntityType:
        """Infer entity type from name patterns"""
        name_lower = entity_name.lower()
        
        # Simple heuristics for type inference
        if any(title in name_lower for title in ['dr.', 'prof.', 'mr.', 'mrs.', 'ms.']):
            return EntityType.PERSON
        
        if any(word in name_lower for word in ['university', 'college', 'institute', 'museum']):
            return EntityType.ORGANIZATION
        
        if any(word in name_lower for word in ['city', 'town', 'county', 'country', 'state']):
            return EntityType.PLACE
        
        # Default to person for most cases
        return EntityType.PERSON


# Example usage
if __name__ == "__main__":
    # Test the reconciliation engine
    engine = ReconciliationEngine()
    
    # Create sample entities
    sample_entities = [
        Entity(
            id="test_1",
            name="William Shakespeare",
            entity_type=EntityType.AUTHOR,
            context={},
            source_row=0
        ),
        Entity(
            id="test_2", 
            name="Paris",
            entity_type=EntityType.PLACE,
            context={},
            source_row=1
        )
    ]
    
    print("Testing reconciliation engine...")
    results = engine.process_entities(sample_entities)
    
    for result in results:
        print(f"Entity: {result.entity.name}")
        print(f"Confidence: {result.confidence.value}")
        print(f"Matches: {len(result.matches)}")
        if result.best_match:
            print(f"Best match: {result.best_match.name} ({result.best_match.score:.2f})")
        print("---")