"""
Entity Reconciliation Engine for processing CSV entities against multiple external sources.
Features caching, batch processing, fuzzy matching, and intelligent ranking.
"""

import hashlib
import json
import time
import asyncio
from typing import Dict, List, Optional, Set, Tuple, Any, Union
from dataclasses import dataclass, asdict
from enum import Enum
from collections import defaultdict
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from functools import wraps

# Third-party imports
import pandas as pd
from fuzzywuzzy import fuzz, process
import redis
from flask_caching import Cache

# Local imports - assuming the data_sources module exists
from .data_sources import (
    WikidataClient, VIAFClient, GettyClient, 
    MatchResult, ConfidenceLevel,
    search_wikidata_persons, search_wikidata_places,
    search_viaf_authors, search_getty_aat, search_getty_tgn, search_getty_ulan
)

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


class ReconciliationStrategy(Enum):
    """Strategies for reconciliation"""
    EXACT_MATCH = "exact_match"
    FUZZY_MATCH = "fuzzy_match"
    MULTI_SOURCE = "multi_source"
    BEST_MATCH = "best_match"


@dataclass
class Entity:
    """Entity to be reconciled"""
    id: str
    name: str
    entity_type: EntityType
    context: Dict[str, Any]  # Additional context from CSV
    source_row: int
    
    def __post_init__(self):
        """Normalize name for consistent processing"""
        self.normalized_name = self.name.strip().lower()
        self.search_key = self._generate_search_key()
    
    def _generate_search_key(self) -> str:
        """Generate a unique key for caching"""
        context_str = json.dumps(self.context, sort_keys=True)
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


class FuzzyMatcher:
    """Advanced fuzzy string matching utilities"""
    
    def __init__(self, threshold: int = 80):
        self.threshold = threshold
    
    def calculate_similarity(self, str1: str, str2: str) -> float:
        """Calculate similarity between two strings using multiple algorithms"""
        # Normalize strings
        s1 = str1.strip().lower()
        s2 = str2.strip().lower()
        
        if s1 == s2:
            return 1.0
        
        # Use multiple fuzzy matching algorithms
        ratio = fuzz.ratio(s1, s2) / 100.0
        partial_ratio = fuzz.partial_ratio(s1, s2) / 100.0
        token_sort_ratio = fuzz.token_sort_ratio(s1, s2) / 100.0
        token_set_ratio = fuzz.token_set_ratio(s1, s2) / 100.0
        
        # Weighted average favoring different aspects
        weighted_score = (
            ratio * 0.3 +
            partial_ratio * 0.2 +
            token_sort_ratio * 0.25 +
            token_set_ratio * 0.25
        )
        
        return weighted_score
    
    def is_match(self, str1: str, str2: str) -> bool:
        """Check if two strings are a fuzzy match"""
        return self.calculate_similarity(str1, str2) >= (self.threshold / 100.0)
    
    def find_best_matches(self, query: str, choices: List[str], limit: int = 5) -> List[Tuple[str, float]]:
        """Find best fuzzy matches from a list of choices"""
        if not choices:
            return []
        
        matches = []
        for choice in choices:
            similarity = self.calculate_similarity(query, choice)
            if similarity >= (self.threshold / 100.0):
                matches.append((choice, similarity))
        
        # Sort by similarity and return top matches
        matches.sort(key=lambda x: x[1], reverse=True)
        return matches[:limit]


class CacheManager:
    """Manages caching for reconciliation results"""
    
    def __init__(self, cache_config: Dict[str, Any] = None):
        self.cache_config = cache_config or {
            'CACHE_TYPE': 'redis',
            'CACHE_REDIS_URL': 'redis://localhost:6379/0',
            'CACHE_DEFAULT_TIMEOUT': 86400  # 24 hours
        }
        
        try:
            self.cache = Cache()
            self.cache.init_app(None, config=self.cache_config)
            self.enabled = True
        except Exception as e:
            logger.warning(f"Cache initialization failed: {e}. Running without cache.")
            self.cache = None
            self.enabled = False
    
    def get(self, key: str) -> Optional[Any]:
        """Get item from cache"""
        if not self.enabled:
            return None
        
        try:
            return self.cache.get(key)
        except Exception as e:
            logger.error(f"Cache get error: {e}")
            return None
    
    def set(self, key: str, value: Any, timeout: int = None) -> bool:
        """Set item in cache"""
        if not self.enabled:
            return False
        
        try:
            return self.cache.set(key, value, timeout=timeout)
        except Exception as e:
            logger.error(f"Cache set error: {e}")
            return False
    
    def get_many(self, keys: List[str]) -> Dict[str, Any]:
        """Get multiple items from cache"""
        if not self.enabled:
            return {}
        
        try:
            return self.cache.get_many(*keys)
        except Exception as e:
            logger.error(f"Cache get_many error: {e}")
            return {}
    
    def set_many(self, mapping: Dict[str, Any], timeout: int = None) -> bool:
        """Set multiple items in cache"""
        if not self.enabled:
            return False
        
        try:
            return self.cache.set_many(mapping, timeout=timeout)
        except Exception as e:
            logger.error(f"Cache set_many error: {e}")
            return False


class ReconciliationEngine:
    """Main reconciliation engine for processing entities"""
    
    def __init__(self, cache_config: Dict[str, Any] = None, 
                 max_workers: int = 10, fuzzy_threshold: int = 80):
        self.cache_manager = CacheManager(cache_config)
        self.fuzzy_matcher = FuzzyMatcher(threshold=fuzzy_threshold)
        self.max_workers = max_workers
        
        # Initialize API clients
        self.wikidata_client = WikidataClient(rate_limit=1.0)
        self.viaf_client = VIAFClient(rate_limit=2.0)
        self.getty_client = GettyClient(rate_limit=2.0)
        
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
                ('wikidata_persons', self.wikidata_client.search_persons),
                ('viaf_authors', self.viaf_client.search_authors),
                ('getty_ulan', self.getty_client.search_ulan_agents)
            ],
            EntityType.PLACE: [
                ('wikidata_places', self.wikidata_client.search_places),
                ('getty_tgn', self.getty_client.search_tgn_places)
            ],
            EntityType.AUTHOR: [
                ('viaf_authors', self.viaf_client.search_authors),
                ('wikidata_persons', self.wikidata_client.search_persons)
            ],
            EntityType.SUBJECT: [
                ('getty_aat', self.getty_client.search_aat_terms)
            ]
        }
    
    def process_csv_file(self, file_path: str, entity_column: str, 
                        type_column: str = None, context_columns: List[str] = None,
                        batch_size: int = 50) -> List[ReconciliationResult]:
        """Process entities from CSV file"""
        logger.info(f"Processing CSV file: {file_path}")
        
        # Read CSV
        df = pd.read_csv(file_path)
        
        # Extract entities
        entities = self._extract_entities_from_dataframe(
            df, entity_column, type_column, context_columns
        )
        
        # Process in batches
        results = []
        for i in range(0, len(entities), batch_size):
            batch = entities[i:i + batch_size]
            batch_results = self.process_batch(batch)
            results.extend(batch_results)
            
            logger.info(f"Processed batch {i//batch_size + 1}/{(len(entities)-1)//batch_size + 1}")
        
        return results
    
    def process_dataframe(self, df: pd.DataFrame, entity_column: str,
                         type_column: str = None, context_columns: List[str] = None,
                         batch_size: int = 50) -> List[ReconciliationResult]:
        """Process entities from DataFrame"""
        entities = self._extract_entities_from_dataframe(
            df, entity_column, type_column, context_columns
        )
        
        results = []
        for i in range(0, len(entities), batch_size):
            batch = entities[i:i + batch_size]
            batch_results = self.process_batch(batch)
            results.extend(batch_results)
        
        return results
    
    def _extract_entities_from_dataframe(self, df: pd.DataFrame, entity_column: str,
                                       type_column: str = None, 
                                       context_columns: List[str] = None) -> List[Entity]:
        """Extract entities from DataFrame"""
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
                if col in row:
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
    
    def process_batch(self, entities: List[Entity]) -> List[ReconciliationResult]:
        """Process a batch of entities with caching and parallel processing"""
        start_time = time.time()
        
        # Check cache for all entities
        cached_results = self._get_cached_results(entities)
        uncached_entities = [e for e in entities if e.search_key not in cached_results]
        
        logger.info(f"Batch processing: {len(cached_results)} cached, {len(uncached_entities)} to process")
        
        # Process uncached entities in parallel
        new_results = {}
        if uncached_entities:
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                future_to_entity = {
                    executor.submit(self._reconcile_entity, entity): entity
                    for entity in uncached_entities
                }
                
                for future in as_completed(future_to_entity):
                    entity = future_to_entity[future]
                    try:
                        result = future.result(timeout=30)
                        new_results[entity.search_key] = result
                    except Exception as e:
                        logger.error(f"Error processing entity {entity.name}: {e}")
                        # Create empty result for failed entities
                        new_results[entity.search_key] = ReconciliationResult(
                            entity=entity,
                            matches=[],
                            best_match=None,
                            confidence=ConfidenceLevel.LOW,
                            reconciliation_time=0,
                            sources_queried=[],
                            cached=False
                        )
        
        # Cache new results
        if new_results:
            self._cache_results(new_results)
        
        # Combine cached and new results
        all_results = list(cached_results.values()) + list(new_results.values())
        
        # Update stats
        with self._lock:
            self._stats['total_processed'] += len(entities)
            self._stats['cache_hits'] += len(cached_results)
            self._stats['api_calls'] += len(new_results)
        
        logger.info(f"Batch completed in {time.time() - start_time:.2f}s")
        return all_results
    
    def _get_cached_results(self, entities: List[Entity]) -> Dict[str, ReconciliationResult]:
        """Get cached results for entities"""
        cache_keys = [entity.search_key for entity in entities]
        cached_data = self.cache_manager.get_many(cache_keys)
        
        results = {}
        for entity in entities:
            if entity.search_key in cached_data:
                cached_result = cached_data[entity.search_key]
                if cached_result:
                    # Update entity reference and mark as cached
                    cached_result.entity = entity
                    cached_result.cached = True
                    results[entity.search_key] = cached_result
        
        return results
    
    def _cache_results(self, results: Dict[str, ReconciliationResult]):
        """Cache reconciliation results"""
        cache_mapping = {}
        for key, result in results.items():
            # Create a serializable version
            cache_mapping[key] = ReconciliationResult(
                entity=result.entity,
                matches=result.matches,
                best_match=result.best_match,
                confidence=result.confidence,
                reconciliation_time=result.reconciliation_time,
                sources_queried=result.sources_queried,
                cached=False
            )
        
        self.cache_manager.set_many(cache_mapping, timeout=86400)  # 24 hours
    
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
                matches = source_func(entity.name, limit=10)
                all_matches.extend(matches)
                sources_queried.append(source_name)
                
                # Add fuzzy matching enhancement
                enhanced_matches = self._enhance_with_fuzzy_matching(entity, matches)
                all_matches.extend(enhanced_matches)
                
            except Exception as e:
                logger.error(f"Error querying {source_name} for {entity.name}: {e}")
                continue
        
        # Remove duplicates and rank matches
        unique_matches = self._deduplicate_matches(all_matches)
        ranked_matches = self._rank_matches(entity, unique_matches)
        
        # Determine best match and overall confidence
        best_match = ranked_matches[0] if ranked_matches else None
        overall_confidence = self._calculate_overall_confidence(ranked_matches)
        
        result = ReconciliationResult(
            entity=entity,
            matches=ranked_matches,
            best_match=best_match,
            confidence=overall_confidence,
            reconciliation_time=time.time() - start_time,
            sources_queried=sources_queried,
            cached=False
        )
        
        return result
    
    def _enhance_with_fuzzy_matching(self, entity: Entity, matches: List[MatchResult]) -> List[MatchResult]:
        """Enhance matches with fuzzy matching scores"""
        enhanced_matches = []
        
        for match in matches:
            # Calculate fuzzy similarity
            fuzzy_score = self.fuzzy_matcher.calculate_similarity(entity.name, match.name)
            
            # Adjust confidence based on fuzzy score
            if fuzzy_score > 0.9:
                adjusted_confidence = ConfidenceLevel.HIGH
                adjusted_score = min(match.score + 0.1, 1.0)
            elif fuzzy_score > 0.7:
                adjusted_confidence = ConfidenceLevel.MEDIUM
                adjusted_score = match.score
            else:
                adjusted_confidence = ConfidenceLevel.LOW
                adjusted_score = max(match.score - 0.1, 0.0)
            
            # Create enhanced match if significantly different
            if abs(adjusted_score - match.score) > 0.05:
                enhanced_match = MatchResult(
                    id=f"fuzzy_{match.id}",
                    name=match.name,
                    description=f"Fuzzy match: {match.description}",
                    confidence=adjusted_confidence,
                    score=adjusted_score,
                    source=f"fuzzy_{match.source}",
                    additional_info={
                        **match.additional_info,
                        'fuzzy_score': fuzzy_score,
                        'original_score': match.score
                    }
                )
                enhanced_matches.append(enhanced_match)
        
        return enhanced_matches
    
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
    
    def _rank_matches(self, entity: Entity, matches: List[MatchResult]) -> List[MatchResult]:
        """Rank matches by multiple criteria"""
        if not matches:
            return []
        
        # Calculate composite scores
        for match in matches:
            composite_score = self._calculate_composite_score(entity, match)
            match.score = composite_score
        
        # Sort by composite score
        matches.sort(key=lambda x: x.score, reverse=True)
        return matches
    
    def _calculate_composite_score(self, entity: Entity, match: MatchResult) -> float:
        """Calculate composite score considering multiple factors"""
        base_score = match.score
        
        # Fuzzy similarity boost
        fuzzy_score = self.fuzzy_matcher.calculate_similarity(entity.name, match.name)
        fuzzy_boost = (fuzzy_score - 0.5) * 0.2 if fuzzy_score > 0.5 else 0
        
        # Source reliability weights
        source_weights = {
            'wikidata': 0.9,
            'viaf': 0.85,
            'getty_aat': 0.8,
            'getty_tgn': 0.8,
            'getty_ulan': 0.8
        }
        
        source_weight = source_weights.get(match.source.split('_')[0], 0.7)
        
        # Context matching (if available)
        context_boost = self._calculate_context_boost(entity, match)
        
        # Combine all factors
        composite_score = (
            base_score * source_weight +
            fuzzy_boost +
            context_boost
        )
        
        return min(composite_score, 1.0)
    
    def _calculate_context_boost(self, entity: Entity, match: MatchResult) -> float:
        """Calculate boost based on context matching"""
        if not entity.context or not match.additional_info:
            return 0.0
        
        boost = 0.0
        
        # Check for matching dates, places, etc.
        if 'birth_date' in entity.context and 'birth_date' in match.additional_info:
            if entity.context['birth_date'] == match.additional_info['birth_date']:
                boost += 0.1
        
        if 'death_date' in entity.context and 'death_date' in match.additional_info:
            if entity.context['death_date'] == match.additional_info['death_date']:
                boost += 0.1
        
        # Add more context matching logic as needed
        
        return boost
    
    def _calculate_overall_confidence(self, matches: List[MatchResult]) -> ConfidenceLevel:
        """Calculate overall confidence based on top matches"""
        if not matches:
            return ConfidenceLevel.LOW
        
        best_match = matches[0]
        
        if best_match.score >= 0.9:
            return ConfidenceLevel.HIGH
        elif best_match.score >= 0.7:
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
    
    def export_results(self, results: List[ReconciliationResult], 
                      output_file: str, format: str = 'csv') -> None:
        """Export reconciliation results to file"""
        if format.lower() == 'csv':
            self._export_to_csv(results, output_file)
        elif format.lower() == 'json':
            self._export_to_json(results, output_file)
        else:
            raise ValueError(f"Unsupported format: {format}")
    
    def _export_to_csv(self, results: List[ReconciliationResult], output_file: str):
        """Export results to CSV"""
        rows = []
        for result in results:
            base_row = {
                'entity_id': result.entity.id,
                'entity_name': result.entity.name,
                'entity_type': result.entity.entity_type.value,
                'source_row': result.entity.source_row,
                'confidence': result.confidence.value,
                'reconciliation_time': result.reconciliation_time,
                'sources_queried': ','.join(result.sources_queried),
                'cached': result.cached,
                'num_matches': len(result.matches)
            }
            
            if result.best_match:
                base_row.update({
                    'best_match_id': result.best_match.id,
                    'best_match_name': result.best_match.name,
                    'best_match_score': result.best_match.score,
                    'best_match_source': result.best_match.source,
                    'best_match_description': result.best_match.description
                })
            
            rows.append(base_row)
        
        df = pd.DataFrame(rows)
        df.to_csv(output_file, index=False)
        logger.info(f"Results exported to {output_file}")
    
    def _export_to_json(self, results: List[ReconciliationResult], output_file: str):
        """Export results to JSON"""
        json_data = []
        for result in results:
            json_data.append({
                'entity': asdict(result.entity),
                'matches': [asdict(match) for match in result.matches],
                'best_match': asdict(result.best_match) if result.best_match else None,
                'confidence': result.confidence.value,
                'reconciliation_time': result.reconciliation_time,
                'sources_queried': result.sources_queried,
                'cached': result.cached
            })
        
        with open(output_file, 'w') as f:
            json.dump(json_data, f, indent=2, default=str)
        
        logger.info(f"Results exported to {output_file}")


# Example usage and testing
if __name__ == "__main__":
    # Initialize reconciliation engine
    engine = ReconciliationEngine(
        cache_config={
            'CACHE_TYPE': 'simple',  # Use simple cache for testing
            'CACHE_DEFAULT_TIMEOUT': 300
        },
        max_workers=5,
        fuzzy_threshold=75
    )
    
    # Test with sample data
    sample_data = pd.DataFrame({
        'name': ['William Shakespeare', 'Jane Austen', 'Paris', 'London'],
        'type': ['author', 'author', 'place', 'place'],
        'birth_year': [1564, 1775, None, None]
    })
    
    print("Processing sample data...")
    results = engine.process_dataframe(
        sample_data, 
        entity_column='name',
        type_column='type',
        context_columns=['birth_year']
    )
    
    print(f"Processed {len(results)} entities")
    for result in results:
        print(f"  {result.entity.name}: {result.confidence.value} "
              f"({len(result.matches)} matches)")
        if result.best_match:
            print(f"    Best: {result.best_match.name} "
                  f"({result.best_match.score:.2f})")
    
    # Show statistics
    stats = engine.get_statistics()
    print(f"\nStatistics: {stats}")
    
    # Export results
    engine.export_results(results, "reconciliation_results.csv")