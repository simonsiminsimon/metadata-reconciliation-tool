# File: app/services/enhanced_reconciliation_engine.py
"""
Updated Reconciliation Engine using the new Cultural Heritage Wikidata Client

This integrates the enhanced Wikidata client with your existing reconciliation system
while maintaining compatibility with the current database structure and API.
"""

import time
from typing import List, Dict, Any, Optional
import pandas as pd
from datetime import datetime

# Import the new Wikidata client
from .wikidata_cultural_client import (
    CulturalHeritageWikidataClient, 
    WikidataMatch, 
    EntityType as WikidataEntityType,
    ConfidenceLevel
)

# Import existing components
from .reconciliation_engine import (
    Entity, EntityType, ReconciliationResult, MatchResult,
    SimpleCache, ConfidenceLevel as OldConfidenceLevel
)

class EnhancedReconciliationEngine:
    """
    Enhanced reconciliation engine using the new Cultural Heritage Wikidata Client
    
    This engine provides:
    - Better matching for cultural heritage entities
    - Enhanced context awareness
    - Improved authority linking (VIAF, LC)
    - Better caching and performance
    """
    
    def __init__(self, cache_size: int = 1000, wikidata_rate_limit: float = 1.0):
        """
        Initialize the enhanced reconciliation engine
        
        Args:
            cache_size: Size of the local cache
            wikidata_rate_limit: Wikidata API rate limit (requests per second)
        """
        # Initialize the enhanced Wikidata client
        self.wikidata_client = CulturalHeritageWikidataClient(
            rate_limit=wikidata_rate_limit,
            cache_enabled=True,
            max_results=10
        )
        
        # Keep the simple cache for backward compatibility
        self.cache = SimpleCache(max_size=cache_size)
        
        # Statistics tracking
        self._stats = {
            'total_processed': 0,
            'cache_hits': 0,
            'wikidata_matches': 0,
            'high_confidence_matches': 0
        }
    
    def _convert_entity_type(self, entity_type: EntityType) -> WikidataEntityType:
        """Convert old entity type to new Wikidata entity type"""
        mapping = {
            EntityType.PERSON: WikidataEntityType.PERSON,
            EntityType.PLACE: WikidataEntityType.PLACE,
            EntityType.ORGANIZATION: WikidataEntityType.ORGANIZATION,
            EntityType.SUBJECT: WikidataEntityType.SUBJECT_TOPIC,
            EntityType.AUTHOR: WikidataEntityType.PERSON,
            EntityType.ARTWORK: WikidataEntityType.ARTWORK,
            EntityType.UNKNOWN: WikidataEntityType.CONCEPT
        }
        return mapping.get(entity_type, WikidataEntityType.CONCEPT)
    
    def _convert_wikidata_match_to_match_result(self, wikidata_match: WikidataMatch) -> MatchResult:
        """Convert WikidataMatch to the existing MatchResult format"""
        
        # Convert confidence level
        confidence_mapping = {
            ConfidenceLevel.VERY_HIGH: OldConfidenceLevel.HIGH,
            ConfidenceLevel.HIGH: OldConfidenceLevel.HIGH,
            ConfidenceLevel.MEDIUM: OldConfidenceLevel.MEDIUM,
            ConfidenceLevel.LOW: OldConfidenceLevel.LOW,
            ConfidenceLevel.VERY_LOW: OldConfidenceLevel.LOW
        }
        
        confidence = confidence_mapping.get(
            wikidata_match.confidence_level, 
            OldConfidenceLevel.LOW
        )
        
        # Build additional info with cultural heritage specific fields
        additional_info = {
            'wikidata_url': f"https://www.wikidata.org/entity/{wikidata_match.wikidata_id}",
            'entity_type': wikidata_match.entity_type.value,
            'aliases': wikidata_match.aliases,
            'confidence_score': wikidata_match.confidence_score
        }
        
        # Add optional fields if available
        if wikidata_match.birth_date:
            additional_info['birth_date'] = wikidata_match.birth_date
        if wikidata_match.death_date:
            additional_info['death_date'] = wikidata_match.death_date
        if wikidata_match.coordinates:
            additional_info['coordinates'] = wikidata_match.coordinates
        if wikidata_match.country:
            additional_info['country'] = wikidata_match.country
        if wikidata_match.inception_date:
            additional_info['inception_date'] = wikidata_match.inception_date
        if wikidata_match.website:
            additional_info['website'] = wikidata_match.website
        if wikidata_match.viaf_id:
            additional_info['viaf_id'] = wikidata_match.viaf_id
            additional_info['viaf_url'] = f"https://viaf.org/viaf/{wikidata_match.viaf_id}"
        if wikidata_match.library_of_congress_id:
            additional_info['lc_id'] = wikidata_match.library_of_congress_id
            additional_info['lc_url'] = f"https://id.loc.gov/authorities/names/{wikidata_match.library_of_congress_id}"
        if wikidata_match.image_url:
            additional_info['image_url'] = wikidata_match.image_url
        if wikidata_match.commons_category:
            additional_info['commons_category'] = wikidata_match.commons_category
        
        # Add external IDs
        if wikidata_match.external_ids:
            additional_info.update(wikidata_match.external_ids)
        
        return MatchResult(
            id=wikidata_match.wikidata_id,
            name=wikidata_match.label,
            description=wikidata_match.description,
            confidence=confidence,
            score=wikidata_match.confidence_score,
            source='wikidata_enhanced',
            additional_info=additional_info
        )
    
    def _extract_context_hints(self, entity: Entity) -> Dict[str, str]:
        """Extract context hints from entity for better matching"""
        context_hints = {}
        
        # Extract useful context from the entity
        if entity.context:
            # Look for date information
            for key, value in entity.context.items():
                if isinstance(value, str):
                    key_lower = key.lower()
                    
                    # Date fields
                    if any(date_word in key_lower for date_word in ['date', 'year', 'birth', 'death', 'created']):
                        context_hints['date'] = str(value)
                    
                    # Location fields
                    elif any(loc_word in key_lower for loc_word in ['location', 'place', 'city', 'state', 'country']):
                        context_hints['location'] = str(value)
                    
                    # Type/occupation fields
                    elif any(type_word in key_lower for type_word in ['type', 'occupation', 'role', 'profession']):
                        context_hints['occupation'] = str(value)
        
        return context_hints
    
    def _reconcile_entity(self, entity: Entity) -> ReconciliationResult:
        """Reconcile a single entity using the enhanced Wikidata client"""
        start_time = time.time()
        
        # Check cache first
        cached_result = self.cache.get(entity.search_key)
        if cached_result:
            self._stats['cache_hits'] += 1
            return cached_result
        
        # Extract context hints for better matching
        context_hints = self._extract_context_hints(entity)
        
        # Convert entity type
        wikidata_entity_type = self._convert_entity_type(entity.entity_type)
        
        # Query Wikidata based on entity type
        wikidata_matches = []
        
        try:
            if wikidata_entity_type == WikidataEntityType.PERSON:
                wikidata_matches = self.wikidata_client.search_persons(
                    entity.name, 
                    context_hints
                )
            elif wikidata_entity_type == WikidataEntityType.PLACE:
                wikidata_matches = self.wikidata_client.search_places(
                    entity.name, 
                    context_hints
                )
            elif wikidata_entity_type == WikidataEntityType.ORGANIZATION:
                wikidata_matches = self.wikidata_client.search_organizations(
                    entity.name, 
                    context_hints
                )
            elif wikidata_entity_type == WikidataEntityType.SUBJECT_TOPIC:
                wikidata_matches = self.wikidata_client.search_subjects(
                    entity.name, 
                    context_hints
                )
            else:
                # For unknown types, try person search first, then places
                wikidata_matches = self.wikidata_client.search_persons(entity.name, context_hints)
                if not wikidata_matches:
                    wikidata_matches = self.wikidata_client.search_places(entity.name, context_hints)
        
        except Exception as e:
            print(f"Error querying Wikidata for {entity.name}: {e}")
            wikidata_matches = []
        
        # Convert Wikidata matches to MatchResult format
        matches = []
        for wikidata_match in wikidata_matches:
            match_result = self._convert_wikidata_match_to_match_result(wikidata_match)
            matches.append(match_result)
        
        # Determine best match and overall confidence
        best_match = matches[0] if matches else None
        overall_confidence = self._calculate_overall_confidence(matches)
        
        # Create reconciliation result
        result = ReconciliationResult(
            entity=entity,
            matches=matches,
            best_match=best_match,
            confidence=overall_confidence,
            reconciliation_time=time.time() - start_time,
            sources_queried=['wikidata_enhanced'],
            cached=False
        )
        
        # Cache the result
        self.cache.set(entity.search_key, result)
        
        # Update statistics
        self._stats['total_processed'] += 1
        if matches:
            self._stats['wikidata_matches'] += 1
        if best_match and best_match.confidence == OldConfidenceLevel.HIGH:
            self._stats['high_confidence_matches'] += 1
        
        return result
    
    def _calculate_overall_confidence(self, matches: List[MatchResult]) -> OldConfidenceLevel:
        """Calculate overall confidence based on matches"""
        if not matches:
            return OldConfidenceLevel.LOW
        
        best_match = matches[0]
        
        if best_match.score >= 0.8:
            return OldConfidenceLevel.HIGH
        elif best_match.score >= 0.6:
            return OldConfidenceLevel.MEDIUM
        else:
            return OldConfidenceLevel.LOW
    
    def process_entities(self, entities: List[Entity]) -> List[ReconciliationResult]:
        """Process a list of entities (main interface method)"""
        results = []
        
        for entity in entities:
            result = self._reconcile_entity(entity)
            results.append(result)
        
        return results
    
    def create_entities_from_dataframe(self, df: pd.DataFrame, entity_column: str,
                                     type_column: str = None, 
                                     context_columns: List[str] = None) -> List[Entity]:
        """Create entities from DataFrame (compatible with existing interface)"""
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
            'institution': EntityType.ORGANIZATION,
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
        
        if any(word in name_lower for word in ['university', 'college', 'institute', 'museum', 'library', 'society']):
            return EntityType.ORGANIZATION
        
        if any(word in name_lower for word in ['city', 'town', 'county', 'country', 'state', 'house', 'opera']):
            return EntityType.PLACE
        
        # Check if it looks like a person name (two or more capitalized words)
        words = entity_name.split()
        if len(words) >= 2 and all(word[0].isupper() for word in words if word):
            return EntityType.PERSON
        
        # Default to unknown
        return EntityType.UNKNOWN
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get processing statistics including Wikidata client stats"""
        stats = self._stats.copy()
        
        # Add Wikidata client statistics
        wikidata_stats = self.wikidata_client.get_statistics()
        stats.update({
            'wikidata_queries': wikidata_stats.get('queries_made', 0),
            'wikidata_cache_hits': wikidata_stats.get('cache_hits', 0),
            'wikidata_cache_hit_rate': wikidata_stats.get('cache_hit_rate', 0.0),
            'wikidata_success_rate': wikidata_stats.get('success_rate', 0.0)
        })
        
        # Calculate derived statistics
        if stats['total_processed'] > 0:
            stats['overall_cache_hit_rate'] = stats['cache_hits'] / stats['total_processed']
            stats['match_rate'] = stats['wikidata_matches'] / stats['total_processed']
            stats['high_confidence_rate'] = stats['high_confidence_matches'] / stats['total_processed']
        else:
            stats['overall_cache_hit_rate'] = 0.0
            stats['match_rate'] = 0.0
            stats['high_confidence_rate'] = 0.0
        
        return stats


# Example usage showing how to integrate with existing system
def demo_integration():
    """Demonstrate how to use the enhanced reconciliation engine"""
    
    print("Enhanced Reconciliation Engine Demo")
    print("=" * 40)
    
    # Create the enhanced engine
    engine = EnhancedReconciliationEngine(
        cache_size=500,
        wikidata_rate_limit=1.0
    )
    
    # Test with sample data from your CSV structure
    sample_data = pd.DataFrame({
        'creator_name': [
            'Minnesota Territorial Legislature',
            'Bijou Opera House', 
            'Hodge, Emma B.',
            'Carleton College. Registrar\'s Office'
        ],
        'entity_type': ['organization', 'organization', 'person', 'organization'],
        'date_created': ['1857', '1898-01-02', '1921', '1867-1868'],
        'location': ['Minnesota', 'Minneapolis, Minnesota', 'Chicago', 'Northfield, Minnesota']
    })
    
    print("Creating entities from sample data...")
    entities = engine.create_entities_from_dataframe(
        sample_data,
        entity_column='creator_name',
        type_column='entity_type',
        context_columns=['date_created', 'location']
    )
    
    print(f"Created {len(entities)} entities")
    
    print("\nProcessing entities...")
    results = engine.process_entities(entities)
    
    print(f"Processed {len(results)} entities")
    print("\nResults:")
    
    for result in results:
        print(f"\nEntity: {result.entity.name}")
        print(f"Type: {result.entity.entity_type.value}")
        print(f"Confidence: {result.confidence.value}")
        print(f"Matches found: {len(result.matches)}")
        
        if result.best_match:
            match = result.best_match
            print(f"Best match: {match.name} (Q{match.id})")
            print(f"Score: {match.score:.2f}")
            print(f"Description: {match.description}")
            
            # Show cultural heritage specific information
            if 'viaf_id' in match.additional_info:
                print(f"VIAF ID: {match.additional_info['viaf_id']}")
            if 'lc_id' in match.additional_info:
                print(f"Library of Congress ID: {match.additional_info['lc_id']}")
            if 'website' in match.additional_info:
                print(f"Website: {match.additional_info['website']}")
    
    print("\nEngine Statistics:")
    stats = engine.get_statistics()
    for key, value in stats.items():
        if isinstance(value, float):
            print(f"{key}: {value:.2f}")
        else:
            print(f"{key}: {value}")


if __name__ == "__main__":
    demo_integration()