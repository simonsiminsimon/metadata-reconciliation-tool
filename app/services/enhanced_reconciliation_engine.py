# File: app/services/enhanced_reconciliation_engine.py
"""
Updated Reconciliation Engine using the new Cultural Heritage Wikidata Client

This integrates the enhanced Wikidata client with your existing reconciliation system
while maintaining compatibility with the current database structure and API.

FIXED: InvalidTypeForm errors in type annotations
"""

import time
from typing import List, Dict, Any, Optional, Union
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
        from .failsafe_wikidata_client import FailsafeWikidataClient
        self.wikidata_client = FailsafeWikidataClient(
            rate_limit=wikidata_rate_limit,
            timeout=10,  # Short timeout
            max_results=5  # Fewer results to speed up
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
            EntityType.UNKNOWN: WikidataEntityType.PERSON  # Default fallback
        }
        return mapping.get(entity_type, WikidataEntityType.PERSON)
    
    def _convert_wikidata_match_to_result(self, wikidata_match: WikidataMatch) -> MatchResult:
        """Convert WikidataMatch to MatchResult for compatibility"""
        # Convert confidence level
        if wikidata_match.confidence_level == ConfidenceLevel.VERY_HIGH:
            old_confidence = OldConfidenceLevel.HIGH
        elif wikidata_match.confidence_level == ConfidenceLevel.HIGH:
            old_confidence = OldConfidenceLevel.HIGH
        elif wikidata_match.confidence_level == ConfidenceLevel.MEDIUM:
            old_confidence = OldConfidenceLevel.MEDIUM
        else:
            old_confidence = OldConfidenceLevel.LOW
        
        # Build additional_info with cultural heritage specific data
        additional_info = {}
        if wikidata_match.viaf_id:
            additional_info['viaf_id'] = wikidata_match.viaf_id
        if wikidata_match.library_of_congress_id:
            additional_info['lc_id'] = wikidata_match.library_of_congress_id
        if wikidata_match.birth_date:
            additional_info['birth_date'] = wikidata_match.birth_date
        if wikidata_match.death_date:
            additional_info['death_date'] = wikidata_match.death_date
        if wikidata_match.coordinates:
            additional_info['coordinates'] = wikidata_match.coordinates
        if wikidata_match.country:
            additional_info['country'] = wikidata_match.country
        if wikidata_match.website:
            additional_info['website'] = wikidata_match.website
        if wikidata_match.image_url:
            additional_info['image_url'] = wikidata_match.image_url
        if wikidata_match.external_ids:
            additional_info.update(wikidata_match.external_ids)
        
        return MatchResult(
            id=wikidata_match.wikidata_id,
            name=wikidata_match.label,
            description=wikidata_match.description or "",
            confidence=old_confidence,
            score=wikidata_match.confidence_score,
            source="wikidata_enhanced",
            additional_info=additional_info
        )
    
    def _extract_context_hints(self, entity: Entity) -> Dict[str, Any]:
        """Extract context hints from entity for better matching"""
        context_hints = {}
        
        # Date context
        for date_field in ['date_created', 'date', 'year', 'birth_date', 'death_date']:
            if date_field in entity.context:
                context_hints['date'] = entity.context[date_field]
                break
        
        # Location context
        for location_field in ['location', 'place', 'city', 'state', 'country']:
            if location_field in entity.context:
                context_hints['location'] = entity.context[location_field]
                break
        
        # Type-specific context
        if entity.entity_type == EntityType.PERSON:
            # Look for birth/death information
            if 'birth_year' in entity.context:
                context_hints['birth_year'] = entity.context['birth_year']
            if 'death_year' in entity.context:
                context_hints['death_year'] = entity.context['death_year']
        
        return context_hints
    
    def _reconcile_entity(self, entity: Entity) -> ReconciliationResult:
        """Reconcile a single entity using the enhanced Wikidata client"""
        start_time = time.time()
    
        # Add debug logging
        print(f"🔍 Reconciling: {entity.name} (Type: {entity.entity_type.value})")
    
        # Check cache first
        cached_result = self.cache.get(entity.search_key)
        if cached_result:
            self._stats['cache_hits'] += 1
            print(f"  ✅ Found in cache")
            return cached_result
    
        # Extract context hints for better matching
        context_hints = self._extract_context_hints(entity)
        print(f"  📋 Context hints: {context_hints}")
        
        # Convert entity type
        wikidata_entity_type = self._convert_entity_type(entity.entity_type)
        print(f"  🔄 Converted type: {entity.entity_type.value} → {wikidata_entity_type.value}")
        
        # Query Wikidata based on entity type
        wikidata_matches = []
    
        try:
            print(f"  🌐 Searching Wikidata...")
            
            if wikidata_entity_type == WikidataEntityType.PERSON:
                wikidata_matches = self.wikidata_client.search_persons(entity.name, context_hints)
            elif wikidata_entity_type == WikidataEntityType.PLACE:
                wikidata_matches = self.wikidata_client.search_places(entity.name, context_hints)
            elif wikidata_entity_type == WikidataEntityType.ORGANIZATION:
                wikidata_matches = self.wikidata_client.search_organizations(entity.name, context_hints)
            elif wikidata_entity_type == WikidataEntityType.SUBJECT_TOPIC:
                wikidata_matches = self.wikidata_client.search_subjects(entity.name, context_hints)
            else:
                # For unknown types, try multiple searches
                print(f"  ⚠️ Unknown type, trying person search first")
                wikidata_matches = self.wikidata_client.search_persons(entity.name, context_hints)
                if not wikidata_matches:
                    print(f"  ⚠️ No person matches, trying organization search")
                    wikidata_matches = self.wikidata_client.search_organizations(entity.name, context_hints)
            
            print(f"  📊 Found {len(wikidata_matches)} Wikidata matches")
        
        except Exception as e:
            print(f"  ❌ Error querying Wikidata: {e}")
            import traceback
            traceback.print_exc()
            wikidata_matches = []
        
        # Convert Wikidata matches to MatchResult objects
        matches = [self._convert_wikidata_match_to_result(match) for match in wikidata_matches]
        
        # Determine best match and overall confidence
        best_match = matches[0] if matches else None
        confidence = self._calculate_overall_confidence(matches)
        
        # Create result
        result = ReconciliationResult(
            entity=entity,
            matches=matches,
            best_match=best_match,
            confidence=confidence,
            reconciliation_time=time.time() - start_time,
            sources_queried=["wikidata_enhanced"],
            cached=False
        )
        
        # Cache the result
        self.cache.put(entity.search_key, result)
        
        # Update statistics
        self._stats['total_processed'] += 1
        if matches:
            self._stats['wikidata_matches'] += 1
        if confidence == OldConfidenceLevel.HIGH:
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
    
    # FIXED: Proper type annotation using pd.DataFrame instead of pd
    def create_entities_from_dataframe(self, 
                                     df: pd.DataFrame, 
                                     entity_column: str,
                                     type_column: Optional[str] = None, 
                                     context_columns: Optional[List[str]] = None) -> List[Entity]:
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
    
    # FIXED: Proper return type annotation
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
    
    # FIXED: Proper return type annotation  
    def _infer_entity_type(self, entity_name: str) -> EntityType:
        """Infer entity type from name patterns"""
        name_lower = entity_name.lower()
        
        # Organization indicators (check these FIRST)
        org_indicators = [
            'legislature', 'league', 'company', 'institute', 'museum', 
            'library', 'society', 'association', 'college', 'university',
            'school', 'department', 'office', 'agency', 'committee',
            'commission', 'council', 'board', 'foundation', 'center',
            'opera house', 'theater', 'theatre'
        ]
        
        # Check for organization patterns
        for indicator in org_indicators:
            if indicator in name_lower:
                print(f"  🏛️ Detected organization pattern: '{indicator}' in '{entity_name}'")
                return EntityType.ORGANIZATION
        
        # Person indicators
        person_indicators = ['dr.', 'prof.', 'mr.', 'mrs.', 'ms.', 'rev.', 'sr.', 'jr.']
        for indicator in person_indicators:
            if indicator in name_lower:
                return EntityType.PERSON
        
        # Check for comma pattern (Last, First) - common for persons
        if ',' in entity_name and len(entity_name.split(',')) == 2:
            parts = entity_name.split(',')
            if all(part.strip() for part in parts):  # Both parts have content
                return EntityType.PERSON
        
        # Place indicators
        place_indicators = ['city', 'town', 'county', 'state', 'country', 'park', 'lake', 'river', 'mountain']
        for indicator in place_indicators:
            if indicator in name_lower:
                return EntityType.PLACE
        
        # Check if it looks like a person name (two or more capitalized words)
        words = entity_name.split()
        if len(words) >= 2 and len(words) <= 4 and all(word[0].isupper() for word in words if word):
            # But exclude if it contains organization words
            if not any(org_word in name_lower for org_word in org_indicators):
                return EntityType.PERSON
        
        # Default to organization for unmatched patterns
        print(f"  ❓ Could not determine type for '{entity_name}', defaulting to ORGANIZATION")
        return EntityType.ORGANIZATION
    
    # FIXED: Proper return type annotation
    def get_statistics(self) -> Dict[str, Union[int, float]]:
        """Get processing statistics"""
        stats = self._stats.copy()
        
        # Calculate derived statistics
        if stats['total_processed'] > 0:
            stats['cache_hit_rate'] = stats['cache_hits'] / stats['total_processed']
            stats['match_rate'] = stats['wikidata_matches'] / stats['total_processed']
            stats['high_confidence_rate'] = stats['high_confidence_matches'] / stats['total_processed']
        else:
            stats['cache_hit_rate'] = 0.0
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
            'Carleton College',
            'Carleton College. Registrar\'s Office'
        ],
        'entity_type': ['organization', 'organization', 'person', 'organization', 'organization'],
        'date_created': ['1857', '1898-01-02', '1921', '1867-1868', '1920'],
        'location': ['Minnesota', 'Minneapolis, Minnesota', 'Chicago', 'Northfield, Minnesota', 'Northfield, Minnesota']
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