# File: app/services/enhanced_reconciliation_engine.py (FIXED VERSION)
"""
Enhanced Reconciliation Engine with improved entity detection and processing.
Fixed issues with entity extraction and CSV parsing.
"""

import pandas as pd
import re
import time
from typing import List, Optional, Dict, Any
from dataclasses import dataclass
from enum import Enum
import logging
from .failsafe_wikidata_client import FailsafeWikidataClient

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

logger = logging.getLogger(__name__)

class EnhancedReconciliationEngine:
    """Enhanced reconciliation engine with improved entity detection"""
    
    def __init__(self, cache_size: int = 1000, wikidata_rate_limit: float = 1.0):
        """
        Initialize the enhanced reconciliation engine
        
        Args:
            cache_size: Size of the local cache
            wikidata_rate_limit: Wikidata API rate limit (requests per second)
        """
        # Initialize the enhanced Wikidata client
        try:
            from .failsafe_wikidata_client import FailsafeWikidataClient
            self.wikidata_client = FailsafeWikidataClient(
                rate_limit=wikidata_rate_limit,
                timeout=10,  # Short timeout
                max_results=5  # Fewer results to speed up
            )
        except ImportError:
            # Fallback to cultural heritage client if failsafe not available
            self.wikidata_client = CulturalHeritageWikidataClient(
                rate_limit=wikidata_rate_limit
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
        self.cache.set(entity.search_key, result)
        
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
    
    def create_entities_from_dataframe(self, 
                                     df: pd.DataFrame, 
                                     entity_column: str,
                                     type_column: Optional[str] = None, 
                                     context_columns: Optional[List[str]] = None) -> List[Entity]:
        """Create entities from DataFrame (compatible with existing interface)"""
        entities = []
        context_columns = context_columns or []
        
        # Fix 1: Case-insensitive column matching
        df_columns_lower = {col.lower(): col for col in df.columns}
        entity_col_actual = df_columns_lower.get(entity_column.lower())
        
        if not entity_col_actual:
            # Try to find similar column names
            for col in df.columns:
                if entity_column.lower() in col.lower() or col.lower() in entity_column.lower():
                    entity_col_actual = col
                    break
            
            if not entity_col_actual:
                logger.error(f"Entity column '{entity_column}' not found in CSV. Available columns: {list(df.columns)}")
                raise ValueError(f"Entity column '{entity_column}' not found in CSV")
        
        # Fix 2: Better type column handling
        type_col_actual = None
        if type_column:
            type_col_actual = df_columns_lower.get(type_column.lower())
            if not type_col_actual:
                for col in df.columns:
                    if type_column.lower() in col.lower():
                        type_col_actual = col
                        break
        
        logger.info(f"Using entity column: '{entity_col_actual}' (from '{entity_column}')")
        if type_col_actual:
            logger.info(f"Using type column: '{type_col_actual}' (from '{type_column}')")
        
        # Fix 3: More lenient entity extraction with better cleaning
        entity_count = 0
        for idx, row in df.iterrows():
            entity_name = str(row[entity_col_actual]).strip()  # Use actual column name
            print(f"🔍 DEBUG: Row {idx}: '{entity_name}' -> valid: {entity_name and entity_name.lower() not in ['nan', 'none', '']}")
            
            if not entity_name or entity_name.lower() in ['nan', 'none', '']:
                continue
            
            # Determine entity type
            if type_col_actual and type_col_actual in row:
                entity_type = self._parse_entity_type(str(row[type_col_actual]))
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
            entity_count += 1
        
        logger.info(f"Created {entity_count} entities from {len(df)} rows")
        return entities
    
    def _parse_entity_type(self, type_str: str) -> EntityType:
        """Parse entity type from string with better matching"""
        if pd.isna(type_str) or not type_str:
            return EntityType.UNKNOWN
        
        type_str = str(type_str).lower().strip()
        
        # Expanded type mapping
        type_mapping = {
            'person': EntityType.PERSON,
            'people': EntityType.PERSON,
            'author': EntityType.AUTHOR,
            'creator': EntityType.PERSON,
            'name': EntityType.PERSON,
            'individual': EntityType.PERSON,
            'place': EntityType.PLACE,
            'location': EntityType.PLACE,
            'geography': EntityType.PLACE,
            'geographic': EntityType.PLACE,
            'city': EntityType.PLACE,
            'country': EntityType.PLACE,
            'organization': EntityType.ORGANIZATION,
            'org': EntityType.ORGANIZATION,
            'institution': EntityType.ORGANIZATION,
            'company': EntityType.ORGANIZATION,
            'subject': EntityType.SUBJECT,
            'topic': EntityType.SUBJECT,
            'theme': EntityType.SUBJECT,
            'category': EntityType.SUBJECT,
            'artwork': EntityType.ARTWORK,
            'art': EntityType.ARTWORK,
            'work': EntityType.ARTWORK
        }
        
        # Try exact match first
        if type_str in type_mapping:
            return type_mapping[type_str]
        
        # Try partial matches
        for key, entity_type in type_mapping.items():
            if key in type_str or type_str in key:
                return entity_type
        
        return EntityType.UNKNOWN
    
    def _infer_entity_type(self, entity_name: str) -> EntityType:
        """Infer entity type from name patterns"""
        name_lower = entity_name.lower()
        
        # Person indicators
        person_indicators = [
            'dr.', 'prof.', 'mr.', 'mrs.', 'ms.', 'miss', 'sir', 'lady',
            'jr.', 'sr.', 'ii', 'iii', 'iv'
        ]
        
        if any(indicator in name_lower for indicator in person_indicators):
            return EntityType.PERSON
        
        # Check for typical person name patterns (First Last, Last, First)
        if re.match(r'^[A-Z][a-z]+\s+[A-Z][a-z]+', entity_name):
            return EntityType.PERSON
        
        if ',' in entity_name and len(entity_name.split(',')) == 2:
            return EntityType.PERSON
        
        # Place indicators
        place_indicators = ['city', 'county', 'state', 'province', 'country', 'region']
        if any(indicator in name_lower for indicator in place_indicators):
            return EntityType.PLACE
        
        # Organization indicators
        org_indicators = ['inc.', 'corp.', 'ltd.', 'llc', 'university', 'college', 'museum', 'library']
        if any(indicator in name_lower for indicator in org_indicators):
            return EntityType.ORGANIZATION
        
        return EntityType.UNKNOWN
    
    def process_entities(self, entities: List[Entity]) -> List[ReconciliationResult]:
        """Process entities for reconciliation"""
        results = []
        
        for entity in entities:
            result = self._reconcile_entity(entity)
            results.append(result)
        
        return results
    
    def get_statistics(self) -> Dict[str, Any]:
        """Return processing statistics"""
        return self._stats.copy()