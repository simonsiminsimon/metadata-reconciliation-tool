# File: app/services/failsafe_wikidata_client.py
"""
Failsafe wrapper for Wikidata client that never fails a job due to timeouts
"""

from typing import List, Dict, Optional
from .wikidata_cultural_client import CulturalHeritageWikidataClient, WikidataMatch, EntityType, ConfidenceLevel
import logging
import time

logger = logging.getLogger(__name__)

class FailsafeWikidataClient:
    """Wrapper that ensures Wikidata queries never crash the reconciliation process"""
    
    def __init__(self, rate_limit: float = 2.0, timeout: int = 10, max_results: int = 5):
        """Initialize with more conservative settings"""
        self.client = CulturalHeritageWikidataClient(
            rate_limit=rate_limit,
            timeout=timeout,  # Much shorter timeout
            max_results=max_results
        )
        self.fallback_mode = False
        
    def _safe_search(self, search_method, *args, **kwargs) -> List[WikidataMatch]:
        """Execute a search method with timeout protection"""
        try:
            # If we're in fallback mode, skip Wikidata entirely
            if self.fallback_mode:
                logger.warning("Skipping Wikidata search - in fallback mode")
                return []
            
            # Try the search with a hard timeout
            start_time = time.time()
            results = search_method(*args, **kwargs)
            elapsed = time.time() - start_time
            
            # If it took too long, enter fallback mode
            if elapsed > 15:
                logger.warning(f"Wikidata search took {elapsed:.1f}s - entering fallback mode")
                self.fallback_mode = True
            
            return results
            
        except Exception as e:
            logger.error(f"Wikidata search failed: {e}")
            # Don't try Wikidata again for this session
            self.fallback_mode = True
            return []
    
    def search_persons(self, name: str, context_hints: Dict[str, str] = None) -> List[WikidataMatch]:
        """Safe person search"""
        return self._safe_search(self.client.search_persons, name, context_hints)
    
    def search_places(self, place_name: str, context_hints: Dict[str, str] = None) -> List[WikidataMatch]:
        """Safe place search"""
        return self._safe_search(self.client.search_places, place_name, context_hints)
    
    def search_organizations(self, org_name: str, context_hints: Dict[str, str] = None) -> List[WikidataMatch]:
        """Safe organization search"""
        return self._safe_search(self.client.search_organizations, org_name, context_hints)
    
    def search_subjects(self, subject_term: str, context_hints: Dict[str, str] = None) -> List[WikidataMatch]:
        """Safe subject search"""
        return self._safe_search(self.client.search_subjects, subject_term, context_hints)
    
    def get_statistics(self) -> Dict:
        """Get statistics including fallback status"""
        stats = self.client.get_statistics()
        stats['fallback_mode'] = self.fallback_mode
        return stats