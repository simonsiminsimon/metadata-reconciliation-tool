# Replace your app/services/failsafe_wikidata_client.py with this improved version:

from typing import List, Dict, Optional
from .wikidata_cultural_client import CulturalHeritageWikidataClient, WikidataMatch, EntityType, ConfidenceLevel
import logging
import time

logger = logging.getLogger(__name__)

class FailsafeWikidataClient:
    """Improved failsafe wrapper that's more resilient to occasional timeouts"""
    
    def __init__(self, rate_limit: float = 1.0, timeout: int = 15, max_results: int = 5):
        """Initialize with more conservative settings"""
        self.client = CulturalHeritageWikidataClient(
            rate_limit=rate_limit,
            timeout=timeout,
            max_results=max_results
        )
        
        # Track failures instead of complete fallback mode
        self.consecutive_failures = 0
        self.max_consecutive_failures = 3  # Allow 3 failures before temporary fallback
        self.last_failure_time = 0
        self.fallback_duration = 60  # 1 minute fallback period
        
        # Track success to know when to reset
        self.last_success_time = time.time()
        
    def _should_skip_search(self) -> bool:
        """Determine if we should skip search based on recent failures"""
        current_time = time.time()
        
        # If we had recent consecutive failures, check if fallback period is over
        if self.consecutive_failures >= self.max_consecutive_failures:
            if current_time - self.last_failure_time < self.fallback_duration:
                return True
            else:
                # Fallback period is over, reset and try again
                logger.info("Failsafe period expired, re-enabling Wikidata searches")
                self.consecutive_failures = 0
                return False
        
        return False
    
    def _record_success(self):
        """Record a successful search"""
        self.consecutive_failures = 0
        self.last_success_time = time.time()
    
    def _record_failure(self):
        """Record a failed search"""
        self.consecutive_failures += 1
        self.last_failure_time = time.time()
        
        if self.consecutive_failures >= self.max_consecutive_failures:
            logger.warning(f"Wikidata: {self.consecutive_failures} consecutive failures, entering temporary fallback mode for {self.fallback_duration}s")
    
    def _safe_search(self, search_method, *args, **kwargs) -> List[WikidataMatch]:
        """Execute a search method with improved timeout protection"""
        
        # Check if we should skip due to recent failures
        if self._should_skip_search():
            logger.warning("Skipping Wikidata search - in temporary fallback mode")
            return []
        
        try:
            # Try the search with timeout monitoring
            start_time = time.time()
            results = search_method(*args, **kwargs)
            elapsed = time.time() - start_time
            
            # Record success and reset failure counter
            self._record_success()
            
            # Log slow searches but don't fail
            if elapsed > 30:
                logger.warning(f"Wikidata search took {elapsed:.1f}s but completed successfully")
            
            return results
            
        except Exception as e:
            logger.error(f"Wikidata search failed: {e}")
            self._record_failure()
            return []
    
    def search_persons(self, name: str, context_hints: Dict[str, str] = None) -> List[WikidataMatch]:
        """Safe person search with improved resilience"""
        return self._safe_search(self.client.search_persons, name, context_hints)
    
    def search_places(self, place_name: str, context_hints: Dict[str, str] = None) -> List[WikidataMatch]:
        """Safe place search with improved resilience"""
        return self._safe_search(self.client.search_places, place_name, context_hints)
    
    def search_organizations(self, org_name: str, context_hints: Dict[str, str] = None) -> List[WikidataMatch]:
        """Safe organization search with improved resilience"""
        return self._safe_search(self.client.search_organizations, org_name, context_hints)
    
    def search_subjects(self, subject_term: str, context_hints: Dict[str, str] = None) -> List[WikidataMatch]:
        """Safe subject search with improved resilience"""
        return self._safe_search(self.client.search_subjects, subject_term, context_hints)
    
    def get_statistics(self) -> Dict:
        """Get statistics including failure tracking"""
        stats = self.client.get_statistics()
        stats.update({
            'consecutive_failures': self.consecutive_failures,
            'in_fallback_mode': self._should_skip_search(),
            'last_success_time': self.last_success_time,
            'max_consecutive_failures': self.max_consecutive_failures
        })
        return stats