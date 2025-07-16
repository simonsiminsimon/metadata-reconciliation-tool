# CRITICAL FIX: Replace your app/services/failsafe_wikidata_client.py with this robust version

from typing import List, Dict, Optional
from .wikidata_cultural_client import CulturalHeritageWikidataClient, WikidataMatch, EntityType, ConfidenceLevel
import logging
import time

logger = logging.getLogger(__name__)

class FailsafeWikidataClient:
    """
    Ultra-robust failsafe wrapper that gracefully handles timeouts and API issues
    
    This client ensures processing never gets completely stuck by:
    - Using very short timeouts
    - Falling back to empty results on repeated failures
    - Tracking failure patterns and adapting behavior
    - Providing circuit breaker functionality
    """
    
    def __init__(self, rate_limit: float = 0.5, timeout: int = 8, max_results: int = 3):
        """Initialize with very conservative settings to avoid timeouts"""
        
        try:
            self.client = CulturalHeritageWikidataClient(
                rate_limit=rate_limit,  # Slower rate = less timeouts
                timeout=timeout,        # Short timeout = faster failures
                max_results=max_results # Fewer results = faster queries
            )
            self.client_available = True
        except Exception as e:
            logger.error(f"Failed to initialize Wikidata client: {e}")
            self.client = None
            self.client_available = False
        
        # Circuit breaker pattern
        self.consecutive_failures = 0
        self.max_consecutive_failures = 2  # Only allow 2 failures before circuit breaker
        self.circuit_breaker_active = False
        self.circuit_breaker_reset_time = 0
        self.circuit_breaker_duration = 30  # 30 seconds
        
        # Performance tracking
        self.total_attempts = 0
        self.total_successes = 0
        self.total_timeouts = 0
        self.last_success_time = time.time()
        
    def _should_skip_request(self) -> bool:
        """Determine if we should skip the request due to circuit breaker"""
        current_time = time.time()
        
        # Check if circuit breaker should be reset
        if self.circuit_breaker_active:
            if current_time >= self.circuit_breaker_reset_time:
                logger.info("🔄 Circuit breaker reset - trying Wikidata again")
                self.circuit_breaker_active = False
                self.consecutive_failures = 0
            else:
                remaining = int(self.circuit_breaker_reset_time - current_time)
                logger.debug(f"⚡ Circuit breaker active - skipping request ({remaining}s remaining)")
                return True
        
        # If client isn't available, skip
        if not self.client_available:
            return True
        
        return False
    
    def _handle_success(self, results: List[WikidataMatch]) -> List[WikidataMatch]:
        """Handle successful request"""
        self.total_attempts += 1
        self.total_successes += 1
        self.consecutive_failures = 0
        self.last_success_time = time.time()
        
        # Reset circuit breaker on success
        if self.circuit_breaker_active:
            logger.info("✅ Wikidata working again - circuit breaker reset")
            self.circuit_breaker_active = False
        
        return results
    
    def _handle_failure(self, error: Exception, search_term: str = "") -> List[WikidataMatch]:
        """Handle failed request with circuit breaker logic"""
        self.total_attempts += 1
        self.consecutive_failures += 1
        
        # Check if we should activate circuit breaker
        if self.consecutive_failures >= self.max_consecutive_failures and not self.circuit_breaker_active:
            self.circuit_breaker_active = True
            self.circuit_breaker_reset_time = time.time() + self.circuit_breaker_duration
            logger.warning(f"⚡ Circuit breaker activated - too many Wikidata failures")
            logger.warning(f"⚡ Will retry in {self.circuit_breaker_duration} seconds")
        
        # Log the specific error
        if "timeout" in str(error).lower():
            self.total_timeouts += 1
            logger.warning(f"⏰ Wikidata timeout for '{search_term}' - returning empty results")
        else:
            logger.warning(f"❌ Wikidata error for '{search_term}': {error}")
        
        return []  # Always return empty list instead of crashing
    
    def search_persons(self, person_name: str, context_hints: Dict[str, str] = None) -> List[WikidataMatch]:
        """Search for persons with robust error handling"""
        if self._should_skip_request():
            return []
        
        try:
            results = self.client.search_persons(person_name, context_hints)
            return self._handle_success(results)
        except Exception as e:
            return self._handle_failure(e, person_name)
    
    def search_places(self, place_name: str, context_hints: Dict[str, str] = None) -> List[WikidataMatch]:
        """Search for places with robust error handling"""
        if self._should_skip_request():
            return []
        
        try:
            results = self.client.search_places(place_name, context_hints)
            return self._handle_success(results)
        except Exception as e:
            return self._handle_failure(e, place_name)
    
    def search_organizations(self, org_name: str, context_hints: Dict[str, str] = None) -> List[WikidataMatch]:
        """Search for organizations with robust error handling"""
        if self._should_skip_request():
            return []
        
        try:
            results = self.client.search_organizations(org_name, context_hints)
            return self._handle_success(results)
        except Exception as e:
            return self._handle_failure(e, org_name)
    
    def search_subjects(self, subject_term: str, context_hints: Dict[str, str] = None) -> List[WikidataMatch]:
        """Search for subjects with robust error handling"""
        if self._should_skip_request():
            return []
        
        try:
            results = self.client.search_subjects(subject_term, context_hints)
            return self._handle_success(results)
        except Exception as e:
            return self._handle_failure(e, subject_term)
    
    def get_statistics(self) -> Dict:
        """Get client statistics including circuit breaker info"""
        stats = {
            'total_attempts': self.total_attempts,
            'total_successes': self.total_successes,
            'total_timeouts': self.total_timeouts,
            'consecutive_failures': self.consecutive_failures,
            'circuit_breaker_active': self.circuit_breaker_active,
            'client_available': self.client_available,
            'success_rate': (self.total_successes / self.total_attempts) if self.total_attempts > 0 else 0,
            'timeout_rate': (self.total_timeouts / self.total_attempts) if self.total_attempts > 0 else 0
        }
        
        # Add underlying client stats if available
        if self.client and self.client_available:
            try:
                client_stats = self.client.get_statistics()
                stats.update(client_stats)
            except:
                pass
        
        return stats
    
    def reset_circuit_breaker(self):
        """Manually reset circuit breaker for testing"""
        self.circuit_breaker_active = False
        self.consecutive_failures = 0
        logger.info("🔄 Circuit breaker manually reset")


# Test the failsafe client
if __name__ == "__main__":
    print("Testing Failsafe Wikidata Client...")
    
    client = FailsafeWikidataClient(rate_limit=0.5, timeout=5, max_results=2)
    
    # Test with some entities
    test_entities = [
        ("William Shakespeare", "person"),
        ("Harvard University", "organization"),
        ("Paris", "place")
    ]
    
    for entity_name, entity_type in test_entities:
        print(f"\n🔍 Testing: {entity_name} ({entity_type})")
        
        if entity_type == "person":
            results = client.search_persons(entity_name)
        elif entity_type == "organization":
            results = client.search_organizations(entity_name)
        elif entity_type == "place":
            results = client.search_places(entity_name)
        
        print(f"   Results: {len(results)} matches found")
        for result in results[:2]:
            print(f"   - {result.label} (confidence: {result.confidence_score:.2f})")
    
    # Show statistics
    stats = client.get_statistics()
    print(f"\n📊 Statistics:")
    print(f"   Success rate: {stats['success_rate']:.1%}")
    print(f"   Timeout rate: {stats['timeout_rate']:.1%}")
    print(f"   Circuit breaker: {'Active' if stats['circuit_breaker_active'] else 'Inactive'}")