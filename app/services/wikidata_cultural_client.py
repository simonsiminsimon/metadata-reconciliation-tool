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
    - Specialized queries for cultural heritage entities
    - Advanced matching algorithms with context awareness
    - Comprehensive result enrichment
    - Built-in caching and rate limiting
    - Cultural heritage authority linking (VIAF, LC, etc.)
    """
    
    def __init__(self, rate_limit: float = 1.0, timeout: int = 30, 
                 cache_enabled: bool = True, max_results: int = 10):
        """
        Initialize the Wikidata client
        
        Args:
            rate_limit: Requests per second (default: 1.0)
            timeout: Request timeout in seconds (default: 30)
            cache_enabled: Enable in-memory caching (default: True)
            max_results: Maximum results per query (default: 10)
        """
        self.sparql_endpoint = "https://query.wikidata.org/sparql"
        self.wikidata_api = "https://www.wikidata.org/w/api.php"
        self.rate_limit = rate_limit
        self.timeout = timeout
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
            'User-Agent': 'CulturalHeritageReconciliation/1.0 (Contact: your-institution@example.org)'
        })
        
        # Statistics
        self.stats = {
            'queries_made': 0,
            'cache_hits': 0,
            'successful_matches': 0
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
    
    def _sparql_query(self, query: str) -> List[Dict]:
        """Execute a SPARQL query against Wikidata"""
        self._respect_rate_limit()
        self.stats['queries_made'] += 1
        
        try:
            response = self.session.get(
                self.sparql_endpoint,
                params={
                    'query': query,
                    'format': 'json'
                },
                timeout=self.timeout
            )
            response.raise_for_status()
            
            data = response.json()
            return data.get('results', {}).get('bindings', [])
            
        except requests.exceptions.RequestException as e:
            logger.error(f"SPARQL query failed: {e}")
            return []
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse SPARQL response: {e}")
            return []
    
    def _calculate_confidence(self, search_term: str, result_label: str, 
                            result_description: str = "", aliases: List[str] = None,
                            context_hints: Dict[str, str] = None) -> Tuple[ConfidenceLevel, float]:
        """
        Calculate confidence score using multiple factors
        
        Args:
            search_term: Original search term
            result_label: Wikidata label
            result_description: Wikidata description
            aliases: Alternative names/aliases
            context_hints: Additional context (dates, places, etc.)
        """
        if aliases is None:
            aliases = []
        if context_hints is None:
            context_hints = {}
        
        # Normalize strings for comparison
        search_lower = search_term.lower().strip()
        label_lower = result_label.lower().strip()
        description_lower = result_description.lower().strip()
        aliases_lower = [alias.lower().strip() for alias in aliases]
        
        score = 0.0
        
        # Exact label match (highest weight)
        if search_lower == label_lower:
            score += 0.4
        elif search_lower in label_lower or label_lower in search_lower:
            # Partial label match
            overlap = len(set(search_lower.split()) & set(label_lower.split()))
            total = len(set(search_lower.split()) | set(label_lower.split()))
            if total > 0:
                score += 0.3 * (overlap / total)
        
        # Alias matching
        for alias in aliases_lower:
            if search_lower == alias:
                score += 0.3
                break
            elif search_lower in alias or alias in search_lower:
                overlap = len(set(search_lower.split()) & set(alias.split()))
                total = len(set(search_lower.split()) | set(alias.split()))
                if total > 0:
                    score += 0.2 * (overlap / total)
                break
        
        # Description relevance
        if description_lower:
            search_words = set(search_lower.split())
            desc_words = set(description_lower.split())
            common_words = search_words & desc_words
            if common_words:
                score += 0.1 * (len(common_words) / len(search_words))
        
        # Context matching bonuses
        if context_hints:
            if 'date' in context_hints and context_hints['date']:
                # This could be enhanced to match birth/death dates, etc.
                score += 0.05
            if 'location' in context_hints and context_hints['location']:
                # This could be enhanced to match geographic context
                score += 0.05
        
        # Ensure score is between 0 and 1
        score = min(max(score, 0.0), 1.0)
        
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
        """
        Search for persons with cultural heritage context
        
        Args:
            name: Person name to search for
            context_hints: Optional context like birth_year, death_year, occupation, nationality
        """
        cache_key = f"person:{name}:{hash(str(context_hints))}"
        cached_results = self._get_from_cache(cache_key)
        if cached_results is not None:
            return cached_results
        
        if context_hints is None:
            context_hints = {}
        
        # Build enhanced SPARQL query
        query = f"""
        SELECT DISTINCT ?person ?personLabel ?personDescription ?birthDate ?deathDate 
               ?occupationLabel ?nationalityLabel ?viafId ?lcId ?image ?coords
               (GROUP_CONCAT(DISTINCT ?altLabel; SEPARATOR="|") AS ?aliases)
        WHERE {{
          # Search for humans with name matching
          ?person wdt:P31 wd:Q5 .  # Instance of human
          ?person rdfs:label ?personLabel .
          
          # Alternative labels (aliases)
          OPTIONAL {{ ?person skos:altLabel ?altLabel . }}
          
          # Basic information
          OPTIONAL {{ ?person schema:description ?personDescription . }}
          OPTIONAL {{ ?person wdt:P569 ?birthDate . }}
          OPTIONAL {{ ?person wdt:P570 ?deathDate . }}
          OPTIONAL {{ ?person wdt:P18 ?image . }}
          OPTIONAL {{ ?person wdt:P625 ?coords . }}
          
          # Occupations and roles
          OPTIONAL {{ 
            ?person wdt:P106 ?occupation . 
            ?occupation rdfs:label ?occupationLabel .
            FILTER(LANG(?occupationLabel) = "en")
          }}
          
          # Nationality/citizenship
          OPTIONAL {{ 
            ?person wdt:P27 ?nationality . 
            ?nationality rdfs:label ?nationalityLabel .
            FILTER(LANG(?nationalityLabel) = "en")
          }}
          
          # External authority IDs
          OPTIONAL {{ ?person wdt:P214 ?viafId . }}      # VIAF ID
          OPTIONAL {{ ?person wdt:P244 ?lcId . }}        # Library of Congress ID
          
          # Language filters
          FILTER(LANG(?personLabel) = "en")
          FILTER(LANG(?personDescription) = "en")
          FILTER(LANG(?altLabel) = "en")
          
          # Name matching (flexible)
          FILTER(
            CONTAINS(LCASE(?personLabel), LCASE("{name}")) ||
            CONTAINS(LCASE(?altLabel), LCASE("{name}"))
          )
        }}
        GROUP BY ?person ?personLabel ?personDescription ?birthDate ?deathDate 
                 ?occupationLabel ?nationalityLabel ?viafId ?lcId ?image ?coords
        LIMIT {self.max_results}
        """
        
        results = self._sparql_query(query)
        matches = []
        
        for binding in results:
            # Extract basic information
            wikidata_id = binding['person']['value'].split('/')[-1]
            label = binding.get('personLabel', {}).get('value', '')
            description = binding.get('personDescription', {}).get('value', '')
            
            # Extract aliases
            aliases_str = binding.get('aliases', {}).get('value', '')
            aliases = [alias.strip() for alias in aliases_str.split('|') if alias.strip()] if aliases_str else []
            
            # Calculate confidence
            confidence_level, confidence_score = self._calculate_confidence(
                name, label, description, aliases, context_hints
            )
            
            # Create enhanced match object
            match = WikidataMatch(
                wikidata_id=wikidata_id,
                label=label,
                description=description,
                confidence_level=confidence_level,
                confidence_score=confidence_score,
                entity_type=EntityType.PERSON,
                aliases=aliases,
                birth_date=binding.get('birthDate', {}).get('value'),
                death_date=binding.get('deathDate', {}).get('value'),
                coordinates=binding.get('coords', {}).get('value'),
                viaf_id=binding.get('viafId', {}).get('value'),
                library_of_congress_id=binding.get('lcId', {}).get('value'),
                image_url=binding.get('image', {}).get('value')
            )
            
            # Add occupation and nationality to external_ids for context
            if binding.get('occupationLabel', {}).get('value'):
                match.external_ids['occupation'] = binding['occupationLabel']['value']
            if binding.get('nationalityLabel', {}).get('value'):
                match.external_ids['nationality'] = binding['nationalityLabel']['value']
            
            matches.append(match)
        
        # Sort by confidence score
        matches.sort(key=lambda x: x.confidence_score, reverse=True)
        
        # Update statistics
        if matches:
            self.stats['successful_matches'] += 1
        
        # Cache results
        self._store_in_cache(cache_key, matches)
        
        return matches
    
    def search_places(self, place_name: str, context_hints: Dict[str, str] = None) -> List[WikidataMatch]:
        """
        Search for places with geographic and administrative context
        
        Args:
            place_name: Place name to search for
            context_hints: Optional context like country, state, type (city, building, etc.)
        """
        cache_key = f"place:{place_name}:{hash(str(context_hints))}"
        cached_results = self._get_from_cache(cache_key)
        if cached_results is not None:
            return cached_results
        
        if context_hints is None:
            context_hints = {}
        
        query = f"""
        SELECT DISTINCT ?place ?placeLabel ?placeDescription ?coords ?countryLabel 
               ?adminLabel ?inceptionDate ?website ?image
               (GROUP_CONCAT(DISTINCT ?altLabel; SEPARATOR="|") AS ?aliases)
        WHERE {{
          # Geographic locations and administrative entities
          {{
            ?place wdt:P31/wdt:P279* wd:Q618123 .  # Geographic locations
          }} UNION {{
            ?place wdt:P31/wdt:P279* wd:Q15642541 . # Human settlements
          }} UNION {{
            ?place wdt:P31/wdt:P279* wd:Q41176 .    # Buildings
          }} UNION {{
            ?place wdt:P31/wdt:P279* wd:Q43229 .    # Organizations (institutions)
          }}
          
          ?place rdfs:label ?placeLabel .
          
          # Alternative labels
          OPTIONAL {{ ?place skos:altLabel ?altLabel . }}
          
          # Basic information
          OPTIONAL {{ ?place schema:description ?placeDescription . }}
          OPTIONAL {{ ?place wdt:P625 ?coords . }}
          OPTIONAL {{ ?place wdt:P571 ?inceptionDate . }}
          OPTIONAL {{ ?place wdt:P856 ?website . }}
          OPTIONAL {{ ?place wdt:P18 ?image . }}
          
          # Administrative context
          OPTIONAL {{ 
            ?place wdt:P17 ?country . 
            ?country rdfs:label ?countryLabel .
            FILTER(LANG(?countryLabel) = "en")
          }}
          OPTIONAL {{ 
            ?place wdt:P131 ?admin . 
            ?admin rdfs:label ?adminLabel .
            FILTER(LANG(?adminLabel) = "en")
          }}
          
          # Language filters
          FILTER(LANG(?placeLabel) = "en")
          FILTER(LANG(?placeDescription) = "en")
          FILTER(LANG(?altLabel) = "en")
          
          # Name matching
          FILTER(
            CONTAINS(LCASE(?placeLabel), LCASE("{place_name}")) ||
            CONTAINS(LCASE(?altLabel), LCASE("{place_name}"))
          )
        }}
        GROUP BY ?place ?placeLabel ?placeDescription ?coords ?countryLabel 
                 ?adminLabel ?inceptionDate ?website ?image
        LIMIT {self.max_results}
        """
        
        results = self._sparql_query(query)
        matches = []
        
        for binding in results:
            wikidata_id = binding['place']['value'].split('/')[-1]
            label = binding.get('placeLabel', {}).get('value', '')
            description = binding.get('placeDescription', {}).get('value', '')
            
            aliases_str = binding.get('aliases', {}).get('value', '')
            aliases = [alias.strip() for alias in aliases_str.split('|') if alias.strip()] if aliases_str else []
            
            confidence_level, confidence_score = self._calculate_confidence(
                place_name, label, description, aliases, context_hints
            )
            
            match = WikidataMatch(
                wikidata_id=wikidata_id,
                label=label,
                description=description,
                confidence_level=confidence_level,
                confidence_score=confidence_score,
                entity_type=EntityType.PLACE,
                aliases=aliases,
                coordinates=binding.get('coords', {}).get('value'),
                country=binding.get('countryLabel', {}).get('value'),
                inception_date=binding.get('inceptionDate', {}).get('value'),
                website=binding.get('website', {}).get('value'),
                image_url=binding.get('image', {}).get('value')
            )
            
            # Add administrative context
            if binding.get('adminLabel', {}).get('value'):
                match.external_ids['administrative_entity'] = binding['adminLabel']['value']
            
            matches.append(match)
        
        matches.sort(key=lambda x: x.confidence_score, reverse=True)
        
        if matches:
            self.stats['successful_matches'] += 1
        
        self._store_in_cache(cache_key, matches)
        return matches
    
    def search_organizations(self, org_name: str, context_hints: Dict[str, str] = None) -> List[WikidataMatch]:
        """
        Search for organizations, institutions, and corporate bodies
        
        Args:
            org_name: Organization name to search for
            context_hints: Optional context like type, location, founding_date
        """
        cache_key = f"organization:{org_name}:{hash(str(context_hints))}"
        cached_results = self._get_from_cache(cache_key)
        if cached_results is not None:
            return cached_results
        
        if context_hints is None:
            context_hints = {}
        
        query = f"""
        SELECT DISTINCT ?org ?orgLabel ?orgDescription ?inception ?location ?website 
               ?countryLabel ?typeLabel ?viafId ?lcId
               (GROUP_CONCAT(DISTINCT ?altLabel; SEPARATOR="|") AS ?aliases)
        WHERE {{
          # Organizations and institutions
          {{
            ?org wdt:P31/wdt:P279* wd:Q43229 .      # Organizations
          }} UNION {{
            ?org wdt:P31/wdt:P279* wd:Q33506 .      # Museums
          }} UNION {{
            ?org wdt:P31/wdt:P279* wd:Q7075 .       # Libraries
          }} UNION {{
            ?org wdt:P31/wdt:P279* wd:Q3918 .       # Universities
          }} UNION {{
            ?org wdt:P31/wdt:P279* wd:Q4671277 .    # Academic institutions
          }}
          
          ?org rdfs:label ?orgLabel .
          
          # Alternative labels
          OPTIONAL {{ ?org skos:altLabel ?altLabel . }}
          
          # Basic information
          OPTIONAL {{ ?org schema:description ?orgDescription . }}
          OPTIONAL {{ ?org wdt:P571 ?inception . }}
          OPTIONAL {{ ?org wdt:P159 ?location . }}
          OPTIONAL {{ ?org wdt:P856 ?website . }}
          
          # Type and country
          OPTIONAL {{ 
            ?org wdt:P31 ?type . 
            ?type rdfs:label ?typeLabel .
            FILTER(LANG(?typeLabel) = "en")
          }}
          OPTIONAL {{ 
            ?org wdt:P17 ?country . 
            ?country rdfs:label ?countryLabel .
            FILTER(LANG(?countryLabel) = "en")
          }}
          
          # External IDs
          OPTIONAL {{ ?org wdt:P214 ?viafId . }}    # VIAF ID
          OPTIONAL {{ ?org wdt:P244 ?lcId . }}      # Library of Congress ID
          
          # Language filters
          FILTER(LANG(?orgLabel) = "en")
          FILTER(LANG(?orgDescription) = "en")
          FILTER(LANG(?altLabel) = "en")
          
          # Name matching
          FILTER(
            CONTAINS(LCASE(?orgLabel), LCASE("{org_name}")) ||
            CONTAINS(LCASE(?altLabel), LCASE("{org_name}"))
          )
        }}
        GROUP BY ?org ?orgLabel ?orgDescription ?inception ?location ?website 
                 ?countryLabel ?typeLabel ?viafId ?lcId
        LIMIT {self.max_results}
        """
        
        results = self._sparql_query(query)
        matches = []
        
        for binding in results:
            wikidata_id = binding['org']['value'].split('/')[-1]
            label = binding.get('orgLabel', {}).get('value', '')
            description = binding.get('orgDescription', {}).get('value', '')
            
            aliases_str = binding.get('aliases', {}).get('value', '')
            aliases = [alias.strip() for alias in aliases_str.split('|') if alias.strip()] if aliases_str else []
            
            confidence_level, confidence_score = self._calculate_confidence(
                org_name, label, description, aliases, context_hints
            )
            
            match = WikidataMatch(
                wikidata_id=wikidata_id,
                label=label,
                description=description,
                confidence_level=confidence_level,
                confidence_score=confidence_score,
                entity_type=EntityType.ORGANIZATION,
                aliases=aliases,
                inception_date=binding.get('inception', {}).get('value'),
                country=binding.get('countryLabel', {}).get('value'),
                website=binding.get('website', {}).get('value'),
                viaf_id=binding.get('viafId', {}).get('value'),
                library_of_congress_id=binding.get('lcId', {}).get('value')
            )
            
            # Add type information
            if binding.get('typeLabel', {}).get('value'):
                match.external_ids['organization_type'] = binding['typeLabel']['value']
            
            matches.append(match)
        
        matches.sort(key=lambda x: x.confidence_score, reverse=True)
        
        if matches:
            self.stats['successful_matches'] += 1
        
        self._store_in_cache(cache_key, matches)
        return matches
    
    def search_subjects(self, subject_term: str, context_hints: Dict[str, str] = None) -> List[WikidataMatch]:
        """
        Search for subjects, topics, and concepts
        
        Args:
            subject_term: Subject/topic to search for
            context_hints: Optional context like field, time_period
        """
        cache_key = f"subject:{subject_term}:{hash(str(context_hints))}"
        cached_results = self._get_from_cache(cache_key)
        if cached_results is not None:
            return cached_results
        
        if context_hints is None:
            context_hints = {}
        
        query = f"""
        SELECT DISTINCT ?concept ?conceptLabel ?conceptDescription ?fieldLabel
               (GROUP_CONCAT(DISTINCT ?altLabel; SEPARATOR="|") AS ?aliases)
        WHERE {{
          # Concepts, academic disciplines, topics
          {{
            ?concept wdt:P31/wdt:P279* wd:Q151885 .   # Concepts
          }} UNION {{
            ?concept wdt:P31/wdt:P279* wd:Q5891 .     # Academic disciplines
          }} UNION {{
            ?concept wdt:P31/wdt:P279* wd:Q1914636 .  # Academic fields
          }} UNION {{
            ?concept wdt:P31/wdt:P279* wd:Q2995644 .  # Academic subjects
          }}
          
          ?concept rdfs:label ?conceptLabel .
          
          # Alternative labels
          OPTIONAL {{ ?concept skos:altLabel ?altLabel . }}
          
          # Basic information
          OPTIONAL {{ ?concept schema:description ?conceptDescription . }}
          
          # Field of study/work
          OPTIONAL {{ 
            ?concept wdt:P101 ?field . 
            ?field rdfs:label ?fieldLabel .
            FILTER(LANG(?fieldLabel) = "en")
          }}
          
          # Language filters
          FILTER(LANG(?conceptLabel) = "en")
          FILTER(LANG(?conceptDescription) = "en")
          FILTER(LANG(?altLabel) = "en")
          
          # Name matching
          FILTER(
            CONTAINS(LCASE(?conceptLabel), LCASE("{subject_term}")) ||
            CONTAINS(LCASE(?altLabel), LCASE("{subject_term}"))
          )
        }}
        GROUP BY ?concept ?conceptLabel ?conceptDescription ?fieldLabel
        LIMIT {self.max_results}
        """
        
        results = self._sparql_query(query)
        matches = []
        
        for binding in results:
            wikidata_id = binding['concept']['value'].split('/')[-1]
            label = binding.get('conceptLabel', {}).get('value', '')
            description = binding.get('conceptDescription', {}).get('value', '')
            
            aliases_str = binding.get('aliases', {}).get('value', '')
            aliases = [alias.strip() for alias in aliases_str.split('|') if alias.strip()] if aliases_str else []
            
            confidence_level, confidence_score = self._calculate_confidence(
                subject_term, label, description, aliases, context_hints
            )
            
            match = WikidataMatch(
                wikidata_id=wikidata_id,
                label=label,
                description=description,
                confidence_level=confidence_level,
                confidence_score=confidence_score,
                entity_type=EntityType.SUBJECT_TOPIC,
                aliases=aliases
            )
            
            # Add field information
            if binding.get('fieldLabel', {}).get('value'):
                match.external_ids['field_of_study'] = binding['fieldLabel']['value']
            
            matches.append(match)
        
        matches.sort(key=lambda x: x.confidence_score, reverse=True)
        
        if matches:
            self.stats['successful_matches'] += 1
        
        self._store_in_cache(cache_key, matches)
        return matches
    
    def get_entity_details(self, wikidata_id: str) -> Optional[WikidataMatch]:
        """
        Get detailed information about a specific Wikidata entity
        
        Args:
            wikidata_id: Wikidata Q-identifier (e.g., 'Q42')
        """
        cache_key = f"details:{wikidata_id}"
        cached_results = self._get_from_cache(cache_key)
        if cached_results is not None and cached_results:
            return cached_results[0]
        
        query = f"""
        SELECT ?entity ?entityLabel ?entityDescription ?birthDate ?deathDate 
               ?coords ?countryLabel ?image ?website ?viafId ?lcId ?inceptionDate
               ?commonsCategory
               (GROUP_CONCAT(DISTINCT ?altLabel; SEPARATOR="|") AS ?aliases)
               (GROUP_CONCAT(DISTINCT ?typeLabel; SEPARATOR="|") AS ?types)
        WHERE {{
          BIND(wd:{wikidata_id} AS ?entity)
          
          ?entity rdfs:label ?entityLabel .
          
          # Alternative labels
          OPTIONAL {{ ?entity skos:altLabel ?altLabel . }}
          
          # Types
          OPTIONAL {{ 
            ?entity wdt:P31 ?type . 
            ?type rdfs:label ?typeLabel .
            FILTER(LANG(?typeLabel) = "en")
          }}
          
          # Basic information
          OPTIONAL {{ ?entity schema:description ?entityDescription . }}
          OPTIONAL {{ ?entity wdt:P569 ?birthDate . }}
          OPTIONAL {{ ?entity wdt:P570 ?deathDate . }}
          OPTIONAL {{ ?entity wdt:P571 ?inceptionDate . }}
          OPTIONAL {{ ?entity wdt:P625 ?coords . }}
          OPTIONAL {{ ?entity wdt:P18 ?image . }}
          OPTIONAL {{ ?entity wdt:P856 ?website . }}
          OPTIONAL {{ ?entity wdt:P373 ?commonsCategory . }}
          
          # Country
          OPTIONAL {{ 
            ?entity wdt:P17 ?country . 
            ?country rdfs:label ?countryLabel .
            FILTER(LANG(?countryLabel) = "en")
          }}
          
          # External IDs
          OPTIONAL {{ ?entity wdt:P214 ?viafId . }}
          OPTIONAL {{ ?entity wdt:P244 ?lcId . }}
          
          # Language filters
          FILTER(LANG(?entityLabel) = "en")
          FILTER(LANG(?entityDescription) = "en")
          FILTER(LANG(?altLabel) = "en")
        }}
        GROUP BY ?entity ?entityLabel ?entityDescription ?birthDate ?deathDate 
                 ?coords ?countryLabel ?image ?website ?viafId ?lcId ?inceptionDate
                 ?commonsCategory
        """
        
        results = self._sparql_query(query)
        
        if not results:
            return None
        
        binding = results[0]
        
        label = binding.get('entityLabel', {}).get('value', '')
        description = binding.get('entityDescription', {}).get('value', '')
        
        aliases_str = binding.get('aliases', {}).get('value', '')
        aliases = [alias.strip() for alias in aliases_str.split('|') if alias.strip()] if aliases_str else []
        
        types_str = binding.get('types', {}).get('value', '')
        types = [t.strip() for t in types_str.split('|') if t.strip()] if types_str else []
        
        # Determine entity type from types
        entity_type = EntityType.CONCEPT  # Default
        if any('human' in t.lower() or 'person' in t.lower() for t in types):
            entity_type = EntityType.PERSON
        elif any('place' in t.lower() or 'location' in t.lower() or 'city' in t.lower() for t in types):
            entity_type = EntityType.PLACE
        elif any('organization' in t.lower() or 'institution' in t.lower() for t in types):
            entity_type = EntityType.ORGANIZATION
        
        match = WikidataMatch(
            wikidata_id=wikidata_id,
            label=label,
            description=description,
            confidence_level=ConfidenceLevel.VERY_HIGH,  # Direct lookup
            confidence_score=1.0,
            entity_type=entity_type,
            aliases=aliases,
            birth_date=binding.get('birthDate', {}).get('value'),
            death_date=binding.get('deathDate', {}).get('value'),
            inception_date=binding.get('inceptionDate', {}).get('value'),
            coordinates=binding.get('coords', {}).get('value'),
            country=binding.get('countryLabel', {}).get('value'),
            website=binding.get('website', {}).get('value'),
            viaf_id=binding.get('viafId', {}).get('value'),
            library_of_congress_id=binding.get('lcId', {}).get('value'),
            image_url=binding.get('image', {}).get('value'),
            commons_category=binding.get('commonsCategory', {}).get('value'),
            external_ids={'entity_types': types}
        )
        
        self._store_in_cache(cache_key, [match])
        return match
    
    def get_statistics(self) -> Dict[str, Union[int, float]]:
        """Get client usage statistics"""
        stats = self.stats.copy()
        if stats['queries_made'] > 0:
            stats['cache_hit_rate'] = stats['cache_hits'] / stats['queries_made']
            stats['success_rate'] = stats['successful_matches'] / stats['queries_made']
        else:
            stats['cache_hit_rate'] = 0.0
            stats['success_rate'] = 0.0
        
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
    
    print("Testing Cultural Heritage Wikidata Client...")
    print("=" * 50)
    
    # Test person search
    print("\n1. Testing Person Search:")
    person_results = client.search_persons("Emma B. Hodge")
    for result in person_results[:3]:
        print(f"  - {result.label} (Q{result.wikidata_id})")
        print(f"    Confidence: {result.confidence_level.value} ({result.confidence_score:.2f})")
        print(f"    Description: {result.description}")
        if result.birth_date:
            print(f"    Born: {result.birth_date}")
        if result.viaf_id:
            print(f"    VIAF: {result.viaf_id}")
        print()
    
    # Test place search
    print("2. Testing Place Search:")
    place_results = client.search_places("Minneapolis Institute of Art")
    for result in place_results[:3]:
        print(f"  - {result.label} (Q{result.wikidata_id})")
        print(f"    Confidence: {result.confidence_level.value} ({result.confidence_score:.2f})")
        print(f"    Description: {result.description}")
        if result.website:
            print(f"    Website: {result.website}")
        print()
    
    # Test organization search
    print("3. Testing Organization Search:")
    org_results = client.search_organizations("Carleton College")
    for result in org_results[:3]:
        print(f"  - {result.label} (Q{result.wikidata_id})")
        print(f"    Confidence: {result.confidence_level.value} ({result.confidence_score:.2f})")
        print(f"    Description: {result.description}")
        print()
    
    # Show statistics
    print("4. Client Statistics:")
    stats = client.get_statistics()
    for key, value in stats.items():
        print(f"  - {key}: {value}")