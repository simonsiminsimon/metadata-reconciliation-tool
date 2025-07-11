"""
External data sources API client for querying Wikidata, VIAF, and Getty vocabularies.
Provides functions for person/place lookups, author name reconciliation, and subject terms.
"""

import requests
import time
import logging
from typing import List, Dict, Any, Optional, Union
from urllib.parse import quote
import json
from dataclasses import dataclass
from enum import Enum
import re

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ConfidenceLevel(Enum):
    """Confidence levels for matching results"""
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


@dataclass
class MatchResult:
    """Standard structure for API match results"""
    id: str
    name: str
    description: Optional[str]
    confidence: ConfidenceLevel
    score: float
    source: str
    additional_info: Dict[str, Any]


class RateLimiter:
    """Simple rate limiter for API calls"""
    
    def __init__(self, calls_per_second: float = 1.0):
        self.calls_per_second = calls_per_second
        self.last_call_time = 0
    
    def wait_if_needed(self):
        """Wait if needed to respect rate limits"""
        current_time = time.time()
        time_since_last_call = current_time - self.last_call_time
        min_interval = 1.0 / self.calls_per_second
        
        if time_since_last_call < min_interval:
            sleep_time = min_interval - time_since_last_call
            time.sleep(sleep_time)
        
        self.last_call_time = time.time()


class WikidataClient:
    """Client for querying Wikidata SPARQL endpoint"""
    
    def __init__(self, rate_limit: float = 1.0):
        self.endpoint = "https://query.wikidata.org/sparql"
        self.rate_limiter = RateLimiter(rate_limit)
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'DataReconciliation/1.0 (https://example.com/contact)'
        })
    
    def _calculate_confidence(self, search_term: str, result_name: str, 
                            has_description: bool = False) -> tuple[ConfidenceLevel, float]:
        """Calculate confidence score for a match"""
        search_lower = search_term.lower().strip()
        result_lower = result_name.lower().strip()
        
        # Exact match
        if search_lower == result_lower:
            return ConfidenceLevel.HIGH, 0.95
        
        # Starts with search term
        if result_lower.startswith(search_lower):
            score = 0.85 if has_description else 0.75
            return ConfidenceLevel.HIGH, score
        
        # Contains search term
        if search_lower in result_lower:
            score = 0.70 if has_description else 0.60
            return ConfidenceLevel.MEDIUM, score
        
        # Fuzzy matching for similar terms
        words_match = len(set(search_lower.split()) & set(result_lower.split()))
        total_words = len(set(search_lower.split()) | set(result_lower.split()))
        
        if total_words > 0:
            similarity = words_match / total_words
            if similarity > 0.5:
                score = 0.50 + (similarity - 0.5) * 0.4
                return ConfidenceLevel.MEDIUM if score > 0.6 else ConfidenceLevel.LOW, score
        
        return ConfidenceLevel.LOW, 0.3
    
    def search_persons(self, search_term: str, limit: int = 10) -> List[MatchResult]:
        """Search for persons in Wikidata"""
        self.rate_limiter.wait_if_needed()
        
        # SPARQL query for persons
        query = f"""
        SELECT ?person ?personLabel ?personDescription ?birthDate ?deathDate ?occupationLabel WHERE {{
          ?person wdt:P31 wd:Q5 .  # Instance of human
          ?person rdfs:label ?personLabel .
          OPTIONAL {{ ?person schema:description ?personDescription . }}
          OPTIONAL {{ ?person wdt:P569 ?birthDate . }}
          OPTIONAL {{ ?person wdt:P570 ?deathDate . }}
          OPTIONAL {{ ?person wdt:P106 ?occupation . ?occupation rdfs:label ?occupationLabel . }}
          FILTER(LANG(?personLabel) = "en")
          FILTER(LANG(?personDescription) = "en")
          FILTER(LANG(?occupationLabel) = "en")
          FILTER(CONTAINS(LCASE(?personLabel), LCASE("{search_term}")))
        }}
        LIMIT {limit}
        """
        
        try:
            response = self.session.get(
                self.endpoint,
                params={
                    'query': query,
                    'format': 'json'
                },
                timeout=30
            )
            response.raise_for_status()
            
            data = response.json()
            results = []
            
            for binding in data['results']['bindings']:
                person_id = binding['person']['value'].split('/')[-1]
                name = binding['personLabel']['value']
                description = binding.get('personDescription', {}).get('value', '')
                
                # Build additional info
                additional_info = {}
                if 'birthDate' in binding:
                    additional_info['birth_date'] = binding['birthDate']['value']
                if 'deathDate' in binding:
                    additional_info['death_date'] = binding['deathDate']['value']
                if 'occupationLabel' in binding:
                    additional_info['occupation'] = binding['occupationLabel']['value']
                
                confidence, score = self._calculate_confidence(
                    search_term, name, bool(description)
                )
                
                results.append(MatchResult(
                    id=person_id,
                    name=name,
                    description=description,
                    confidence=confidence,
                    score=score,
                    source='wikidata',
                    additional_info=additional_info
                ))
            
            # Sort by confidence score
            results.sort(key=lambda x: x.score, reverse=True)
            return results
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error querying Wikidata for persons: {e}")
            return []
        except Exception as e:
            logger.error(f"Unexpected error in Wikidata person search: {e}")
            return []
    
    def search_places(self, search_term: str, limit: int = 10) -> List[MatchResult]:
        """Search for places in Wikidata"""
        self.rate_limiter.wait_if_needed()
        
        # SPARQL query for places
        query = f"""
        SELECT ?place ?placeLabel ?placeDescription ?countryLabel ?coordinateLocation WHERE {{
          ?place wdt:P31/wdt:P279* wd:Q2221906 .  # Geographic location
          ?place rdfs:label ?placeLabel .
          OPTIONAL {{ ?place schema:description ?placeDescription . }}
          OPTIONAL {{ ?place wdt:P17 ?country . ?country rdfs:label ?countryLabel . }}
          OPTIONAL {{ ?place wdt:P625 ?coordinateLocation . }}
          FILTER(LANG(?placeLabel) = "en")
          FILTER(LANG(?placeDescription) = "en")
          FILTER(LANG(?countryLabel) = "en")
          FILTER(CONTAINS(LCASE(?placeLabel), LCASE("{search_term}")))
        }}
        LIMIT {limit}
        """
        
        try:
            response = self.session.get(
                self.endpoint,
                params={
                    'query': query,
                    'format': 'json'
                },
                timeout=30
            )
            response.raise_for_status()
            
            data = response.json()
            results = []
            
            for binding in data['results']['bindings']:
                place_id = binding['place']['value'].split('/')[-1]
                name = binding['placeLabel']['value']
                description = binding.get('placeDescription', {}).get('value', '')
                
                # Build additional info
                additional_info = {}
                if 'countryLabel' in binding:
                    additional_info['country'] = binding['countryLabel']['value']
                if 'coordinateLocation' in binding:
                    additional_info['coordinates'] = binding['coordinateLocation']['value']
                
                confidence, score = self._calculate_confidence(
                    search_term, name, bool(description)
                )
                
                results.append(MatchResult(
                    id=place_id,
                    name=name,
                    description=description,
                    confidence=confidence,
                    score=score,
                    source='wikidata',
                    additional_info=additional_info
                ))
            
            # Sort by confidence score
            results.sort(key=lambda x: x.score, reverse=True)
            return results
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error querying Wikidata for places: {e}")
            return []
        except Exception as e:
            logger.error(f"Unexpected error in Wikidata place search: {e}")
            return []


class VIAFClient:
    """Client for querying VIAF API for author name reconciliation"""
    
    def __init__(self, rate_limit: float = 2.0):
        self.base_url = "https://viaf.org/viaf"
        self.rate_limiter = RateLimiter(rate_limit)
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'DataReconciliation/1.0 (https://example.com/contact)'
        })
    
    def _calculate_confidence(self, search_term: str, result_name: str, 
                            source_count: int = 1) -> tuple[ConfidenceLevel, float]:
        """Calculate confidence score for a VIAF match"""
        search_lower = search_term.lower().strip()
        result_lower = result_name.lower().strip()
        
        # Boost score based on number of sources
        source_boost = min(source_count / 10.0, 0.2)
        
        # Exact match
        if search_lower == result_lower:
            return ConfidenceLevel.HIGH, min(0.95 + source_boost, 1.0)
        
        # Very close match (accounting for middle initials, etc.)
        if self._names_are_similar(search_lower, result_lower):
            return ConfidenceLevel.HIGH, min(0.85 + source_boost, 1.0)
        
        # Contains search term
        if search_lower in result_lower or result_lower in search_lower:
            score = min(0.70 + source_boost, 1.0)
            return ConfidenceLevel.MEDIUM, score
        
        # Fuzzy matching for author names
        words_match = len(set(search_lower.split()) & set(result_lower.split()))
        total_words = len(set(search_lower.split()) | set(result_lower.split()))
        
        if total_words > 0:
            similarity = words_match / total_words
            if similarity > 0.5:
                score = min(0.50 + (similarity - 0.5) * 0.4 + source_boost, 1.0)
                return ConfidenceLevel.MEDIUM if score > 0.6 else ConfidenceLevel.LOW, score
        
        return ConfidenceLevel.LOW, 0.3
    
    def _names_are_similar(self, name1: str, name2: str) -> bool:
        """Check if two names are similar (handles initials, etc.)"""
        # Remove common title words
        titles = {'mr', 'mrs', 'ms', 'dr', 'prof', 'sir', 'dame'}
        
        def clean_name(name):
            words = name.split()
            return ' '.join(word for word in words if word not in titles)
        
        clean1 = clean_name(name1)
        clean2 = clean_name(name2)
        
        # Check if one is a subset of the other (for initials)
        words1 = set(clean1.split())
        words2 = set(clean2.split())
        
        return words1.issubset(words2) or words2.issubset(words1)
    
    def search_authors(self, search_term: str, limit: int = 10) -> List[MatchResult]:
        """Search for authors in VIAF"""
        self.rate_limiter.wait_if_needed()
        
        try:
            # Use VIAF AutoSuggest API
            response = self.session.get(
                f"{self.base_url}/AutoSuggest",
                params={
                    'query': search_term,
                    'callback': 'jsonp'  # We'll clean this up
                },
                timeout=30
            )
            response.raise_for_status()
            
            # Clean up JSONP response
            json_text = response.text
            if json_text.startswith('jsonp('):
                json_text = json_text[6:-1]  # Remove 'jsonp(' and ')'
            
            data = json.loads(json_text)
            results = []
            
            for item in data.get('result', [])[:limit]:
                viaf_id = item.get('viafid', '')
                display_form = item.get('displayForm', '')
                
                if not viaf_id or not display_form:
                    continue
                
                # Get additional details
                additional_info = {
                    'record_id': item.get('recordID', ''),
                    'source_count': len(item.get('source', [])),
                    'sources': item.get('source', [])
                }
                
                confidence, score = self._calculate_confidence(
                    search_term, display_form, additional_info['source_count']
                )
                
                results.append(MatchResult(
                    id=viaf_id,
                    name=display_form,
                    description=f"VIAF record with {additional_info['source_count']} sources",
                    confidence=confidence,
                    score=score,
                    source='viaf',
                    additional_info=additional_info
                ))
            
            # Sort by confidence score
            results.sort(key=lambda x: x.score, reverse=True)
            return results
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error querying VIAF: {e}")
            return []
        except Exception as e:
            logger.error(f"Unexpected error in VIAF search: {e}")
            return []


class GettyClient:
    """Client for querying Getty vocabularies for subject terms"""
    
    def __init__(self, rate_limit: float = 2.0):
        self.base_url = "http://vocab.getty.edu"
        self.rate_limiter = RateLimiter(rate_limit)
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'DataReconciliation/1.0 (https://example.com/contact)',
            'Accept': 'application/json'
        })
    
    def _calculate_confidence(self, search_term: str, result_name: str, 
                            result_type: str = '') -> tuple[ConfidenceLevel, float]:
        """Calculate confidence score for a Getty match"""
        search_lower = search_term.lower().strip()
        result_lower = result_name.lower().strip()
        
        # Boost for preferred terms
        type_boost = 0.1 if 'preferred' in result_type.lower() else 0.0
        
        # Exact match
        if search_lower == result_lower:
            return ConfidenceLevel.HIGH, min(0.95 + type_boost, 1.0)
        
        # Starts with search term
        if result_lower.startswith(search_lower):
            score = min(0.85 + type_boost, 1.0)
            return ConfidenceLevel.HIGH, score
        
        # Contains search term
        if search_lower in result_lower:
            score = min(0.70 + type_boost, 1.0)
            return ConfidenceLevel.MEDIUM, score
        
        # Word-based matching
        words_match = len(set(search_lower.split()) & set(result_lower.split()))
        total_words = len(set(search_lower.split()) | set(result_lower.split()))
        
        if total_words > 0:
            similarity = words_match / total_words
            if similarity > 0.5:
                score = min(0.50 + (similarity - 0.5) * 0.4 + type_boost, 1.0)
                return ConfidenceLevel.MEDIUM if score > 0.6 else ConfidenceLevel.LOW, score
        
        return ConfidenceLevel.LOW, 0.3
    
    def search_aat_terms(self, search_term: str, limit: int = 10) -> List[MatchResult]:
        """Search Art & Architecture Thesaurus (AAT) terms"""
        return self._search_vocabulary('aat', search_term, limit)
    
    def search_tgn_places(self, search_term: str, limit: int = 10) -> List[MatchResult]:
        """Search Thesaurus of Geographic Names (TGN) places"""
        return self._search_vocabulary('tgn', search_term, limit)
    
    def search_ulan_agents(self, search_term: str, limit: int = 10) -> List[MatchResult]:
        """Search Union List of Artist Names (ULAN) agents"""
        return self._search_vocabulary('ulan', search_term, limit)
    
    def _search_vocabulary(self, vocab: str, search_term: str, limit: int) -> List[MatchResult]:
        """Generic search method for Getty vocabularies"""
        self.rate_limiter.wait_if_needed()
        
        try:
            # Use Getty's SPARQL endpoint
            sparql_query = f"""
            SELECT ?subject ?prefLabel ?scopeNote ?broader ?type WHERE {{
              ?subject a skos:Concept ;
                       skos:inScheme <{self.base_url}/{vocab}/> ;
                       skos:prefLabel ?prefLabel .
              OPTIONAL {{ ?subject skos:scopeNote ?scopeNote . }}
              OPTIONAL {{ ?subject skos:broader ?broader . }}
              OPTIONAL {{ ?subject a ?type . }}
              FILTER(LANG(?prefLabel) = "en")
              FILTER(CONTAINS(LCASE(?prefLabel), LCASE("{search_term}")))
            }}
            LIMIT {limit}
            """
            
            response = self.session.get(
                f"{self.base_url}/sparql",
                params={
                    'query': sparql_query,
                    'format': 'json'
                },
                timeout=30
            )
            response.raise_for_status()
            
            data = response.json()
            results = []
            
            for binding in data['results']['bindings']:
                subject_uri = binding['subject']['value']
                getty_id = subject_uri.split('/')[-1]
                pref_label = binding['prefLabel']['value']
                scope_note = binding.get('scopeNote', {}).get('value', '')
                
                # Build additional info
                additional_info = {
                    'uri': subject_uri,
                    'vocabulary': vocab.upper(),
                    'scope_note': scope_note
                }
                
                if 'broader' in binding:
                    additional_info['broader_term'] = binding['broader']['value']
                if 'type' in binding:
                    additional_info['type'] = binding['type']['value']
                
                confidence, score = self._calculate_confidence(
                    search_term, pref_label, additional_info.get('type', '')
                )
                
                results.append(MatchResult(
                    id=getty_id,
                    name=pref_label,
                    description=scope_note,
                    confidence=confidence,
                    score=score,
                    source=f'getty_{vocab}',
                    additional_info=additional_info
                ))
            
            # Sort by confidence score
            results.sort(key=lambda x: x.score, reverse=True)
            return results
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error querying Getty {vocab.upper()}: {e}")
            return []
        except Exception as e:
            logger.error(f"Unexpected error in Getty {vocab.upper()} search: {e}")
            return []


# Convenience functions for easy usage
def search_wikidata_persons(search_term: str, limit: int = 10) -> List[MatchResult]:
    """Search for persons in Wikidata"""
    client = WikidataClient()
    return client.search_persons(search_term, limit)


def search_wikidata_places(search_term: str, limit: int = 10) -> List[MatchResult]:
    """Search for places in Wikidata"""
    client = WikidataClient()
    return client.search_places(search_term, limit)


def search_viaf_authors(search_term: str, limit: int = 10) -> List[MatchResult]:
    """Search for authors in VIAF"""
    client = VIAFClient()
    return client.search_authors(search_term, limit)


def search_getty_aat(search_term: str, limit: int = 10) -> List[MatchResult]:
    """Search Getty AAT terms"""
    client = GettyClient()
    return client.search_aat_terms(search_term, limit)


def search_getty_tgn(search_term: str, limit: int = 10) -> List[MatchResult]:
    """Search Getty TGN places"""
    client = GettyClient()
    return client.search_tgn_places(search_term, limit)


def search_getty_ulan(search_term: str, limit: int = 10) -> List[MatchResult]:
    """Search Getty ULAN agents"""
    client = GettyClient()
    return client.search_ulan_agents(search_term, limit)


# Example usage
if __name__ == "__main__":
    # Test searches
    print("Testing Wikidata person search...")
    persons = search_wikidata_persons("Shakespeare", limit=3)
    for person in persons:
        print(f"  {person.name} (ID: {person.id}, Confidence: {person.confidence.value}, Score: {person.score:.2f})")
    
    print("\nTesting VIAF author search...")
    authors = search_viaf_authors("Jane Austen", limit=3)
    for author in authors:
        print(f"  {author.name} (ID: {author.id}, Confidence: {author.confidence.value}, Score: {author.score:.2f})")
    
    print("\nTesting Getty AAT search...")
    aat_terms = search_getty_aat("painting", limit=3)
    for term in aat_terms:
        print(f"  {term.name} (ID: {term.id}, Confidence: {term.confidence.value}, Score: {term.score:.2f})")