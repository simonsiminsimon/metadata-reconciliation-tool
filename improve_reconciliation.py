# File: improve_reconciliation.py
# Improvements to enhance your reconciliation results

from app.services.reconciliation_engine import ReconciliationEngine, EntityType
from app.services.data_sources import WikidataClient, VIAFClient
from app.database import JobManager, ResultsManager
import pandas as pd
from typing import List

class ImprovedReconciliationEngine(ReconciliationEngine):
    """Enhanced reconciliation engine with better entity type detection and matching"""
    
    def __init__(self, cache_size: int = 1000):
        super().__init__(cache_size)
        
        # Enhanced patterns for better entity type detection
        self.organization_patterns = [
            # Educational institutions
            r'.*college.*', r'.*university.*', r'.*school.*', r'.*institute.*',
            # Cultural institutions  
            r'.*museum.*', r'.*library.*', r'.*archive.*', r'.*society.*',
            # Performance venues
            r'.*opera house.*', r'.*theater.*', r'.*theatre.*', r'.*hall.*',
            # Business entities
            r'.*company.*', r'.*corporation.*', r'.*inc\..*', r'.*llc.*',
            # Government/Organizations
            r'.*league.*', r'.*association.*', r'.*department.*', r'.*office.*',
            # Religious institutions
            r'.*church.*', r'.*cathedral.*', r'.*chapel.*'
        ]
        
        self.place_patterns = [
            r'.*building.*', r'.*house.*', r'.*hall.*', r'.*center.*',
            r'.*venue.*', r'.*location.*', r'.*site.*'
        ]
    
    def _infer_entity_type(self, entity_name: str) -> EntityType:
        """Improved entity type inference"""
        name_lower = entity_name.lower()
        
        # Check for organization patterns first (more specific)
        for pattern in self.organization_patterns:
            if self._matches_pattern(pattern, name_lower):
                return EntityType.ORGANIZATION
        
        # Check for place patterns
        for pattern in self.place_patterns:
            if self._matches_pattern(pattern, name_lower):
                return EntityType.PLACE
        
        # Check for person indicators
        if any(title in name_lower for title in ['dr.', 'prof.', 'mr.', 'mrs.', 'ms.']):
            return EntityType.PERSON
        
        # Look for personal name patterns (Lastname, Firstname)
        if ',' in entity_name and len(entity_name.split(',')) == 2:
            parts = entity_name.split(',')
            if all(part.strip().replace('.', '').replace(' ', '').isalpha() for part in parts):
                return EntityType.PERSON
        
        # Default logic based on content
        words = name_lower.split()
        
        # If it has organizational keywords, classify as organization
        org_keywords = ['league', 'society', 'association', 'institute', 'college', 
                       'company', 'corporation', 'office', 'department']
        if any(keyword in name_lower for keyword in org_keywords):
            return EntityType.ORGANIZATION
        
        # If it's all caps or has institutional formatting, likely organization
        if entity_name.isupper() or any(char in entity_name for char in ['&', 'Inc', 'LLC']):
            return EntityType.ORGANIZATION
        
        # Default to person for unclear cases
        return EntityType.PERSON
    
    def _matches_pattern(self, pattern: str, text: str) -> bool:
        """Check if text matches a regex pattern"""
        import re
        try:
            return bool(re.search(pattern, text, re.IGNORECASE))
        except:
            return False
    
    def create_entities_from_dataframe(self, df: pd.DataFrame, entity_column: str,
                                     type_column: str = None, 
                                     context_columns: List[str] = None) -> List:
        """Enhanced entity creation with deduplication"""
        
        # First, create entities using parent method
        entities = super().create_entities_from_dataframe(
            df, entity_column, type_column, context_columns
        )
        
        # Deduplicate entities by name and type
        unique_entities = {}
        for entity in entities:
            key = (entity.name.strip(), entity.entity_type)
            if key not in unique_entities:
                unique_entities[key] = entity
            else:
                # Merge context from duplicates
                existing_entity = unique_entities[key]
                for k, v in entity.context.items():
                    if k not in existing_entity.context:
                        existing_entity.context[k] = v
        
        print(f"🔄 Deduplicated: {len(entities)} → {len(unique_entities)} entities")
        
        return list(unique_entities.values())


class EnhancedWikidataQueries:
    """Enhanced Wikidata queries for better organization matching"""
    
    @staticmethod
    def search_organizations(client: WikidataClient, search_term: str, limit: int = 10):
        """Specialized query for organizations"""
        
        # Clean up common organizational suffixes for better matching
        clean_term = search_term.replace(' (organization)', '').replace(' Inc.', '').strip()
        
        query = f"""
        SELECT ?org ?orgLabel ?orgDescription ?locationLabel ?foundedDate ?websiteLabel WHERE {{
          {{
            ?org wdt:P31/wdt:P279* wd:Q43229 .  # organization
          }} UNION {{
            ?org wdt:P31/wdt:P279* wd:Q4830453 .  # business
          }} UNION {{
            ?org wdt:P31/wdt:P279* wd:Q2659904 .  # government organization
          }} UNION {{
            ?org wdt:P31/wdt:P279* wd:Q15936437 . # cultural institution
          }}
          
          ?org rdfs:label ?orgLabel .
          OPTIONAL {{ ?org schema:description ?orgDescription . }}
          OPTIONAL {{ ?org wdt:P131 ?location . ?location rdfs:label ?locationLabel . }}
          OPTIONAL {{ ?org wdt:P571 ?foundedDate . }}
          OPTIONAL {{ ?org wdt:P856 ?website . ?website rdfs:label ?websiteLabel . }}
          
          FILTER(LANG(?orgLabel) = "en")
          FILTER(LANG(?orgDescription) = "en")
          FILTER(LANG(?locationLabel) = "en")
          FILTER(CONTAINS(LCASE(?orgLabel), LCASE("{clean_term}")))
        }}
        LIMIT {limit}
        """
        
        # Use the existing client's session and endpoint
        try:
            response = client.session.get(
                client.endpoint,
                params={'query': query, 'format': 'json'},
                timeout=30
            )
            response.raise_for_status()
            
            data = response.json()
            matches = []
            
            for binding in data['results']['bindings']:
                org_id = binding['org']['value'].split('/')[-1]
                name = binding['orgLabel']['value']
                description = binding.get('orgDescription', {}).get('value', '')
                
                # Calculate enhanced confidence for organizations
                confidence, score = client._calculate_confidence(
                    search_term, name, bool(description)
                )
                
                # Boost score for organizations (they're often institutional matches)
                score = min(score + 0.1, 1.0)
                if score >= 0.8:
                    confidence = client._calculate_confidence(search_term, name, True)[0]
                
                additional_info = {}
                if 'locationLabel' in binding:
                    additional_info['location'] = binding['locationLabel']['value']
                if 'foundedDate' in binding:
                    additional_info['founded'] = binding['foundedDate']['value']
                
                from app.services.data_sources import MatchResult
                matches.append(MatchResult(
                    id=org_id,
                    name=name,
                    description=description,
                    confidence=confidence,
                    score=score,
                    source='wikidata',
                    additional_info=additional_info
                ))
            
            matches.sort(key=lambda x: x.score, reverse=True)
            return matches
            
        except Exception as e:
            print(f"Enhanced organization query failed: {e}")
            return []


def reprocess_failed_job(job_id: str):
    """Reprocess a job with improved reconciliation"""
    
    print(f"🔄 Reprocessing job {job_id} with improvements...")
    
    # Get the original job
    job = JobManager.get_job(job_id)
    if not job:
        print("❌ Job not found")
        return
    
    print(f"📄 Original job: {job['filename']}")
    print(f"📊 Original results: {job['successful_matches']}/{job['total_entities']} matches")
    
    try:
        # Read the original CSV
        df = pd.read_csv(job['filepath'])
        
        # Use improved reconciliation engine
        engine = ImprovedReconciliationEngine()
        
        # Create entities with improved classification
        entities = engine.create_entities_from_dataframe(
            df,
            entity_column=job['entity_column'],
            type_column=job.get('type_column'),
            context_columns=job.get('context_columns', [])
        )
        
        print(f"🎯 Reprocessing {len(entities)} unique entities...")
        
        # Enhanced processing for organizations
        enhanced_results = []
        wikidata_client = WikidataClient()
        
        for entity in entities:
            print(f"🔍 Processing: {entity.name} ({entity.entity_type.value})")
            
            # Use enhanced queries for organizations
            if entity.entity_type == EntityType.ORGANIZATION:
                try:
                    org_matches = EnhancedWikidataQueries.search_organizations(
                        wikidata_client, entity.name, limit=5
                    )
                    if org_matches:
                        print(f"   ✅ Found {len(org_matches)} enhanced matches")
                        # You would create a ReconciliationResult here
                        # This is a simplified example
                    else:
                        print(f"   ⚠️  No enhanced matches found")
                except Exception as e:
                    print(f"   ❌ Enhanced query failed: {e}")
            
        print(f"🎉 Reprocessing complete!")
        
    except Exception as e:
        print(f"❌ Reprocessing failed: {e}")


def analyze_reconciliation_patterns(job_id: str):
    """Analyze patterns in reconciliation results to suggest improvements"""
    
    print(f"📊 Analyzing reconciliation patterns for job {job_id}...")
    
    results, total_count = ResultsManager.get_results(job_id, page=1, per_page=1000)
    
    if not results:
        print("❌ No results found")
        return
    
    # Analyze entity types
    type_stats = {}
    confidence_stats = {}
    source_stats = {}
    
    for result in results:
        entity_type = result['entity']['type']
        confidence = result['confidence']
        
        # Count entity types
        type_stats[entity_type] = type_stats.get(entity_type, 0) + 1
        
        # Count confidence levels
        confidence_stats[confidence] = confidence_stats.get(confidence, 0) + 1
        
        # Count sources
        for source in result['sources_queried']:
            source_stats[source] = source_stats.get(source, 0) + 1
    
    print(f"\n📈 Analysis Results:")
    print(f"Entity Types: {type_stats}")
    print(f"Confidence Levels: {confidence_stats}")
    print(f"Sources Queried: {source_stats}")
    
    # Suggestions
    print(f"\n💡 Suggestions:")
    
    if type_stats.get('person', 0) > type_stats.get('organization', 0) * 2:
        print("   🔄 Many entities classified as 'person' - consider improving type detection")
    
    if confidence_stats.get('low', 0) > confidence_stats.get('high', 0):
        print("   📈 Many low confidence matches - consider enhancing matching algorithms")
    
    if 'wikidata' in source_stats and source_stats['wikidata'] > 0:
        print("   ✅ Wikidata queries working - consider adding more specialized queries")


if __name__ == "__main__":
    # Test with your job ID
    job_id = "0ae85458-27e0-44e6-984a-11c54de94391"  # Your actual job ID
    
    print("🔍 Analyzing your reconciliation results...")
    analyze_reconciliation_patterns(job_id)
    
    print(f"\n🛠️  To reprocess with improvements:")
    print(f"   python improve_reconciliation.py")