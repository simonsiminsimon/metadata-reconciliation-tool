"""
Metadata Parser Service

This module provides functionality to parse CSV files and extract metadata
including person names, places, and subjects. It handles various CSV structures
and performs data cleaning operations.
"""

import pandas as pd
import re
from typing import Dict, List, Set, Optional, Tuple
from pathlib import Path
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class MetadataParser:
    """
    A flexible parser for extracting metadata from CSV files.
    
    Identifies columns containing person names, places, and subjects,
    then extracts and cleans unique entities from these columns.
    """
    
    def __init__(self):
        """Initialize the metadata parser with common patterns."""
        # Common column name patterns for different entity types
        self.person_patterns = [
            r'.*name.*', r'.*author.*', r'.*creator.*', r'.*contributor.*',
            r'.*person.*', r'.*people.*', r'.*staff.*', r'.*member.*',
            r'.*user.*', r'.*contact.*', r'.*responsible.*', r'.*owner.*'
        ]
        
        self.place_patterns = [
            r'.*location.*', r'.*place.*', r'.*city.*', r'.*state.*',
            r'.*country.*', r'.*region.*', r'.*address.*', r'.*venue.*',
            r'.*site.*', r'.*area.*', r'.*zone.*', r'.*territory.*', r'.*city or township.*',
            r'.*state or province.*', r'.*district.*', r'.*neighborhood.*', r'.*locality.*'
        ]
        
        self.subject_patterns = [
            r'.*subject.*', r'.*topic.*', r'.*category.*', r'.*theme.*',
            r'.*tag.*', r'.*keyword.*', r'.*genre.*', r'.*type.*',
            r'.*class.*', r'.*description.*', r'.*content.*', r'.*field.*'
        ]
        
        # Common separators for multi-value fields
        self.separators = [';', '|', ',', ' and ', ' & ', ' / ', '\n', '\t']
        
    def read_csv_flexible(self, file_path: str) -> pd.DataFrame:
        """
        Read CSV file with flexible encoding and delimiter detection.
        
        Args:
            file_path: Path to the CSV file
            
        Returns:
            DataFrame with the CSV data
        """
        encodings = ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']
        delimiters = [',', ';', '\t', '|']
        
        for encoding in encodings:
            for delimiter in delimiters:
                try:
                    df = pd.read_csv(file_path, encoding=encoding, delimiter=delimiter)
                    # Check if we got reasonable columns (not just one big column)
                    if len(df.columns) > 1 or (len(df.columns) == 1 and len(df) > 0):
                        logger.info(f"Successfully read CSV with encoding: {encoding}, delimiter: '{delimiter}'")
                        return df
                except Exception as e:
                    continue
        
        # Fallback: try pandas' default behavior
        try:
            df = pd.read_csv(file_path)
            logger.info("Successfully read CSV with default pandas settings")
            return df
        except Exception as e:
            logger.error(f"Failed to read CSV file: {e}")
            raise
    
    def classify_columns(self, df: pd.DataFrame) -> Dict[str, List[str]]:
        """
        Classify columns by their likely content type.
        
        Args:
            df: DataFrame to analyze
            
        Returns:
            Dictionary with column classifications
        """
        classifications = {
            'person': [],
            'place': [],
            'subject': [],
            'other': []
        }
        
        for column in df.columns:
            column_lower = column.lower().strip()
            
            # Check for person patterns
            if any(re.match(pattern, column_lower, re.IGNORECASE) for pattern in self.person_patterns):
                classifications['person'].append(column)
            # Check for place patterns
            elif any(re.match(pattern, column_lower, re.IGNORECASE) for pattern in self.place_patterns):
                classifications['place'].append(column)
            # Check for subject patterns
            elif any(re.match(pattern, column_lower, re.IGNORECASE) for pattern in self.subject_patterns):
                classifications['subject'].append(column)
            else:
                # Additional heuristic: check sample data
                sample_data = df[column].dropna().head(10).astype(str)
                if self._looks_like_person_names(sample_data):
                    classifications['person'].append(column)
                elif self._looks_like_places(sample_data):
                    classifications['place'].append(column)
                elif self._looks_like_subjects(sample_data):
                    classifications['subject'].append(column)
                else:
                    classifications['other'].append(column)
        
        return classifications
    
    def _looks_like_person_names(self, sample_data: pd.Series) -> bool:
        """Check if sample data looks like person names."""
        if len(sample_data) == 0:
            return False
        
        # Check for common name patterns
        name_indicators = 0
        for value in sample_data:
            value_str = str(value).strip()
            if not value_str:
                continue
            
            # Check for typical name patterns
            words = value_str.split()
            if len(words) >= 2:  # At least two words (first + last name)
                # Check for title patterns
                if any(word.lower() in ['mr', 'mrs', 'ms', 'dr', 'prof', 'sr', 'jr'] for word in words):
                    name_indicators += 1
                # Check for capitalized words (names are usually capitalized)
                elif all(word[0].isupper() for word in words if word):
                    name_indicators += 1

        return name_indicators >= len(sample_data) * 0.2  # 20% threshold

    def _looks_like_places(self, sample_data: pd.Series) -> bool:
        """Check if sample data looks like place names."""
        if len(sample_data) == 0:
            return False
        
        # Common place indicators
        place_indicators = ['city', 'state', 'country', 'street', 'avenue', 'road', 'county']
        place_suffixes = ['ton', 'ville', 'burg', 'field', 'ford', 'land', 'wood']
        
        matches = 0
        for value in sample_data:
            value_str = str(value).lower().strip()
            if any(indicator in value_str for indicator in place_indicators):
                matches += 1
            elif any(value_str.endswith(suffix) for suffix in place_suffixes):
                matches += 1
        
        return matches >= len(sample_data) * 0.2  # 20% threshold
    
    def _looks_like_subjects(self, sample_data: pd.Series) -> bool:
        """Check if sample data looks like subject/topic data."""
        if len(sample_data) == 0:
            return False
        
        # Check for common subject patterns
        subject_indicators = 0
        for value in sample_data:
            value_str = str(value).strip()
            if not value_str:
                continue
            
            # Check for multiple values separated by common delimiters
            for sep in self.separators:
                if sep in value_str:
                    subject_indicators += 1
                    break
            
            # Check for academic/topical terms
            if any(term in value_str.lower() for term in ['history', 'science', 'art', 'literature', 'music', 'education']):
                subject_indicators += 1
        
        return subject_indicators >= len(sample_data) * 0.2  # 20% threshold
    
    def extract_entities(self, df: pd.DataFrame, columns: List[str]) -> Set[str]:
        """
        Extract unique entities from specified columns.
        
        Args:
            df: DataFrame containing the data
            columns: List of column names to extract entities from
            
        Returns:
            Set of unique entities
        """
        entities = set()
        
        for column in columns:
            if column not in df.columns:
                logger.warning(f"Column '{column}' not found in DataFrame")
                continue
            
            # Get all non-null values from the column
            values = df[column].dropna().astype(str)
            
            for value in values:
                # Clean the value
                cleaned_value = self._clean_value(value)
                if not cleaned_value:
                    continue
                
                # Split by common separators to handle multi-value fields
                split_values = self._split_multi_value(cleaned_value)
                
                for split_value in split_values:
                    final_value = self._clean_value(split_value)
                    if final_value and len(final_value) > 1:  # Avoid single characters
                        entities.add(final_value)
        
        return entities
    
    def _clean_value(self, value: str) -> str:
        """
        Clean individual values by removing extra whitespace and common issues.
        
        Args:
            value: String value to clean
            
        Returns:
            Cleaned string value
        """
        if pd.isna(value) or value is None:
            return ""
        
        # Convert to string and strip whitespace
        cleaned = str(value).strip()
        
        # Remove extra whitespace
        cleaned = re.sub(r'\s+', ' ', cleaned)
        
        # Remove common prefixes/suffixes that might be metadata
        cleaned = re.sub(r'^[\[\(]|[\]\)]$', '', cleaned)
        
        # Remove quotes if they wrap the entire value
        if (cleaned.startswith('"') and cleaned.endswith('"')) or \
           (cleaned.startswith("'") and cleaned.endswith("'")):
            cleaned = cleaned[1:-1]
        
        return cleaned.strip()
    
    def _split_multi_value(self, value: str) -> List[str]:
        """
        Split multi-value fields by common separators.
        
        Args:
            value: String that might contain multiple values
            
        Returns:
            List of individual values
        """
        # Try each separator
        for separator in self.separators:
            if separator in value:
                return [v.strip() for v in value.split(separator) if v.strip()]
        
        # If no separator found, return as single value
        return [value]
    
    def remove_duplicates_and_empty(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Remove duplicate rows and handle empty values.
        
        Args:
            df: DataFrame to clean
            
        Returns:
            Cleaned DataFrame
        """
        # Remove completely empty rows
        df = df.dropna(how='all')
        
        # Remove duplicate rows
        df = df.drop_duplicates()
        
        # Reset index
        df = df.reset_index(drop=True)
        
        logger.info(f"Data cleaning complete. Final shape: {df.shape}")
        return df
    
    def parse_csv_metadata(self, file_path: str) -> Dict:
        """
        Main method to parse CSV and extract all metadata.
        
        Args:
            file_path: Path to the CSV file
            
        Returns:
            Dictionary containing all extracted metadata
        """
        try:
            # Read the CSV file
            df = self.read_csv_flexible(file_path)
            logger.info(f"Loaded CSV with shape: {df.shape}")
            
            # Clean the data
            df = self.remove_duplicates_and_empty(df)
            
            # Classify columns
            classifications = self.classify_columns(df)
            logger.info(f"Column classifications: {classifications}")
            
            # Extract entities for each type
            metadata = {
                'file_info': {
                    'filename': Path(file_path).name,
                    'total_rows': len(df),
                    'total_columns': len(df.columns),
                    'columns': list(df.columns)
                },
                'column_classifications': classifications,
                'entities': {}
            }
            
            # Extract entities for each classification
            for entity_type, columns in classifications.items():
                if columns and entity_type != 'other':
                    entities = self.extract_entities(df, columns)
                    metadata['entities'][entity_type] = sorted(list(entities))
                    logger.info(f"Extracted {len(entities)} unique {entity_type} entities")
            
            # Add summary statistics
            metadata['summary'] = {
                'total_persons': len(metadata['entities'].get('person', [])),
                'total_places': len(metadata['entities'].get('place', [])),
                'total_subjects': len(metadata['entities'].get('subject', []))
            }
            
            return metadata
            
        except Exception as e:
            logger.error(f"Error parsing CSV metadata: {e}")
            raise


def main():
    """Example usage of the MetadataParser."""
    parser = MetadataParser()
    
    # Example usage (uncomment to test with actual file)
    # metadata = parser.parse_csv_metadata('path/to/your/file.csv')
    # print(f"Found {metadata['summary']['total_persons']} persons")
    # print(f"Found {metadata['summary']['total_places']} places")
    # print(f"Found {metadata['summary']['total_subjects']} subjects")


if __name__ == "__main__":
    main()