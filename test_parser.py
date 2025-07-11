##
from app.services.metadata_parser import MetadataParser

parser = MetadataParser()
metadata = parser.parse_csv_metadata('data/input/page1test.csv')

print(f"Found {metadata['summary']['total_persons']} people")
print(f"Found {metadata['summary']['total_places']} places")
print(f"Found {metadata['summary']['total_subjects']} subjects")

