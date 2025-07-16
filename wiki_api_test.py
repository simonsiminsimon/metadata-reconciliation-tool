import requests

# Test basic Wikidata API
response = requests.get(
    "https://www.wikidata.org/w/api.php",
    params={
        'action': 'wbsearchentities',
        'search': 'William Shakespeare',
        'language': 'en',
        'format': 'json',
        'limit': 5
    }
)

print(f"Status: {response.status_code}")
print(f"Results: {len(response.json().get('search', []))}")