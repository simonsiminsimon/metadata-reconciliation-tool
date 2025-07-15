import requests
import json
import time

BASE_URL = "http://localhost:5000"

def test_endpoints():
    """Test all major endpoints"""
    endpoints = [
        ("/", "Home page"),
        ("/upload", "Upload page"),
        ("/jobs", "Jobs page"),
        ("/api/system_status", "System status API")
    ]
    
    for endpoint, name in endpoints:
        try:
            response = requests.get(f"{BASE_URL}{endpoint}")
            if response.status_code in [200, 302]:
                print(f"✅ {name}: OK")
            else:
                print(f"❌ {name}: Error {response.status_code}")
        except Exception as e:
            print(f"❌ {name}: {e}")

if __name__ == "__main__":
    test_endpoints()