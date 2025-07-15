#!/usr/bin/env python3
"""
Test script to verify API endpoints are working
Save as test_api_endpoints.py and run while your Flask app is running
"""

import requests
import json

BASE_URL = "http://localhost:5000"

def test_api_endpoints():
    """Test the API endpoints that JavaScript uses"""
    
    print("🧪 Testing API Endpoints...")
    print("=" * 50)
    
    # Test system status (should always work)
    try:
        response = requests.get(f"{BASE_URL}/api/system_status")
        if response.status_code == 200:
            print("✅ /api/system_status - OK")
            data = response.json()
            print(f"   Background jobs available: {data.get('background_jobs_available', False)}")
        else:
            print(f"❌ /api/system_status - Error {response.status_code}")
    except Exception as e:
        print(f"❌ /api/system_status - Connection failed: {e}")
        return False
    
    # Test jobs metrics
    try:
        response = requests.get(f"{BASE_URL}/api/jobs/metrics")
        if response.status_code == 200:
            print("✅ /api/jobs/metrics - OK")
            data = response.json()
            print(f"   Total jobs: {data.get('total', 0)}")
        else:
            print(f"❌ /api/jobs/metrics - Error {response.status_code}")
    except Exception as e:
        print(f"❌ /api/jobs/metrics - {e}")
    
    # Get a test job ID if any jobs exist
    try:
        from app.database import JobManager
        jobs = JobManager.get_all_jobs()
        if jobs:
            test_job_id = jobs[0]['id']
            print(f"\n🔍 Testing with job ID: {test_job_id}")
            
            # Test job status
            response = requests.get(f"{BASE_URL}/api/jobs/{test_job_id}/status")
            if response.status_code == 200:
                print("✅ /api/jobs/{job_id}/status - OK")
                data = response.json()
                print(f"   Job status: {data.get('status', 'unknown')}")
            else:
                print(f"❌ /api/jobs/{test_job_id}/status - Error {response.status_code}")
                
            # Test job progress
            response = requests.get(f"{BASE_URL}/api/jobs/{test_job_id}/progress")
            if response.status_code == 200:
                print("✅ /api/jobs/{job_id}/progress - OK")
            else:
                print(f"❌ /api/jobs/{test_job_id}/progress - Error {response.status_code}")
        else:
            print("\n⚠️  No jobs found to test job-specific endpoints")
            print("   Upload a CSV file first to create a test job")
            
    except Exception as e:
        print(f"❌ Error testing job endpoints: {e}")
    
    print("\n" + "=" * 50)
    print("✅ API endpoint test completed!")
    print("\nIf you see errors:")
    print("1. Make sure your Flask app is running on http://localhost:5000")
    print("2. Check that the API routes are properly added to api.py")
    print("3. Restart your Flask app after making changes")

if __name__ == "__main__":
    test_api_endpoints()