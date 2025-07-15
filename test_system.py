#!/usr/bin/env python3
"""
System Test Script for Metadata Reconciliation Tool
Run this after implementing the fixes to verify everything works!

Usage: python test_system.py
"""

import os
import sys
import time
import json
import requests
import pandas as pd
from colorama import init, Fore, Style

# Initialize colorama for colored output
init(autoreset=True)

# Configuration
BASE_URL = "http://localhost:5000"  # Adjust if your app runs on different port
TEST_CSV = "test_metadata.csv"

def print_header(text):
    """Print a formatted header"""
    print(f"\n{Fore.CYAN}{'=' * 60}")
    print(f"{Fore.CYAN}{text.center(60)}")
    print(f"{Fore.CYAN}{'=' * 60}{Style.RESET_ALL}\n")

def print_success(text):
    """Print success message"""
    print(f"{Fore.GREEN}✅ {text}{Style.RESET_ALL}")

def print_error(text):
    """Print error message"""
    print(f"{Fore.RED}❌ {text}{Style.RESET_ALL}")

def print_info(text):
    """Print info message"""
    print(f"{Fore.YELLOW}ℹ️  {text}{Style.RESET_ALL}")

def create_test_csv():
    """Create a test CSV file"""
    print_info("Creating test CSV file...")
    
    data = {
        'creator_name': [
            'Emma Goldman',
            'Minneapolis Institute of Art',
            'Walker Art Center',
            'Frank Lloyd Wright',
            'Saint Paul Public Library'
        ],
        'entity_type': [
            'person',
            'organization',
            'organization',
            'person',
            'organization'
        ],
        'date_created': [
            '1910',
            '1915',
            '1927',
            '1935',
            '1882'
        ],
        'location': [
            'New York',
            'Minneapolis, Minnesota',
            'Minneapolis, Minnesota',
            'Wisconsin',
            'Saint Paul, Minnesota'
        ]
    }
    
    df = pd.DataFrame(data)
    df.to_csv(TEST_CSV, index=False)
    print_success(f"Created {TEST_CSV} with {len(df)} test records")
    return TEST_CSV

def test_server_running():
    """Test if the Flask server is running"""
    print_header("Testing Server Connection")
    
    try:
        response = requests.get(f"{BASE_URL}/", timeout=5)
        if response.status_code in [200, 302]:  # 302 is redirect
            print_success("Server is running!")
            return True
        else:
            print_error(f"Server returned status code: {response.status_code}")
            return False
    except requests.exceptions.ConnectionError:
        print_error("Cannot connect to server. Is it running?")
        print_info("Start the server with: python run.py")
        return False
    except Exception as e:
        print_error(f"Unexpected error: {e}")
        return False

def test_api_endpoints():
    """Test API endpoints"""
    print_header("Testing API Endpoints")
    
    endpoints = [
        ("/api/system_status", "System Status"),
        ("/api/stats", "Statistics")
    ]
    
    all_passed = True
    
    for endpoint, name in endpoints:
        try:
            response = requests.get(f"{BASE_URL}{endpoint}")
            if response.status_code == 200:
                data = response.json()
                print_success(f"{name} endpoint working")
                print(f"   Response: {json.dumps(data, indent=2)[:100]}...")
            else:
                print_error(f"{name} endpoint failed: {response.status_code}")
                all_passed = False
        except Exception as e:
            print_error(f"{name} endpoint error: {e}")
            all_passed = False
    
    return all_passed

def test_file_upload():
    """Test file upload functionality"""
    print_header("Testing File Upload")
    
    # Check if test CSV exists
    if not os.path.exists(TEST_CSV):
        create_test_csv()
    
    # Prepare the upload
    with open(TEST_CSV, 'rb') as f:
        files = {'file': (TEST_CSV, f, 'text/csv')}
        data = {
            'entity_column': 'creator_name',
            'type_column': 'entity_type',
            'context_columns': 'date_created,location',
            'confidence_threshold': '0.6',
            'data_sources': ['wikidata', 'viaf']
        }
        
        try:
            print_info("Uploading test file...")
            response = requests.post(
                f"{BASE_URL}/upload",
                files=files,
                data=data,
                allow_redirects=False
            )
            
            if response.status_code == 302:  # Redirect after successful upload
                print_success("File uploaded successfully!")
                
                # Extract job ID from redirect URL
                location = response.headers.get('Location', '')
                if '/processing/' in location:
                    job_id = location.split('/processing/')[-1]
                    print_success(f"Job created with ID: {job_id}")
                    return job_id
                else:
                    print_error("Upload succeeded but no job ID found")
                    return None
            else:
                print_error(f"Upload failed with status: {response.status_code}")
                print(f"Response: {response.text[:200]}")
                return None
                
        except Exception as e:
            print_error(f"Upload error: {e}")
            return None

def test_job_processing(job_id):
    """Test job processing status"""
    print_header("Testing Job Processing")
    
    if not job_id:
        print_error("No job ID provided")
        return False
    
    print_info(f"Monitoring job {job_id}...")
    
    # Poll job status
    max_attempts = 30  # 30 seconds timeout
    for i in range(max_attempts):
        try:
            response = requests.get(f"{BASE_URL}/api/job/{job_id}/status")
            if response.status_code == 200:
                data = response.json()
                status = data.get('status', 'unknown')
                progress = data.get('progress', 0)
                
                print(f"\r{Fore.YELLOW}Status: {status} | Progress: {progress}%{Style.RESET_ALL}", end='')
                
                if status == 'completed':
                    print()  # New line
                    print_success("Job completed successfully!")
                    print(f"   Total entities: {data.get('total_entities', 0)}")
                    print(f"   Successful matches: {data.get('successful_matches', 0)}")
                    print(f"   Match rate: {data.get('match_rate', 0)}%")
                    return True
                elif status == 'failed':
                    print()  # New line
                    print_error(f"Job failed: {data.get('error_message', 'Unknown error')}")
                    return False
                
                time.sleep(1)
            else:
                print_error(f"Failed to get job status: {response.status_code}")
                return False
                
        except Exception as e:
            print_error(f"Error checking job status: {e}")
            return False
    
    print()  # New line
    print_error("Job processing timed out")
    return False

def run_all_tests():
    """Run all system tests"""
    print_header("METADATA RECONCILIATION SYSTEM TEST")
    
    # Keep track of test results
    results = {
        'server': False,
        'api': False,
        'upload': False,
        'processing': False
    }
    
    # Test 1: Server running
    results['server'] = test_server_running()
    if not results['server']:
        print_error("\nCannot continue tests without server running.")
        print_info("Start your server with: python run.py")
        return
    
    # Test 2: API endpoints
    results['api'] = test_api_endpoints()
    
    # Test 3: File upload
    job_id = test_file_upload()
    results['upload'] = job_id is not None
    
    # Test 4: Job processing
    if job_id:
        results['processing'] = test_job_processing(job_id)
    
    # Summary
    print_header("TEST SUMMARY")
    
    total_tests = len(results)
    passed_tests = sum(1 for v in results.values() if v)
    
    for test_name, passed in results.items():
        status = "PASSED" if passed else "FAILED"
        color = Fore.GREEN if passed else Fore.RED
        print(f"{color}{test_name.capitalize():<15} {status}{Style.RESET_ALL}")
    
    print(f"\n{Fore.CYAN}Total: {passed_tests}/{total_tests} tests passed{Style.RESET_ALL}")
    
    if passed_tests == total_tests:
        print_success("\n🎉 All tests passed! Your system is working correctly!")
    else:
        print_error("\n⚠️  Some tests failed. Check the errors above.")
    
    # Cleanup
    if os.path.exists(TEST_CSV):
        os.remove(TEST_CSV)
        print_info(f"\nCleaned up {TEST_CSV}")

if __name__ == "__main__":
    try:
        run_all_tests()
    except KeyboardInterrupt:
        print("\n\nTest interrupted by user")
        sys.exit(1)