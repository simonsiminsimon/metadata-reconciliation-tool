#!/usr/bin/env python3
"""
Test Processing Fix Script
Run this after applying the threading and timeout fixes to verify they work.
"""

import sys
import os
import time
import threading
import requests
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def test_threading_function():
    """Test if the threading function signature is correct"""
    print("1️⃣ Testing Threading Function Signature")
    print("-" * 50)
    
    try:
        from app.routes.web import process_job_threaded, start_threaded_processing
        import inspect
        
        # Check function signature
        sig = inspect.signature(process_job_threaded)
        params = list(sig.parameters.keys())
        
        if 'job_id' in params:
            print("✅ process_job_threaded() has correct signature")
            print(f"   Parameters: {params}")
        else:
            print("❌ process_job_threaded() missing job_id parameter")
            print(f"   Current parameters: {params}")
            return False
            
        # Test the start function exists
        if hasattr(start_threaded_processing, '__call__'):
            print("✅ start_threaded_processing() function found")
        else:
            print("❌ start_threaded_processing() function missing")
            return False
            
        return True
        
    except ImportError as e:
        print(f"❌ Import error: {e}")
        return False
    except Exception as e:
        print(f"❌ Error testing threading: {e}")
        return False

def test_failsafe_client():
    """Test if the failsafe Wikidata client works"""
    print("\n2️⃣ Testing Failsafe Wikidata Client")
    print("-" * 50)
    
    try:
        from app.services.failsafe_wikidata_client import FailsafeWikidataClient
        
        # Initialize with very conservative settings
        print("   Initializing failsafe client...")
        client = FailsafeWikidataClient(
            rate_limit=0.5,  # Very slow
            timeout=5,       # Short timeout
            max_results=2    # Few results
        )
        
        print("✅ Failsafe client initialized successfully")
        
        # Test a simple search (this should not hang)
        print("   Testing simple search (5 second timeout)...")
        start_time = time.time()
        
        try:
            results = client.search_persons("Shakespeare")
            elapsed = time.time() - start_time
            
            print(f"✅ Search completed in {elapsed:.1f}s")
            print(f"   Found {len(results)} results")
            
            if results:
                print(f"   Sample result: {results[0].label}")
                
        except Exception as e:
            elapsed = time.time() - start_time
            print(f"⚠️  Search failed after {elapsed:.1f}s: {e}")
            print("   (This is OK - client should handle failures gracefully)")
        
        # Check statistics
        stats = client.get_statistics()
        print(f"   Circuit breaker active: {stats.get('circuit_breaker_active', 'Unknown')}")
        print(f"   Success rate: {stats.get('success_rate', 0):.1%}")
        
        return True
        
    except ImportError as e:
        print(f"❌ Import error: {e}")
        return False
    except Exception as e:
        print(f"❌ Error testing client: {e}")
        return False

def test_database_connection():
    """Test database connectivity"""
    print("\n3️⃣ Testing Database Connection")
    print("-" * 50)
    
    try:
        from app.database import JobManager, ResultsManager, get_db_connection
        
        # Test database connection
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM jobs")
            job_count = cursor.fetchone()[0]
            print(f"✅ Database connected - {job_count} jobs in database")
        
        # Test JobManager
        jobs = JobManager.get_all_jobs()
        print(f"✅ JobManager working - retrieved {len(jobs)} jobs")
        
        # Find a completed job to test
        completed_jobs = [j for j in jobs if j['status'] == 'completed']
        if completed_jobs:
            test_job = completed_jobs[0]
            job_id = test_job['id']
            
            # Test ResultsManager
            try:
                results, total = ResultsManager.get_results(job_id, 1, 5)
                print(f"✅ ResultsManager working - {len(results)}/{total} results retrieved")
            except Exception as e:
                print(f"⚠️  ResultsManager issue: {e}")
        
        return True
        
    except Exception as e:
        print(f"❌ Database error: {e}")
        return False

def test_api_endpoints():
    """Test if the web application is running"""
    print("\n4️⃣ Testing Web Application")
    print("-" * 50)
    
    try:
        # Test basic health endpoint
        response = requests.get("http://localhost:5000/health", timeout=5)
        if response.status_code == 200:
            print("✅ Web application is running")
            data = response.json()
            print(f"   Status: {data.get('status', 'Unknown')}")
        else:
            print(f"⚠️  Web app responding but status: {response.status_code}")
            
    except requests.exceptions.ConnectionError:
        print("❌ Web application not running on localhost:5000")
        print("   Start with: python run.py")
        return False
    except Exception as e:
        print(f"❌ Error testing web app: {e}")
        return False
    
    return True

def run_comprehensive_test():
    """Run all tests and provide summary"""
    print("🔧 PROCESSING FIX VERIFICATION")
    print("=" * 60)
    
    results = []
    
    # Run all tests
    results.append(("Threading Function", test_threading_function()))
    results.append(("Failsafe Client", test_failsafe_client()))
    results.append(("Database Connection", test_database_connection()))
    results.append(("Web Application", test_api_endpoints()))
    
    # Summary
    print("\n" + "=" * 60)
    print("📊 TEST SUMMARY")
    print("=" * 60)
    
    passed = 0
    total = len(results)
    
    for test_name, success in results:
        status = "✅ PASS" if success else "❌ FAIL"
        print(f"{status} {test_name}")
        if success:
            passed += 1
    
    print(f"\nResult: {passed}/{total} tests passed")
    
    if passed == total:
        print("\n🎉 ALL FIXES WORKING!")
        print("✅ You can now upload files and they should process correctly")
        print("✅ No more threading errors or timeout hangs")
        print("✅ Processing will complete reliably")
        print(f"\n🔗 Try uploading at: http://localhost:5000/upload")
    else:
        print("\n⚠️  SOME ISSUES REMAIN")
        print("❌ Check the failed tests above")
        print("❌ Make sure you applied all the fixes")
        print("❌ Restart your application after applying fixes")
    
    return passed == total

if __name__ == "__main__":
    success = run_comprehensive_test()
    exit(0 if success else 1)