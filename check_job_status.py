# File: check_job_status.py
# Run this to see what's happening with your job

from app.database import JobManager
import json

def check_job_status():
    """Check the status of recent jobs"""
    print("🔍 Checking job status...")
    print("=" * 50)
    
    try:
        # Get all jobs
        jobs = JobManager.get_all_jobs()
        
        if not jobs:
            print("❌ No jobs found in database")
            return
        
        print(f"📊 Found {len(jobs)} job(s)")
        
        # Show details of recent jobs
        for i, job in enumerate(jobs[:3]):  # Show last 3 jobs
            print(f"\n🔄 Job {i+1}:")
            print(f"   ID: {job['id']}")
            print(f"   Filename: {job['filename']}")
            print(f"   Status: {job['status']}")
            print(f"   Progress: {job['progress']}%")
            print(f"   Created: {job['created_at']}")
            
            # Get full job details
            full_job = JobManager.get_job(job['id'])
            if full_job:
                print(f"   Entity Column: {full_job.get('entity_column', 'Not set')}")
                print(f"   Data Sources: {full_job.get('data_sources', 'Not set')}")
                print(f"   Task ID: {full_job.get('task_id', 'No background task')}")
                
                if full_job.get('error_message'):
                    print(f"   ❌ Error: {full_job['error_message']}")
                
                # Check if file exists
                import os
                if os.path.exists(full_job['filepath']):
                    print(f"   ✅ CSV file exists: {full_job['filepath']}")
                else:
                    print(f"   ❌ CSV file missing: {full_job['filepath']}")
    
    except Exception as e:
        print(f"❌ Error checking jobs: {e}")

def check_background_jobs():
    """Check if background jobs are working"""
    print("\n🔧 Checking background job system...")
    print("=" * 50)
    
    try:
        from app.routes.web import BACKGROUND_JOBS_AVAILABLE
        print(f"Background jobs available: {BACKGROUND_JOBS_AVAILABLE}")
        
        if BACKGROUND_JOBS_AVAILABLE:
            print("✅ Background jobs are enabled")
            
            # Test Redis connection
            try:
                from app.background_jobs import test_redis_connection
                success, message = test_redis_connection()
                print(f"Redis connection: {message}")
                
                if success:
                    print("✅ Redis is working")
                else:
                    print("❌ Redis connection failed")
                    print("   This explains why jobs aren't processing!")
            except Exception as e:
                print(f"❌ Cannot test Redis: {e}")
        else:
            print("⚠️  Background jobs not available - using threaded processing")
            print("   Jobs should still process, but without real-time progress")
    
    except Exception as e:
        print(f"❌ Error checking background jobs: {e}")

def suggest_solutions():
    """Suggest solutions based on findings"""
    print("\n💡 Potential Solutions:")
    print("=" * 50)
    
    jobs = JobManager.get_all_jobs()
    if jobs:
        latest_job = jobs[0]
        
        if latest_job['status'] == 'uploaded':
            print("🔄 Job is stuck in 'uploaded' status:")
            print("   1. Check if Redis is running: redis-server")
            print("   2. Check if Celery worker is running:")
            print("      celery -A app.background_jobs worker --loglevel=info")
            print("   3. Or restart the job to use threaded processing")
            
        elif latest_job['status'] == 'processing':
            print("🔄 Job is processing:")
            print("   1. This is normal - wait for it to complete")
            print("   2. Check Celery worker console for progress")
            
        elif latest_job['status'] == 'failed':
            print("❌ Job failed:")
            print("   1. Check the error message above")
            print("   2. Try restarting the job")
            print("   3. Check your CSV file format")
    
    print("\n🚀 Quick fixes:")
    print("   1. Restart Flask app: Ctrl+C then python run.py")
    print("   2. Delete stuck job and upload again")
    print("   3. Use threaded processing (no Redis needed)")

if __name__ == "__main__":
    check_job_status()
    check_background_jobs()
    suggest_solutions()