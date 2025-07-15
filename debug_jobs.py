#!/usr/bin/env python3
"""
Debug script to check job statuses and understand what's happening
Save as debug_jobs.py and run it
"""

import sys
import os

# Add the project root to Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def debug_jobs():
    """Check the status of all jobs and identify issues"""
    print("🔍 Debugging Job Status...")
    print("=" * 60)
    
    try:
        from app.database import JobManager
        from app.routes.web import BACKGROUND_JOBS_AVAILABLE
        
        # Check system configuration
        print(f"Background jobs available: {BACKGROUND_JOBS_AVAILABLE}")
        
        # Get all jobs
        jobs = JobManager.get_all_jobs()
        print(f"Total jobs found: {len(jobs)}")
        print()
        
        if not jobs:
            print("No jobs found. Upload a CSV file first.")
            return
        
        # Analyze each job
        for i, job in enumerate(jobs, 1):
            print(f"Job {i}: {job['id']}")
            print(f"  Filename: {job['filename']}")
            print(f"  Status: {job['status']}")
            print(f"  Progress: {job.get('progress', 0)}%")
            print(f"  Created: {job.get('created_at', 'Unknown')}")
            print(f"  Entities: {job.get('total_entities', 0)}")
            print(f"  Matches: {job.get('successful_matches', 0)}")
            
            if job.get('error_message'):
                print(f"  Error: {job['error_message']}")
            
            # Identify stuck jobs
            if job['status'] == 'uploaded':
                print("  ⚠️  ISSUE: Job stuck in 'uploaded' status")
                print("     This means processing never started")
            elif job['status'] == 'processing' and job.get('progress', 0) == 0:
                print("  ⚠️  ISSUE: Job stuck at 0% progress")
                print("     Processing started but isn't making progress")
            
            print()
        
        # Summary
        status_counts = {}
        for job in jobs:
            status = job['status']
            status_counts[status] = status_counts.get(status, 0) + 1
        
        print("Status Summary:")
        for status, count in status_counts.items():
            print(f"  {status}: {count}")
        
        # Recommendations
        print("\n" + "=" * 60)
        print("🔧 Recommendations:")
        
        stuck_jobs = [j for j in jobs if j['status'] == 'uploaded']
        if stuck_jobs:
            print("1. You have jobs stuck in 'uploaded' status")
            print("   - This means the processing never started")
            print("   - Try the 'Start Job' button on the jobs page")
            print("   - Or fix the threaded processing function")
        
        processing_jobs = [j for j in jobs if j['status'] == 'processing' and j.get('progress', 0) == 0]
        if processing_jobs:
            print("2. You have jobs stuck at 0% progress")
            print("   - Processing started but isn't working")
            print("   - Check the threaded processing function")
        
        if not BACKGROUND_JOBS_AVAILABLE:
            print("3. Background jobs (Celery/Redis) not available")
            print("   - Using threaded processing fallback")
            print("   - Make sure the threaded function is working")
        
        print(f"\n4. To restart a stuck job, you can:")
        print(f"   - Go to http://localhost:5000/jobs")
        print(f"   - Click 'START' on any uploaded job")
        print(f"   - Or use the API: POST /api/jobs/{{job_id}}/start")
        
    except Exception as e:
        print(f"❌ Error debugging jobs: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    debug_jobs()