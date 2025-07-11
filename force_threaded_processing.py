# File: force_threaded_processing.py
# Run this to restart any stuck jobs in threaded mode

from app.database import JobManager
import threading

def restart_stuck_jobs():
    """Restart any stuck jobs using threaded processing"""
    print("🔄 Looking for stuck jobs...")
    
    try:
        jobs = JobManager.get_all_jobs()
        stuck_jobs = [job for job in jobs if job['status'] in ['uploaded', 'queued']]
        
        if not stuck_jobs:
            print("✅ No stuck jobs found")
            return
        
        print(f"Found {len(stuck_jobs)} stuck job(s)")
        
        for job in stuck_jobs:
            print(f"\n🚀 Restarting job: {job['filename']}")
            
            # Reset job status
            JobManager.update_job(job['id'], {
                'status': 'processing',
                'progress': 0,
                'error_message': None,
                'task_id': None  # Clear background task ID
            })
            
            # Start threaded processing
            from app.routes.web import process_job_with_reconciliation
            
            threading.Thread(
                target=process_job_with_reconciliation,
                args=(job['id'],),
                daemon=True
            ).start()
            
            print(f"   ✅ Started threaded processing for {job['id'][:8]}...")
        
        print(f"\n🎉 Restarted {len(stuck_jobs)} job(s) in threaded mode")
        print("   Check your browser - processing should now continue")
    
    except Exception as e:
        print(f"❌ Error restarting jobs: {e}")

if __name__ == "__main__":
    restart_stuck_jobs()