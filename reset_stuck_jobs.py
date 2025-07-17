#!/usr/bin/env python3
"""
Reset Stuck Jobs Script
Run this to reset all stuck processing jobs to uploadled state
so you can restart them with the fixed processing logic.

Usage: python reset_stuck_jobs.py
"""

import os
import sys
from datetime import datetime, timedelta

# Add the app directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__)))

def reset_stuck_jobs():
    """Reset all jobs stuck in processing state"""
    try:
        from app.database import JobManager, get_db_connection
        
        print("🔧 Resetting stuck jobs...")
        
        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            # Find jobs stuck in processing for more than 5 minutes
            five_minutes_ago = datetime.now() - timedelta(minutes=5)
            
            cursor.execute('''
                SELECT id, filename, status, created_at, progress 
                FROM jobs 
                WHERE status IN ('processing', 'queued') 
                AND created_at < ?
            ''', (five_minutes_ago.isoformat(),))
            
            stuck_jobs = cursor.fetchall()
            
            if not stuck_jobs:
                print("✅ No stuck jobs found")
                return
            
            print(f"🔍 Found {len(stuck_jobs)} stuck jobs:")
            for job in stuck_jobs:
                print(f"  📄 {job['filename']} (ID: {job['id'][:8]}...) - {job['status']} at {job['progress']}%")
            
            # Reset them to uploaded state
            cursor.execute('''
                UPDATE jobs 
                SET status = 'uploaded', 
                    progress = 0, 
                    error_message = 'Reset from stuck processing state',
                    task_id = NULL
                WHERE status IN ('processing', 'queued') 
                AND created_at < ?
            ''', (five_minutes_ago.isoformat(),))
            
            reset_count = cursor.rowcount
            conn.commit()
            
            print(f"✅ Reset {reset_count} stuck jobs to 'uploaded' state")
            print("💡 You can now restart these jobs from the web interface")
            
    except Exception as e:
        print(f"❌ Error resetting stuck jobs: {e}")


def list_all_jobs():
    """List all current jobs with their status"""
    try:
        from app.database import JobManager
        
        jobs = JobManager.get_all_jobs()
        
        print(f"\n📋 Current Jobs ({len(jobs)} total):")
        print("-" * 80)
        
        for job in jobs:
            status_emoji = {
                'uploaded': '📤',
                'queued': '⏳', 
                'processing': '🔄',
                'completed': '✅',
                'failed': '❌',
                'cancelled': '🚫'
            }.get(job['status'], '❓')
            
            progress = job.get('progress', 0)
            entities = job.get('total_entities', 0)
            matches = job.get('successful_matches', 0)
            
            print(f"{status_emoji} {job['filename']}")
            print(f"   ID: {job['id']}")
            print(f"   Status: {job['status']} ({progress}%)")
            print(f"   Entities: {entities}, Matches: {matches}")
            print(f"   Created: {job.get('created_at', 'Unknown')}")
            if job.get('error_message'):
                print(f"   Error: {job['error_message']}")
            print()
            
    except Exception as e:
        print(f"❌ Error listing jobs: {e}")


def main():
    """Main function"""
    print("🛠️ Stuck Jobs Reset Tool")
    print("=" * 50)
    
    # List current jobs
    list_all_jobs()
    
    # Ask user if they want to reset stuck jobs
    response = input("\n❓ Do you want to reset stuck processing jobs? (y/N): ").lower()
    
    if response in ['y', 'yes']:
        reset_stuck_jobs()
        print("\n📋 Updated job list:")
        list_all_jobs()
    else:
        print("🚫 No changes made")
    
    print("\n💡 Next steps:")
    print("1. Apply the fixed processing logic to your code")
    print("2. Restart your Flask application")
    print("3. Try uploading a small test CSV file")
    print("4. Monitor the processing logs for detailed progress")


if __name__ == "__main__":
    main()