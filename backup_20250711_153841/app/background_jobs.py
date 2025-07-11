# File: app/background_jobs.py
"""
Background job processing using Celery.

Think of Celery like a restaurant kitchen:
- Flask (the waiter) takes orders from customers
- Celery (the kitchen) cooks the food in the background
- Redis (the order board) keeps track of what needs to be done
- Customers can check on their order status without waiting in the kitchen

This lets users upload files and continue using the app while processing happens behind the scenes.
"""

from celery import Celery
import os
import pandas as pd
from datetime import datetime

# Import our components
from app.services.metadata_parser import MetadataParser
from app.services.reconciliation_engine import ReconciliationEngine
from app.database import JobManager, ResultsManager

# Configure Celery
def make_celery(app_name=__name__):
    """
    Create and configure Celery instance.
    Redis acts like a message board where tasks are posted.
    """
    # Try to import Redis config, fallback to defaults
    try:
        from config.redis_config import RedisConfig
        redis_url = RedisConfig.REDIS_URL
        print(f"📡 Using Redis at: {redis_url}")
    except ImportError:
        # Fallback to environment variable or default
        redis_url = os.environ.get('REDIS_URL', 'redis://localhost:6379/0')
        print(f"📡 Using default Redis config: {redis_url}")
    
    celery = Celery(
        app_name,
        broker=redis_url,
        backend=redis_url,
        include=['app.background_jobs']  # Tell Celery where to find tasks
    )
    
    # Configure Celery
    celery.conf.update(
        task_serializer='json',
        accept_content=['json'],
        result_serializer='json',
        timezone='UTC',
        enable_utc=True,
        task_track_started=True,  # Track when tasks start
        task_time_limit=3600,     # 1 hour timeout
        worker_prefetch_multiplier=1,  # Process one task at a time
        task_routes={
            'app.background_jobs.process_reconciliation_job': {'queue': 'reconciliation'},
            'app.background_jobs.cleanup_old_jobs': {'queue': 'maintenance'},
        }
    )
    
    return celery

# Create Celery instance
celery_app = make_celery()


@celery_app.task(bind=True)
def process_reconciliation_job(self, job_id):
    """
    Background task to process a reconciliation job.
    
    The 'bind=True' parameter gives us access to 'self' so we can:
    - Update task progress
    - Handle errors gracefully
    - Provide status updates
    """
    
    def update_progress(percent, message):
        """Helper function to update both Celery and database"""
        self.update_state(
            state='PROGRESS',
            meta={'percent': percent, 'message': message}
        )
        JobManager.update_job(job_id, {'progress': percent})
        print(f"📈 {percent}% - {message}")
    
    try:
        # Step 1: Initialize
        update_progress(5, "Starting reconciliation process...")
        job = JobManager.get_job(job_id)
        if not job:
            raise Exception(f"Job {job_id} not found")
        
        JobManager.update_job(job_id, {'status': 'processing'})
        
        # Step 2: Parse CSV file
        update_progress(15, "Reading and parsing CSV file...")
        try:
            df = pd.read_csv(job['filepath'])
            print(f"📄 Loaded CSV with {len(df)} rows and {len(df.columns)} columns")
        except Exception as e:
            raise Exception(f"Failed to read CSV file: {e}")
        
        # Step 3: Initialize reconciliation engine
        update_progress(25, "Initializing reconciliation engine...")
        engine = ReconciliationEngine()
        
        # Step 4: Create entities
        update_progress(35, "Extracting entities from CSV...")
        entities = engine.create_entities_from_dataframe(
            df,
            entity_column=job['entity_column'],
            type_column=job.get('type_column'),
            context_columns=job.get('context_columns', [])
        )
        
        total_entities = len(entities)
        if total_entities == 0:
            raise Exception("No entities found to reconcile. Check your column settings.")
        
        JobManager.update_job(job_id, {'total_entities': total_entities})
        print(f"🎯 Found {total_entities} entities to reconcile")
        
        # Step 5: Process entities in batches with progress tracking
        update_progress(45, f"Processing {total_entities} entities...")
        
        all_results = []
        batch_size = min(10, max(1, total_entities // 10))  # Adaptive batch size
        
        for i in range(0, len(entities), batch_size):
            batch_start = i
            batch_end = min(i + batch_size, len(entities))
            batch = entities[batch_start:batch_end]
            
            # Update progress for this batch
            batch_progress = 45 + int((batch_start / total_entities) * 35)
            update_progress(
                batch_progress, 
                f"Processing entities {batch_start + 1}-{batch_end} of {total_entities}..."
            )
            
            try:
                # Process this batch
                batch_results = engine.process_entities(batch)
                all_results.extend(batch_results)
                
                # Log progress
                matches_found = sum(1 for r in batch_results if r.best_match)
                print(f"📊 Batch {i//batch_size + 1}: {matches_found}/{len(batch)} matches found")
                
            except Exception as e:
                print(f"⚠️  Error processing batch {i//batch_size + 1}: {e}")
                # Continue with other batches instead of failing completely
                continue
        
        # Step 6: Save results to database
        update_progress(85, "Saving results to database...")
        try:
            saved_count = ResultsManager.save_results(job_id, all_results)
            print(f"💾 Saved {saved_count} results to database")
        except Exception as e:
            raise Exception(f"Failed to save results: {e}")
        
        # Step 7: Calculate final statistics
        update_progress(95, "Calculating final statistics...")
        successful_matches = sum(1 for r in all_results if r.best_match)
        
        # Get engine statistics if available
        try:
            engine_stats = engine.get_statistics()
            cache_hit_rate = engine_stats.get('cache_hit_rate', 0)
        except:
            cache_hit_rate = 0
        
        # Step 8: Mark job as completed
        update_progress(100, "Reconciliation completed successfully!")
        
        completion_data = {
            'status': 'completed',
            'progress': 100,
            'successful_matches': successful_matches,
            'completed_at': datetime.now().isoformat()
        }
        
        JobManager.update_job(job_id, completion_data)
        
        # Return final results summary
        result_summary = {
            'job_id': job_id,
            'total_entities': total_entities,
            'successful_matches': successful_matches,
            'match_rate': successful_matches / total_entities if total_entities > 0 else 0,
            'cache_hit_rate': cache_hit_rate,
            'status': 'completed'
        }
        
        print(f"✅ Job {job_id} completed successfully!")
        print(f"📊 Results: {successful_matches}/{total_entities} matches ({successful_matches/total_entities*100:.1f}%)")
        
        return result_summary
        
    except Exception as e:
        # Handle errors gracefully
        error_message = str(e)
        print(f"❌ Job {job_id} failed: {error_message}")
        
        # Update job status
        JobManager.update_job(job_id, {
            'status': 'failed',
            'error_message': error_message,
            'completed_at': datetime.now().isoformat()
        })
        
        # Update Celery task state
        self.update_state(
            state='FAILURE',
            meta={'error': error_message, 'job_id': job_id}
        )
        
        # Re-raise the exception so Celery knows the task failed
        raise


@celery_app.task
def cleanup_old_jobs():
    """
    Background task to clean up old job files and data.
    This would run periodically (e.g., daily) to free up disk space.
    """
    try:
        from datetime import datetime, timedelta
        import os
        
        # Remove job files older than 30 days
        cutoff_date = datetime.now() - timedelta(days=30)
        
        # This is a placeholder - you'd implement the actual cleanup logic
        print(f"🧹 Cleaning up jobs older than {cutoff_date}")
        
        # Example cleanup tasks:
        # 1. Delete old CSV files
        # 2. Archive old job records
        # 3. Clean up temporary files
        
        return {"status": "cleanup_completed", "cutoff_date": cutoff_date.isoformat()}
        
    except Exception as e:
        print(f"❌ Cleanup task failed: {e}")
        raise


# Utility functions for checking task status

def get_task_status(task_id):
    """
    Get the status of a Celery task.
    This is useful for the web interface to check progress.
    """
    task = celery_app.AsyncResult(task_id)
    
    if task.state == 'PENDING':
        return {
            'state': 'PENDING',
            'percent': 0,
            'message': 'Task is waiting to start...'
        }
    elif task.state == 'PROGRESS':
        return {
            'state': 'PROGRESS',
            'percent': task.info.get('percent', 0),
            'message': task.info.get('message', 'Processing...')
        }
    elif task.state == 'SUCCESS':
        return {
            'state': 'SUCCESS',
            'percent': 100,
            'message': 'Task completed successfully!',
            'result': task.info
        }
    elif task.state == 'FAILURE':
        return {
            'state': 'FAILURE',
            'percent': 0,
            'message': f'Task failed: {task.info.get("error", "Unknown error")}',
            'error': str(task.info)
        }
    else:
        return {
            'state': task.state,
            'percent': 0,
            'message': f'Unknown state: {task.state}'
        }


def cancel_task(task_id):
    """Cancel a running task"""
    celery_app.control.revoke(task_id, terminate=True)
    return {"status": "cancelled", "task_id": task_id}


def test_redis_connection():
    """Test Redis connection for debugging"""
    try:
        from config.redis_config import RedisConfig
        success, message = RedisConfig.test_connection()
        return success, message
    except ImportError:
        try:
            import redis
            r = redis.Redis.from_url('redis://localhost:6379/0')
            r.ping()
            return True, "Redis connection successful (default config)"
        except Exception as e:
            return False, f"Redis connection failed: {e}"


# Configuration for periodic tasks (optional)
from celery.schedules import crontab

celery_app.conf.beat_schedule = {
    'cleanup-old-jobs': {
        'task': 'app.background_jobs.cleanup_old_jobs',
        'schedule': crontab(hour=2, minute=0),  # Run at 2 AM daily
    },
}


if __name__ == '__main__':
    """
    For testing the background tasks directly.
    Run with: python -m app.background_jobs
    """
    
    print("Testing background job system...")
    
    # Test Redis connection
    success, message = test_redis_connection()
    print(f"Redis connection: {message}")
    
    if success:
        print("✅ Background job system is ready!")
    else:
        print("❌ Redis connection failed. Background jobs will not work.")
    
    print("Background job system loaded successfully!")


@celery_app.task(bind=True)
def process_reconciliation_job(self, job_id):
    """
    Background task to process a reconciliation job.
    
    The 'bind=True' parameter gives us access to 'self' so we can:
    - Update task progress
    - Handle errors gracefully
    - Provide status updates
    """
    
    def update_progress(percent, message):
        """Helper function to update both Celery and database"""
        self.update_state(
            state='PROGRESS',
            meta={'percent': percent, 'message': message}
        )
        JobManager.update_job(job_id, {'progress': percent})
        print(f"📈 {percent}% - {message}")
    
    try:
        # Step 1: Initialize
        update_progress(5, "Starting reconciliation process...")
        job = JobManager.get_job(job_id)
        if not job:
            raise Exception(f"Job {job_id} not found")
        
        JobManager.update_job(job_id, {'status': 'processing'})
        
        # Step 2: Parse CSV file
        update_progress(15, "Reading and parsing CSV file...")
        try:
            df = pd.read_csv(job['filepath'])
            print(f"📄 Loaded CSV with {len(df)} rows and {len(df.columns)} columns")
        except Exception as e:
            raise Exception(f"Failed to read CSV file: {e}")
        
        # Step 3: Initialize reconciliation engine
        update_progress(25, "Initializing reconciliation engine...")
        engine = ReconciliationEngine()
        
        # Step 4: Create entities
        update_progress(35, "Extracting entities from CSV...")
        entities = engine.create_entities_from_dataframe(
            df,
            entity_column=job['entity_column'],
            type_column=job.get('type_column'),
            context_columns=job.get('context_columns', [])
        )
        
        total_entities = len(entities)
        if total_entities == 0:
            raise Exception("No entities found to reconcile. Check your column settings.")
        
        JobManager.update_job(job_id, {'total_entities': total_entities})
        print(f"🎯 Found {total_entities} entities to reconcile")
        
        # Step 5: Process entities in batches with progress tracking
        update_progress(45, f"Processing {total_entities} entities...")
        
        all_results = []
        batch_size = min(10, max(1, total_entities // 10))  # Adaptive batch size
        
        for i in range(0, len(entities), batch_size):
            batch_start = i
            batch_end = min(i + batch_size, len(entities))
            batch = entities[batch_start:batch_end]
            
            # Update progress for this batch
            batch_progress = 45 + int((batch_start / total_entities) * 35)
            update_progress(
                batch_progress, 
                f"Processing entities {batch_start + 1}-{batch_end} of {total_entities}..."
            )
            
            try:
                # Process this batch
                batch_results = engine.process_entities(batch)
                all_results.extend(batch_results)
                
                # Log progress
                matches_found = sum(1 for r in batch_results if r.best_match)
                print(f"📊 Batch {i//batch_size + 1}: {matches_found}/{len(batch)} matches found")
                
            except Exception as e:
                print(f"⚠️  Error processing batch {i//batch_size + 1}: {e}")
                # Continue with other batches instead of failing completely
                continue
        
        # Step 6: Save results to database
        update_progress(85, "Saving results to database...")
        try:
            saved_count = ResultsManager.save_results(job_id, all_results)
            print(f"💾 Saved {saved_count} results to database")
        except Exception as e:
            raise Exception(f"Failed to save results: {e}")
        
        # Step 7: Calculate final statistics
        update_progress(95, "Calculating final statistics...")
        successful_matches = sum(1 for r in all_results if r.best_match)
        
        # Get engine statistics if available
        try:
            engine_stats = engine.get_statistics()
            cache_hit_rate = engine_stats.get('cache_hit_rate', 0)
        except:
            cache_hit_rate = 0
        
        # Step 8: Mark job as completed
        update_progress(100, "Reconciliation completed successfully!")
        
        completion_data = {
            'status': 'completed',
            'progress': 100,
            'successful_matches': successful_matches,
            'completed_at': datetime.now().isoformat()
        }
        
        JobManager.update_job(job_id, completion_data)
        
        # Return final results summary
        result_summary = {
            'job_id': job_id,
            'total_entities': total_entities,
            'successful_matches': successful_matches,
            'match_rate': successful_matches / total_entities if total_entities > 0 else 0,
            'cache_hit_rate': cache_hit_rate,
            'status': 'completed'
        }
        
        print(f"✅ Job {job_id} completed successfully!")
        print(f"📊 Results: {successful_matches}/{total_entities} matches ({successful_matches/total_entities*100:.1f}%)")
        
        return result_summary
        
    except Exception as e:
        # Handle errors gracefully
        error_message = str(e)
        print(f"❌ Job {job_id} failed: {error_message}")
        
        # Update job status
        JobManager.update_job(job_id, {
            'status': 'failed',
            'error_message': error_message,
            'completed_at': datetime.now().isoformat()
        })
        
        # Update Celery task state
        self.update_state(
            state='FAILURE',
            meta={'error': error_message, 'job_id': job_id}
        )
        
        # Re-raise the exception so Celery knows the task failed
        raise


@celery_app.task
def cleanup_old_jobs():
    """
    Background task to clean up old job files and data.
    This would run periodically (e.g., daily) to free up disk space.
    """
    try:
        from datetime import datetime, timedelta
        import os
        
        # Remove job files older than 30 days
        cutoff_date = datetime.now() - timedelta(days=30)
        
        # This is a placeholder - you'd implement the actual cleanup logic
        print(f"🧹 Cleaning up jobs older than {cutoff_date}")
        
        # Example cleanup tasks:
        # 1. Delete old CSV files
        # 2. Archive old job records
        # 3. Clean up temporary files
        
        return {"status": "cleanup_completed", "cutoff_date": cutoff_date.isoformat()}
        
    except Exception as e:
        print(f"❌ Cleanup task failed: {e}")
        raise


# Utility functions for checking task status

def get_task_status(task_id):
    """
    Get the status of a Celery task.
    This is useful for the web interface to check progress.
    """
    task = celery_app.AsyncResult(task_id)
    
    if task.state == 'PENDING':
        return {
            'state': 'PENDING',
            'percent': 0,
            'message': 'Task is waiting to start...'
        }
    elif task.state == 'PROGRESS':
        return {
            'state': 'PROGRESS',
            'percent': task.info.get('percent', 0),
            'message': task.info.get('message', 'Processing...')
        }
    elif task.state == 'SUCCESS':
        return {
            'state': 'SUCCESS',
            'percent': 100,
            'message': 'Task completed successfully!',
            'result': task.info
        }
    elif task.state == 'FAILURE':
        return {
            'state': 'FAILURE',
            'percent': 0,
            'message': f'Task failed: {task.info.get("error", "Unknown error")}',
            'error': str(task.info)
        }
    else:
        return {
            'state': task.state,
            'percent': 0,
            'message': f'Unknown state: {task.state}'
        }


def cancel_task(task_id):
    """Cancel a running task"""
    celery_app.control.revoke(task_id, terminate=True)
    return {"status": "cancelled", "task_id": task_id}


# Configuration for periodic tasks (optional)
from celery.schedules import crontab

celery_app.conf.beat_schedule = {
    'cleanup-old-jobs': {
        'task': 'app.background_jobs.cleanup_old_jobs',
        'schedule': crontab(hour=2, minute=0),  # Run at 2 AM daily
    },
}


if __name__ == '__main__':
    """
    For testing the background tasks directly.
    Run with: python -m app.background_jobs
    """
    
    # Test the task system
    print("Testing background job system...")
    
    # This would normally be called from the web interface
    # result = process_reconciliation_job.delay('test_job_id')
    # print(f"Started task: {result.id}")
    
    print("Background job system loaded successfully!")


"""
DEPLOYMENT NOTES:

To use this background job system, you need to:

1. Install Redis:
   - Windows: Download from https://github.com/microsoftarchive/redis/releases
   - Mac: brew install redis
   - Linux: sudo apt-get install redis-server

2. Install Celery:
   pip install celery redis

3. Start Redis server:
   redis-server

4. Start Celery worker (in a separate terminal):
   celery -A app.background_jobs worker --loglevel=info

5. Start Celery beat (for periodic tasks, optional):
   celery -A app.background_jobs beat --loglevel=info

6. Your Flask app will then queue tasks to be processed by the workers.

For production deployment:
- Use a proper Redis server (not local)
- Run multiple Celery workers for scalability
- Use a process manager like systemd or supervisor
- Monitor with tools like Flower: pip install flower
"""