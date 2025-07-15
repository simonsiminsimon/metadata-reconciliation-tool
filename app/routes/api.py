# File: app/routes/api.py
"""
API routes for the Metadata Reconciliation System.
Provides JSON endpoints for frontend interactions and system monitoring.
"""

from flask import jsonify, request
import pandas as pd
from io import StringIO
import logging

from app.database import JobManager, ResultsManager

# Set up logging
logger = logging.getLogger(__name__)

# Check background job availability
try:
    from app.background_jobs import celery_app
    BACKGROUND_JOBS_AVAILABLE = True
except ImportError:
    BACKGROUND_JOBS_AVAILABLE = False


def register_api_routes(app):
    """Register all API routes with the Flask app"""
    
    @app.route('/api/system_status')
    def system_status():
        """
        Get current system status.
        Returns information about queue, workers, and system health.
        """
        status = {
            'status': 'operational',
            'background_jobs_available': BACKGROUND_JOBS_AVAILABLE,
            'features': {
                'background_processing': BACKGROUND_JOBS_AVAILABLE,
                'real_time_progress': BACKGROUND_JOBS_AVAILABLE,
                'job_cancellation': BACKGROUND_JOBS_AVAILABLE,
                'database_storage': True,
                'multiple_export_formats': True,
                'threaded_fallback': True
            }
        }
        
        # Add queue information if Celery is available
        if BACKGROUND_JOBS_AVAILABLE:
            try:
                # Get Celery stats
                inspector = celery_app.control.inspect()
                stats = inspector.stats()
                active = inspector.active()
                
                # Count total active tasks
                active_count = 0
                if active:
                    for worker, tasks in active.items():
                        active_count += len(tasks)
                
                status['queue_count'] = active_count
                status['workers_online'] = len(stats) if stats else 0
                
            except Exception as e:
                logger.error(f"Failed to get Celery stats: {e}")
                status['queue_count'] = 0
                status['workers_online'] = 0
        else:
            status['queue_count'] = 0
            status['workers_online'] = 0
        
        return jsonify(status)
    
    @app.route('/api/job/<job_id>/status')
    def job_status(job_id):
        """Get detailed status for a specific job"""
        job = JobManager.get_job(job_id)
        if not job:
            return jsonify({'error': 'Job not found'}), 404
        
        # Build response
        response = {
            'job_id': job_id,
            'status': job['status'],
            'progress': job['progress'],
            'filename': job['filename'],
            'total_entities': job.get('total_entities', 0),
            'successful_matches': job.get('successful_matches', 0),
            'created_at': job.get('created_at'),
            'completed_at': job.get('completed_at'),
            'error_message': job.get('error_message')
        }
        
        # Add human-readable status message
        response['message'] = get_status_message(job)
        
        # Add match rate if applicable
        if job['total_entities'] > 0 and job['status'] == 'completed':
            match_rate = (job['successful_matches'] / job['total_entities']) * 100
            response['match_rate'] = round(match_rate, 1)
        
        return jsonify(response)
    
    @app.route('/api/preview_columns', methods=['POST'])
    def preview_columns():
        """
        Preview column names from uploaded CSV.
        Helps users select the correct column for entity reconciliation.
        """
        try:
            if 'file' not in request.files:
                return jsonify({'error': 'No file provided'}), 400
            
            file = request.files['file']
            
            # Read first few lines to get columns
            content = file.read(1024).decode('utf-8')  # Read first 1KB
            lines = content.split('\n')
            
            if not lines:
                return jsonify({'error': 'Empty file'}), 400
            
            # Parse header
            header = lines[0]
            columns = [col.strip() for col in header.split(',')]
            
            # Get sample data if available
            sample_data = []
            if len(lines) > 1:
                for line in lines[1:4]:  # Get up to 3 sample rows
                    if line.strip():
                        values = [val.strip() for val in line.split(',')]
                        sample_data.append(dict(zip(columns, values)))
            
            return jsonify({
                'success': True,
                'columns': columns,
                'sample_data': sample_data,
                'column_count': len(columns)
            })
            
        except Exception as e:
            logger.error(f"Column preview failed: {e}")
            return jsonify({'error': str(e)}), 500
    
    @app.route('/api/job/<job_id>/cancel', methods=['POST'])
    def cancel_job(job_id):
        """Cancel a running job"""
        job = JobManager.get_job(job_id)
        if not job:
            return jsonify({'error': 'Job not found'}), 404
        
        if job['status'] not in ['queued', 'processing']:
            return jsonify({'error': 'Job cannot be cancelled in current state'}), 400
        
        try:
            # Update job status
            JobManager.update_job(job_id, {
                'status': 'cancelled',
                'error_message': 'Cancelled by user'
            })
            
            # If using Celery, try to revoke the task
            if BACKGROUND_JOBS_AVAILABLE and job.get('task_id'):
                try:
                    celery_app.control.revoke(job['task_id'], terminate=True)
                except Exception as e:
                    logger.error(f"Failed to revoke Celery task: {e}")
            
            return jsonify({'success': True, 'message': 'Job cancelled'})
            
        except Exception as e:
            logger.error(f"Failed to cancel job {job_id}: {e}")
            return jsonify({'error': str(e)}), 500
    
    @app.route('/api/job/<job_id>/approve_match', methods=['POST'])
    def approve_match(job_id):
        """
        Approve or reject a specific match.
        This allows users to provide feedback on reconciliation results.
        """
        try:
            data = request.get_json()
            if not data:
                return jsonify({'error': 'No data provided'}), 400
            
            entity_id = data.get('entity_id')
            match_id = data.get('match_id')
            approved = data.get('approved', True)
            
            if not entity_id or not match_id:
                return jsonify({'error': 'Missing entity_id or match_id'}), 400
            
            # Store the feedback (you could extend this to save to database)
            feedback_data = {
                'job_id': job_id,
                'entity_id': entity_id,
                'match_id': match_id,
                'approved': approved,
                'timestamp': pd.Timestamp.now().isoformat()
            }
            
            # Here you could save this feedback to improve future matching
            logger.info(f"Match feedback for job {job_id}: {feedback_data}")
            
            return jsonify({
                'success': True,
                'message': 'Feedback recorded'
            })
            
        except Exception as e:
            logger.error(f"Failed to record match feedback: {e}")
            return jsonify({'error': str(e)}), 500
    
    @app.route('/api/stats')
    def get_stats():
        """Get overall system statistics"""
        try:
            # Get all jobs
            all_jobs = JobManager.get_all_jobs()
            
            # Calculate statistics
            total_jobs = len(all_jobs)
            completed_jobs = sum(1 for job in all_jobs if job['status'] == 'completed')
            failed_jobs = sum(1 for job in all_jobs if job['status'] == 'failed')
            processing_jobs = sum(1 for job in all_jobs if job['status'] in ['queued', 'processing'])
            
            # Calculate totals
            total_entities = sum(job.get('total_entities', 0) for job in all_jobs)
            total_matches = sum(job.get('successful_matches', 0) for job in all_jobs)
            
            # Calculate average match rate for completed jobs
            completed_with_entities = [
                job for job in all_jobs 
                if job['status'] == 'completed' and job.get('total_entities', 0) > 0
            ]
            
            if completed_with_entities:
                avg_match_rate = sum(
                    (job['successful_matches'] / job['total_entities']) * 100
                    for job in completed_with_entities
                ) / len(completed_with_entities)
            else:
                avg_match_rate = 0
            
            return jsonify({
                'total_jobs': total_jobs,
                'completed_jobs': completed_jobs,
                'failed_jobs': failed_jobs,
                'processing_jobs': processing_jobs,
                'total_entities_processed': total_entities,
                'total_matches_found': total_matches,
                'average_match_rate': round(avg_match_rate, 1),
                'system_status': 'operational'
            })
            
        except Exception as e:
            logger.error(f"Failed to get statistics: {e}")
            return jsonify({'error': str(e)}), 500
# Add these endpoints to your app/routes/api.py file inside the register_api_routes function:

    # URL compatibility - JavaScript uses plural 'jobs', but we defined singular 'job'
    @app.route('/api/jobs/<job_id>/status')
    def jobs_status_compat(job_id):
        """Job status endpoint with plural URL for JavaScript compatibility"""
        return job_status(job_id)
    
    @app.route('/api/jobs/<job_id>/cancel', methods=['POST'])
    def jobs_cancel_compat(job_id):
        """Job cancel endpoint with plural URL for JavaScript compatibility"""
        return cancel_job(job_id)
    
    # NEW ENDPOINTS that were missing:
    
    @app.route('/api/jobs/<job_id>/pause', methods=['POST'])
    def pause_job(job_id):
        """Pause a running job"""
        job = JobManager.get_job(job_id)
        if not job:
            return jsonify({'error': 'Job not found'}), 404
        
        if job['status'] != 'processing':
            return jsonify({'error': 'Job is not currently processing'}), 400
        
        try:
            JobManager.update_job(job_id, {
                'status': 'paused',
                'error_message': 'Paused by user'
            })
            
            logger.info(f"Job {job_id} paused by user")
            return jsonify({'success': True, 'message': 'Job paused'})
            
        except Exception as e:
            logger.error(f"Failed to pause job {job_id}: {e}")
            return jsonify({'error': str(e)}), 500
    
    @app.route('/api/jobs/<job_id>/start', methods=['POST'])
    def start_job_api(job_id):
        """Start or resume a job"""
        job = JobManager.get_job(job_id)
        if not job:
            return jsonify({'error': 'Job not found'}), 404
        
        if job['status'] not in ['uploaded', 'paused', 'failed']:
            return jsonify({'error': f'Job cannot be started. Current status: {job["status"]}'}, 400)
        
        try:
            # Update job status and start processing
            JobManager.update_job(job_id, {
                'status': 'processing',
                'error_message': None
            })
            
            # Import here to avoid circular imports
            from app.routes.web import start_threaded_processing
            start_threaded_processing(job_id)
            
            logger.info(f"Job {job_id} started by user")
            return jsonify({'success': True, 'message': 'Job started'})
            
        except Exception as e:
            logger.error(f"Failed to start job {job_id}: {e}")
            JobManager.update_job(job_id, {'status': 'failed', 'error_message': str(e)})
            return jsonify({'error': str(e)}), 500
    
    @app.route('/api/jobs/metrics')
    def jobs_metrics():
        """Get job metrics for dashboard"""
        try:
            all_jobs = JobManager.get_all_jobs()
            
            metrics = {
                'total': len(all_jobs),
                'processing': len([j for j in all_jobs if j['status'] == 'processing']),
                'completed': len([j for j in all_jobs if j['status'] == 'completed']),
                'failed': len([j for j in all_jobs if j['status'] == 'failed']),
                'queued': len([j for j in all_jobs if j['status'] in ['queued', 'uploaded']]),
                'paused': len([j for j in all_jobs if j['status'] == 'paused'])
            }
            
            return jsonify(metrics)
            
        except Exception as e:
            logger.error(f"Failed to get job metrics: {e}")
            return jsonify({'error': str(e)}), 500
    
    @app.route('/api/jobs/<job_id>/progress')
    def job_progress(job_id):
        """Get progress for a specific job"""
        job = JobManager.get_job(job_id)
        if not job:
            return jsonify({'error': 'Job not found'}), 404
        
        return jsonify({
            'job_id': job_id,
            'progress': job.get('progress', 0),
            'status': job['status'],
            'total_entities': job.get('total_entities', 0),
            'successful_matches': job.get('successful_matches', 0),
            'message': f"Processing {job.get('progress', 0)}% complete"
        })


def get_status_message(job):
    """Generate human-readable status message for a job"""
    status = job['status']
    progress = job['progress']
    
    messages = {
        'uploaded': 'File uploaded, waiting to start processing',
        'queued': 'Job queued for processing',
        'completed': 'Processing complete!',
        'failed': f"Processing failed: {job.get('error_message', 'Unknown error')}",
        'cancelled': 'Job was cancelled'
    }
    
    if status == 'processing':
        if progress < 20:
            return 'Reading CSV file...'
        elif progress < 40:
            return 'Extracting entities...'
        elif progress < 80:
            return 'Querying external authorities...'
        else:
            return 'Saving results...'
    
    return messages.get(status, 'Unknown status')