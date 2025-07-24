# File: app/routes/api.py (FIXED VERSION)
"""
API routes for the Metadata Reconciliation System.
Provides JSON endpoints for frontend interactions and system monitoring.
FIXED: Removed duplicate route definitions that were causing Flask errors.
"""

from flask import jsonify, request
import pandas as pd
from io import StringIO
import logging

from app.database import JobManager, ResultsManager, get_db_connection

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
            if file.filename == '':
                return jsonify({'error': 'No file selected'}), 400
            
            # Read a few rows to get column names
            content = file.read(2048).decode('utf-8', errors='ignore')
            file.seek(0)  # Reset file pointer
            
            # Parse CSV to get columns
            import io
            df_sample = pd.read_csv(io.StringIO(content), nrows=5)
            
            columns = df_sample.columns.tolist()
            sample_data = df_sample.head(3).to_dict('records')
            
            return jsonify({
                'columns': columns,
                'sample_data': sample_data,
                'total_columns': len(columns)
            })
            
        except Exception as e:
            logger.error(f"Error previewing CSV: {e}")
            return jsonify({'error': f'Failed to preview CSV: {str(e)}'}), 500

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

    @app.route('/api/jobs/<job_id>/status')
    def get_job_status(job_id):
        """Get current status of a processing job (SINGLE DEFINITION)"""
        try:
            job = JobManager.get_job(job_id)
            if not job:
                return jsonify({'error': 'Job not found'}), 404
            
            # Get actual match count from database for completed jobs
            if job['status'] == 'completed':
                try:
                    # Query database for actual successful matches count
                    with get_db_connection() as conn:
                        cursor = conn.cursor()
                        cursor.execute("""
                            SELECT COUNT(*) as total_entities,
                                COUNT(CASE WHEN EXISTS(
                                    SELECT 1 FROM matches 
                                    WHERE matches.result_id = results.id 
                                    AND matches.match_score > 0.5
                                ) THEN 1 END) as successful_matches
                            FROM results 
                            WHERE job_id = ?
                        """, (job_id,))
                        
                        result = cursor.fetchone()
                        actual_total = result['total_entities'] if result else 0
                        actual_matches = result['successful_matches'] if result else 0
                        
                        # Update job record if counts don't match
                        if actual_matches != job.get('successful_matches', 0):
                            logger.info(f"🔄 Updating job {job_id}: {actual_matches} matches (was {job.get('successful_matches', 0)})")
                            JobManager.update_job(job_id, {
                                'successful_matches': actual_matches,
                                'total_entities': actual_total
                            })
                            # Refresh job data
                            job = JobManager.get_job(job_id)
                            
                except Exception as e:
                    logger.error(f"Error getting match counts: {e}")
                    # Fall back to stored values
                    actual_matches = job.get('successful_matches', 0)
                    actual_total = job.get('total_entities', 0)
            else:
                actual_matches = job.get('successful_matches', 0)
                actual_total = job.get('total_entities', 0)
            
            response_data = {
                'status': job['status'],
                'progress': job.get('progress', 0),
                'message': f"Processing {actual_total} entities..." if job['status'] == 'processing' else 'Ready',
                'created_at': job.get('created_at'),
                'metrics': {
                    'total_entities': actual_total,
                    'successful_matches': actual_matches,
                    'processed_entities': actual_total if job['status'] == 'completed' else job.get('progress', 0) * actual_total // 100,
                    'match_rate': (actual_matches / actual_total * 100) if actual_total > 0 else 0
                }
            }
            
            return jsonify(response_data)
            
        except Exception as e:
            logger.error(f"Error getting job status: {e}")
            return jsonify({'error': str(e)}), 500

    @app.route('/api/jobs/<job_id>/progress')
    def get_job_progress(job_id):
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

    @app.route('/api/jobs/<job_id>/cancel', methods=['POST'])
    def cancel_job(job_id):
        """Cancel a running job"""
        job = JobManager.get_job(job_id)
        if not job:
            return jsonify({'error': 'Job not found'}), 404
        
        if job['status'] not in ['processing', 'queued', 'uploaded']:
            return jsonify({'error': 'Job cannot be cancelled'}), 400
        
        try:
            JobManager.update_job(job_id, {
                'status': 'cancelled',
                'error_message': 'Cancelled by user'
            })
            
            logger.info(f"Job {job_id} cancelled by user")
            return jsonify({'success': True, 'message': 'Job cancelled'})
            
        except Exception as e:
            logger.error(f"Failed to cancel job {job_id}: {e}")
            return jsonify({'error': str(e)}), 500

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

    @app.route('/api/statistics')
    def get_statistics():
        """Get system-wide statistics"""
        try:
            all_jobs = JobManager.get_all_jobs()
            
            # Calculate statistics
            total_jobs = len(all_jobs)
            completed_jobs = [j for j in all_jobs if j['status'] == 'completed']
            
            total_entities = sum(job.get('total_entities', 0) for job in completed_jobs)
            total_matches = sum(job.get('successful_matches', 0) for job in completed_jobs)
            
            avg_match_rate = (total_matches / total_entities * 100) if total_entities > 0 else 0
            
            return jsonify({
                'total_jobs': total_jobs,
                'completed_jobs': len(completed_jobs),
                'total_entities_processed': total_entities,
                'total_matches_found': total_matches,
                'average_match_rate': round(avg_match_rate, 1),
                'system_status': 'operational'
            })
            
        except Exception as e:
            logger.error(f"Failed to get statistics: {e}")
            return jsonify({'error': str(e)}), 500
    @app.route('/api/matches/<match_id>/approve', methods=['POST'])
    def approve_match(match_id):
        """Approve or reject a specific match"""
        try:
            data = request.get_json()
            if not data:
                return jsonify({'error': 'No data provided'}), 400
            
            entity_id = data.get('entity_id')
            approved = data.get('approved', True)
            
            if not entity_id:
                return jsonify({'error': 'entity_id is required'}), 400
            
            # You'll need to get the job_id - this might come from the request data
            # or you might need to look it up based on the entity_id
            job_id = data.get('job_id')  # If provided in frontend
            
            if not job_id:
                # Alternative: Look up job_id from the database based on entity_id
                # This requires a query to find which job contains this entity
                with get_db_connection() as conn:
                    cursor = conn.cursor()
                    cursor.execute('''
                    SELECT job_id FROM results WHERE entity_id = ? LIMIT 1
                    ''', (entity_id,))
                    result = cursor.fetchone()
                    if result:
                        job_id = result['job_id']
                    else:
                        return jsonify({'error': 'Could not find job for this entity'}), 404
            
            # Use the existing approve_match method from ResultsManager
            success = ResultsManager.approve_match(job_id, entity_id, match_id, approved)
            
            if success:
                logger.info(f"Match {match_id} {'approved' if approved else 'rejected'} for entity {entity_id}")
                return jsonify({
                    'success': True,
                    'message': f"Match {'approved' if approved else 'rejected'} successfully",
                    'match_id': match_id,
                    'entity_id': entity_id,
                    'approved': approved
                })
            else:
                return jsonify({'error': 'Failed to update match status'}), 500
                
        except Exception as e:
            logger.error(f"Error approving match {match_id}: {e}")
            return jsonify({'error': str(e)}), 500

def get_status_message(job):
    """Generate human-readable status message for a job"""
    status = job['status']
    progress = job.get('progress', 0)
    
    messages = {
        'uploaded': 'File uploaded, waiting to start processing',
        'queued': 'Job queued for processing',
        'completed': 'Processing complete!',
        'failed': f"Processing failed: {job.get('error_message', 'Unknown error')}",
        'cancelled': 'Job was cancelled',
        'paused': 'Job is paused'
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