# File: app/routes/api.py (COMPLETE FIXED VERSION)
"""
API routes for the Metadata Reconciliation System.
FIXED: Complete implementation of all job management endpoints.
"""

from flask import jsonify, request
import pandas as pd
from io import StringIO
import logging
import os

from app.database import JobManager, ResultsManager

# Set up logging
logger = logging.getLogger(__name__)

# Check background job availability
try:
    from app.background_jobs import celery_app, process_reconciliation_job
    BACKGROUND_JOBS_AVAILABLE = True
except ImportError:
    BACKGROUND_JOBS_AVAILABLE = False


def register_api_routes(app):
    """Register all API routes with the Flask app"""
    
    @app.route('/api/system_status')
    def system_status():
        """Get current system status"""
        try:
            jobs = JobManager.get_all_jobs()
            processing_count = len([j for j in jobs if j['status'] == 'processing'])
            
            return jsonify({
                'status': 'operational',
                'background_jobs_available': BACKGROUND_JOBS_AVAILABLE,
                'total_jobs': len(jobs),
                'processing_jobs': processing_count,
                'queue_count': processing_count,
                'timestamp': pd.Timestamp.now().isoformat()
            })
        except Exception as e:
            logger.error(f"System status check failed: {e}")
            return jsonify({'status': 'error', 'message': str(e)}), 500
    
    # FIXED: Complete job deletion endpoint
    @app.route('/api/jobs/<job_id>', methods=['DELETE'])
    def delete_job(job_id):
        """Delete a job and its associated files"""
        try:
            job = JobManager.get_job(job_id)
            if not job:
                return jsonify({'error': 'Job not found'}), 404
            
            # Cancel if running
            if job['status'] == 'processing' and BACKGROUND_JOBS_AVAILABLE:
                try:
                    if job.get('task_id'):
                        celery_app.control.revoke(job['task_id'], terminate=True)
                except Exception as e:
                    logger.warning(f"Could not cancel Celery task: {e}")
            
            # Delete associated files
            try:
                if job.get('filepath') and os.path.exists(job['filepath']):
                    os.remove(job['filepath'])
                    logger.info(f"Deleted file: {job['filepath']}")
            except Exception as e:
                logger.warning(f"Could not delete file: {e}")
            
            # Delete from database
            success = JobManager.delete_job(job_id)
            if success:
                logger.info(f"Job {job_id} deleted successfully")
                return jsonify({'success': True, 'message': 'Job deleted successfully'})
            else:
                return jsonify({'error': 'Failed to delete job from database'}), 500
                
        except Exception as e:
            logger.error(f"Error deleting job {job_id}: {e}")
            return jsonify({'error': str(e)}), 500
    
    # FIXED: Complete job start endpoint
    @app.route('/api/jobs/<job_id>/start', methods=['POST'])
    def start_job(job_id):
        """Start or resume a job"""
        try:
            job = JobManager.get_job(job_id)
            if not job:
                return jsonify({'error': 'Job not found'}), 404
            
            if job['status'] not in ['uploaded', 'paused', 'failed']:
                return jsonify({
                    'error': f'Job cannot be started. Current status: {job["status"]}'
                }), 400
            
            # Update job status
            JobManager.update_job(job_id, {
                'status': 'queued',
                'error_message': None,
                'progress': 0
            })
            
            # Start processing
            if BACKGROUND_JOBS_AVAILABLE:
                # Use Celery for background processing
                task = process_reconciliation_job.delay(job_id)
                JobManager.update_job(job_id, {
                    'task_id': task.id,
                    'status': 'processing'
                })
                logger.info(f"Job {job_id} started with Celery task {task.id}")
            else:
                # Use threaded processing
                from app.routes.web import start_threaded_processing
                start_threaded_processing(job_id)
                logger.info(f"Job {job_id} started with threaded processing")
            
            return jsonify({'success': True, 'message': 'Job started successfully'})
            
        except Exception as e:
            logger.error(f"Error starting job {job_id}: {e}")
            JobManager.update_job(job_id, {'status': 'failed', 'error_message': str(e)})
            return jsonify({'error': str(e)}), 500
    
    # FIXED: Job status endpoint
    @app.route('/api/jobs/<job_id>/status')
    def get_job_status(job_id):
        """Get detailed status of a specific job"""
        try:
            job = JobManager.get_job(job_id)
            if not job:
                return jsonify({'error': 'Job not found'}), 404
            
            # Get additional status info for processing jobs
            status_info = {
                'job_id': job_id,
                'status': job['status'],
                'progress': job.get('progress', 0),
                'total_entities': job.get('total_entities', 0),
                'successful_matches': job.get('successful_matches', 0),
                'error_message': job.get('error_message'),
                'created_at': job.get('created_at'),
                'message': get_status_message(job)
            }
            
            # Add Celery task info if available
            if BACKGROUND_JOBS_AVAILABLE and job.get('task_id'):
                try:
                    from app.background_jobs import get_task_status
                    task_status = get_task_status(job['task_id'])
                    status_info['task_status'] = task_status
                except Exception as e:
                    logger.warning(f"Could not get task status: {e}")
            
            return jsonify(status_info)
            
        except Exception as e:
            logger.error(f"Error getting job status {job_id}: {e}")
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

    @app.route('/api/jobs/<job_id>/status')
    def get_job_status(job_id):
        """Get current status of a processing job"""
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
                    'successful_matches': actual_matches,  # This will fix the TOTAL_MATCHES log
                    'processed_entities': actual_total if job['status'] == 'completed' else job.get('progress', 0) * actual_total // 100,
                    'match_rate': (actual_matches / actual_total * 100) if actual_total > 0 else 0
                }
            }
            
            return jsonify(response_data)
            
        except Exception as e:
            logger.error(f"Error getting job status: {e}")
            return jsonify({'error': str(e)}), 500
    
    @app.route('/api/jobs/<job_id>/cancel', methods=['POST'])
    def cancel_job(job_id):
        """Cancel a running job"""
        try:
            job = JobManager.get_job(job_id)
            if not job:
                return jsonify({'error': 'Job not found'}), 404
            
            if job['status'] not in ['queued', 'processing']:
                return jsonify({
                    'error': f'Job cannot be cancelled. Current status: {job["status"]}'
                }), 400
            
            # Cancel Celery task if applicable
            if BACKGROUND_JOBS_AVAILABLE and job.get('task_id'):
                try:
                    celery_app.control.revoke(job['task_id'], terminate=True)
                    logger.info(f"Cancelled Celery task {job['task_id']}")
                except Exception as e:
                    logger.warning(f"Could not cancel Celery task: {e}")
            
            # Update job status
            JobManager.update_job(job_id, {
                'status': 'cancelled',
                'error_message': 'Cancelled by user'
            })
            
            return jsonify({'success': True, 'message': 'Job cancelled successfully'})
            
        except Exception as e:
            logger.error(f"Error cancelling job {job_id}: {e}")
            return jsonify({'error': str(e)}), 500
    
    # FIXED: Job retry endpoint
    @app.route('/api/jobs/<job_id>/retry', methods=['POST'])
    def retry_job(job_id):
        """Retry a failed job"""
        try:
            job = JobManager.get_job(job_id)
            if not job:
                return jsonify({'error': 'Job not found'}), 404
            
            if job['status'] not in ['failed', 'cancelled']:
                return jsonify({
                    'error': f'Job cannot be retried. Current status: {job["status"]}'
                }), 400
            
            # Reset job to initial state
            JobManager.update_job(job_id, {
                'status': 'uploaded',
                'progress': 0,
                'error_message': None,
                'task_id': None,
                'total_entities': 0,
                'successful_matches': 0
            })
            
            return jsonify({'success': True, 'message': 'Job reset for retry'})
            
        except Exception as e:
            logger.error(f"Error retrying job {job_id}: {e}")
            return jsonify({'error': str(e)}), 500
    
    # Job metrics endpoint
    @app.route('/api/jobs/metrics')
    def get_jobs_metrics():
        """Get job metrics for dashboard"""
        try:
            jobs = JobManager.get_all_jobs()
            
            metrics = {
                'total': len(jobs),
                'processing': len([j for j in jobs if j['status'] == 'processing']),
                'completed': len([j for j in jobs if j['status'] == 'completed']),
                'failed': len([j for j in jobs if j['status'] == 'failed']),
                'queued': len([j for j in jobs if j['status'] in ['queued', 'uploaded']]),
                'cancelled': len([j for j in jobs if j['status'] == 'cancelled'])
            }
            
            return jsonify(metrics)
            
        except Exception as e:
            logger.error(f"Error getting job metrics: {e}")
            return jsonify({'error': str(e)}), 500
    
    # CSV preview endpoint
    @app.route('/api/preview_csv', methods=['POST'])
    def preview_csv():
        """Preview CSV columns and sample data"""
        try:
            if 'file' not in request.files:
                return jsonify({'error': 'No file provided'}), 400
            
            file = request.files['file']
            
            # Read first few lines
            content = file.read(2048).decode('utf-8', errors='ignore')
            file.seek(0)  # Reset file pointer
            
            lines = content.split('\n')
            if not lines:
                return jsonify({'error': 'Empty file'}), 400
            
            # Parse header
            header = lines[0].strip()
            columns = [col.strip().strip('"\'') for col in header.split(',')]
            
            # Get sample data
            sample_data = []
            for line in lines[1:4]:  # Get up to 3 sample rows
                if line.strip():
                    values = [val.strip().strip('"\'') for val in line.split(',')]
                    # Pad with empty strings if not enough values
                    while len(values) < len(columns):
                        values.append('')
                    sample_data.append(dict(zip(columns, values[:len(columns)])))
            
            return jsonify({
                'success': True,
                'columns': columns,
                'sample_data': sample_data,
                'column_count': len(columns)
            })
            
        except Exception as e:
            logger.error(f"CSV preview failed: {e}")
            return jsonify({'error': str(e)}), 500


def get_status_message(job):
    """Generate human-readable status message for a job"""
    status = job['status']
    progress = job.get('progress', 0)
    
    messages = {
        'uploaded': 'File uploaded, ready to start processing',
        'queued': 'Job queued for processing',
        'completed': 'Processing completed successfully!',
        'failed': f"Processing failed: {job.get('error_message', 'Unknown error')}",
        'cancelled': 'Job was cancelled by user'
    }
    
    if status == 'processing':
        if progress < 20:
            return 'Reading and validating CSV file...'
        elif progress < 40:
            return 'Extracting entities from data...'
        elif progress < 80:
            return 'Querying external authority sources...'
        else:
            return 'Finalizing results and saving...'
    
    return messages.get(status, f'Status: {status}')