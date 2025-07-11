# File: app/routes/api.py
from flask import jsonify, request
import pandas as pd
from io import StringIO
from app.database import JobManager, ResultsManager

# Import background job functions with fallback
try:
    from app.background_jobs import get_task_status
    BACKGROUND_JOBS_AVAILABLE = True
except ImportError:
    BACKGROUND_JOBS_AVAILABLE = False

def register_api_routes(app):
    
    @app.route('/api/preview_columns', methods=['POST'])
    def preview_columns():
        try:
            file = request.files['file']
            content = file.read().decode('utf-8')
            df = pd.read_csv(StringIO(content), nrows=0)
            
            return jsonify({
                'success': True,
                'columns': list(df.columns)
            })
        except Exception as e:
            return jsonify({
                'success': False,
                'error': str(e)
            })
    
    @app.route('/api/preview_file', methods=['POST'])
    def preview_file():
        try:
            file = request.files['file']
            content = file.read().decode('utf-8')
            df = pd.read_csv(StringIO(content), nrows=5)
            
            return jsonify({
                'success': True,
                'preview': {
                    'headers': list(df.columns),
                    'rows': df.values.tolist()
                }
            })
        except Exception as e:
            return jsonify({
                'success': False,
                'error': str(e)
            })
    
    @app.route('/api/job/<job_id>/status')
    def job_status(job_id):
        job = JobManager.get_job(job_id)
        if not job:
            return jsonify({'error': 'Job not found'}), 404
        
        response = {
            'status': job['status'],
            'progress': job['progress'],
            'message': get_status_message(job),
            'total_entities': job['total_entities'],
            'successful_matches': job['successful_matches'],
            'statistics': {
                'cache_hit_rate': 0.3  # This would come from the reconciliation engine
            },
            'error': job.get('error_message'),
            'background_available': BACKGROUND_JOBS_AVAILABLE
        }
        
        # If we have a background task, get its detailed status
        if BACKGROUND_JOBS_AVAILABLE and job.get('task_id'):
            try:
                task_status = get_task_status(job['task_id'])
                response['task_status'] = task_status
                
                # Use task progress if more recent than DB progress
                if task_status.get('percent', 0) > job['progress']:
                    response['progress'] = task_status['percent']
                    response['message'] = task_status.get('message', response['message'])
                
            except Exception as e:
                response['task_error'] = str(e)
        
        return jsonify(response)
    
    @app.route('/api/job/<job_id>/approve_match', methods=['POST'])
    def approve_match(job_id):
        try:
            data = request.get_json()
            entity_id = data.get('entity_id')
            match_id = data.get('match_id')
            approved = data.get('approved')
            
            if not all([entity_id, match_id, approved is not None]):
                return jsonify({
                    'success': False,
                    'error': 'Missing required parameters'
                }), 400
            
            # Update the match approval in database
            success = ResultsManager.approve_match(job_id, entity_id, match_id, approved)
            
            if success:
                return jsonify({
                    'success': True,
                    'message': f"Match {'approved' if approved else 'rejected'}"
                })
            else:
                return jsonify({
                    'success': False,
                    'error': 'Match not found or update failed'
                }), 404
                
        except Exception as e:
            return jsonify({
                'success': False,
                'error': str(e)
            }), 500
    
    @app.route('/api/system/status')
    def system_status():
        """Get system status information"""
        
        # Test Redis connection if background jobs are available
        redis_status = 'not_available'
        if BACKGROUND_JOBS_AVAILABLE:
            try:
                from app.background_jobs import celery_app
                # Try to ping Redis through Celery
                inspect = celery_app.control.inspect()
                active_workers = inspect.active()
                if active_workers:
                    redis_status = 'connected'
                else:
                    redis_status = 'no_workers'
            except Exception as e:
                redis_status = f'error: {str(e)}'
        
        return jsonify({
            'background_jobs_available': BACKGROUND_JOBS_AVAILABLE,
            'redis_status': redis_status,
            'database_connected': True,  # We could add a DB health check here
            'version': '1.0.0',
            'features': {
                'background_processing': BACKGROUND_JOBS_AVAILABLE,
                'real_time_progress': BACKGROUND_JOBS_AVAILABLE,
                'job_cancellation': BACKGROUND_JOBS_AVAILABLE,
                'database_storage': True,
                'multiple_export_formats': True,
                'threaded_fallback': True
            }
        })

def get_status_message(job):
    """Get a human-readable status message"""
    status = job['status']
    progress = job['progress']
    
    if status == 'uploaded':
        return 'Waiting to start processing...'
    elif status == 'queued':
        return 'Job queued for background processing...'
    elif status == 'processing':
        if progress < 30:
            return 'Parsing CSV file...'
        elif progress < 50:
            return 'Creating entities for reconciliation...'
        elif progress < 80:
            return 'Querying external authorities...'
        else:
            return 'Saving results...'
    elif status == 'completed':
        return 'Processing complete!'
    elif status == 'failed':
        return f"Processing failed: {job.get('error_message', 'Unknown error')}"
    elif status == 'cancelled':
        return 'Job was cancelled'
    else:
        return 'Unknown status'
        try:
            file = request.files['file']
            content = file.read().decode('utf-8')
            df = pd.read_csv(StringIO(content), nrows=0)
            
            return jsonify({
                'success': True,
                'columns': list(df.columns)
            })
        except Exception as e:
            return jsonify({
                'success': False,
                'error': str(e)
            })
    
    @app.route('/api/preview_file', methods=['POST'])
    def preview_file():
        try:
            file = request.files['file']
            content = file.read().decode('utf-8')
            df = pd.read_csv(StringIO(content), nrows=5)
            
            return jsonify({
                'success': True,
                'preview': {
                    'headers': list(df.columns),
                    'rows': df.values.tolist()
                }
            })
        except Exception as e:
            return jsonify({
                'success': False,
                'error': str(e)
            })
    
    @app.route('/api/job/<job_id>/status')
    def job_status(job_id):
        job = JobManager.get_job(job_id)
        if not job:
            return jsonify({'error': 'Job not found'}), 404
        
        return jsonify({
            'status': job['status'],
            'progress': job['progress'],
            'message': get_status_message(job),
            'total_entities': job['total_entities'],
            'successful_matches': job['successful_matches'],
            'statistics': {
                'cache_hit_rate': 0.3  # This would come from the reconciliation engine
            },
            'error': job.get('error_message')
        })
    
    @app.route('/api/job/<job_id>/approve_match', methods=['POST'])
    def approve_match(job_id):
        try:
            data = request.get_json()
            entity_id = data.get('entity_id')
            match_id = data.get('match_id')
            approved = data.get('approved')
            
            if not all([entity_id, match_id, approved is not None]):
                return jsonify({
                    'success': False,
                    'error': 'Missing required parameters'
                }), 400
            
            # Update the match approval in database
            success = ResultsManager.approve_match(job_id, entity_id, match_id, approved)
            
            if success:
                return jsonify({
                    'success': True,
                    'message': f"Match {'approved' if approved else 'rejected'}"
                })
            else:
                return jsonify({
                    'success': False,
                    'error': 'Match not found or update failed'
                }), 404
                
        except Exception as e:
            return jsonify({
                'success': False,
                'error': str(e)
            }), 500

def get_status_message(job):
    """Get a human-readable status message"""
    status = job['status']
    progress = job['progress']
    
    if status == 'uploaded':
        return 'Waiting to start processing...'
    elif status == 'processing':
        if progress < 30:
            return 'Parsing CSV file...'
        elif progress < 50:
            return 'Creating entities for reconciliation...'
        elif progress < 80:
            return 'Querying external authorities...'
        else:
            return 'Saving results...'
    elif status == 'completed':
        return 'Processing complete!'
    elif status == 'failed':
        return f"Processing failed: {job.get('error_message', 'Unknown error')}"
    else:
        return 'Unknown status'