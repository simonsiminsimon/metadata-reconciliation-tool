# File: app/routes/web_with_background.py
# This shows how to integrate background processing

from flask import render_template, request, redirect, url_for, flash, send_file, jsonify
from werkzeug.utils import secure_filename
import os
import uuid
import json
import pandas as pd
from datetime import datetime
from io import StringIO, BytesIO

# Import our components
from app.database import JobManager, ResultsManager

# Import background job functions
try:
    from app.background_jobs import process_reconciliation_job, get_task_status, cancel_task
    BACKGROUND_JOBS_AVAILABLE = True
except ImportError:
    # Fallback if Celery is not installed
    BACKGROUND_JOBS_AVAILABLE = False
    print("⚠️  Background jobs not available. Install Celery and Redis for background processing.")

def register_web_routes(app):
    
    @app.route('/')
    def index():
        return redirect(url_for('upload'))
    
    @app.route('/upload', methods=['GET', 'POST'])
    def upload():
        if request.method == 'POST':
            # Handle file upload
            if 'file' not in request.files:
                flash('No file selected', 'error')
                return redirect(request.url)
            
            file = request.files['file']
            if file.filename == '':
                flash('No file selected', 'error')
                return redirect(request.url)
            
            if file and file.filename.lower().endswith('.csv'):
                # Save file
                filename = secure_filename(file.filename)
                job_id = str(uuid.uuid4())
                filepath = os.path.join(app.config['UPLOAD_FOLDER'], f"{job_id}_{filename}")
                file.save(filepath)
                
                # Get form data
                entity_column = request.form.get('entity_column')
                type_column = request.form.get('type_column')
                context_columns = request.form.getlist('context_columns')
                data_sources = request.form.getlist('data_sources')
                confidence_threshold = float(request.form.get('confidence_threshold', 0.8))
                
                # Create job record in database
                job_data = {
                    'id': job_id,
                    'filename': filename,
                    'filepath': filepath,
                    'entity_column': entity_column,
                    'type_column': type_column,
                    'context_columns': context_columns,
                    'data_sources': data_sources,
                    'confidence_threshold': confidence_threshold
                }
                
                JobManager.create_job(job_data)
                
                # Start background processing if available
                if BACKGROUND_JOBS_AVAILABLE:
                    # Queue the reconciliation job
                    task = process_reconciliation_job.delay(job_id)
                    
                    # Store the task ID so we can check progress
                    JobManager.update_job(job_id, {
                        'status': 'queued',
                        'task_id': task.id
                    })
                    
                    flash(f'File uploaded successfully! Processing started in background.', 'success')
                else:
                    # Fallback to immediate processing
                    flash(f'File uploaded successfully! Processing will start immediately.', 'success')
                
                return redirect(url_for('processing', job_id=job_id))
            else:
                flash('Please upload a CSV file', 'error')
        
        return render_template('upload.html')
    
    @app.route('/jobs')
    def jobs_list():
        jobs = JobManager.get_all_jobs()
        return render_template('jobs.html', jobs=jobs)
    
    @app.route('/processing/<job_id>')
    def processing(job_id):
        job = JobManager.get_job(job_id)
        if not job:
            flash('Job not found', 'error')
            return redirect(url_for('jobs_list'))
        
        # If background jobs are not available, fall back to immediate processing
        if not BACKGROUND_JOBS_AVAILABLE and job['status'] == 'uploaded':
            try:
                from app.routes.web_with_background import process_job_with_reconciliation
                process_job_with_reconciliation(job_id)
            except Exception as e:
                JobManager.update_job(job_id, {
                    'status': 'failed',
                    'error_message': str(e)
                })
                flash(f'Processing failed: {e}', 'error')
        
        return render_template('processing.html', job=job)
    
    @app.route('/review/<job_id>')
    def review(job_id):
        job = JobManager.get_job(job_id)
        if not job:
            flash('Job not found', 'error')
            return redirect(url_for('jobs_list'))
        
        # Get paginated results from database
        page = int(request.args.get('page', 1))
        per_page = 10
        
        results, total_count = ResultsManager.get_results(job_id, page, per_page)
        
        # Calculate pagination
        total_pages = (total_count + per_page - 1) // per_page
        pagination = {
            'page': page,
            'pages': total_pages,
            'total': total_count,
            'has_prev': page > 1,
            'has_next': page < total_pages,
            'prev_num': page - 1 if page > 1 else None,
            'next_num': page + 1 if page < total_pages else None
        }
        
        return render_template('review.html', job=job, results=results, pagination=pagination)
    
    @app.route('/export/<job_id>')
    def export_page(job_id):
        job = JobManager.get_job(job_id)
        if not job:
            flash('Job not found', 'error')
            return redirect(url_for('jobs_list'))
        
        return render_template('export.html', job=job)
    
    @app.route('/download/<job_id>/<format>')
    def download_results(job_id, format):
        """Download reconciliation results in various formats"""
        job = JobManager.get_job(job_id)
        if not job:
            flash('Job not found', 'error')
            return redirect(url_for('jobs_list'))
        
        # Get all results for export (no pagination)
        results, _ = ResultsManager.get_results(job_id, page=1, per_page=10000)
        
        if not results:
            flash('No results found for this job', 'error')
            return redirect(url_for('export_page', job_id=job_id))
        
        try:
            # Import export functions
            from app.routes.web_with_background import download_csv, download_json, download_rdf, download_report
            
            if format == 'csv':
                return download_csv(job, results)
            elif format == 'json':
                return download_json(job, results)
            elif format == 'rdf':
                return download_rdf(job, results)
            elif format == 'report':
                return download_report(job, results)
            else:
                flash(f'Unsupported format: {format}', 'error')
                return redirect(url_for('export_page', job_id=job_id))
                
        except Exception as e:
            flash(f'Export failed: {e}', 'error')
            return redirect(url_for('export_page', job_id=job_id))
    
    # NEW: Background job management routes
    
    @app.route('/cancel_job/<job_id>', methods=['POST'])
    def cancel_job(job_id):
        """Cancel a running background job"""
        if not BACKGROUND_JOBS_AVAILABLE:
            flash('Background job cancellation not available', 'error')
            return redirect(url_for('processing', job_id=job_id))
        
        job = JobManager.get_job(job_id)
        if not job:
            flash('Job not found', 'error')
            return redirect(url_for('jobs_list'))
        
        task_id = job.get('task_id')
        if task_id and job['status'] in ['queued', 'processing']:
            try:
                cancel_task(task_id)
                JobManager.update_job(job_id, {
                    'status': 'cancelled',
                    'completed_at': datetime.now().isoformat()
                })
                flash('Job cancelled successfully', 'success')
            except Exception as e:
                flash(f'Failed to cancel job: {e}', 'error')
        else:
            flash('Job cannot be cancelled (not running)', 'warning')
        
        return redirect(url_for('processing', job_id=job_id))
    
    @app.route('/restart_job/<job_id>', methods=['POST'])
    def restart_job(job_id):
        """Restart a failed job"""
        job = JobManager.get_job(job_id)
        if not job:
            flash('Job not found', 'error')
            return redirect(url_for('jobs_list'))
        
        if job['status'] in ['failed', 'cancelled']:
            if BACKGROUND_JOBS_AVAILABLE:
                # Start new background task
                task = process_reconciliation_job.delay(job_id)
                JobManager.update_job(job_id, {
                    'status': 'queued',
                    'task_id': task.id,
                    'progress': 0,
                    'error_message': None
                })
                flash('Job restarted successfully', 'success')
            else:
                # Reset status for immediate processing
                JobManager.update_job(job_id, {
                    'status': 'uploaded',
                    'progress': 0,
                    'error_message': None
                })
                flash('Job reset for immediate processing', 'success')
        else:
            flash('Job cannot be restarted (not failed or cancelled)', 'warning')
        
        return redirect(url_for('processing', job_id=job_id))


# Update API routes to handle background jobs

def register_api_routes_with_background(app):
    
    # Include all the existing API routes
    from app.routes.api import register_api_routes
    register_api_routes(app)
    
    # Add background job specific API endpoints
    
    @app.route('/api/job/<job_id>/background_status')
    def background_job_status(job_id):
        """Get detailed status including background task progress"""
        job = JobManager.get_job(job_id)
        if not job:
            return jsonify({'error': 'Job not found'}), 404
        
        response = {
            'job_id': job_id,
            'status': job['status'],
            'progress': job['progress'],
            'total_entities': job['total_entities'],
            'successful_matches': job['successful_matches'],
            'error_message': job.get('error_message'),
            'background_available': BACKGROUND_JOBS_AVAILABLE
        }
        
        # If we have a background task, get its status too
        if BACKGROUND_JOBS_AVAILABLE and job.get('task_id'):
            try:
                task_status = get_task_status(job['task_id'])
                response['task_status'] = task_status
                
                # Use task progress if more recent than DB progress
                if task_status.get('percent', 0) > job['progress']:
                    response['progress'] = task_status['percent']
                    response['message'] = task_status.get('message', '')
                
            except Exception as e:
                response['task_error'] = str(e)
        
        return jsonify(response)
    
    @app.route('/api/system/status')
    def system_status():
        """Get system status information"""
        return jsonify({
            'background_jobs_available': BACKGROUND_JOBS_AVAILABLE,
            'database_connected': True,  # We could add a DB health check here
            'version': '1.0.0',
            'features': {
                'background_processing': BACKGROUND_JOBS_AVAILABLE,
                'real_time_progress': BACKGROUND_JOBS_AVAILABLE,
                'job_cancellation': BACKGROUND_JOBS_AVAILABLE,
                'database_storage': True,
                'multiple_export_formats': True
            }
        })


# Helper function to check if system is ready
def check_system_health():
    """
    Check if all system components are working.
    This is useful for deployment and monitoring.
    """
    health = {
        'status': 'healthy',
        'components': {},
        'timestamp': datetime.now().isoformat()
    }
    
    # Check database
    try:
        from app.database import JobManager
        test_jobs = JobManager.get_all_jobs()
        health['components']['database'] = 'healthy'
    except Exception as e:
        health['components']['database'] = f'unhealthy: {e}'
        health['status'] = 'unhealthy'
    
    # Check background jobs
    if BACKGROUND_JOBS_AVAILABLE:
        try:
            # You could ping Redis or check Celery workers here
            health['components']['background_jobs'] = 'available'
        except Exception as e:
            health['components']['background_jobs'] = f'error: {e}'
    else:
        health['components']['background_jobs'] = 'not_available'
    
    # Check file system
    try:
        os.makedirs('data/input', exist_ok=True)
        health['components']['file_system'] = 'healthy'
    except Exception as e:
        health['components']['file_system'] = f'unhealthy: {e}'
        health['status'] = 'unhealthy'
    
    return health


if __name__ == '__main__':
    # Test system health
    health = check_system_health()
    print("System Health Check:")
    print(json.dumps(health, indent=2))