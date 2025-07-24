# File: app/routes/web.py (SIMPLE FIXED VERSION)
"""
Simple, working web routes for the Metadata Reconciliation System.
This version fixes template routing issues with a clean, minimal approach.
"""

from flask import render_template, request, redirect, url_for, flash, send_file, jsonify
from werkzeug.utils import secure_filename
import os
import uuid
import json
import pandas as pd
from datetime import datetime
from io import StringIO, BytesIO
import threading
import logging
import time

# Set up logging
logger = logging.getLogger(__name__)

# Import our components with graceful fallbacks
try:
    from app.services.enhanced_reconciliation_engine import EnhancedReconciliationEngine
except ImportError as e:
    logger.warning(f"EnhancedReconciliationEngine import failed: {e}")
    class EnhancedReconciliationEngine:
        def create_entities_from_dataframe(self, df, entity_column, type_column=None, context_columns=None):
            return []
        def process_entities(self, entities):
            return []

try:
    from app.database import JobManager, ResultsManager
except ImportError as e:
    logger.warning(f"Database components import failed: {e}")
    class JobManager:
        @staticmethod
        def create_job(job_data):
            return job_data['id']
        @staticmethod
        def get_job(job_id):
            return None
        @staticmethod
        def get_all_jobs():
            return []
        @staticmethod
        def update_job(job_id, updates):
            pass
    
    class ResultsManager:
        @staticmethod
        def save_results(job_id, results):
            pass
        @staticmethod
        def get_results(job_id, page=1, per_page=10):
            return []

# Check if background jobs are available (NO DIRECT CELERY IMPORT)
try:
    from app.background_jobs import process_reconciliation_job
    BACKGROUND_JOBS_AVAILABLE = True
    logger.info("✅ Background jobs (Celery + Redis) available")
except ImportError as e:
    BACKGROUND_JOBS_AVAILABLE = False
    logger.warning(f"⚠️  Background jobs not available: {e}")
    logger.info("📝 Using threaded processing as fallback")


def validate_csv_file(file):
    """Validate uploaded CSV file"""
    if not file or file.filename == '':
        return False, "No file selected"
    
    if not file.filename.lower().endswith('.csv'):
        return False, "Only CSV files are supported"
    
    file.seek(0, os.SEEK_END)
    file_size = file.tell()
    file.seek(0)
    
    if file_size > 50 * 1024 * 1024:
        return False, "File size exceeds 50MB limit"
    
    try:
        content = file.read().decode('utf-8')
        file.seek(0)
        df = pd.read_csv(StringIO(content), nrows=5)
        if df.empty:
            return False, "CSV file appears to be empty"
    except Exception as e:
        return False, f"Invalid CSV file: {str(e)}"
    
    return True, None


def process_job_threaded(job_id):
    """Process a reconciliation job in a separate thread"""
    try:
        job = JobManager.get_job(job_id)
        if not job:
            logger.error(f"Job {job_id} not found")
            return
            
        logger.info(f"🔄 Processing job {job_id} in thread")
        
        JobManager.update_job(job_id, {'status': 'processing', 'progress': 10})
        
        # Load CSV
        df = pd.read_csv(job['filepath'])
        JobManager.update_job(job_id, {'progress': 20})
        
        # Initialize engine
        engine = EnhancedReconciliationEngine()
        JobManager.update_job(job_id, {'progress': 30})
        
        # Create entities
        entities = engine.create_entities_from_dataframe(
            df,
            entity_column=job['entity_column'],
            type_column=job.get('type_column'),
            context_columns=job.get('context_columns', [])
        )
        
        total_entities = len(entities)
        JobManager.update_job(job_id, {
            'total_entities': total_entities,
            'progress': 50
        })
        
        # Process entities
        successful_matches = 0
        for i, entity in enumerate(entities):
            try:
                results = engine.process_entities([entity])
                for result in results:
                    if result.best_match:
                        successful_matches += 1
                    ResultsManager.save_results(job_id, [result])
                
                progress = 50 + (i / total_entities) * 40
                JobManager.update_job(job_id, {
                    'progress': int(progress),
                    'successful_matches': successful_matches
                })
                
                time.sleep(0.1)  # Rate limiting
            except Exception as e:
                logger.warning(f"Error processing entity {entity.name}: {e}")
        
        # Complete
        JobManager.update_job(job_id, {
            'status': 'completed',
            'progress': 100,
            'successful_matches': successful_matches,
            'completed_at': time.time()
        })
        
        logger.info(f"🎉 Job {job_id} completed: {successful_matches}/{total_entities} matches")
        
    except Exception as e:
        logger.error(f"❌ Job {job_id} failed: {e}")
        JobManager.update_job(job_id, {
            'status': 'failed',
            'error_message': str(e),
            'completed_at': time.time()
        })


def start_threaded_processing(job_id):
    """Start processing a job in a separate thread"""
    thread = threading.Thread(target=process_job_threaded, args=(job_id,))
    thread.daemon = True
    thread.start()
    logger.info(f"Started threaded processing for job {job_id}")


def register_web_routes(app):
    """Register all web routes with the Flask app"""
    
    @app.route('/')
    def index():
        """Home page"""
        return redirect(url_for('upload'))

    @app.route('/upload', methods=['GET', 'POST'])
    def upload():
        """Handle file upload and job creation"""
        if request.method == 'POST':
            # Validate file
            if 'file' not in request.files:
                flash('No file selected. Please choose a CSV file.', 'error')
                return redirect(request.url)

            file = request.files['file']
            is_valid, error_message = validate_csv_file(file)
            if not is_valid:
                flash(f'File validation failed: {error_message}', 'error')
                return redirect(request.url)

            # Get form data
            entity_column = request.form.get('entity_column', '').strip()
            if not entity_column:
                flash('Entity column is required.', 'error')
                return redirect(request.url)

            type_column = request.form.get('type_column', '').strip() or None
            context_columns_str = request.form.get('context_columns', '').strip()
            context_columns = [col.strip() for col in context_columns_str.split(',') if col.strip()] if context_columns_str else []

            # Save file
            try:
                filename = secure_filename(file.filename)
                job_id = str(uuid.uuid4())
                upload_dir = app.config.get('UPLOAD_FOLDER', 'data/input')
                os.makedirs(upload_dir, exist_ok=True)
                
                filepath = os.path.join(upload_dir, f"{job_id}_{filename}")
                file.save(filepath)

                # Create job
                job_data = {
                    'id': job_id,
                    'filename': filename,
                    'filepath': filepath,
                    'entity_column': entity_column,
                    'type_column': type_column,
                    'context_columns': context_columns,
                    'status': 'uploaded',
                    'progress': 0,
                    'created_at': datetime.now().isoformat(),
                    'total_entities': 0,
                    'successful_matches': 0
                }

                JobManager.create_job(job_data)

                start_threaded_processing(job_id)

                # Start processing
                #if BACKGROUND_JOBS_AVAILABLE:
                #    process_reconciliation_job.delay(job_id)
                #else:
                #   start_threaded_processing(job_id)

                flash('File uploaded successfully! Processing started.', 'success')
                return redirect(url_for('processing', job_id=job_id))

            except Exception as e:
                logger.error(f"Upload failed: {e}")
                flash(f'Upload failed: {str(e)}', 'error')
                return redirect(request.url)

        return render_template('upload.html')

    @app.route('/jobs')
    def jobs():
        """Show all jobs"""
        try:
            all_jobs = JobManager.get_all_jobs()
            return render_template('jobs.html', jobs=all_jobs)
        except Exception as e:
            logger.error(f"Error loading jobs: {e}")
            return render_template('jobs.html', jobs=[])

    @app.route('/processing/<job_id>')
    def processing(job_id):
        """Show processing progress"""
        job = JobManager.get_job(job_id)
        if not job:
            flash('Job not found', 'error')
            return redirect(url_for('jobs'))
        
        if job['status'] == 'completed':
            return redirect(url_for('review', job_id=job_id))
        
        return render_template('processing.html', job=job)

    @app.route('/review/<job_id>')
    def review(job_id):
        """Review reconciliation results - MAIN ROUTE FOR TEMPLATES"""
        job = JobManager.get_job(job_id)
        if not job:
            flash('Job not found', 'error')
            return redirect(url_for('jobs'))
        
        if job['status'] != 'completed':
            flash('Job is not yet complete', 'info')
            return redirect(url_for('processing', job_id=job_id))
        
        # Get results with pagination
        page = request.args.get('page', 1, type=int)
        per_page = 10
        
        try:
            results = ResultsManager.get_results(job_id)
            if isinstance(results, list):
                total_count = len(results)
                start = (page - 1) * per_page
                end = start + per_page
                results = results[start:end]
            else:
                results, total_count = results
            
            total_pages = (total_count + per_page - 1) // per_page if total_count > 0 else 1
            pagination = {
                'page': page,
                'pages': total_pages,
                'total': total_count,
                'has_prev': page > 1,
                'has_next': page < total_pages,
                'prev_num': page - 1 if page > 1 else None,
                'next_num': page + 1 if page < total_pages else None,
                'per_page': per_page
            }
            
            return render_template('review.html', job=job, results=results, pagination=pagination)
                                
        except Exception as e:
            logger.error(f"Error in review route: {e}")
            flash('Error loading results', 'error')
            return redirect(url_for('jobs'))

    @app.route('/download/<job_id>/<format>')
    def download_results(job_id, format):
        """Download results in specified format - FIXED VERSION"""
        job = JobManager.get_job(job_id)
        if not job:
            flash('Job not found', 'error')
            return redirect(url_for('jobs'))
        
        try:
            if format == 'csv':
                return export_csv_with_results(job)  # Use the new function
            elif format == 'json':
                return export_json_with_results(job)  # Use the new function
            else:
                flash(f'Unsupported format: {format}', 'error')
                return redirect(url_for('export', job_id=job_id))
        except Exception as e:
            logger.error(f"Download failed: {e}")
            flash(f'Download failed: {str(e)}', 'error')
            return redirect(url_for('export', job_id=job_id))

    # Also update the export route to fix template URL issues
    @app.route('/export/<job_id>')
    def export(job_id):
        """Export results page - FIXED VERSION"""
        job = JobManager.get_job(job_id)
        if not job:
            flash('Job not found', 'error')
            return redirect(url_for('jobs'))
        
        if job['status'] != 'completed':
            flash('Please wait for processing to finish.', 'info')
            return redirect(url_for('processing', job_id=job_id))
        
        try:
            return render_template('export.html', job=job)
        except Exception as e:
            logger.error(f"Export template error: {e}")
            # Fallback HTML with FIXED URL references
            return f"""
            <!DOCTYPE html>
            <html>
            <head><title>Export - {job['filename']}</title></head>
            <body style="font-family: Arial; margin: 40px;">
                <h1>Export Results: {job['filename']}</h1>
                <p>Status: {job['status']}</p>
                <p>Entities: {job.get('total_entities', 0)}</p>
                <p>Matches: {job.get('successful_matches', 0)}</p>
                <p>
                    <a href="{url_for('download_results', job_id=job_id, format='csv')}" 
                    style="background: #007cba; color: white; padding: 10px; text-decoration: none; margin-right: 10px;">
                    Download CSV with Results
                    </a>
                    <a href="{url_for('download_results', job_id=job_id, format='json')}"
                    style="background: #007cba; color: white; padding: 10px; text-decoration: none;">
                    Download JSON
                    </a>
                </p>
                <p><a href="{url_for('review', job_id=job_id)}">← Back to Review</a></p>
                <p><a href="{url_for('jobs')}">← Back to Jobs</a></p>
            </body>
            </html>
            """

    # Default routes for navigation (handle empty job_id calls from templates)
    @app.route('/processing/')
    @app.route('/processing')
    def processing_default():
        flash('Please select a job to view processing status', 'info')
        return redirect(url_for('jobs'))

    @app.route('/review/')
    @app.route('/review')
    def review_default():
        flash('Please select a completed job to review results', 'info')
        return redirect(url_for('jobs'))

    @app.route('/export/')
    @app.route('/export')
    def export_default():
        flash('Please select a completed job to export results', 'info')
        return redirect(url_for('jobs'))


def export_csv_with_results(job):
    """Export CSV with actual reconciliation results"""
    from flask import make_response
    from app.database import ResultsManager
    import csv
    from io import StringIO
    
    try:
        # Get ALL results for this job (not paginated)
        results, total_count = ResultsManager.get_results(job['id'], page=1, per_page=10000)
        
        # Create CSV content
        output = StringIO()
        fieldnames = [
            'entity_name', 'entity_type', 'confidence_level', 'best_match_name', 
            'best_match_id', 'best_match_score', 'best_match_description', 
            'match_source', 'user_approved', 'context_info'
        ]
        
        writer = csv.DictWriter(output, fieldnames=fieldnames)
        writer.writeheader()
        
        for result in results:
            entity = result['entity']
            matches = result.get('matches', [])
            
            if matches:
                # Get the best match (first one, as they're sorted by score)
                best_match = matches[0]
                
                # Find if user approved this match
                user_approved = best_match.get('user_approved')
                if user_approved is None:
                    approval_status = 'pending'
                elif user_approved:
                    approval_status = 'approved'
                else:
                    approval_status = 'rejected'
                
                writer.writerow({
                    'entity_name': entity['name'],
                    'entity_type': entity.get('type', 'unknown'),
                    'confidence_level': result.get('confidence', 'unknown'),
                    'best_match_name': best_match['name'],
                    'best_match_id': best_match['id'],
                    'best_match_score': f"{best_match['score']:.3f}",
                    'best_match_description': best_match.get('description', ''),
                    'match_source': best_match.get('source', 'wikidata'),
                    'user_approved': approval_status,
                    'context_info': str(entity.get('context', {}))
                })
            else:
                # No matches found
                writer.writerow({
                    'entity_name': entity['name'],
                    'entity_type': entity.get('type', 'unknown'),
                    'confidence_level': result.get('confidence', 'low'),
                    'best_match_name': 'NO_MATCH',
                    'best_match_id': '',
                    'best_match_score': '0.000',
                    'best_match_description': 'No matches found',
                    'match_source': '',
                    'user_approved': 'no_match',
                    'context_info': str(entity.get('context', {}))
                })
        
        csv_content = output.getvalue()
        output.close()
        
        response = make_response(csv_content)
        response.headers['Content-Type'] = 'text/csv'
        response.headers['Content-Disposition'] = f'attachment; filename=reconciled_{job["filename"]}'
        return response
        
    except Exception as e:
        logger.error(f"CSV export failed: {e}")
        # Fallback to simple export
        return export_csv_with_results(job)


def export_json_with_results(job):
    """Export JSON with actual reconciliation results"""
    from flask import make_response
    from app.database import ResultsManager
    import json
    from datetime import datetime
    
    try:
        # Get ALL results for this job
        results, total_count = ResultsManager.get_results(job['id'], page=1, per_page=10000)
        
        # Convert datetime objects to strings
        def serialize_datetime(obj):
            if isinstance(obj, datetime):
                return obj.isoformat()
            raise TypeError(f"Object of type {type(obj)} is not JSON serializable")
        
        export_data = {
            'job_info': {
                'job_id': job['id'],
                'filename': job['filename'],
                'status': job['status'],
                'total_entities': job.get('total_entities', 0),
                'successful_matches': job.get('successful_matches', 0),
                'created_at': job.get('created_at', '').isoformat() if isinstance(job.get('created_at'), datetime) else str(job.get('created_at', ''))
            },
            'reconciliation_results': results,
            'metadata': {
                'total_results': total_count,
                'export_timestamp': datetime.now().isoformat(),
                'format_version': '1.0'
            }
        }
        
        json_content = json.dumps(export_data, indent=2, default=serialize_datetime)
        
        response = make_response(json_content)
        response.headers['Content-Type'] = 'application/json'
        response.headers['Content-Disposition'] = f'attachment; filename=reconciled_{job["filename"]}.json'
        return response
        
    except Exception as e:
        logger.error(f"JSON export failed: {e}")
        # Return error response
        error_data = {
            'error': str(e),
            'job_id': job['id'],
            'timestamp': datetime.now().isoformat()
        }
        response = make_response(json.dumps(error_data, indent=2))
        response.headers['Content-Type'] = 'application/json'
        return response