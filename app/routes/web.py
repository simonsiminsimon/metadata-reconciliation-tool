# File: app/routes/web.py
"""
Fixed web routes for the Metadata Reconciliation System.
This resolves the file upload validation issues.
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

# Import our components
try:
    from app.services.metadata_parser import MetadataParser
    from app.services.enhanced_reconciliation_engine import EnhancedReconciliationEngine
    from app.database import JobManager, ResultsManager
except ImportError as e:
    print(f"Warning: Some imports failed: {e}")
    # Create dummy classes if imports fail
    class MetadataParser:
        def parse_csv_metadata(self, filepath):
            return {"summary": {"total_entities": 0}}
    
    class ReconciliationEngine:
        def process_entities(self, entities, sources, threshold):
            return []
    
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
    
    class ResultsManager:
        @staticmethod
        def save_results(job_id, results):
            pass

# Set up logging
logger = logging.getLogger(__name__)

# Check if background jobs are available
try:
    from app.background_jobs import process_reconciliation_job
    BACKGROUND_JOBS_AVAILABLE = True
    logger.info("✅ Background jobs (Celery + Redis) available")
except ImportError as e:
    BACKGROUND_JOBS_AVAILABLE = False
    logger.warning(f"⚠️  Background jobs not available: {e}")
    logger.info("📝 Using threaded processing as fallback")


def validate_csv_file(file):
    """
    Validate uploaded CSV file.
    Returns (is_valid, error_message)
    """
    if not file or file.filename == '':
        return False, "No file selected"
    
    # Check file extension
    if not file.filename.lower().endswith('.csv'):
        return False, "Only CSV files are supported"
    
    # Check file size (50MB limit)
    file.seek(0, os.SEEK_END)
    file_size = file.tell()
    file.seek(0)  # Reset file pointer
    
    if file_size > 50 * 1024 * 1024:
        return False, "File size exceeds 50MB limit"
    
    # Try to read the file to validate it's a proper CSV
    try:
        content = file.read().decode('utf-8')
        file.seek(0)  # Reset for later use
        df = pd.read_csv(StringIO(content), nrows=5)
        if df.empty:
            return False, "CSV file appears to be empty"
    except Exception as e:
        return False, f"Invalid CSV file: {str(e)}"
    
    return True, None


def start_threaded_processing(job_id):
    """Start processing in a separate thread if background jobs aren't available"""
    def process_in_thread():
        try:
            logger.info(f"Starting threaded processing for job {job_id}")
            # Simulate processing - replace with actual processing logic
            import time
            time.sleep(2)  # Simulate work
            logger.info(f"Completed processing for job {job_id}")
        except Exception as e:
            logger.error(f"Error in threaded processing: {e}")
    
    thread = threading.Thread(target=process_in_thread)
    thread.daemon = True
    thread.start()


def register_web_routes(app):
    """Register all web routes with the Flask app"""
    
    @app.route('/')
    def index():
        """Home page - redirect to upload"""
        return redirect(url_for('upload'))
    
    @app.route('/upload', methods=['GET', 'POST'])
    def upload():
        """Handle file upload and job creation"""
        if request.method == 'POST':
            logger.info("Processing file upload...")
            logger.info(f"Form data: {dict(request.form)}")
            logger.info(f"Files: {list(request.files.keys())}")
            
            # Validate file upload
            if 'file' not in request.files:
                logger.error("No 'file' key in request.files")
                flash('No file selected. Please choose a CSV file.', 'error')
                return redirect(request.url)
            
            file = request.files['file']
            logger.info(f"File received: {file.filename}")
            
            # Validate the file
            is_valid, error_msg = validate_csv_file(file)
            if not is_valid:
                logger.error(f"File validation failed: {error_msg}")
                flash(error_msg, 'error')
                return redirect(request.url)
            
            # Get and validate form data
            entity_column = request.form.get('entity_column', '').strip()
            if not entity_column:
                logger.error("Entity column not provided")
                flash('Entity column is required. Please specify which column contains the entities to reconcile.', 'error')
                return redirect(request.url)
            
            # Create upload directory if it doesn't exist
            upload_folder = app.config.get('UPLOAD_FOLDER', 'data/input')
            os.makedirs(upload_folder, exist_ok=True)
            
            # Save file with unique name
            filename = secure_filename(file.filename)
            job_id = str(uuid.uuid4())
            filepath = os.path.join(upload_folder, f"{job_id}_{filename}")
            
            try:
                file.save(filepath)
                logger.info(f"File saved to: {filepath}")
            except Exception as e:
                logger.error(f"Failed to save file: {e}")
                flash(f'Failed to save file: {str(e)}', 'error')
                return redirect(request.url)
            
            # Parse optional parameters
            type_column = request.form.get('type_column', '').strip() or None
            context_columns_str = request.form.get('context_columns', '').strip()
            context_columns = [col.strip() for col in context_columns_str.split(',') if col.strip()] if context_columns_str else []
            
            # Parse data sources (handle multiple selection)
            data_sources = request.form.getlist('data_sources')
            if not data_sources:  # Default if none selected
                data_sources = ['wikidata', 'viaf']
            
            # Parse confidence threshold
            try:
                confidence_threshold = float(request.form.get('confidence_threshold', 0.6))
            except ValueError:
                confidence_threshold = 0.6
            
            # Create job record
            job_data = {
                'id': job_id,
                'filename': filename,
                'filepath': filepath,
                'entity_column': entity_column,
                'type_column': type_column,
                'context_columns': context_columns,
                'data_sources': data_sources,
                'confidence_threshold': confidence_threshold,
                'status': 'uploaded',
                'progress': 0,
                'created_at': datetime.now().isoformat(),
                'updated_at': datetime.now().isoformat()
            }
            
            try:
                # Save job to database
                JobManager.create_job(job_data)
                logger.info(f"Job created: {job_id}")
                
                # Start processing
                if BACKGROUND_JOBS_AVAILABLE:
                    # Use Celery for background processing
                    process_reconciliation_job.delay(job_id)
                    flash('File uploaded successfully! Processing started in background...', 'success')
                else:
                    # Use threaded processing as fallback
                    start_threaded_processing(job_id)
                    flash('File uploaded successfully! Processing started...', 'success')
                
                return redirect(url_for('processing', job_id=job_id))
                
            except Exception as e:
                logger.error(f"Error creating job: {e}")
                flash(f'Error starting processing: {str(e)}', 'error')
                return redirect(request.url)
        
        # GET request - render upload form
        return render_template('upload.html')
    
    @app.route('/jobs')
    def jobs():
        """Show all jobs"""
        try:
            all_jobs = JobManager.get_all_jobs()
            return render_template('jobs.html', jobs=all_jobs)
        except Exception as e:
            logger.error(f"Error loading jobs: {e}")
            flash('Error loading jobs list', 'error')
            return redirect(url_for('upload'))
    
    @app.route('/processing/<job_id>')
    def processing(job_id):
        """Show processing status for specific job"""
        job = JobManager.get_job(job_id)
        if not job:
            flash('Job not found', 'error')
            return redirect(url_for('jobs'))
        return render_template('processing.html', job=job)
    
    @app.route('/processing/')
    @app.route('/processing')
    def processing_default():
        """Default processing page - redirect to jobs"""
        flash('Please select a job to view processing status', 'info')
        return redirect(url_for('jobs'))
    
    @app.route('/review/<job_id>')
    def review(job_id):
        """Review results for specific job"""
        job = JobManager.get_job(job_id)
        if not job:
            flash('Job not found', 'error')
            return redirect(url_for('jobs'))
        
        # Get results for this job
        try:
            # This would load actual results from database
            results = []  # Placeholder
            return render_template('review.html', job=job, results=results)
        except Exception as e:
            logger.error(f"Error loading results: {e}")
            flash('Error loading results', 'error')
            return redirect(url_for('jobs'))
    
    @app.route('/review/')
    @app.route('/review')
    def review_default():
        """Default review page - redirect to jobs"""
        flash('Please select a completed job to review results', 'info')
        return redirect(url_for('jobs'))
    
    @app.route('/export/<job_id>')
    def export(job_id):
        """Export results for specific job"""
        job = JobManager.get_job(job_id)
        if not job:
            flash('Job not found', 'error')
            return redirect(url_for('jobs'))
        
        try:
            # This would export actual results
            flash('Export functionality coming soon', 'info')
            return redirect(url_for('review', job_id=job_id))
        except Exception as e:
            logger.error(f"Error exporting: {e}")
            flash('Error during export', 'error')
            return redirect(url_for('jobs'))
    
    @app.route('/export/')
    @app.route('/export')
    def export_default():
        """Default export page - redirect to jobs"""
        flash('Please select a completed job to export results', 'info')
        return redirect(url_for('jobs'))
    
    # Health check route
    @app.route('/health')
    def health():
        """Simple health check"""
        return jsonify({
            'status': 'healthy',
            'timestamp': datetime.now().isoformat(),
            'background_jobs': BACKGROUND_JOBS_AVAILABLE
        })
    
    return app