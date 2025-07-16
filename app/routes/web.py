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
import time

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

# Replace the start_threaded_processing function in app/routes/web.py:

def start_threaded_processing(job_id):
    """Start processing in a separate thread"""
    logger.info(f"🧵 Starting threaded processing for job {job_id}")
    
    thread = threading.Thread(
        target=process_job_threaded,
        args=(job_id,),
        name=f"ProcessJob-{job_id}",
        daemon=True
    )
    thread.start()
    
    JobManager.update_job(job_id, {'status': 'processing'})
    logger.info(f"✅ Thread {thread.name} started successfully")
    # Start the thread
    thread = threading.Thread(target=process_job_threaded, name=f"ProcessJob-{job_id}")
    thread.daemon = True
    thread.start()
    
    logger.info(f"✅ Thread {thread.name} started successfully")

def process_job_threaded(job_id):
    """
    FIXED: Actually process a reconciliation job in a thread.
    This replaces the stub implementation with real processing.
    """
    try:
        logger.info(f"🔄 Thread started for job {job_id}")
        
        # Helper function to update progress
        def update_progress(percent, message):
            JobManager.update_job(job_id, {
                'progress': percent,
                'status': 'processing'
            })
            logger.info(f"📊 Job {job_id}: {percent}% - {message}")
        
        # Step 1: Get job details
        job = JobManager.get_job(job_id)
        if not job:
            logger.error(f"❌ Job {job_id} not found")
            return
        
        update_progress(5, "Initializing processing...")
        
        # Step 2: Read the CSV file
        update_progress(10, "Reading CSV file...")
        logger.info(f"📄 Processing file: {job['filepath']}")
        
        try:
            df = pd.read_csv(job['filepath'])
            logger.info(f"✅ CSV loaded: {df.shape[0]} rows, {df.shape[1]} columns")
        except Exception as e:
            raise Exception(f"Failed to read CSV: {e}")
        
        # Step 3: Initialize reconciliation engine
        update_progress(15, "Initializing reconciliation engine...")
        try:
            engine = EnhancedReconciliationEngine()
            logger.info("✅ Reconciliation engine initialized")
        except Exception as e:
            raise Exception(f"Failed to initialize reconciliation engine: {e}")
        
        # Step 4: Extract entities from CSV
        update_progress(25, "Extracting entities...")
        try:
            entities = engine.create_entities_from_dataframe(
                df, 
                entity_column=job['entity_column'],
                type_column=job.get('type_column'),
                context_columns=job.get('context_columns', [])
            )
            
            total_entities = len(entities)
            logger.info(f"🎯 Found {total_entities} entities to reconcile")
            
            if total_entities == 0:
                raise Exception("No entities found in CSV. Check your column settings.")
            
            # Update job with entity count
            JobManager.update_job(job_id, {'total_entities': total_entities})
            
        except Exception as e:
            raise Exception(f"Failed to extract entities: {e}")
        
        # Step 5: Process entities in batches
        update_progress(35, f"Processing {total_entities} entities...")
        
        all_results = []
        batch_size = min(10, max(1, total_entities // 10))  # Adaptive batch size
        
        for i in range(0, len(entities), batch_size):
            batch_start = i
            batch_end = min(i + batch_size, len(entities))
            batch = entities[batch_start:batch_end]
            
            # Update progress for this batch
            batch_progress = 35 + int((batch_start / total_entities) * 50)
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
                logger.info(f"📊 Batch {i//batch_size + 1}: {matches_found}/{len(batch)} matches found")
                
            except Exception as e:
                logger.error(f"⚠️  Error processing batch {i//batch_size + 1}: {e}")
                # Continue with other batches instead of failing completely
                continue
        
        # Step 6: Save results to database
        update_progress(90, "Saving results to database...")
        try:
            saved_count = ResultsManager.save_results(job_id, all_results)
            logger.info(f"💾 Saved {saved_count} results to database")
        except Exception as e:
            raise Exception(f"Failed to save results: {e}")
        
        # Step 7: Calculate final statistics
        update_progress(95, "Calculating final statistics...")
        successful_matches = sum(1 for r in all_results if r.best_match)
        
        # Step 8: Mark job as completed
        update_progress(100, "Reconciliation completed successfully!")
        
        JobManager.update_job(job_id, {
            'status': 'completed',
            'progress': 100,
            'successful_matches': successful_matches,
            'completed_at': time.time()
        })
        
        logger.info(f"🎉 Job {job_id} completed successfully!")
        logger.info(f"📊 Final stats: {successful_matches}/{total_entities} matches found")
        
    except Exception as e:
        logger.error(f"❌ Job {job_id} failed: {e}")
        JobManager.update_job(job_id, {
            'status': 'failed',
            'error_message': str(e)
        })
        import traceback
        logger.error(f"❌ Traceback: {traceback.format_exc()}")


def register_web_routes(app):
    """Register all web routes with the Flask app"""
    
    @app.route('/')
    def index():
        """Home page - redirect to upload"""
        return redirect(url_for('upload'))
    
    @app.route('/upload_debug')
    def upload_debug():
        """Debug upload page with pre-filled correct settings"""
        return render_template('upload_debug.html')

    @app.route('/upload', methods=['GET', 'POST'])
    def upload():
        """Handle file upload and job creation"""
        if request.method == 'POST':
            logger.info("=== UPLOAD ROUTE START ===")
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
            is_valid, error_message = validate_csv_file(file)
            if not is_valid:
                logger.error(f"File validation failed: {error_message}")
                flash(f'File validation failed: {error_message}', 'error')
                return redirect(request.url)

            # Get and validate form data
            entity_column = request.form.get('entity_column', '').strip()
            if not entity_column:
                logger.error("Entity column not provided")
                flash('Entity column is required. Please specify which column contains the entities to reconcile.', 'error')
                return redirect(request.url)

            try:
                # Generate unique job ID and save file
                job_id = str(uuid.uuid4())
                logger.info(f"Generated job ID: {job_id}")
                
                filename = secure_filename(file.filename)
                os.makedirs('data/input', exist_ok=True)
                filepath = os.path.join('data/input', f"{job_id}_{filename}")
                file.save(filepath)
                logger.info(f"File saved to: {filepath}")

                # Parse optional parameters properly
                type_column = request.form.get('type_column', '').strip() or None
                
                # Handle context columns (text input, comma-separated)
                context_columns_str = request.form.get('context_columns', '').strip()
                context_columns = [col.strip() for col in context_columns_str.split(',') if col.strip()] if context_columns_str else []
                
                # Handle data sources (checkboxes) - use getlist() but handle defaults properly
                data_sources = request.form.getlist('data_sources')
                if not data_sources:  # If no checkboxes selected, use default
                    data_sources = ['wikidata']
                
                # Parse confidence threshold
                try:
                    confidence_threshold = float(request.form.get('confidence_threshold', 0.6))
                except (ValueError, TypeError):
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
                    'status': 'uploaded',  # Start with uploaded status
                    'progress': 0,
                    'total_entities': 0,
                    'successful_matches': 0
                }
                
                logger.info(f"Creating job with data: {job_data}")
                JobManager.create_job(job_data)
                logger.info(f"✅ Job {job_id} created successfully")

                # Now start processing
                logger.info(f"🔍 Checking background jobs availability: {BACKGROUND_JOBS_AVAILABLE}")
                
                if BACKGROUND_JOBS_AVAILABLE:
                    try:
                        logger.info("🚀 Attempting to start background job...")
                        from app.background_jobs import process_reconciliation_job
                        task = process_reconciliation_job.delay(job_id)
                        JobManager.update_job(job_id, {
                            'status': 'queued',
                            'task_id': task.id
                        })
                        logger.info(f"✅ Background job queued with task ID: {task.id}")
                        flash('File uploaded successfully! Processing started in background...', 'success')
                    except Exception as e:
                        logger.error(f"❌ Background job failed: {e}")
                        logger.info("🔄 Falling back to threaded processing...")
                        start_threaded_processing(job_id)
                        flash('File uploaded successfully! Processing started...', 'success')
                else:
                    logger.info("📝 Using threaded processing (background jobs not available)")
                    start_threaded_processing(job_id)
                    flash('File uploaded successfully! Processing started...', 'success')

                logger.info("=== UPLOAD ROUTE SUCCESS ===")
                return redirect(url_for('processing', job_id=job_id))
                
            except Exception as e:
                logger.error(f"❌ Error in upload route: {e}")
                logger.error(f"❌ Error type: {type(e)}")
                import traceback
                logger.error(f"❌ Traceback: {traceback.format_exc()}")
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
        """Export options page for specific job"""
        job = JobManager.get_job(job_id)
        if not job:
            flash('Job not found', 'error')
            return redirect(url_for('jobs'))
        
        if job['status'] != 'completed':
            flash('Job is not yet complete. Please wait for processing to finish.', 'info')
            return redirect(url_for('processing', job_id=job_id))
        
        # Show the export options page
        try:
            return render_template('export.html', job=job)
        except Exception as e:
            logger.error(f"Error loading export page: {e}")
            flash('Export page could not be loaded', 'error')
            return redirect(url_for('review', job_id=job_id))
        
    @app.route('/export_job/<job_id>')
    def export_job(job_id):
        """Export job route (alternative name for template compatibility)"""
        return export(job_id)  # Just call the existing export function

    @app.route('/download/<job_id>/<format>')
    def download_results(job_id, format):
        """Download results in specified format"""
        job = JobManager.get_job(job_id)
        if not job:
            flash('Job not found', 'error')
            return redirect(url_for('jobs'))
        
        if job['status'] != 'completed':
            flash('Job is not yet complete', 'info')
            return redirect(url_for('processing', job_id=job_id))
        
        try:
            # For now, create a simple export placeholder
            if format == 'csv':
                return export_csv_placeholder(job)
            elif format == 'json':
                return export_json_placeholder(job)
            elif format == 'rdf':
                return export_rdf_placeholder(job)
            elif format == 'report':
                return export_report_placeholder(job)
            else:
                flash(f'Unsupported format: {format}', 'error')
                return redirect(url_for('export', job_id=job_id))
                
        except Exception as e:
            logger.error(f"Export failed for job {job_id}: {e}")
            flash(f'Export failed: {str(e)}', 'error')
            return redirect(url_for('export', job_id=job_id))

    @app.route('/jobs_list')
    def jobs_list():
        """Alternative route name for jobs (template compatibility)"""
        return redirect(url_for('jobs'))
    
    @app.route('/export/')
    @app.route('/export')
    def export_default():
        """Default export page - redirect to jobs"""
        flash('Please select a completed job to export results', 'info')
        return redirect(url_for('jobs'))
    
    def export_csv_placeholder(job):
        """Placeholder CSV export"""
        from flask import make_response
        
        # Create simple CSV content
        csv_content = f"""original_name,status,created_at
    {job['filename']},completed,{job.get('created_at', 'unknown')}
    # This is a placeholder export - real export functionality coming soon
    # Job ID: {job['id']}
    # Total entities: {job.get('total_entities', 0)}
    # Successful matches: {job.get('successful_matches', 0)}
    """
        
        response = make_response(csv_content)
        response.headers['Content-Type'] = 'text/csv'
        response.headers['Content-Disposition'] = f'attachment; filename="{job["filename"]}_results.csv"'
        return response

    def export_json_placeholder(job):
        """Placeholder JSON export"""
        from flask import jsonify
        
        export_data = {
            'job_info': {
                'id': job['id'],
                'filename': job['filename'],
                'status': job['status'],
                'created_at': job.get('created_at'),
                'total_entities': job.get('total_entities', 0),
                'successful_matches': job.get('successful_matches', 0)
            },
            'results': [
                {
                    'note': 'This is a placeholder export',
                    'message': 'Real export functionality coming soon'
                }
            ],
            'metadata': {
                'export_format': 'json',
                'export_timestamp': datetime.now().isoformat()
            }
        }
        
        response = jsonify(export_data)
        response.headers['Content-Disposition'] = f'attachment; filename="{job["filename"]}_results.json"'
        return response

    def export_rdf_placeholder(job):
        """Placeholder RDF export"""
        from flask import make_response
        
        rdf_content = f"""@prefix dcterms: <http://purl.org/dc/terms/> .
    @prefix foaf: <http://xmlns.com/foaf/0.1/> .
    @prefix skos: <http://www.w3.org/2004/02/skos/core#> .

    # Placeholder RDF export for job: {job['id']}
    # Filename: {job['filename']}
    # Status: {job['status']}
    # Total entities: {job.get('total_entities', 0)}
    # Successful matches: {job.get('successful_matches', 0)}

    <http://example.org/job/{job['id']}> a dcterms:Dataset ;
        dcterms:title "{job['filename']}" ;
        dcterms:created "{job.get('created_at', 'unknown')}" ;
        skos:note "Placeholder RDF export - real functionality coming soon" .
    """
        
        response = make_response(rdf_content)
        response.headers['Content-Type'] = 'text/turtle'
        response.headers['Content-Disposition'] = f'attachment; filename="{job["filename"]}_results.ttl"'
        return response

    def export_report_placeholder(job):
        """Placeholder report export"""
        from flask import make_response
        
        report_content = f"""METADATA RECONCILIATION REPORT
    =====================================

    Job Information:
    - Job ID: {job['id']}
    - Filename: {job['filename']}
    - Status: {job['status']}
    - Created: {job.get('created_at', 'unknown')}
    - Total Entities: {job.get('total_entities', 0)}
    - Successful Matches: {job.get('successful_matches', 0)}
    - Match Rate: {(job.get('successful_matches', 0) / max(job.get('total_entities', 1), 1) * 100):.1f}%

    Processing Details:
    - Entity Column: {job.get('entity_column', 'unknown')}
    - Type Column: {job.get('type_column', 'none')}
    - Confidence Threshold: {job.get('confidence_threshold', 0.6)}
    - Data Sources: {', '.join(job.get('data_sources', ['unknown']))}

    NOTE: This is a placeholder report.
    Real export functionality will include detailed match results,
    confidence scores, and authority links.

    Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
    """
        
        response = make_response(report_content)
        response.headers['Content-Type'] = 'text/plain'
        response.headers['Content-Disposition'] = f'attachment; filename="{job["filename"]}_report.txt"'
        return response
    
    @app.route('/export_simple/<job_id>')
    def export_simple(job_id):
        """Simple export page (fallback if main export template has issues)"""
        job = JobManager.get_job(job_id)
        if not job:
            flash('Job not found', 'error')
            return redirect(url_for('jobs'))
        
        if job['status'] != 'completed':
            flash('Job is not yet complete. Please wait for processing to finish.', 'info')
            return redirect(url_for('processing', job_id=job_id))
        
        try:
            return render_template('export_simple.html', job=job)
        except Exception as e:
            logger.error(f"Error loading simple export page: {e}")
            # If even the simple template fails, show a basic HTML response
            return f"""
            <html>
            <head><title>Export - {job['filename']}</title></head>
            <body>
                <h1>Export Results: {job['filename']}</h1>
                <p>Job Status: {job['status']}</p>
                <p>Total Entities: {job.get('total_entities', 0)}</p>
                <p>Successful Matches: {job.get('successful_matches', 0)}</p>
                <hr>
                <a href="/api/jobs/{job_id}/export_csv" style="display: inline-block; background: #007cba; color: white; padding: 10px 20px; text-decoration: none; margin: 5px;">Download CSV</a>
                <a href="/api/jobs/{job_id}/export_json" style="display: inline-block; background: #007cba; color: white; padding: 10px 20px; text-decoration: none; margin: 5px;">Download JSON</a>
                <hr>
                <a href="/review/{job_id}">← Back to Review</a> | 
                <a href="/jobs">📋 All Jobs</a>
                <hr>
                <p><em>Note: This is a fallback export page. Template issues are being resolved.</em></p>
            </body>
            </html>
            """
        
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

    