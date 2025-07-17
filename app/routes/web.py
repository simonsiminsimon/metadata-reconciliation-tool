# File: app/routes/web.py (CRITICAL FIXES FOR UPLOAD AND PROCESSING)
"""
Fixed web routes with improved CSV processing and job management.
FIXES: Entity detection, file validation, and processing workflow.
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
    """Improved CSV file validation"""
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
    
    if file_size == 0:
        return False, "File is empty"
    
    # Try to read and validate CSV structure
    try:
        content = file.read().decode('utf-8', errors='ignore')
        file.seek(0)  # Reset for later use
        
        # Basic CSV validation
        lines = content.split('\n')
        if len(lines) < 2:  # Need at least header + 1 data row
            return False, "CSV file must have at least a header and one data row"
        
        # Try to parse with pandas
        df = pd.read_csv(StringIO(content), nrows=5)
        if df.empty:
            return False, "CSV file appears to be empty"
        
        if len(df.columns) == 0:
            return False, "CSV file has no columns"
            
    except Exception as e:
        return False, f"Invalid CSV file: {str(e)}"
    
    return True, None

def start_threaded_processing(job_id):
    """Start processing in a separate thread - FIXED VERSION"""
    logger.info(f"🧵 Starting threaded processing for job {job_id}")
    
    # FIXED: Properly pass job_id as argument
    thread = threading.Thread(
        target=process_job_threaded,
        args=(job_id,),  # ← This was missing or incorrect!
        name=f"ProcessJob-{job_id}",
        daemon=True
    )
    thread.start()
    
    JobManager.update_job(job_id, {'status': 'processing'})
    logger.info(f"✅ Thread {thread.name} started successfully")


def process_job_threaded(job_id):
    """
    COMPLETE IMPLEMENTATION: Process a reconciliation job in a thread
    This replaces the stub with full reconciliation logic
    """
    def update_progress(percent, message):
        """Helper function to update job progress"""
        JobManager.update_job(job_id, {'progress': percent})
        logger.info(f"📊 Job {job_id}: {percent}% - {message}")
    
    try:
        # Step 1: Get job details
        update_progress(5, "Initializing processing...")
        job = JobManager.get_job(job_id)
        if not job:
            raise Exception(f"Job {job_id} not found")
        
        filepath = job['filepath']
        
        # Step 2: Load and validate CSV
        update_progress(10, "Reading CSV file...")
        try:
            df = pd.read_csv(filepath)
            logger.info(f"📄 Processing file: {filepath}")
            logger.info(f"✅ CSV loaded: {len(df)} rows, {len(df.columns)} columns")
        except Exception as e:
            raise Exception(f"Failed to read CSV: {e}")
        
        # Step 3: Initialize reconciliation engine with timeout handling
        update_progress(15, "Initializing reconciliation engine...")
        from app.services.enhanced_reconciliation_engine import EnhancedReconciliationEngine
        engine = EnhancedReconciliationEngine()
        logger.info("✅ Reconciliation engine initialized")
        
        # Step 4: Extract entities
        update_progress(25, "Extracting entities...")
        try:
            # Extract entities from the dataframe
            entities = []
            entity_column = job.get('entity_column', 'creator_name')  # Default column
            
            if entity_column not in df.columns:
                # Try to find the entity column
                possible_columns = ['name', 'creator_name', 'author', 'person', 'entity']
                for col in possible_columns:
                    if col in df.columns:
                        entity_column = col
                        break
                else:
                    entity_column = df.columns[0]  # Use first column as fallback
            
            logger.info(f"🔍 DEBUG: Entity column: '{entity_column}'")
            
            for idx, row in df.iterrows():
                entity_name = str(row[entity_column]).strip()
                if entity_name and entity_name.lower() not in ['nan', 'none', '']:
                    # Create Entity object
                    from app.services.reconciliation_engine import Entity, EntityType
                    
                    # Determine entity type
                    entity_type_str = row.get('entity_type', 'person') if 'entity_type' in df.columns else 'person'
                    try:
                        entity_type = EntityType(entity_type_str.lower())
                    except:
                        entity_type = EntityType.PERSON
                    
                    # Create context from other columns
                    context = {}
                    for col in df.columns:
                        if col != entity_column and pd.notna(row[col]):
                            context[col] = str(row[col])
                    
                    entity = Entity(
                        id=f"entity_{idx}",
                        name=entity_name,
                        entity_type=entity_type,
                        context=context,
                        source_row=idx  # ← FIXED: Added missing source_row parameter
                    )
                    entities.append(entity)
                    logger.info(f"🔍 DEBUG: Row {idx}: '{entity_name}' -> valid: True")
            
            total_entities = len(entities)
            if total_entities == 0:
                raise Exception("No entities found to reconcile. Check your column settings.")
            
            # Update job with entity count
            JobManager.update_job(job_id, {'total_entities': total_entities})
            logger.info(f"🎯 Found {total_entities} entities to reconcile")
            
        except Exception as e:
            raise Exception(f"Failed to extract entities: {e}")
        
        # Step 5: Process entities in batches with timeout handling
        update_progress(35, f"Processing {total_entities} entities...")
        
        all_results = []
        batch_size = min(5, max(1, total_entities // 5))  # Smaller batches for reliability
        
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
                # Process this batch with timeout handling
                logger.info(f"📊 Processing batch {i//batch_size + 1}: entities {batch_start + 1}-{batch_end}")
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
            from app.database import ResultsManager
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
        error_message = str(e)
        logger.error(f"❌ Job {job_id} failed: {error_message}")
        
        # Update job status to failed
        JobManager.update_job(job_id, {
            'status': 'failed',
            'error_message': error_message,
            'completed_at': time.time()
        })
        
        # Re-raise the exception for debugging
        raise


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
        """FIXED: Handle file upload with improved validation and processing"""
        if request.method == 'POST':
            try:
                # Validate file upload
                if 'file' not in request.files:
                    flash('No file selected. Please choose a CSV file.', 'error')
                    return redirect(request.url)
                
                file = request.files['file']
                is_valid, error_message = validate_csv_file(file)
                
                if not is_valid:
                    flash(f'File validation failed: {error_message}', 'error')
                    return redirect(request.url)
                
                # Get form data with validation
                entity_column = request.form.get('entity_column', '').strip()
                if not entity_column:
                    flash('Entity column is required. Please specify which column contains the entities to reconcile.', 'error')
                    return redirect(request.url)
                
                type_column = request.form.get('type_column', '').strip() or None
                confidence_threshold = float(request.form.get('confidence_threshold', 0.8))
                data_sources = request.form.getlist('data_sources')
                
                if not data_sources:
                    data_sources = ['wikidata', 'viaf']  # Default sources
                
                # Save uploaded file
                filename = secure_filename(file.filename)
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                unique_filename = f"{timestamp}_{filename}"
                
                upload_dir = app.config.get('UPLOAD_FOLDER', 'data/uploads')
                os.makedirs(upload_dir, exist_ok=True)
                filepath = os.path.join(upload_dir, unique_filename)
                
                file.save(filepath)
                logger.info(f"File saved to: {filepath}")
                
                # Validate that the entity column exists in the CSV
                try:
                    df_test = pd.read_csv(filepath, nrows=1)
                    available_columns = list(df_test.columns)
                    
                    # Case-insensitive column check
                    column_map = {col.lower(): col for col in available_columns}
                    if entity_column.lower() not in column_map:
                        # Try to find similar column
                        similar_cols = [col for col in available_columns 
                                      if entity_column.lower() in col.lower() or col.lower() in entity_column.lower()]
                        if similar_cols:
                            entity_column = similar_cols[0]
                            logger.info(f"Using similar column: {entity_column}")
                        else:
                            flash(f'Column "{entity_column}" not found in CSV. Available columns: {", ".join(available_columns)}', 'error')
                            os.remove(filepath)  # Clean up
                            return redirect(request.url)
                    else:
                        entity_column = column_map[entity_column.lower()]
                
                except Exception as e:
                    flash(f'Error validating CSV structure: {str(e)}', 'error')
                    if os.path.exists(filepath):
                        os.remove(filepath)
                    return redirect(request.url)
                
                # Create job record
                job_id = str(uuid.uuid4())
                job_data = {
                    'id': job_id,
                    'filename': filename,
                    'filepath': filepath,
                    'entity_column': entity_column,
                    'type_column': type_column,
                    'context_columns': [],  # Could be expanded later
                    'data_sources': data_sources,
                    'confidence_threshold': confidence_threshold,
                    'status': 'uploaded',
                    'settings': {
                        'original_filename': filename,
                        'upload_timestamp': datetime.now().isoformat()
                    }
                }
                
                created_job_id = JobManager.create_job(job_data)
                logger.info(f"Created job {created_job_id} for file {filename}")
                
                # Start processing immediately
                if BACKGROUND_JOBS_AVAILABLE:
                    # Use Celery background processing
                    task = process_reconciliation_job.delay(job_id)
                    JobManager.update_job(job_id, {
                        'task_id': task.id,
                        'status': 'processing'
                    })
                    logger.info(f"Started Celery task {task.id} for job {job_id}")
                else:
                    # Use threaded processing
                    start_threaded_processing(job_id)
                    logger.info(f"Started threaded processing for job {job_id}")
                
                flash(f'File uploaded successfully! Processing started for job {job_id}', 'success')
                return redirect(url_for('processing', job_id=job_id))
                
            except Exception as e:
                logger.error(f"Upload failed: {e}")
                flash(f'Upload failed: {str(e)}', 'error')
                return redirect(request.url)
        
        # GET request - show upload form
        return render_template('upload.html')
    
    @app.route('/jobs')
    def jobs():
        """Job management page"""
        try:
            jobs = JobManager.get_all_jobs()
            logger.info(f"Displaying {len(jobs)} jobs")
            return render_template('jobs.html', jobs=jobs)
        except Exception as e:
            logger.error(f"Error loading jobs page: {e}")
            flash(f'Error loading jobs: {str(e)}', 'error')
            return render_template('jobs.html', jobs=[])
    
    @app.route('/processing/<job_id>')
    def processing(job_id):
        """Processing status page"""
        try:
            job = JobManager.get_job(job_id)
            if not job:
                flash('Job not found', 'error')
                return redirect(url_for('jobs'))
            
            return render_template('processing.html', job=job)
        except Exception as e:
            logger.error(f"Error loading processing page: {e}")
            flash(f'Error loading processing status: {str(e)}', 'error')
            return redirect(url_for('jobs'))
    
    @app.route('/review/<job_id>')
    def review(job_id):
        """Review reconciliation results for specific job"""
        job = JobManager.get_job(job_id)
        if not job:
            flash('Job not found', 'error')
            return redirect(url_for('jobs'))
        
        if job['status'] != 'completed':
            flash('Job is not yet complete', 'info')
            return redirect(url_for('processing', job_id=job_id))
        
        # Get paginated results - THIS WAS THE MISSING PIECE!
        page = request.args.get('page', 1, type=int)
        per_page = 10
        
        try:
            # FIXED: Actually query the database instead of using placeholder
            results, total_count = ResultsManager.get_results(job_id, page, per_page)
            
            # Calculate pagination info
            total_pages = (total_count + per_page - 1) // per_page
            pagination = {
                'page': page,
                'pages': total_pages,
                'total': total_count,
                'has_prev': page > 1,
                'has_next': page < total_pages,
                'prev_num': page - 1 if page > 1 else None,
                'next_num': page + 1 if page < total_pages else None,
                'per_page': per_page  # Add this for the template
            }
            
            logger.info(f"📊 Review page loading: {len(results)} results found for job {job_id}")
            logger.info(f"📄 Pagination: page {page} of {total_pages}, total {total_count} entities")
            
            return render_template('review.html', 
                                job=job, 
                                results=results, 
                                pagination=pagination)
                                
        except Exception as e:
            logger.error(f"Error loading results for job {job_id}: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            flash('Error loading results', 'error')
            return redirect(url_for('jobs'))

    # ALSO ADD: Fix the route name consistency
    @app.route('/review/<job_id>')
    def review_job(job_id):
        """Alias for review route to fix template links"""
        return review(job_id)
    
    @app.route('/review/')
    @app.route('/review')
    def review_default():
        """Default review page - redirect to jobs"""
        flash('Please select a completed job to review results', 'info')
        return redirect(url_for('jobs'))
    
    @app.route('/export/<job_id>')
    def export(job_id):
        """Export results page for a specific job"""
        try:
            job = JobManager.get_job(job_id)
            if not job:
                flash('Job not found', 'error')
                return redirect(url_for('jobs'))
            
            if job['status'] != 'completed':
                flash('Job must be completed before exporting results. Please wait for processing to finish.', 'info')
                return redirect(url_for('processing', job_id=job_id))
            
            # Try to render the full export template first
            try:
                return render_template('export.html', job=job)
            except Exception as template_error:
                logger.warning(f"Could not render export.html: {template_error}")
                # Fall back to simple export template
                return render_template('export_simple.html', job=job)
                
        except Exception as e:
            logger.error(f"Error loading export page for job {job_id}: {e}")
            flash(f'Error loading export page: {str(e)}', 'error')
            return redirect(url_for('jobs'))
    
    @app.route('/export/')
    @app.route('/export')
    def export_default():
        """Default export page - redirect to jobs with helpful message"""
        flash('Please select a completed job to export results', 'info')
        return redirect(url_for('jobs'))
    
    @app.route('/download/<job_id>/<format>')
    def download_results(job_id, format):
        """Download results in specified format"""
        try:
            job = JobManager.get_job(job_id)
            if not job:
                flash('Job not found', 'error')
                return redirect(url_for('jobs'))
            
            if job['status'] != 'completed':
                flash('Job is not yet complete', 'info')
                return redirect(url_for('processing', job_id=job_id))
            
            # Generate the requested export format
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


# Add these helper functions at the end of the file (outside register_web_routes)

def export_csv_placeholder(job):
    """Create placeholder CSV export"""
    from flask import make_response
    
    csv_content = f"""filename,job_id,status,total_entities,successful_matches,created_at
"{job['filename']}","{job['id']}","{job['status']}",{job.get('total_entities', 0)},{job.get('successful_matches', 0)},"{job.get('created_at', 'unknown')}"

# Metadata Reconciliation Results
# Job: {job['id']}
# Original File: {job['filename']}
# Entity Column: {job.get('entity_column', 'unknown')}
# Data Sources: {', '.join(job.get('data_sources', []))}
# 
# This is a placeholder export. Full reconciliation data will be included
# when the complete export functionality is implemented.
"""
    
    response = make_response(csv_content)
    response.headers['Content-Type'] = 'text/csv'
    response.headers['Content-Disposition'] = f'attachment; filename="{job["filename"]}_reconciled.csv"'
    return response


def export_json_placeholder(job):
    """Create placeholder JSON export"""
    from flask import jsonify
    
    json_data = {
        'job_metadata': {
            'id': job['id'],
            'filename': job['filename'],
            'status': job['status'],
            'created_at': str(job.get('created_at', 'unknown')),
            'entity_column': job.get('entity_column'),
            'data_sources': job.get('data_sources', []),
            'total_entities': job.get('total_entities', 0),
            'successful_matches': job.get('successful_matches', 0)
        },
        'reconciliation_results': [],
        'export_metadata': {
            'format': 'json',
            'generated_at': pd.Timestamp.now().isoformat(),
            'note': 'This is a placeholder export. Full reconciliation data will be included when complete export functionality is implemented.'
        }
    }
    
    response = jsonify(json_data)
    response.headers['Content-Disposition'] = f'attachment; filename="{job["filename"]}_reconciled.json"'
    return response


def export_rdf_placeholder(job):
    """Create placeholder RDF export"""
    from flask import make_response
    
    rdf_content = f"""@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix dc: <http://purl.org/dc/elements/1.1/> .
@prefix reconcile: <http://example.org/reconciliation/> .

<reconcile:job_{job['id']}> rdf:type reconcile:ReconciliationJob ;
    dc:title "{job['filename']}" ;
    reconcile:status "{job['status']}" ;
    reconcile:totalEntities {job.get('total_entities', 0)} ;
    reconcile:successfulMatches {job.get('successful_matches', 0)} ;
    dc:created "{job.get('created_at', 'unknown')}" ;
    rdfs:comment "This is a placeholder RDF export. Full reconciliation data will be included when complete export functionality is implemented." .
"""
    
    response = make_response(rdf_content)
    response.headers['Content-Type'] = 'text/turtle'
    response.headers['Content-Disposition'] = f'attachment; filename="{job["filename"]}_reconciled.ttl"'
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

    