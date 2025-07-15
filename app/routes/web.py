# File: app/routes/web.py
"""
Optimized web routes for the Metadata Reconciliation System.
This handles all web requests and form submissions.

Key improvements:
1. Better error handling and user feedback
2. Proper form data validation
3. Support for both threaded and background processing
4. Clear separation of concerns
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
from app.services.metadata_parser import MetadataParser
from app.services.reconciliation_engine import ReconciliationEngine
from app.database import JobManager, ResultsManager

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
            # Validate file upload
            if 'file' not in request.files:
                flash('No file selected. Please choose a CSV file.', 'error')
                return redirect(request.url)
            
            file = request.files['file']
            is_valid, error_msg = validate_csv_file(file)
            
            if not is_valid:
                flash(error_msg, 'error')
                return redirect(request.url)
            
            # Get and validate form data
            entity_column = request.form.get('entity_column', '').strip()
            if not entity_column:
                flash('Entity column is required. Please specify which column contains the entities to reconcile.', 'error')
                return redirect(request.url)
            
            # Save file with unique name
            filename = secure_filename(file.filename)
            job_id = str(uuid.uuid4())
            filepath = os.path.join(app.config['UPLOAD_FOLDER'], f"{job_id}_{filename}")
            
            try:
                file.save(filepath)
            except Exception as e:
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
                'total_entities': 0,
                'successful_matches': 0
            }
            
            try:
                JobManager.create_job(job_data)
                logger.info(f"Created job {job_id} for file {filename}")
            except Exception as e:
                flash(f'Failed to create job: {str(e)}', 'error')
                # Clean up uploaded file
                if os.path.exists(filepath):
                    os.remove(filepath)
                return redirect(request.url)
            
            # Start processing
            if BACKGROUND_JOBS_AVAILABLE:
                try:
                    # Queue background job
                    task = process_reconciliation_job.delay(job_id)
                    JobManager.update_job(job_id, {
                        'status': 'queued',
                        'task_id': task.id
                    })
                    flash('File uploaded successfully! Processing in background...', 'success')
                    logger.info(f"Queued background task {task.id} for job {job_id}")
                except Exception as e:
                    logger.error(f"Failed to queue background job: {e}")
                    # Fallback to threaded processing
                    start_threaded_processing(job_id)
                    flash('File uploaded successfully! Processing started...', 'success')
            else:
                # Use threaded processing
                start_threaded_processing(job_id)
                flash('File uploaded successfully! Processing started...', 'success')
            
            return redirect(url_for('processing', job_id=job_id))
        
        # GET request - show upload form
        return render_template('upload.html')
    
    @app.route('/jobs')
    def jobs():
        """Show all jobs"""
        try:
            all_jobs = JobManager.get_all_jobs()
            # Sort by created_at descending (newest first)
            all_jobs.sort(key=lambda x: x.get('created_at', ''), reverse=True)
            return render_template('jobs.html', jobs=all_jobs)
        except Exception as e:
            logger.error(f"Error loading jobs: {e}")
            flash('Error loading jobs list', 'error')
            return redirect(url_for('upload'))
    
    @app.route('/processing/<job_id>')
    def processing(job_id):
        """Show processing status"""
        job = JobManager.get_job(job_id)
        if not job:
            flash('Job not found', 'error')
            return redirect(url_for('jobs'))
        
        return render_template('processing.html', job=job)
    
    @app.route('/review/<job_id>')
    def review(job_id):
        """Review reconciliation results"""
        job = JobManager.get_job(job_id)
        if not job:
            flash('Job not found', 'error')
            return redirect(url_for('jobs'))
        
        if job['status'] != 'completed':
            flash('Job is not yet complete', 'info')
            return redirect(url_for('processing', job_id=job_id))
        
        # Get paginated results
        page = request.args.get('page', 1, type=int)
        per_page = 10
        
        try:
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
                'next_num': page + 1 if page < total_pages else None
            }
            
            return render_template('review.html', 
                                 job=job, 
                                 results=results, 
                                 pagination=pagination)
        except Exception as e:
            logger.error(f"Error loading results for job {job_id}: {e}")
            flash('Error loading results', 'error')
            return redirect(url_for('jobs'))
    
    @app.route('/export/<job_id>')
    def export(job_id):
        """Export options page"""
        job = JobManager.get_job(job_id)
        if not job:
            flash('Job not found', 'error')
            return redirect(url_for('jobs'))
        
        if job['status'] != 'completed':
            flash('Job is not yet complete', 'info')
            return redirect(url_for('processing', job_id=job_id))
        
        return render_template('export.html', job=job)
    
    @app.route('/download/<job_id>/<format>')
    def download_results(job_id, format):
        """Download results in specified format"""
        job = JobManager.get_job(job_id)
        if not job:
            flash('Job not found', 'error')
            return redirect(url_for('jobs'))
        
        # Get all results for export
        try:
            results, total = ResultsManager.get_results(job_id, page=1, per_page=10000)
            if not results:
                flash('No results found for this job', 'error')
                return redirect(url_for('export', job_id=job_id))
            
            if format == 'csv':
                return export_csv(job, results)
            elif format == 'json':
                return export_json(job, results)
            elif format == 'rdf':
                return export_rdf(job, results)
            else:
                flash(f'Unsupported format: {format}', 'error')
                return redirect(url_for('export', job_id=job_id))
                
        except Exception as e:
            logger.error(f"Export failed for job {job_id}: {e}")
            flash(f'Export failed: {str(e)}', 'error')
            return redirect(url_for('export', job_id=job_id))


def start_threaded_processing(job_id):
    """Start processing in a separate thread"""
    thread = threading.Thread(
        target=process_job_threaded,
        args=(job_id,),
        daemon=True
    )
    thread.start()
    
    JobManager.update_job(job_id, {'status': 'processing'})
    logger.info(f"Started threaded processing for job {job_id}")


def process_job_threaded(job_id):
    """
    Process a reconciliation job in a thread.
    This is the fallback when Celery is not available.
    """
    try:
        job = JobManager.get_job(job_id)
        logger.info(f"Processing job {job_id} in thread")
        
        # Update status
        JobManager.update_job(job_id, {
            'status': 'processing',
            'progress': 10
        })
        
        # Load CSV
        df = pd.read_csv(job['filepath'])
        total_rows = len(df)
        
        # Initialize reconciliation engine
        engine = ReconciliationEngine()
        
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
            'progress': 30
        })
        
        # Process entities in batches
        batch_size = 10
        successful_matches = 0
        
        for i in range(0, total_entities, batch_size):
            batch = entities[i:i+batch_size]
            results = engine.process_entities(batch)
            
            # Store results
            for result in results:
                if result.best_match:
                    successful_matches += 1
                
                result_data = {
                    'entity': {
                        'id': result.entity.id,
                        'name': result.entity.name,
                        'type': result.entity.entity_type.value,
                        'context': result.entity.context
                    },
                    'best_match': {
                        'id': result.best_match.id,
                        'name': result.best_match.name,
                        'score': result.best_match.score,
                        'source': result.best_match.source,
                        'description': result.best_match.description
                    } if result.best_match else None,
                    'matches': [
                        {
                            'id': match.id,
                            'name': match.name,
                            'score': match.score,
                            'source': match.source,
                            'description': match.description
                        } for match in result.matches[:5]  # Top 5 matches
                    ],
                    'confidence': result.confidence.value,
                    'sources_queried': result.sources_queried,
                    'cached': result.cached
                }
                
                ResultsManager.add_result(job_id, result_data)
            
            # Update progress
            progress = 30 + int((i + batch_size) / total_entities * 60)
            JobManager.update_job(job_id, {
                'progress': min(progress, 90),
                'successful_matches': successful_matches
            })
        
        # Mark as complete
        JobManager.update_job(job_id, {
            'status': 'completed',
            'progress': 100,
            'completed_at': datetime.now().isoformat(),
            'successful_matches': successful_matches
        })
        
        logger.info(f"Completed job {job_id}: {successful_matches}/{total_entities} matches")
        
    except Exception as e:
        logger.error(f"Job {job_id} failed: {e}")
        JobManager.update_job(job_id, {
            'status': 'failed',
            'error_message': str(e),
            'completed_at': datetime.now().isoformat()
        })


# Export functions
def export_csv(job, results):
    """Export results as CSV"""
    rows = []
    
    for result in results:
        entity = result['entity']
        best_match = result.get('best_match')
        
        row = {
            'original_name': entity['name'],
            'entity_type': entity['type'],
            'confidence': result['confidence'],
            'match_found': 'Yes' if best_match else 'No'
        }
        
        # Add context columns
        for key, value in entity.get('context', {}).items():
            row[f'context_{key}'] = value
        
        # Add match details
        if best_match:
            row.update({
                'matched_name': best_match['name'],
                'match_score': best_match['score'],
                'match_source': best_match['source'],
                'match_id': best_match['id'],
                'match_description': best_match['description'][:200]  # Truncate long descriptions
            })
        
        rows.append(row)
    
    # Create DataFrame and convert to CSV
    df = pd.DataFrame(rows)
    output = BytesIO()
    df.to_csv(output, index=False, encoding='utf-8')
    output.seek(0)
    
    return send_file(
        output,
        mimetype='text/csv',
        as_attachment=True,
        download_name=f"{job['filename']}_reconciled.csv"
    )


def export_json(job, results):
    """Export results as JSON"""
    export_data = {
        'job_info': {
            'id': job['id'],
            'filename': job['filename'],
            'processed_at': job.get('completed_at'),
            'total_entities': job['total_entities'],
            'successful_matches': job['successful_matches']
        },
        'results': results
    }
    
    output = BytesIO()
    json_str = json.dumps(export_data, indent=2, ensure_ascii=False)
    output.write(json_str.encode('utf-8'))
    output.seek(0)
    
    return send_file(
        output,
        mimetype='application/json',
        as_attachment=True,
        download_name=f"{job['filename']}_reconciled.json"
    )


def export_rdf(job, results):
    """Export results as RDF/XML"""
    # Simple RDF export - could be enhanced with proper RDF libraries
    rdf_content = '''<?xml version="1.0" encoding="UTF-8"?>
<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
         xmlns:rdfs="http://www.w3.org/2000/01/rdf-schema#"
         xmlns:owl="http://www.w3.org/2002/07/owl#">
'''
    
    for result in results:
        entity = result['entity']
        best_match = result.get('best_match')
        
        if best_match:
            rdf_content += f'''
    <rdf:Description rdf:about="local:{entity['id']}">
        <rdfs:label>{entity['name']}</rdfs:label>
        <owl:sameAs rdf:resource="{best_match['source']}:{best_match['id']}"/>
    </rdf:Description>
'''
    
    rdf_content += '</rdf:RDF>'
    
    output = BytesIO()
    output.write(rdf_content.encode('utf-8'))
    output.seek(0)
    
    return send_file(
        output,
        mimetype='application/rdf+xml',
        as_attachment=True,
        download_name=f"{job['filename']}_reconciled.rdf"
    )