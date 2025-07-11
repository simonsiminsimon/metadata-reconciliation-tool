# File: app/routes/web.py
# Replace your existing web.py with this version

from flask import render_template, request, redirect, url_for, flash, send_file, jsonify
from werkzeug.utils import secure_filename
import os
import uuid
import json
import pandas as pd
from datetime import datetime
from io import StringIO, BytesIO
import threading

# Import our components
from app.services.metadata_parser import MetadataParser
from app.services.reconciliation_engine import ReconciliationEngine, EntityType
from app.database import JobManager, ResultsManager

# Import background job functions with fallback
try:
    from app.background_jobs import process_reconciliation_job, get_task_status, cancel_task
    BACKGROUND_JOBS_AVAILABLE = True
    print("✅ Background jobs (Celery + Redis) available")
except ImportError as e:
    BACKGROUND_JOBS_AVAILABLE = False
    print(f"⚠️  Background jobs not available: {e}")
    print("📝 Falling back to threaded processing")

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
                
                # Start processing (background or threaded)
                if BACKGROUND_JOBS_AVAILABLE:
                    try:
                        # Queue the reconciliation job in Celery
                        task = process_reconciliation_job.delay(job_id)
                        
                        # Store the task ID so we can check progress
                        JobManager.update_job(job_id, {
                            'status': 'queued',
                            'task_id': task.id
                        })
                        
                        flash(f'File uploaded successfully! Processing started in background. Task ID: {task.id[:8]}...', 'success')
                        print(f"🚀 Started background task {task.id} for job {job_id}")
                        
                    except Exception as e:
                        print(f"❌ Failed to start background task: {e}")
                        flash(f'Upload successful, but background processing failed. Using fallback method.', 'warning')
                        start_threaded_processing(job_id)
                else:
                    # Fallback to threaded processing
                    start_threaded_processing(job_id)
                    flash(f'File uploaded successfully! Processing started (threaded mode).', 'success')
                
                return redirect(url_for('processing', job_id=job_id))
            else:
                flash('Please upload a CSV file', 'error')
        
        return render_template('upload.html')
    
    @app.route('/jobs')
    def jobs_list():
        jobs = JobManager.get_all_jobs()
        
        # Add background job status info
        for job in jobs:
            if BACKGROUND_JOBS_AVAILABLE and job.get('task_id'):
                try:
                    task_status = get_task_status(job['task_id'])
                    job['background_status'] = task_status.get('state', 'UNKNOWN')
                except:
                    job['background_status'] = 'ERROR'
            else:
                job['background_status'] = 'THREADED' if job['status'] == 'processing' else 'N/A'
        
        return render_template('jobs.html', jobs=jobs)
    
    @app.route('/processing/<job_id>')
    def processing(job_id):
        job = JobManager.get_job(job_id)
        if not job:
            flash('Job not found', 'error')
            return redirect(url_for('jobs_list'))
        
        # Start processing if not already started (fallback for non-background mode)
        if not BACKGROUND_JOBS_AVAILABLE and job['status'] == 'uploaded':
            start_threaded_processing(job_id)
        
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
    
    # Background job management routes
    @app.route('/cancel_job/<job_id>', methods=['POST'])
    def cancel_job_route(job_id):
        """Cancel a running background job"""
        if not BACKGROUND_JOBS_AVAILABLE:
            flash('Job cancellation not available in threaded mode', 'warning')
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
                print(f"🛑 Cancelled background task {task_id} for job {job_id}")
            except Exception as e:
                flash(f'Failed to cancel job: {e}', 'error')
                print(f"❌ Failed to cancel task {task_id}: {e}")
        else:
            flash('Job cannot be cancelled (not running)', 'warning')
        
        return redirect(url_for('processing', job_id=job_id))
    
    @app.route('/restart_job/<job_id>', methods=['POST'])
    def restart_job_route(job_id):
        """Restart a failed job"""
        job = JobManager.get_job(job_id)
        if not job:
            flash('Job not found', 'error')
            return redirect(url_for('jobs_list'))
        
        if job['status'] in ['failed', 'cancelled']:
            if BACKGROUND_JOBS_AVAILABLE:
                try:
                    # Start new background task
                    task = process_reconciliation_job.delay(job_id)
                    JobManager.update_job(job_id, {
                        'status': 'queued',
                        'task_id': task.id,
                        'progress': 0,
                        'error_message': None
                    })
                    flash(f'Job restarted successfully in background. Task ID: {task.id[:8]}...', 'success')
                    print(f"🔄 Restarted background task {task.id} for job {job_id}")
                except Exception as e:
                    flash(f'Failed to restart background job: {e}', 'error')
                    start_threaded_processing(job_id)
                    flash('Fell back to threaded processing', 'warning')
            else:
                # Reset status for threaded processing
                JobManager.update_job(job_id, {
                    'status': 'uploaded',
                    'progress': 0,
                    'error_message': None
                })
                flash('Job reset for threaded processing', 'success')
        else:
            flash('Job cannot be restarted (not failed or cancelled)', 'warning')
        
        return redirect(url_for('processing', job_id=job_id))


def start_threaded_processing(job_id):
    """Start processing in a separate thread (fallback method)"""
    threading.Thread(
        target=process_job_with_reconciliation, 
        args=(job_id,),
        daemon=True
    ).start()
    
    JobManager.update_job(job_id, {'status': 'processing'})
    print(f"🧵 Started threaded processing for job {job_id}")


def process_job_with_reconciliation(job_id):
    """
    Process a job with real reconciliation (threaded version).
    This is the fallback when background jobs aren't available.
    """
    try:
        # Update job status
        JobManager.update_job(job_id, {'status': 'processing', 'progress': 10})
        
        job = JobManager.get_job(job_id)
        print(f"🚀 Starting reconciliation for job {job_id}")
        
        # Step 1: Parse the CSV file
        print(f"📄 Parsing CSV file: {job['filename']}")
        df = pd.read_csv(job['filepath'])
        JobManager.update_job(job_id, {'progress': 20})
        
        # Step 2: Create reconciliation engine
        print("🔧 Initializing reconciliation engine...")
        engine = ReconciliationEngine()
        JobManager.update_job(job_id, {'progress': 30})
        
        # Step 3: Create entities from the CSV
        print("🎯 Creating entities for reconciliation...")
        entities = engine.create_entities_from_dataframe(
            df,
            entity_column=job['entity_column'],
            type_column=job.get('type_column'),
            context_columns=job.get('context_columns', [])
        )
        
        total_entities = len(entities)
        JobManager.update_job(job_id, {
            'total_entities': total_entities,
            'progress': 40
        })
        
        print(f"📊 Found {total_entities} entities to reconcile")
        
        # Step 4: Process entities in batches
        print("🔍 Starting reconciliation process...")
        all_results = []
        batch_size = 10
        
        for i in range(0, len(entities), batch_size):
            batch = entities[i:i + batch_size]
            batch_results = engine.process_entities(batch)
            all_results.extend(batch_results)
            
            # Update progress
            progress = 40 + int((i + len(batch)) / len(entities) * 40)
            JobManager.update_job(job_id, {'progress': progress})
            
            print(f"📈 Processed {i + len(batch)}/{len(entities)} entities")
        
        JobManager.update_job(job_id, {'progress': 80})
        
        # Step 5: Save results to database
        print("💾 Saving results to database...")
        saved_count = ResultsManager.save_results(job_id, all_results)
        
        # Step 6: Update job completion
        successful_matches = sum(1 for r in all_results if r.best_match)
        JobManager.update_job(job_id, {
            'status': 'completed',
            'progress': 100,
            'successful_matches': successful_matches,
            'completed_at': datetime.now().isoformat()
        })
        
        print(f"✅ Reconciliation complete! {successful_matches}/{total_entities} matches found")
        print(f"💾 Saved {saved_count} results to database")
        
    except Exception as e:
        print(f"❌ Error processing job {job_id}: {e}")
        JobManager.update_job(job_id, {
            'status': 'failed',
            'error_message': str(e)
        })


# Export functions (same as before)
def download_csv(job, results):
    """Create and download a CSV file with reconciliation results"""
    print(f"📊 Creating CSV export for job {job['id']}")
    
    csv_rows = []
    for result in results:
        entity = result['entity']
        
        base_row = {
            'original_name': entity['name'],
            'entity_type': entity['type'],
            'confidence': result['confidence'],
            'sources_queried': ', '.join(result['sources_queried']),
            'cached': result['cached'],
            'num_matches': len(result['matches'])
        }
        
        # Add context columns
        for key, value in entity['context'].items():
            base_row[f'context_{key}'] = value
        
        # Add best match info
        if result['matches']:
            best_match = result['matches'][0]
            base_row.update({
                'matched_uri': f"https://example.org/{best_match['source']}/{best_match['id']}",
                'matched_name': best_match['name'],
                'match_score': best_match['score'],
                'match_source': best_match['source'],
                'match_description': best_match['description']
            })
        else:
            base_row.update({
                'matched_uri': '',
                'matched_name': '',
                'match_score': '',
                'match_source': '',
                'match_description': 'No matches found'
            })
        
        csv_rows.append(base_row)
    
    # Create DataFrame and convert to CSV
    df = pd.DataFrame(csv_rows)
    
    # Create file in memory
    output = StringIO()
    df.to_csv(output, index=False)
    output.seek(0)
    
    # Convert to bytes for download
    csv_bytes = BytesIO()
    csv_bytes.write(output.getvalue().encode('utf-8'))
    csv_bytes.seek(0)
    
    filename = f"{job['filename']}_reconciled.csv"
    
    return send_file(
        csv_bytes,
        mimetype='text/csv',
        as_attachment=True,
        download_name=filename
    )


def download_json(job, results):
    """Create and download a JSON file with reconciliation results"""
    print(f"📄 Creating JSON export for job {job['id']}")
    
    export_data = {
        'job_info': {
            'id': job['id'],
            'filename': job['filename'],
            'processed_at': job.get('completed_at', datetime.now().isoformat()),
            'total_entities': job['total_entities'],
            'successful_matches': job['successful_matches']
        },
        'results': results
    }
    
    json_bytes = BytesIO()
    json_str = json.dumps(export_data, indent=2, ensure_ascii=False)
    json_bytes.write(json_str.encode('utf-8'))
    json_bytes.seek(0)
    
    filename = f"{job['filename']}_reconciled.json"
    
    return send_file(
        json_bytes,
        mimetype='application/json',
        as_attachment=True,
        download_name=filename
    )


def download_rdf(job, results):
    """Create and download an RDF/XML file"""
    print(f"🔗 Creating RDF export for job {job['id']}")
    
    rdf_content = '''<?xml version="1.0" encoding="UTF-8"?>
<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
         xmlns:skos="http://www.w3.org/2004/02/skos/core#"
         xmlns:dcterms="http://purl.org/dc/terms/">
'''
    
    for result in results:
        entity = result['entity']
        if result['matches']:
            best_match = result['matches'][0]
            
            rdf_content += f'''
  <rdf:Description rdf:about="urn:entity:{entity['id']}">
    <dcterms:title>{entity['name']}</dcterms:title>
    <skos:exactMatch rdf:resource="https://example.org/{best_match['source']}/{best_match['id']}"/>
    <dcterms:type>{entity['type']}</dcterms:type>
  </rdf:Description>
'''
    
    rdf_content += '</rdf:RDF>'
    
    rdf_bytes = BytesIO()
    rdf_bytes.write(rdf_content.encode('utf-8'))
    rdf_bytes.seek(0)
    
    filename = f"{job['filename']}_reconciled.rdf"
    
    return send_file(
        rdf_bytes,
        mimetype='application/rdf+xml',
        as_attachment=True,
        download_name=filename
    )


def download_report(job, results):
    """Create and download a reconciliation report"""
    print(f"📋 Creating report for job {job['id']}")
    
    # Calculate statistics
    total_entities = len(results)
    matched_entities = sum(1 for r in results if r['matches'])
    high_confidence = sum(1 for r in results if r['confidence'] == 'high')
    medium_confidence = sum(1 for r in results if r['confidence'] == 'medium')
    low_confidence = sum(1 for r in results if r['confidence'] == 'low')
    
    report_content = f"""RECONCILIATION REPORT
====================

Job Information:
- Job ID: {job['id']}
- File: {job['filename']}
- Processed: {job.get('completed_at', 'Unknown')}
- Status: {job['status']}
- Processing Mode: {'Background (Celery)' if BACKGROUND_JOBS_AVAILABLE else 'Threaded'}

Statistics:
- Total Entities: {total_entities}
- Successfully Matched: {matched_entities} ({matched_entities/total_entities*100:.1f}%)
- High Confidence: {high_confidence}
- Medium Confidence: {medium_confidence}
- Low Confidence: {low_confidence}

Detailed Results (First 20):
"""
    
    for i, result in enumerate(results[:20], 1):
        entity = result['entity']
        report_content += f"\n{i}. {entity['name']} ({entity['type']})\n"
        report_content += f"   Confidence: {result['confidence']}\n"
        
        if result['matches']:
            best_match = result['matches'][0]
            report_content += f"   Best Match: {best_match['name']} (Score: {best_match['score']:.2f})\n"
            report_content += f"   Source: {best_match['source']}\n"
        else:
            report_content += "   No matches found\n"
    
    if len(results) > 20:
        report_content += f"\n... and {len(results) - 20} more results"
    
    report_bytes = BytesIO()
    report_bytes.write(report_content.encode('utf-8'))
    report_bytes.seek(0)
    
    filename = f"{job['filename']}_report.txt"
    
    return send_file(
        report_bytes,
        mimetype='text/plain',
        as_attachment=True,
        download_name=filename
    )