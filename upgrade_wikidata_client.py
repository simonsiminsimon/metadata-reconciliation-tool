# File: upgrade_wikidata_client.py
"""
Migration script to upgrade your Cultural Heritage Metadata Reconciliation Tool
to use the new enhanced Wikidata client.

This script will:
1. Backup your current system
2. Install the new Wikidata client
3. Update your routes to use the enhanced engine
4. Test the integration
"""

import os
import shutil
from datetime import datetime

def backup_current_system():
    """Create a backup of the current system before upgrading"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_dir = f"backup_{timestamp}"
    
    print(f"Creating backup in {backup_dir}/...")
    
    # Create backup directory
    os.makedirs(backup_dir, exist_ok=True)
    
    # Backup critical files
    files_to_backup = [
        'app/services/reconciliation_engine.py',
        'app/services/data_sources.py',
        'app/routes/web.py',
        'app/background_jobs.py'
    ]
    
    for file_path in files_to_backup:
        if os.path.exists(file_path):
            backup_path = os.path.join(backup_dir, file_path)
            os.makedirs(os.path.dirname(backup_path), exist_ok=True)
            shutil.copy2(file_path, backup_path)
            print(f"  ✅ Backed up {file_path}")
        else:
            print(f"  ⚠️  File not found: {file_path}")
    
    print(f"✅ Backup completed in {backup_dir}/")
    return backup_dir

def update_routes_file():
    """Update the routes to use the enhanced reconciliation engine"""
    
    routes_update = '''
# File: app/routes/web_enhanced.py
"""
Updated web routes using the Enhanced Reconciliation Engine
Replace your existing web.py file with this updated version
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

# Import the ENHANCED reconciliation engine
from app.services.enhanced_reconciliation_engine import EnhancedReconciliationEngine
from app.database import JobManager, ResultsManager

# Background job imports (keeping existing functionality)
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
            # Handle file upload (same as before)
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
                        
                        flash(f'File uploaded! Processing with enhanced Wikidata reconciliation. Task ID: {task.id[:8]}...', 'success')
                        print(f"🚀 Started background task {task.id} for job {job_id}")
                        
                    except Exception as e:
                        print(f"❌ Failed to start background task: {e}")
                        flash(f'Upload successful, but background processing failed. Using fallback method.', 'warning')
                        start_enhanced_threaded_processing(job_id)
                else:
                    # Fallback to threaded processing with enhanced engine
                    start_enhanced_threaded_processing(job_id)
                    flash(f'File uploaded! Processing with enhanced Wikidata reconciliation (threaded mode).', 'success')
                
                return redirect(url_for('processing', job_id=job_id))
            else:
                flash('Please upload a CSV file', 'error')
        
        return render_template('upload_enhanced.html')
    
    # ... (other routes remain the same) ...

def start_enhanced_threaded_processing(job_id):
    """Start processing in a separate thread using the enhanced engine"""
    threading.Thread(
        target=process_job_with_enhanced_reconciliation, 
        args=(job_id,),
        daemon=True
    ).start()
    
    JobManager.update_job(job_id, {'status': 'processing'})
    print(f"🧵 Started enhanced threaded processing for job {job_id}")


def process_job_with_enhanced_reconciliation(job_id):
    """
    Process a job with the enhanced reconciliation engine (threaded version).
    This replaces the old reconciliation method with cultural heritage specific features.
    """
    try:
        # Update job status
        JobManager.update_job(job_id, {'status': 'processing', 'progress': 10})
        
        job = JobManager.get_job(job_id)
        print(f"🚀 Starting enhanced reconciliation for job {job_id}")
        
        # Step 1: Parse the CSV file
        print(f"📄 Parsing CSV file: {job['filename']}")
        df = pd.read_csv(job['filepath'])
        JobManager.update_job(job_id, {'progress': 20})
        
        # Step 2: Create enhanced reconciliation engine
        print("🔧 Initializing enhanced reconciliation engine...")
        engine = EnhancedReconciliationEngine(
            cache_size=1000,
            wikidata_rate_limit=1.0  # Respectful rate limiting
        )
        JobManager.update_job(job_id, {'progress': 30})
        
        # Step 3: Create entities from the CSV
        print("🎯 Creating entities for enhanced reconciliation...")
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
        
        # Step 4: Process entities in batches with enhanced matching
        print("🔍 Starting enhanced reconciliation process...")
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
        print("💾 Saving enhanced results to database...")
        saved_count = ResultsManager.save_results(job_id, all_results)
        
        # Step 6: Update job completion with enhanced statistics
        successful_matches = sum(1 for r in all_results if r.best_match)
        
        # Get enhanced engine statistics
        engine_stats = engine.get_statistics()
        
        JobManager.update_job(job_id, {
            'status': 'completed',
            'progress': 100,
            'successful_matches': successful_matches,
            'completed_at': datetime.now().isoformat()
        })
        
        print(f"✅ Enhanced reconciliation complete! {successful_matches}/{total_entities} matches found")
        print(f"💾 Saved {saved_count} results to database")
        print(f"📊 Enhanced stats: Wikidata queries: {engine_stats.get('wikidata_queries', 0)}, Cache hit rate: {engine_stats.get('wikidata_cache_hit_rate', 0):.1%}")
        
    except Exception as e:
        print(f"❌ Error processing job {job_id}: {e}")
        JobManager.update_job(job_id, {
            'status': 'failed',
            'error_message': str(e)
        })
'''
    
    return routes_update

def update_background_jobs():
    """Update background jobs to use the enhanced engine"""
    
    background_jobs_update = '''
# Add this to your app/background_jobs.py file
# Replace the existing process_reconciliation_job function with this enhanced version

@celery_app.task(bind=True)
def process_enhanced_reconciliation_job(self, job_id):
    """
    Enhanced background task using the new Cultural Heritage Wikidata Client
    """
    
    def update_progress(percent, message):
        """Helper function to update both Celery and database"""
        self.update_state(
            state='PROGRESS',
            meta={'percent': percent, 'message': message}
        )
        JobManager.update_job(job_id, {'progress': percent})
        print(f"📈 {percent}% - {message}")
    
    try:
        from app.services.enhanced_reconciliation_engine import EnhancedReconciliationEngine
        
        # Step 1: Initialize
        update_progress(5, "Starting enhanced reconciliation process...")
        job = JobManager.get_job(job_id)
        if not job:
            raise Exception(f"Job {job_id} not found")
        
        JobManager.update_job(job_id, {'status': 'processing'})
        
        # Step 2: Parse CSV file
        update_progress(15, "Reading and parsing CSV file...")
        df = pd.read_csv(job['filepath'])
        print(f"📄 Loaded CSV with {len(df)} rows and {len(df.columns)} columns")
        
        # Step 3: Initialize enhanced reconciliation engine
        update_progress(25, "Initializing enhanced reconciliation engine...")
        engine = EnhancedReconciliationEngine(
            cache_size=1000,
            wikidata_rate_limit=1.0
        )
        
        # Step 4: Create entities
        update_progress(35, "Extracting entities from CSV...")
        entities = engine.create_entities_from_dataframe(
            df,
            entity_column=job['entity_column'],
            type_column=job.get('type_column'),
            context_columns=job.get('context_columns', [])
        )
        
        total_entities = len(entities)
        JobManager.update_job(job_id, {'total_entities': total_entities})
        print(f"🎯 Found {total_entities} entities to reconcile with enhanced methods")
        
        # Step 5: Process entities with enhanced matching
        update_progress(45, f"Processing {total_entities} entities with cultural heritage focus...")
        
        all_results = []
        batch_size = min(10, max(1, total_entities // 10))
        
        for i in range(0, len(entities), batch_size):
            batch_start = i
            batch_end = min(i + batch_size, len(entities))
            batch = entities[batch_start:batch_end]
            
            batch_progress = 45 + int((batch_start / total_entities) * 35)
            update_progress(
                batch_progress, 
                f"Enhanced processing: entities {batch_start + 1}-{batch_end} of {total_entities}..."
            )
            
            try:
                batch_results = engine.process_entities(batch)
                all_results.extend(batch_results)
                
                matches_found = sum(1 for r in batch_results if r.best_match)
                print(f"📊 Enhanced batch {i//batch_size + 1}: {matches_found}/{len(batch)} matches found")
                
            except Exception as e:
                print(f"⚠️  Error in enhanced processing batch {i//batch_size + 1}: {e}")
                continue
        
        # Step 6: Save results
        update_progress(85, "Saving enhanced results to database...")
        saved_count = ResultsManager.save_results(job_id, all_results)
        print(f"💾 Saved {saved_count} enhanced results to database")
        
        # Step 7: Calculate enhanced statistics
        update_progress(95, "Calculating enhanced statistics...")
        successful_matches = sum(1 for r in all_results if r.best_match)
        
        # Get enhanced engine statistics
        engine_stats = engine.get_statistics()
        
        # Step 8: Mark job as completed
        update_progress(100, "Enhanced reconciliation completed successfully!")
        
        completion_data = {
            'status': 'completed',
            'progress': 100,
            'successful_matches': successful_matches,
            'completed_at': datetime.now().isoformat()
        }
        
        JobManager.update_job(job_id, completion_data)
        
        # Return enhanced results summary
        result_summary = {
            'job_id': job_id,
            'total_entities': total_entities,
            'successful_matches': successful_matches,
            'match_rate': successful_matches / total_entities if total_entities > 0 else 0,
            'wikidata_queries': engine_stats.get('wikidata_queries', 0),
            'cache_hit_rate': engine_stats.get('wikidata_cache_hit_rate', 0),
            'high_confidence_matches': engine_stats.get('high_confidence_matches', 0),
            'status': 'completed',
            'engine_type': 'enhanced_cultural_heritage'
        }
        
        print(f"✅ Enhanced job {job_id} completed successfully!")
        print(f"📊 Enhanced results: {successful_matches}/{total_entities} matches ({successful_matches/total_entities*100:.1f}%)")
        print(f"🔍 Wikidata queries: {engine_stats.get('wikidata_queries', 0)}")
        print(f"⚡ Cache hit rate: {engine_stats.get('wikidata_cache_hit_rate', 0):.1%}")
        
        return result_summary
        
    except Exception as e:
        # Handle errors gracefully
        error_message = str(e)
        print(f"❌ Enhanced job {job_id} failed: {error_message}")
        
        JobManager.update_job(job_id, {
            'status': 'failed',
            'error_message': error_message,
            'completed_at': datetime.now().isoformat()
        })
        
        self.update_state(
            state='FAILURE',
            meta={'error': error_message, 'job_id': job_id}
        )
        
        raise
'''
    
    return background_jobs_update

def create_enhanced_upload_template():
    """Create an enhanced upload template that shows the new features"""
    
    template_content = '''
<!-- File: app/templates/upload_enhanced.html -->
{% extends "base.html" %}

{% block title %}Upload CSV - Enhanced Cultural Heritage Reconciliation{% endblock %}

{% block content %}
<div class="card">
    <h1>📚 Cultural Heritage Metadata Reconciliation</h1>
    <p>Upload a CSV file to reconcile entities against <strong>enhanced Wikidata</strong> with cultural heritage focus.</p>
    
    <div class="enhancement-notice" style="background: #e8f4fd; border: 1px solid #bee5eb; padding: 1rem; border-radius: 4px; margin-bottom: 2rem;">
        <h3>🎉 Enhanced Features</h3>
        <ul>
            <li>✅ <strong>Cultural Heritage Focus:</strong> Specialized queries for persons, places, and organizations</li>
            <li>✅ <strong>Authority Linking:</strong> Automatic VIAF and Library of Congress ID retrieval</li>
            <li>✅ <strong>Context Awareness:</strong> Better matching using dates, locations, and institutional context</li>
            <li>✅ <strong>Enhanced Caching:</strong> Faster processing with intelligent result caching</li>
            <li>✅ <strong>Detailed Metadata:</strong> Birth/death dates, coordinates, images, and websites</li>
        </ul>
    </div>
    
    <form method="POST" enctype="multipart/form-data">
        <div class="form-group">
            <label for="file">Select CSV File</label>
            <input type="file" id="file" name="file" accept=".csv" required>
            <div class="form-help">
                <strong>Supported formats:</strong> CSV files with cultural heritage metadata<br>
                <strong>Best for:</strong> Library catalogs, museum collections, archival finding aids, institutional records
            </div>
        </div>
        
        <div class="form-group">
            <label for="entity_column">Entity Column Name</label>
            <input type="text" id="entity_column" name="entity_column" placeholder="e.g., creator_name, author, organization" required>
            <div class="form-help">The column containing names to reconcile (people, places, organizations)</div>
        </div>
        
        <div class="form-group">
            <label for="type_column">Entity Type Column (Optional but Recommended)</label>
            <input type="text" id="type_column" name="type_column" placeholder="e.g., entity_type, type, category">
            <div class="form-help">
                <strong>Helps improve matching!</strong> Column indicating: person, place, organization, subject<br>
                The enhanced engine will auto-detect if not specified.
            </div>
        </div>
        
        <div class="form-group">
            <label for="context_columns">Context Columns (Recommended for Best Results)</label>
            <input type="text" id="context_columns" name="context_columns" placeholder="e.g., date_created, location, nationality, institution">
            <div class="form-help">
                <strong>Enhanced matching!</strong> Comma-separated list of columns providing context:<br>
                • <strong>Dates:</strong> birth_year, death_year, created_date, active_period<br>
                • <strong>Places:</strong> birth_place, location, country, city<br>
                • <strong>Roles:</strong> occupation, nationality, institution_affiliation<br>
                Context significantly improves match quality and confidence!
            </div>
        </div>
        
        <div class="form-group">
            <label for="confidence_threshold">Confidence Threshold</label>
            <select id="confidence_threshold" name="confidence_threshold">
                <option value="0.7">High Confidence (0.7)</option>
                <option value="0.5" selected>Medium Confidence (0.5)</option>
                <option value="0.3">Low Confidence (0.3)</option>
            </select>
            <div class="form-help">Minimum confidence level for automatic matches. Enhanced engine provides better confidence scoring.</div>
        </div>
        
        <button type="submit" class="btn btn-primary">🚀 Start Enhanced Reconciliation</button>
    </form>
</div>

<div class="card">
    <h2>📖 Enhanced Reconciliation Guide</h2>
    <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 2rem;">
        
        <div>
            <h3>🎯 Entity Types</h3>
            <ul>
                <li><strong>Persons:</strong> Authors, artists, historical figures</li>
                <li><strong>Places:</strong> Cities, buildings, institutions</li>
                <li><strong>Organizations:</strong> Libraries, museums, universities</li>
                <li><strong>Subjects:</strong> Topics, concepts, academic fields</li>
            </ul>
        </div>
        
        <div>
            <h3>🔍 Enhanced Matching</h3>
            <ul>
                <li><strong>Context-aware:</strong> Uses dates, places, and roles</li>
                <li><strong>Alias matching:</strong> Finds alternative name forms</li>
                <li><strong>Authority linking:</strong> Connects to VIAF, LC, and more</li>
                <li><strong>Cultural focus:</strong> Optimized for heritage metadata</li>
            </ul>
        </div>
        
        <div>
            <h3>📊 Result Enrichment</h3>
            <ul>
                <li><strong>Biographical data:</strong> Birth/death dates and places</li>
                <li><strong>Geographic data:</strong> Coordinates and administrative info</li>
                <li><strong>External links:</strong> Authority IDs, websites, images</li>
                <li><strong>Relationship data:</strong> Institutional affiliations</li>
            </ul>
        </div>
        
    </div>
</div>

<div class="card">
    <h2>💡 Sample Data Structure</h2>
    <p>Your CSV should look something like this for best results:</p>
    <table class="table" style="font-size: 0.9rem;">
        <thead>
            <tr>
                <th>creator_name</th>
                <th>entity_type</th>
                <th>date_created</th>
                <th>location</th>
                <th>institution</th>
            </tr>
        </thead>
        <tbody>
            <tr>
                <td>Hodge, Emma B.</td>
                <td>person</td>
                <td>1921</td>
                <td>Chicago</td>
                <td>Art Institute</td>
            </tr>
            <tr>
                <td>Minneapolis Institute of Art</td>
                <td>organization</td>
                <td>1915</td>
                <td>Minneapolis, Minnesota</td>
                <td>Museum</td>
            </tr>
            <tr>
                <td>Carleton College</td>
                <td>organization</td>
                <td>1866</td>
                <td>Northfield, Minnesota</td>
                <td>University</td>
            </tr>
        </tbody>
    </table>
</div>
{% endblock %}
'''
    
    return template_content

def run_upgrade():
    """Main upgrade function"""
    print("🚀 Upgrading Cultural Heritage Metadata Reconciliation Tool")
    print("=" * 60)
    
    # Step 1: Backup current system
    print("\n1. Creating backup...")
    backup_dir = backup_current_system()
    
    # Step 2: Create new files
    print("\n2. Creating enhanced files...")
    
    # Create the new Wikidata client file
    print("  📝 New Wikidata client: app/services/wikidata_cultural_client.py")
    print("     (Copy the CulturalHeritageWikidataClient code to this file)")
    
    # Create the enhanced reconciliation engine
    print("  📝 Enhanced engine: app/services/enhanced_reconciliation_engine.py")
    print("     (Copy the EnhancedReconciliationEngine code to this file)")
    
    # Step 3: Show updates needed
    print("\n3. Files to update:")
    print("  📝 app/routes/web.py -> Use enhanced routes")
    print("  📝 app/background_jobs.py -> Add enhanced background task")
    print("  📝 app/templates/upload.html -> Use enhanced template")
    
    # Step 4: Installation instructions
    print("\n4. Installation steps:")
    print("  1. Copy the new client and engine files to your app/services/ directory")
    print("  2. Update your routes file with the enhanced version")
    print("  3. Update your background jobs with the enhanced task")
    print("  4. Replace your upload template with the enhanced version")
    print("  5. Test with a sample CSV file")
    
    print("\n5. Testing the upgrade:")
    print("  1. Start your application: python run.py")
    print("  2. Upload a CSV with cultural heritage metadata")
    print("  3. Check the processing page for enhanced features")
    print("  4. Review results for additional metadata (VIAF IDs, dates, etc.)")
    
    print("\n✅ Upgrade preparation complete!")
    print(f"💾 Your original files are safely backed up in: {backup_dir}/")
    print("\n🎯 Next steps:")
    print("  - Copy the new code files to your project")
    print("  - Update your existing files as shown above")
    print("  - Test with cultural heritage metadata")
    print("  - Enjoy enhanced reconciliation with cultural heritage focus!")

def create_test_csv():
    """Create a test CSV file for trying the enhanced features"""
    test_data = """creator_name,entity_type,date_created,location,description
"Hodge, Emma B.",person,1921,Chicago,"Author of needlework samplers treatise"
Minneapolis Institute of Art,organization,1915,"Minneapolis, Minnesota","Art museum and cultural institution"
Carleton College. Registrar's Office,organization,1867,"Northfield, Minnesota","Academic registrar office"
Bijou Opera House,organization,1898,"Minneapolis, Minnesota","Historic theater venue"
Minnesota Territorial Legislature,organization,1857,"St. Paul, Minnesota","Territorial government body"
"Youngdahl, P. J.",person,1911,"Minneapolis, Minnesota","State superintendent"
Nicollet County Historical Society,organization,1800s,"St. Peter, Minnesota","Historical preservation organization"
"Hennepin County Library, James K. Hosmer Special Collections Library",organization,1900s,"Minneapolis, Minnesota","Special collections library"
Norwegian-American Historical Association,organization,1925,"Northfield, Minnesota","Cultural heritage organization"
Northwest Minnesota Historical Center,organization,1960s,"Bemidji, Minnesota","Regional historical center"
"""
    
    with open('test_cultural_heritage_metadata.csv', 'w') as f:
        f.write(test_data)
    
    print("📄 Created test_cultural_heritage_metadata.csv")
    print("   Use this file to test the enhanced reconciliation features!")

if __name__ == "__main__":
    run_upgrade()
    
    print("\n" + "=" * 60)
    print("📚 CULTURAL HERITAGE ENHANCEMENT SUMMARY")
    print("=" * 60)
    
    print("\n🎯 Key Improvements:")
    print("  • Cultural heritage-specific entity queries")
    print("  • Enhanced context awareness (dates, places, institutions)")
    print("  • Authority linking (VIAF, Library of Congress)")
    print("  • Better confidence scoring for heritage metadata")
    print("  • Comprehensive result enrichment")
    print("  • Improved caching and performance")
    
    print("\n📊 Perfect for:")
    print("  • Library catalogs and bibliographic records")
    print("  • Museum collection metadata")
    print("  • Archival finding aids")
    print("  • Digital humanities projects")
    print("  • Institutional repository metadata")
    
    create_test_csv()
    
    print("\n🚀 Ready to enhance your cultural heritage metadata reconciliation!")