# File: app/database.py
"""
Database setup and models for the reconciliation system.

Think of this like creating the blueprint for our filing system:
- Jobs table: stores information about each upload/processing job
- Results table: stores the reconciliation results for each entity
- Matches table: stores individual matches for each entity

Using SQLite because it's simple and doesn't require a separate database server.
"""

import sqlite3
import json
import os
from datetime import datetime
from typing import List, Dict, Any, Optional
from contextlib import contextmanager

# Database file location
DB_PATH = 'data/reconciliation.db'

def init_database():
    """
    Create the database tables if they don't exist.
    Like setting up the filing cabinet with the right drawers and labels.
    """
    # Make sure the data directory exists
    os.makedirs('data', exist_ok=True)
    
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        
        # Jobs table - stores processing job information
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS jobs (
            id TEXT PRIMARY KEY,
            filename TEXT NOT NULL,
            filepath TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'uploaded',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            completed_at TIMESTAMP,
            entity_column TEXT,
            type_column TEXT,
            context_columns TEXT,  -- JSON array
            data_sources TEXT,     -- JSON array
            confidence_threshold REAL,
            progress INTEGER DEFAULT 0,
            total_entities INTEGER DEFAULT 0,
            successful_matches INTEGER DEFAULT 0,
            error_message TEXT,
            settings TEXT          -- JSON for additional settings
        )
        ''')
        
        # Results table - stores reconciliation results for each entity
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS results (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id TEXT NOT NULL,
            entity_id TEXT NOT NULL,
            entity_name TEXT NOT NULL,
            entity_type TEXT NOT NULL,
            context TEXT,              -- JSON
            confidence TEXT NOT NULL,
            sources_queried TEXT,      -- JSON array
            cached BOOLEAN DEFAULT 0,
            reconciliation_time REAL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (job_id) REFERENCES jobs (id)
        )
        ''')
        
        # Matches table - stores individual matches for each result
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS matches (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            result_id INTEGER NOT NULL,
            match_id TEXT NOT NULL,
            match_name TEXT NOT NULL,
            match_source TEXT NOT NULL,
            match_score REAL NOT NULL,
            match_description TEXT,
            additional_info TEXT,      -- JSON
            is_best_match BOOLEAN DEFAULT 0,
            user_approved BOOLEAN,     -- NULL=not reviewed, 1=approved, 0=rejected
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (result_id) REFERENCES results (id)
        )
        ''')
        
        # Create indexes for better performance
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs (status)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_results_job_id ON results (job_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_matches_result_id ON matches (result_id)')
        
        conn.commit()
        print("✅ Database initialized successfully")


@contextmanager
def get_db_connection():
    """
    Get a database connection with proper error handling.
    Like safely opening and closing the filing cabinet.
    """
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row  # This lets us access columns by name
    try:
        yield conn
    finally:
        conn.close()


class JobManager:
    """
    Manages job records in the database.
    Think of this as the person who organizes the filing cabinet.
    """
    @staticmethod
    def _parse_datetime(date_string):
        """Convert database datetime string to datetime object"""
        if not date_string:
            return None
        try:
            # If it's already a datetime object, return as-is
            if isinstance(date_string, datetime):
                return date_string
            # Handle ISO format strings
            if isinstance(date_string, str):
                # Remove 'Z' and handle timezone info
                clean_string = date_string.replace('Z', '+00:00')
                return datetime.fromisoformat(clean_string)
            return date_string
        except (ValueError, AttributeError) as e:
            print(f"DEBUG: Failed to parse datetime '{date_string}': {e}")
            return None
        
    @staticmethod
    def create_job(job_data: Dict[str, Any]) -> str:
        """Create a new job record"""
        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            cursor.execute('''
            INSERT INTO jobs (
                id, filename, filepath, entity_column, type_column, 
                context_columns, data_sources, confidence_threshold, settings
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                job_data['id'],
                job_data['filename'],
                job_data['filepath'],
                job_data.get('entity_column'),
                job_data.get('type_column'),
                json.dumps(job_data.get('context_columns', [])),
                json.dumps(job_data.get('data_sources', [])),
                job_data.get('confidence_threshold', 0.8),
                json.dumps(job_data.get('settings', {}))
            ))
            
            conn.commit()
            return job_data['id']
    
    @staticmethod
    def get_job(job_id: str) -> Optional[Dict[str, Any]]:
        """Get a job by ID"""
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM jobs WHERE id = ?', (job_id,))
            row = cursor.fetchone()
            
            if row:
                job_data = {
                    'id': row['id'],
                    'filename': row['filename'],
                    'filepath': row['filepath'],
                    'status': row['status'],
                    'created_at': JobManager._parse_datetime(row['created_at']),
                    'completed_at': JobManager._parse_datetime(row['completed_at']),
                    'entity_column': row['entity_column'],
                    'type_column': row['type_column'],
                    'context_columns': json.loads(row['context_columns'] or '[]'),
                    'data_sources': json.loads(row['data_sources'] or '[]'),
                    'confidence_threshold': row['confidence_threshold'],
                    'progress': row['progress'],
                    'total_entities': row['total_entities'],
                    'successful_matches': row['successful_matches'],
                    'error_message': row['error_message'],
                    'settings': json.loads(row['settings'] or '{}')
                }
                
                # DEBUG: Print what we're returning
                print(f"DEBUG: Returning job with created_at type: {type(job_data['created_at'])}")
                print(f"DEBUG: created_at value: {job_data['created_at']}")
                
                return job_data
    
    @staticmethod
    def update_job(job_id: str, updates: Dict[str, Any]):
        """Update job fields"""
        if not updates:
            return
        
        # Build the SET clause dynamically
        set_parts = []
        values = []
        
        for key, value in updates.items():
            if key in ['context_columns', 'data_sources', 'settings'] and isinstance(value, (list, dict)):
                value = json.dumps(value)
            
            set_parts.append(f'{key} = ?')
            values.append(value)
        
        values.append(job_id)  # For the WHERE clause
        
        with get_db_connection() as conn:
            cursor = conn.cursor()
            query = f'UPDATE jobs SET {", ".join(set_parts)} WHERE id = ?'
            cursor.execute(query, values)
            conn.commit()
    
    @staticmethod
    def get_all_jobs() -> List[Dict[str, Any]]:
        """Get all jobs ordered by creation date with proper datetime conversion"""
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM jobs ORDER BY created_at DESC')
            rows = cursor.fetchall()
            
            jobs = []
            for row in rows:
                jobs.append({
                    'id': row['id'],
                    'filename': row['filename'],
                    'status': row['status'],
                    'created_at': JobManager._parse_datetime(row['created_at']),  # ← This is the key fix
                    'progress': row['progress'],
                    'total_entities': row['total_entities'],
                    'successful_matches': row['successful_matches']
                })
            
            return jobs

class ResultsManager:
    """
    Manages reconciliation results in the database.
    """
    
    @staticmethod
    def save_results(job_id: str, reconciliation_results: List) -> int:
        """Save reconciliation results to database"""
        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            saved_count = 0
            
            for result in reconciliation_results:
                # Insert the main result record
                cursor.execute('''
                INSERT INTO results (
                    job_id, entity_id, entity_name, entity_type, context,
                    confidence, sources_queried, cached, reconciliation_time
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    job_id,
                    result.entity.id,
                    result.entity.name,
                    result.entity.entity_type.value,
                    json.dumps(result.entity.context),
                    result.confidence.value,
                    json.dumps(result.sources_queried),
                    result.cached,
                    result.reconciliation_time
                ))
                
                result_id = cursor.lastrowid
                
                # Insert matches for this result
                for i, match in enumerate(result.matches):
                    is_best = (i == 0 and result.best_match and match.id == result.best_match.id)
                    
                    cursor.execute('''
                    INSERT INTO matches (
                        result_id, match_id, match_name, match_source, match_score,
                        match_description, additional_info, is_best_match
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        result_id,
                        match.id,
                        match.name,
                        match.source,
                        match.score,
                        match.description,
                        json.dumps(match.additional_info),
                        is_best
                    ))
                
                saved_count += 1
            
            conn.commit()
            return saved_count
    
    @staticmethod
    def get_results(job_id: str, page: int = 1, per_page: int = 10) -> tuple:
        """Get paginated results for a job"""
        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            # Get total count
            cursor.execute('SELECT COUNT(*) FROM results WHERE job_id = ?', (job_id,))
            total_count = cursor.fetchone()[0]
            
            # Get paginated results
            offset = (page - 1) * per_page
            cursor.execute('''
            SELECT * FROM results 
            WHERE job_id = ? 
            ORDER BY created_at 
            LIMIT ? OFFSET ?
            ''', (job_id, per_page, offset))
            
            result_rows = cursor.fetchall()
            
            # Get matches for each result
            formatted_results = []
            for result_row in result_rows:
                # Get matches for this result
                cursor.execute('''
                SELECT * FROM matches 
                WHERE result_id = ? 
                ORDER BY match_score DESC
                ''', (result_row['id'],))
                
                match_rows = cursor.fetchall()
                
                matches = []
                for match_row in match_rows:
                    matches.append({
                        'id': match_row['match_id'],
                        'name': match_row['match_name'],
                        'source': match_row['match_source'],
                        'score': match_row['match_score'],
                        'description': match_row['match_description'],
                        'additional_info': json.loads(match_row['additional_info'] or '{}'),
                        'user_approved': match_row['user_approved']
                    })
                
                # Format result
                formatted_result = {
                    'entity': {
                        'id': result_row['entity_id'],
                        'name': result_row['entity_name'],
                        'type': result_row['entity_type'],
                        'context': json.loads(result_row['context'] or '{}')
                    },
                    'confidence': result_row['confidence'],
                    'sources_queried': json.loads(result_row['sources_queried'] or '[]'),
                    'cached': bool(result_row['cached']),
                    'matches': matches
                }
                
                formatted_results.append(formatted_result)
            
            return formatted_results, total_count
    
    @staticmethod
    def approve_match(job_id: str, entity_id: str, match_id: str, approved: bool) -> bool:
        """Approve or reject a match"""
        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            # Find the match to update
            cursor.execute('''
            UPDATE matches 
            SET user_approved = ? 
            WHERE match_id = ? 
            AND result_id IN (
                SELECT id FROM results 
                WHERE job_id = ? AND entity_id = ?
            )
            ''', (approved, match_id, job_id, entity_id))
            
            conn.commit()
            return cursor.rowcount > 0


# Initialize database when module is imported
try:
    init_database()
except Exception as e:
    print(f"⚠️  Database initialization failed: {e}")


# Example usage and testing
if __name__ == "__main__":
    print("Testing database functionality...")
    
    # Test job creation
    job_data = {
        'id': 'test_job_123',
        'filename': 'test.csv',
        'filepath': '/path/to/test.csv',
        'entity_column': 'name',
        'data_sources': ['wikidata', 'viaf']
    }
    
    job_id = JobManager.create_job(job_data)
    print(f"Created job: {job_id}")
    
    # Test job retrieval
    job = JobManager.get_job(job_id)
    print(f"Retrieved job: {job['filename']}")
    
    # Test job update
    JobManager.update_job(job_id, {'status': 'completed', 'progress': 100})
    print("Updated job status")
    
    print("Database test completed!")