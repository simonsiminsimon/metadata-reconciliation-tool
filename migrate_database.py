# File: migrate_database.py
# Run this script to update your database schema

import sqlite3
import os
from pathlib import Path

DB_PATH = 'data/reconciliation.db'

def backup_database():
    """Create a backup of the current database"""
    if os.path.exists(DB_PATH):
        backup_path = f'{DB_PATH}.backup'
        import shutil
        shutil.copy2(DB_PATH, backup_path)
        print(f"✅ Database backed up to: {backup_path}")
        return backup_path
    return None

def check_column_exists(cursor, table_name, column_name):
    """Check if a column exists in a table"""
    cursor.execute(f"PRAGMA table_info({table_name})")
    columns = cursor.fetchall()
    column_names = [column[1] for column in columns]
    return column_name in column_names

def migrate_jobs_table():
    """Add missing columns to the jobs table"""
    print("🔄 Migrating jobs table...")
    
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        
        # Check what columns we need to add
        missing_columns = []
        
        # List of columns that should exist for background jobs
        required_columns = {
            'task_id': 'TEXT',
            'entity_column': 'TEXT',
            'type_column': 'TEXT', 
            'context_columns': 'TEXT',
            'data_sources': 'TEXT',
            'confidence_threshold': 'REAL',
            'settings': 'TEXT'
        }
        
        for column_name, column_type in required_columns.items():
            if not check_column_exists(cursor, 'jobs', column_name):
                missing_columns.append((column_name, column_type))
        
        # Add missing columns
        for column_name, column_type in missing_columns:
            try:
                query = f"ALTER TABLE jobs ADD COLUMN {column_name} {column_type}"
                cursor.execute(query)
                print(f"  ✅ Added column: {column_name}")
            except sqlite3.OperationalError as e:
                if "duplicate column name" in str(e).lower():
                    print(f"  ⚠️  Column {column_name} already exists")
                else:
                    print(f"  ❌ Failed to add column {column_name}: {e}")
        
        conn.commit()
        
        if missing_columns:
            print(f"✅ Successfully migrated jobs table ({len(missing_columns)} columns added)")
        else:
            print("✅ Jobs table already up to date")

def verify_schema():
    """Verify the database schema is correct"""
    print("🔍 Verifying database schema...")
    
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        
        # Check jobs table
        cursor.execute("PRAGMA table_info(jobs)")
        job_columns = [column[1] for column in cursor.fetchall()]
        
        required_job_columns = [
            'id', 'filename', 'filepath', 'status', 'created_at', 'completed_at',
            'entity_column', 'type_column', 'context_columns', 'data_sources',
            'confidence_threshold', 'progress', 'total_entities', 'successful_matches',
            'error_message', 'settings', 'task_id'
        ]
        
        missing_job_columns = [col for col in required_job_columns if col not in job_columns]
        
        if missing_job_columns:
            print(f"❌ Missing columns in jobs table: {missing_job_columns}")
            return False
        else:
            print("✅ Jobs table schema is correct")
        
        # Check other tables exist
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [table[0] for table in cursor.fetchall()]
        
        required_tables = ['jobs', 'results', 'matches']
        missing_tables = [table for table in required_tables if table not in tables]
        
        if missing_tables:
            print(f"❌ Missing tables: {missing_tables}")
            return False
        else:
            print("✅ All required tables exist")
        
        return True

def run_migration():
    """Run the complete database migration"""
    print("🚀 Starting database migration...")
    print("=" * 50)
    
    # Create data directory if it doesn't exist
    os.makedirs('data', exist_ok=True)
    
    # Backup existing database
    backup_path = backup_database()
    
    try:
        # Run the migration
        migrate_jobs_table()
        
        # Verify everything worked
        if verify_schema():
            print("\n" + "=" * 50)
            print("🎉 Database migration completed successfully!")
            print("\nYour database now supports:")
            print("  ✅ Background job tracking")
            print("  ✅ Task ID storage")
            print("  ✅ Enhanced job configuration")
            
            if backup_path:
                print(f"\n📁 Backup created at: {backup_path}")
                print("   (You can delete this once you verify everything works)")
        else:
            print("\n❌ Migration verification failed!")
            return False
            
    except Exception as e:
        print(f"\n❌ Migration failed: {e}")
        if backup_path:
            print(f"Your original database backup is at: {backup_path}")
        return False
    
    return True

if __name__ == "__main__":
    success = run_migration()
    
    if success:
        print("\n🚀 You can now restart your Flask application!")
        print("   The background job features should work correctly.")
    else:
        print("\n💡 If you continue to have issues, you can:")
        print("   1. Delete the database file to start fresh")
        print("   2. Or restore from the backup and try again")
    
    exit(0 if success else 1)