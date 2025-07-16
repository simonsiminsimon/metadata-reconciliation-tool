#!/usr/bin/env python3
"""
Database Schema Fix - Add Missing task_id Column
Run this to fix the "no such column: task_id" error
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.database import get_db_connection
import logging

logger = logging.getLogger(__name__)

def fix_database_schema():
    """Add missing task_id column to jobs table"""
    print("🔧 FIXING DATABASE SCHEMA")
    print("=" * 50)
    
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            # Check if task_id column exists
            cursor.execute("PRAGMA table_info(jobs)")
            columns = [row[1] for row in cursor.fetchall()]
            
            if 'task_id' in columns:
                print("✅ task_id column already exists")
                return True
            
            print("⚠️  task_id column missing - adding it now...")
            
            # Add the missing column
            cursor.execute("ALTER TABLE jobs ADD COLUMN task_id TEXT")
            conn.commit()
            
            print("✅ task_id column added successfully")
            
            # Verify the addition
            cursor.execute("PRAGMA table_info(jobs)")
            new_columns = [row[1] for row in cursor.fetchall()]
            
            if 'task_id' in new_columns:
                print("✅ Database schema fix verified")
                return True
            else:
                print("❌ Failed to add task_id column")
                return False
                
    except Exception as e:
        print(f"❌ Database schema fix failed: {e}")
        return False

def check_database_health():
    """Check overall database health"""
    print("\n📊 DATABASE HEALTH CHECK")
    print("=" * 50)
    
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            # Check tables exist
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = [row[0] for row in cursor.fetchall()]
            
            required_tables = ['jobs', 'results', 'matches']
            
            for table in required_tables:
                if table in tables:
                    cursor.execute(f"SELECT COUNT(*) FROM {table}")
                    count = cursor.fetchone()[0]
                    print(f"✅ {table} table: {count} records")
                else:
                    print(f"❌ {table} table: MISSING")
            
            # Check jobs table schema
            cursor.execute("PRAGMA table_info(jobs)")
            job_columns = [row[1] for row in cursor.fetchall()]
            
            required_job_columns = [
                'id', 'filename', 'filepath', 'status', 'progress', 
                'total_entities', 'successful_matches', 'created_at'
            ]
            
            print(f"\n📋 Jobs table columns:")
            for col in required_job_columns:
                status = "✅" if col in job_columns else "❌"
                print(f"   {status} {col}")
            
            if 'task_id' in job_columns:
                print(f"   ✅ task_id (after fix)")
            
            return True
            
    except Exception as e:
        print(f"❌ Database health check failed: {e}")
        return False

if __name__ == "__main__":
    print("🔧 METADATA RECONCILIATION DATABASE FIX")
    print("=" * 60)
    
    # Fix the schema
    schema_fixed = fix_database_schema()
    
    # Check health
    health_ok = check_database_health()
    
    if schema_fixed and health_ok:
        print("\n🎉 DATABASE FIXES COMPLETE!")
        print("✅ Background jobs should now work properly")
        print("✅ Threaded processing fallback will work")
        print("✅ You can now upload files without database errors")
    else:
        print("\n⚠️  SOME ISSUES REMAIN")
        print("❌ Check the errors above")
        print("❌ You may need to recreate the database")
    
    print(f"\n🔗 Try uploading again at: http://localhost:5000/upload")