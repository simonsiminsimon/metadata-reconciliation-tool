# Test database connectivity
from app.database import get_db_connection, ResultsManager

# Check if results exist
with get_db_connection() as conn:
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM results WHERE job_id = ?", (/api/jobs/d9f65325-de6f-4ed0-9eef-edb22d2a4562),)
    result_count = cursor.fetchone()[0]
    print(f"📊 Found {result_count} results in database")
    
    cursor.execute("SELECT COUNT(*) FROM matches")
    match_count = cursor.fetchone()[0]
    print(f"🎯 Found {match_count} matches in database")