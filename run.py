# File: run.py
"""
Fixed entry point for the Metadata Reconciliation Tool.
This removes the duplicate route registration that was causing the Flask error.
"""

from app.main import create_app
from datetime import datetime

def format_datetime(value, format='%Y-%m-%d %H:%M:%S'):
    """Custom Jinja2 filter to safely format datetime strings or objects"""
    if not value:
        return 'Unknown'
    
    # If it's already a datetime object, use it directly
    if hasattr(value, 'strftime'):
        return value.strftime(format)
    
    # If it's a string, try to parse it
    if isinstance(value, str):
        try:
            # Try to parse ISO format
            dt = datetime.fromisoformat(value.replace('Z', '+00:00'))
            return dt.strftime(format)
        except (ValueError, AttributeError):
            # If parsing fails, return the string as-is (truncated if needed)
            return value[:19] if len(value) > 19 else value
    
    return str(value)

if __name__ == '__main__':
    # Create the app (routes are already registered in create_app())
    app = create_app()
    
    # DO NOT register routes again here - they're already registered in create_app()
    # The following line was causing the duplicate endpoint error:
    # register_web_routes(app)  # ❌ REMOVE THIS LINE
    
    # Start the development server
    print("🚀 Starting Metadata Reconciliation Tool...")
    print("📍 Server will be available at: http://localhost:5000")
    print("💡 Press Ctrl+C to stop the server")
    
    app.run(
        debug=True,
        host='0.0.0.0',
        port=5000,
        use_reloader=True,
        use_debugger=True
    )
    app.jinja_env.filters['datetime'] = format_datetime