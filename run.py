# File: run.py
"""
Fixed entry point for the Metadata Reconciliation Tool.
This removes the duplicate route registration that was causing the Flask error.
"""

from app.main import create_app

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