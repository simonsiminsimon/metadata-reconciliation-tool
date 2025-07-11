# Add this to app/main.py
from flask import Flask
import os

def create_app():
    app = Flask(__name__)
    app.config['SECRET_KEY'] = 'your-secret-key-change-this'
    app.config['UPLOAD_FOLDER'] = 'data/input'
    app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024  # 50MB max file size
    
    # Ensure directories exist
    os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
    os.makedirs('data/output', exist_ok=True)
    os.makedirs('data/cache', exist_ok=True)
    
    # Initialize database (this is new!)
    from app.database import init_database
    init_database()
    
    # Import and register routes
    from app.routes.web_with_background import register_web_routes
    from app.routes.api import register_api_routes
    
    register_web_routes(app)
    register_api_routes(app)
    
    return app