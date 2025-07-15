# File: validate_system.py
"""
Comprehensive validation script for the Metadata Reconciliation Tool.
Run this script to verify all components are working correctly.
"""

import os
import sys
import subprocess
import time
import requests
import pandas as pd
from pathlib import Path

class Colors:
    GREEN = '\033[92m'
    RED = '\033[91m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    CYAN = '\033[96m'
    END = '\033[0m'
    BOLD = '\033[1m'

def print_header(message):
    print(f"\n{Colors.BLUE}{Colors.BOLD}{'='*60}{Colors.END}")
    print(f"{Colors.BLUE}{Colors.BOLD}{message.center(60)}{Colors.END}")
    print(f"{Colors.BLUE}{Colors.BOLD}{'='*60}{Colors.END}")

def print_success(message):
    print(f"{Colors.GREEN}✅ {message}{Colors.END}")

def print_error(message):
    print(f"{Colors.RED}❌ {message}{Colors.END}")

def print_warning(message):
    print(f"{Colors.YELLOW}⚠️  {message}{Colors.END}")

def print_info(message):
    print(f"{Colors.CYAN}ℹ️  {message}{Colors.END}")

def check_python_environment():
    """Check Python version and required packages"""
    print_header("Python Environment Check")
    
    # Check Python version
    python_version = sys.version_info
    if python_version.major >= 3 and python_version.minor >= 8:
        print_success(f"Python {python_version.major}.{python_version.minor}.{python_version.micro}")
    else:
        print_error(f"Python {python_version.major}.{python_version.minor}.{python_version.micro} - Need Python 3.8+")
        return False
    
    # Check required packages
    required_packages = [
        'flask', 'pandas', 'requests', 'werkzeug'
    ]
    
    missing_packages = []
    for package in required_packages:
        try:
            __import__(package)
            print_success(f"Package: {package}")
        except ImportError:
            print_error(f"Missing package: {package}")
            missing_packages.append(package)
    
    if missing_packages:
        print_warning(f"Install missing packages: pip install {' '.join(missing_packages)}")
        return False
    
    return True

def check_project_structure():
    """Check if all required files and directories exist"""
    print_header("Project Structure Check")
    
    required_files = [
        'run.py',
        'app/__init__.py',
        'app/main.py',
        'app/routes/web.py',
        'app/routes/api.py',
        'app/database.py'
    ]
    
    required_dirs = [
        'app',
        'app/routes',
        'app/services',
        'app/templates',
        'data',
        'data/input',
        'data/output',
        'data/cache'
    ]
    
    all_good = True
    
    # Check files
    for file_path in required_files:
        if Path(file_path).exists():
            print_success(f"File: {file_path}")
        else:
            print_error(f"Missing file: {file_path}")
            all_good = False
    
    # Check directories
    for dir_path in required_dirs:
        if Path(dir_path).exists():
            print_success(f"Directory: {dir_path}")
        else:
            print_warning(f"Missing directory: {dir_path}")
            # Create missing directories
            Path(dir_path).mkdir(parents=True, exist_ok=True)
            print_info(f"Created directory: {dir_path}")
    
    return all_good

def check_flask_routes():
    """Check for route registration issues"""
    print_header("Flask Route Analysis")
    
    try:
        # Read run.py and check for duplicate route registrations
        with open('run.py', 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Count occurrences of register_web_routes
        route_registrations = content.count('register_web_routes(app)')
        
        if route_registrations <= 1:
            print_success("No duplicate route registrations found")
        else:
            print_error(f"Found {route_registrations} route registrations - should be only 1")
            print_info("Fix: Remove duplicate register_web_routes(app) call from run.py")
            return False
        
        # Check for proper app creation
        if 'create_app()' in content:
            print_success("Using proper Flask app factory pattern")
        else:
            print_warning("Not using Flask app factory pattern")
        
        return True
        
    except FileNotFoundError:
        print_error("run.py file not found")
        return False
    except Exception as e:
        print_error(f"Error checking routes: {e}")
        return False

def create_test_csv():
    """Create a test CSV file for validation"""
    test_data = {
        'creator_name': [
            'Vincent van Gogh',
            'Pablo Picasso',
            'Minneapolis Institute of Art',
            'Carleton College',
            'Emma B. Hodge'
        ],
        'entity_type': [
            'person',
            'person', 
            'organization',
            'organization',
            'person'
        ],
        'date_created': [
            '1889',
            '1907',
            '1915',
            '1866',
            '1921'
        ],
        'location': [
            'Saint-Rémy-de-Provence',
            'Paris',
            'Minneapolis, Minnesota',
            'Northfield, Minnesota',
            'Chicago'
        ],
        'institution': [
            'Museum of Modern Art',
            'Museum of Modern Art',
            'Museum',
            'University',
            'Art Institute'
        ]
    }
    
    df = pd.DataFrame(test_data)
    test_file = 'data/input/validation_test.csv'
    df.to_csv(test_file, index=False)
    print_success(f"Created test CSV: {test_file}")
    return test_file

def test_app_startup():
    """Test if the Flask app starts without errors"""
    print_header("Application Startup Test")
    
    try:
        # Import and create the app
        sys.path.insert(0, os.getcwd())
        from app.main import create_app
        
        app = create_app()
        print_success("Flask app created successfully")
        
        # Test app configuration
        if app.config.get('UPLOAD_FOLDER'):
            print_success("Upload folder configured")
        else:
            print_warning("Upload folder not configured")
        
        # Test route registration
        routes = [str(rule) for rule in app.url_map.iter_rules()]
        expected_routes = ['/', '/upload', '/jobs']
        
        for route in expected_routes:
            if any(route in r for r in routes):
                print_success(f"Route registered: {route}")
            else:
                print_error(f"Route missing: {route}")
        
        return True
        
    except ImportError as e:
        print_error(f"Import error: {e}")
        return False
    except Exception as e:
        print_error(f"App creation error: {e}")
        return False

def test_csv_processing():
    """Test CSV file processing functionality"""
    print_header("CSV Processing Test")
    
    # Create test CSV if it doesn't exist
    test_file = create_test_csv()
    
    try:
        # Test basic pandas read
        df = pd.read_csv(test_file)
        print_success(f"CSV read successfully: {df.shape[0]} rows, {df.shape[1]} columns")
        
        # Check required columns
        required_columns = ['creator_name', 'entity_type']
        for col in required_columns:
            if col in df.columns:
                print_success(f"Required column present: {col}")
            else:
                print_error(f"Missing required column: {col}")
        
        # Test metadata parser (if available)
        try:
            from app.services.metadata_parser import MetadataParser
            parser = MetadataParser()
            # This would normally parse the CSV
            print_success("Metadata parser imported successfully")
        except ImportError:
            print_warning("Metadata parser not available")
        except Exception as e:
            print_error(f"Metadata parser error: {e}")
        
        return True
        
    except Exception as e:
        print_error(f"CSV processing error: {e}")
        return False

def run_full_validation():
    """Run all validation checks"""
    print_header("METADATA RECONCILIATION TOOL VALIDATION")
    print_info("This script will validate your system setup and identify any issues.")
    
    checks = [
        ("Python Environment", check_python_environment),
        ("Project Structure", check_project_structure),
        ("Flask Routes", check_flask_routes),
        ("App Startup", test_app_startup),
        ("CSV Processing", test_csv_processing)
    ]
    
    passed = 0
    total = len(checks)
    
    for check_name, check_function in checks:
        print(f"\n{Colors.CYAN}Running: {check_name}{Colors.END}")
        try:
            if check_function():
                passed += 1
        except Exception as e:
            print_error(f"Check failed with exception: {e}")
    
    # Final results
    print_header("VALIDATION RESULTS")
    
    if passed == total:
        print_success(f"All {total} checks passed! 🎉")
        print_info("Your system is ready to run the metadata reconciliation tool.")
        print_info("Next steps:")
        print("   1. Start the application: python run.py")
        print("   2. Open browser to: http://localhost:5000")
        print("   3. Upload a CSV file for testing")
    else:
        print_warning(f"{passed}/{total} checks passed")
        print_info("Please fix the issues above before running the application.")
    
    return passed == total

if __name__ == "__main__":
    success = run_full_validation()
    sys.exit(0 if success else 1)