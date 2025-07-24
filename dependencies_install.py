#!/usr/bin/env python3
"""
Quick dependency installer for the Metadata Reconciliation Tool
Run this to install missing dependencies automatically
"""

import subprocess
import sys
import importlib

REQUIRED_PACKAGES = {
    'requests': 'requests>=2.31.0',
    'pandas': 'pandas>=2.0.3', 
    'flask': 'Flask>=2.3.3',
    'redis': 'redis>=5.0.1',
    'celery': 'celery>=5.3.4'
}

OPTIONAL_PACKAGES = {
    'beautifulsoup4': 'beautifulsoup4>=4.12.2',
    'lxml': 'lxml>=4.9.3',
    'python-dotenv': 'python-dotenv>=1.0.0'
}

def check_package(package_name):
    """Check if a package is installed"""
    try:
        importlib.import_module(package_name)
        return True
    except ImportError:
        return False

def install_package(package_spec):
    """Install a package using pip"""
    try:
        subprocess.check_call([sys.executable, '-m', 'pip', 'install', package_spec])
        return True
    except subprocess.CalledProcessError:
        return False

def main():
    print("🔍 Checking dependencies for Metadata Reconciliation Tool...")
    print("=" * 60)
    
    missing_required = []
    missing_optional = []
    
    # Check required packages
    print("\n📦 Required packages:")
    for package_name, package_spec in REQUIRED_PACKAGES.items():
        if check_package(package_name):
            print(f"  ✅ {package_name} - installed")
        else:
            print(f"  ❌ {package_name} - missing")
            missing_required.append(package_spec)
    
    # Check optional packages
    print("\n📦 Optional packages:")
    for package_name, package_spec in OPTIONAL_PACKAGES.items():
        if check_package(package_name):
            print(f"  ✅ {package_name} - installed")
        else:
            print(f"  ⚠️  {package_name} - missing (optional)")
            missing_optional.append(package_spec)
    
    # Install missing packages
    if missing_required:
        print(f"\n🚀 Installing {len(missing_required)} required packages...")
        for package_spec in missing_required:
            print(f"  Installing {package_spec}...")
            if install_package(package_spec):
                print(f"    ✅ Successfully installed {package_spec}")
            else:
                print(f"    ❌ Failed to install {package_spec}")
                print(f"    Try manually: pip install {package_spec}")
    
    if missing_optional and input("\n❓ Install optional packages too? (y/n): ").lower().startswith('y'):
        print(f"\n🚀 Installing {len(missing_optional)} optional packages...")
        for package_spec in missing_optional:
            print(f"  Installing {package_spec}...")
            if install_package(package_spec):
                print(f"    ✅ Successfully installed {package_spec}")
            else:
                print(f"    ❌ Failed to install {package_spec}")
    
    # Final check
    print("\n" + "=" * 60)
    print("🔍 Final dependency check:")
    
    all_good = True
    for package_name in REQUIRED_PACKAGES.keys():
        if check_package(package_name):
            print(f"  ✅ {package_name}")
        else:
            print(f"  ❌ {package_name} - still missing!")
            all_good = False
    
    if all_good:
        print("\n🎉 All required dependencies are installed!")
        print("You can now run: python run.py")
    else:
        print("\n❌ Some required dependencies are still missing.")
        print("Try installing them manually:")
        for package_name, package_spec in REQUIRED_PACKAGES.items():
            if not check_package(package_name):
                print(f"  pip install {package_spec}")
        
        print("\nOr try: pip install -r requirements.txt")

if __name__ == "__main__":
    main()