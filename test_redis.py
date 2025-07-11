# File: test_redis.py
# Run this to test if Redis is working

import sys

def test_redis_basic():
    """Test basic Redis connection"""
    try:
        import redis
        print("✅ Redis library installed")
        
        # Try to connect
        r = redis.Redis(host='localhost', port=6379, db=0)
        response = r.ping()
        
        if response:
            print("✅ Redis server is running and accessible")
            
            # Test basic operations
            r.set('test_key', 'Hello Redis!')
            value = r.get('test_key')
            print(f"✅ Redis read/write test: {value.decode('utf-8')}")
            
            # Clean up
            r.delete('test_key')
            print("✅ Redis cleanup successful")
            
            return True
            
    except redis.ConnectionError:
        print("❌ Cannot connect to Redis server")
        print("   Make sure Redis is running on localhost:6379")
        return False
    except ImportError:
        print("❌ Redis library not installed")
        print("   Run: pip install redis")
        return False
    except Exception as e:
        print(f"❌ Redis test failed: {e}")
        return False

def test_celery_connection():
    """Test Celery with Redis"""
    try:
        from app.background_jobs import celery_app, test_redis_connection
        print("✅ Celery background jobs module loaded")
        
        # Test Redis connection through Celery config
        success, message = test_redis_connection()
        if success:
            print(f"✅ {message}")
        else:
            print(f"❌ {message}")
            return False
        
        # Test Celery broker connection
        try:
            inspect = celery_app.control.inspect()
            stats = inspect.stats()
            if stats:
                print("✅ Celery broker connection successful")
                print(f"   Active workers: {len(stats)}")
            else:
                print("⚠️  Celery broker connected but no workers running")
                print("   You'll need to start a worker to process jobs")
        except Exception as e:
            print(f"⚠️  Celery broker test failed: {e}")
            print("   This is normal if no workers are running yet")
        
        return True
        
    except ImportError as e:
        print(f"❌ Cannot import background jobs: {e}")
        print("   Make sure you have celery installed: pip install celery")
        return False
    except Exception as e:
        print(f"❌ Celery test failed: {e}")
        return False

def test_full_system():
    """Test the complete system"""
    print("🔍 Testing Redis + Celery Integration")
    print("=" * 50)
    
    # Test 1: Basic Redis
    print("\n1. Testing Redis Connection...")
    redis_ok = test_redis_basic()
    
    if not redis_ok:
        print("\n❌ Redis test failed. Please fix Redis connection first.")
        return False
    
    # Test 2: Celery
    print("\n2. Testing Celery Integration...")
    celery_ok = test_celery_connection()
    
    if not celery_ok:
        print("\n❌ Celery test failed. Check your background_jobs.py file.")
        return False
    
    # Test 3: System status
    print("\n3. Testing System Status API...")
    try:
        from app.routes.api import BACKGROUND_JOBS_AVAILABLE
        if BACKGROUND_JOBS_AVAILABLE:
            print("✅ Background jobs are available in the web application")
        else:
            print("❌ Background jobs not available in web application")
            return False
    except Exception as e:
        print(f"⚠️  Could not test web application: {e}")
    
    print("\n" + "=" * 50)
    print("🎉 All tests passed! Your Redis + Celery setup is working.")
    print("\nNext steps:")
    print("1. Start a Celery worker: celery -A app.background_jobs worker --loglevel=info")
    print("2. Start your Flask app: python run.py")
    print("3. Upload a CSV file to test background processing")
    
    return True

if __name__ == "__main__":
    success = test_full_system()
    sys.exit(0 if success else 1)