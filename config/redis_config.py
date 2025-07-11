# File: config/redis_config.py
import os

class RedisConfig:
    """Redis configuration settings"""
    
    # Default Redis settings
    REDIS_HOST = os.environ.get('REDIS_HOST', 'localhost')
    REDIS_PORT = int(os.environ.get('REDIS_PORT', 6379))
    REDIS_DB = int(os.environ.get('REDIS_DB', 0))
    REDIS_PASSWORD = os.environ.get('REDIS_PASSWORD', None)
    
    # Construct Redis URL
    if REDIS_PASSWORD:
        REDIS_URL = f"redis://:{REDIS_PASSWORD}@{REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}"
    else:
        REDIS_URL = f"redis://{REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}"
    
    # Celery settings
    CELERY_BROKER_URL = os.environ.get('CELERY_BROKER_URL', REDIS_URL)
    CELERY_RESULT_BACKEND = os.environ.get('CELERY_RESULT_BACKEND', REDIS_URL)
    
    @classmethod
    def test_connection(cls):
        """Test Redis connection"""
        try:
            import redis
            r = redis.Redis.from_url(cls.REDIS_URL)
            r.ping()
            return True, "Redis connection successful"
        except Exception as e:
            return False, f"Redis connection failed: {e}"


# File: .env.example
# Create this file as an example for users
"""
# Redis Configuration (optional - defaults to localhost)
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_DB=0
REDIS_PASSWORD=

# For production, you might use:
# REDIS_URL=redis://user:password@hostname:port/db
# CELERY_BROKER_URL=redis://user:password@hostname:port/db
# CELERY_RESULT_BACKEND=redis://user:password@hostname:port/db
"""