import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    SECRET_KEY = os.environ.get('SECRET_KEY') or 'dev-secret-key'
    SQLALCHEMY_DATABASE_URI = os.environ.get('DATABASE_URL') or 'sqlite:///metadata_reconciliation.db'
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    
    # Cache configuration
    CACHE_TYPE = 'redis'
    CACHE_REDIS_URL = os.environ.get('REDIS_URL') or 'redis://localhost:6379/0'
    
    # External API settings
    WIKIDATA_API_URL = os.environ.get('WIKIDATA_API_URL') or 'https://www.wikidata.org/w/api.php'
    VIAF_API_URL = os.environ.get('VIAF_API_URL') or 'http://viaf.org/viaf/search'