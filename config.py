# config.py
import os
import sys
from datetime import timedelta
from dotenv import load_dotenv
load_dotenv()

# Environment detection
ENVIRONMENT = os.getenv("FLASK_ENV", "development")
IS_PRODUCTION = ENVIRONMENT == "production"

# Flask App Config - No hardcoded fallbacks for sensitive values
SECRET_KEY = os.getenv("SECRET_KEY")
DATABASE_URL = os.getenv("DATABASE_URL")

# Validate critical environment variables early
if not SECRET_KEY:
    if IS_PRODUCTION:
        print("❌ CRITICAL: SECRET_KEY environment variable is required in production")
        sys.exit(1)
    else:
        print("⚠️  WARNING: Using development SECRET_KEY - DO NOT use in production")
        SECRET_KEY = "dev-only-secret-key-change-in-production"

if not DATABASE_URL:
    if IS_PRODUCTION:
        print("❌ CRITICAL: DATABASE_URL environment variable is required in production")
        sys.exit(1)
    else:
        print("⚠️  WARNING: DATABASE_URL not set - using localhost default for development")
        DATABASE_URL = "postgresql://postgres:postgres@localhost:5432/vigilbuild"
WEATHER_API_KEY = os.getenv("WEATHER_API_KEY")
OLLAMA_HOST = os.getenv("OLLAMA_HOST", "http://127.0.0.1:11434")
MAX_IMAGE_SIZE = 5 * 1024 * 1024  # 5MB
ALLOWED_IMAGE_EXTENSIONS = {'png', 'jpg', 'jpeg', 'gif', 'webp'}
ALLOWED_DOCUMENT_EXTENSIONS = {'pdf', 'doc', 'docx', 'xls', 'xlsx'}

# External Services
ES_HOST = os.getenv("ES_HOST", "http://localhost:9200")
ES_USER = os.getenv("ES_USER", "elastic")
ES_PASS = os.getenv("ES_PASS")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY")  # Optional
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY")  # Optional
S3_BUCKET = os.getenv("S3_BUCKET")  # Optional
SMTP_SERVER = os.getenv("SMTP_SERVER", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", 587))
SMTP_USER = os.getenv("SMTP_USER")
SMTP_PASS = os.getenv("SMTP_PASS")
OUTLOOK_CLIENT_ID = os.getenv("OUTLOOK_CLIENT_ID")
OUTLOOK_CLIENT_SECRET = os.getenv("OUTLOOK_CLIENT_SECRET")
OUTLOOK_AUTHORITY = os.getenv("OUTLOOK_AUTHORITY", "https://login.microsoftonline.com/common")
OUTLOOK_REDIRECT_URI = os.getenv("OUTLOOK_REDIRECT_URI", "http://localhost:5000/outlook_callback")
OUTLOOK_SCOPES = ["https://graph.microsoft.com/Mail.Send", "https://graph.microsoft.com/User.Read"]
NOTIFICATION_RECIPIENTS = os.getenv("NOTIFICATION_RECIPIENTS", "recipient1@example.com,recipient2@example.com").split(",")
UPLOAD_FOLDER = os.getenv("UPLOAD_FOLDER", "Uploads")
DOCUMENT_FOLDER = os.getenv("DOCUMENT_FOLDER", "Uploads")
INVENTORY_IMAGE_FOLDER = os.path.join(UPLOAD_FOLDER, 'inventory_images')
ALLOWED_EXTENSIONS = {'pdf', 'docx', 'txt'}
## moved earlier to ensure UPLOAD_FOLDER is defined

# ML Model settings for usage prediction
ML_MODEL_ENABLED = os.environ.get('ML_MODEL_ENABLED', 'false').lower() == 'true'
ML_MODEL_UPDATE_INTERVAL = timedelta(days=7)  # Retrain model weekly
MIN_HISTORICAL_DATA_POINTS = 10  # Minimum job records needed for predictions

# Inventory alert settings
LOW_STOCK_ALERT_THRESHOLD = 0.2  # Alert when stock is 20% of reorder point
CRITICAL_STOCK_ALERT_THRESHOLD = 0.1  # Critical when stock is 10% of reorder point
NOTIFICATION_RECIPIENTS = os.environ.get('NOTIFICATION_RECIPIENTS', '').split(',')

# Bulk import settings
MAX_BULK_IMPORT_ROWS = 10000
BULK_IMPORT_TIMEOUT = 300  # 5 minutes

# Application Settings - DEBUG is NEVER true in production
DEBUG = os.environ.get('DEBUG', 'false').lower() == 'true' and not IS_PRODUCTION
TESTING = os.environ.get('TESTING', 'false').lower() == 'true'

# Security: Force DEBUG off in production regardless of env var
if IS_PRODUCTION and DEBUG:
    print("⚠️  WARNING: DEBUG was set to True in production - forcing to False for security")
    DEBUG = False

# Logging Configuration
LOG_LEVEL = os.environ.get('LOG_LEVEL', 'INFO')
LOG_FILE = os.path.join(UPLOAD_FOLDER, 'application.log')

# Database Connection Pool Settings
DB_POOL_SIZE = int(os.environ.get('DB_POOL_SIZE', '20'))
DB_POOL_OVERFLOW = int(os.environ.get('DB_POOL_OVERFLOW', '30'))

# Session Configuration
PERMANENT_SESSION_LIFETIME = timedelta(hours=int(os.environ.get('SESSION_LIFETIME_HOURS', '24')))

# Pagination Settings
ITEMS_PER_PAGE = int(os.environ.get('ITEMS_PER_PAGE', '50'))
MAX_ITEMS_PER_PAGE = int(os.environ.get('MAX_ITEMS_PER_PAGE', '200'))

# Cache Configuration
CACHE_TYPE = os.environ.get('CACHE_TYPE', 'simple')
CACHE_DEFAULT_TIMEOUT = int(os.environ.get('CACHE_DEFAULT_TIMEOUT', '300'))

# Email Configuration (if notifications are enabled)
MAIL_SERVER = os.environ.get('MAIL_SERVER', 'localhost')
MAIL_PORT = int(os.environ.get('MAIL_PORT', '587'))
MAIL_USE_TLS = os.environ.get('MAIL_USE_TLS', 'true').lower() == 'true'
MAIL_USERNAME = os.environ.get('MAIL_USERNAME', '')
MAIL_PASSWORD = os.environ.get('MAIL_PASSWORD', '')
MAIL_DEFAULT_SENDER = os.environ.get('MAIL_DEFAULT_SENDER', 'noreply@potelco.com')

# Backup Configuration
BACKUP_ENABLED = os.environ.get('BACKUP_ENABLED', 'true').lower() == 'true'
BACKUP_SCHEDULE = os.environ.get('BACKUP_SCHEDULE', '0 2 * * *')  # Daily at 2 AM
BACKUP_RETENTION_DAYS = int(os.environ.get('BACKUP_RETENTION_DAYS', '30'))

# Configuration status output
print(f"[OK] Environment: {ENVIRONMENT}")
print(f"[OK] Debug mode: {DEBUG}")
print(f"[OK] Configuration loaded - Upload folder: {UPLOAD_FOLDER}")
print(f"[OK] Inventory images folder: {INVENTORY_IMAGE_FOLDER}")
print(f"[OK] Database URL configured: {'Yes' if DATABASE_URL else 'No'}")
print(f"[OK] ML Model enabled: {ML_MODEL_ENABLED}")
print(f"[OK] Notification recipients: {len(NOTIFICATION_RECIPIENTS)}")

# Validate required env vars based on environment
if IS_PRODUCTION:
    REQUIRED_VARS = ["SECRET_KEY", "DATABASE_URL", "ES_HOST", "ES_USER", "ES_PASS"]
    OPTIONAL_WARNED_VARS = ["WEATHER_API_KEY", "SMTP_USER", "SMTP_PASS"]

    for var in REQUIRED_VARS:
        if not os.getenv(var):
            print(f"❌ CRITICAL: Missing required environment variable: {var}")
            sys.exit(1)

    for var in OPTIONAL_WARNED_VARS:
        if not os.getenv(var):
            print(f"⚠️  WARNING: Missing optional environment variable: {var} - Some features may be disabled")
else:
    # Development mode - warn but don't fail
    REQUIRED_VARS = ["SECRET_KEY", "DATABASE_URL"]
    for var in REQUIRED_VARS:
        if not os.getenv(var):
            print(f"⚠️  DEV WARNING: Missing environment variable: {var} - using defaults")

# Validate directories
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(DOCUMENT_FOLDER, exist_ok=True)
os.makedirs(INVENTORY_IMAGE_FOLDER, exist_ok=True)