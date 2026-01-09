# services/config_validator.py
"""
Configuration validation and environment setup for inventory system
"""

import os
import sys
import logging
import json
from typing import List, Dict, Any, Optional
from pathlib import Path
from services.db import db_connection, get_connection, release_connection
import psycopg2  # Keep for OperationalError handling

logger = logging.getLogger(__name__)

class ConfigValidator:
    """Validates system configuration and environment setup"""
    
    def __init__(self):
        self.errors = []
        self.warnings = []
        self.config_items = {}
    
    def validate_all(self) -> Dict[str, Any]:
        """Run all configuration validations"""
        self.errors = []
        self.warnings = []
        
        print("🔧 Validating Inventory System Configuration...")
        print("=" * 60)
        
        # Core validations
        self._validate_environment_variables()
        self._validate_database_connection()
        self._validate_file_system()
        self._validate_python_dependencies()
        self._validate_database_schema()
        self._validate_permissions()
        
        # Generate report
        report = {
            'status': 'pass' if not self.errors else 'fail',
            'errors': self.errors,
            'warnings': self.warnings,
            'config_items': self.config_items,
            'recommendations': self._generate_recommendations()
        }
        
        self._print_report(report)
        return report
    
    def _validate_environment_variables(self):
        """Validate required environment variables"""
        print("📋 Checking environment variables...")
        
        required_vars = {
            'DATABASE_URL': 'PostgreSQL database connection string',
            'DOCUMENT_FOLDER': 'Base folder for file uploads',
            'WEATHER_API_KEY': 'OpenWeatherMap API key',
            'OLLAMA_HOST': 'Ollama LLM service host'
        }
        
        optional_vars = {
            'NOTIFICATION_RECIPIENTS': 'Email addresses for alerts (comma-separated)',
            'ML_MODEL_ENABLED': 'Enable ML predictions (true/false)',
            'ES_HOST': 'Elasticsearch host for search',
            'ES_USER': 'Elasticsearch username',
            'ES_PASS': 'Elasticsearch password'
        }
        
        for var, description in required_vars.items():
            value = os.getenv(var)
            if not value:
                self.errors.append(f"Required environment variable {var} not set: {description}")
            else:
                self.config_items[var] = value
                print(f"  ✅ {var}: {'*' * min(len(value), 20)}")
        
        for var, description in optional_vars.items():
            value = os.getenv(var)
            if value:
                self.config_items[var] = value
                print(f"  ✅ {var}: {value}")
            else:
                self.warnings.append(f"Optional variable {var} not set: {description}")
                print(f"  ⚠️  {var}: Not set ({description})")
    
    def _validate_database_connection(self):
        """Validate database connectivity"""
        print("\n🗄️  Testing database connection...")

        database_url = self.config_items.get('DATABASE_URL')
        if not database_url:
            self.errors.append("Cannot test database - DATABASE_URL not available")
            return

        try:
            # Test basic connection
            with db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT version()")
                version = cursor.fetchone()[0]
                print(f"  ✅ Connected to PostgreSQL: {version}")

                # Test permissions
                cursor.execute("SELECT current_user, current_database()")
                user, database = cursor.fetchone()
                print(f"  ✅ User: {user}, Database: {database}")

                # Check if we can create tables (for migrations)
                cursor.execute("""
                    SELECT has_table_privilege(current_user, 'information_schema.tables', 'SELECT')
                """)
                can_read_schema = cursor.fetchone()[0]
                if not can_read_schema:
                    self.warnings.append("User may not have sufficient privileges to read schema")

        except psycopg2.OperationalError as e:
            self.errors.append(f"Database connection failed: {str(e)}")
        except Exception as e:
            self.errors.append(f"Database validation error: {str(e)}")
    
    def _validate_file_system(self):
        """Validate file system setup"""
        print("\n📁 Checking file system setup...")
        
        document_folder = self.config_items.get('DOCUMENT_FOLDER', 'documents')
        
        try:
            # Create base folder
            os.makedirs(document_folder, exist_ok=True)
            print(f"  ✅ Base upload folder: {os.path.abspath(document_folder)}")
            
            # Create inventory subfolder
            inventory_folder = os.path.join(document_folder, 'inventory_images')
            os.makedirs(inventory_folder, exist_ok=True)
            print(f"  ✅ Inventory images folder: {os.path.abspath(inventory_folder)}")
            
            # Test write permissions
            test_file = os.path.join(document_folder, 'test_write.tmp')
            with open(test_file, 'w') as f:
                f.write('test')
            os.remove(test_file)
            print(f"  ✅ Write permissions confirmed")
            
            # Check disk space
            stat = os.statvfs(document_folder)
            free_space_gb = (stat.f_frsize * stat.f_available) / (1024**3)
            if free_space_gb < 1:
                self.warnings.append(f"Low disk space: {free_space_gb:.1f}GB available")
            else:
                print(f"  ✅ Disk space: {free_space_gb:.1f}GB available")
                
        except PermissionError:
            self.errors.append(f"Permission denied creating folders in {document_folder}")
        except Exception as e:
            self.errors.append(f"File system validation error: {str(e)}")
    
    def _validate_python_dependencies(self):
        """Validate Python package dependencies"""
        print("\n🐍 Checking Python dependencies...")
        
        required_packages = [
            ('psycopg2', 'PostgreSQL adapter'),
            ('pandas', 'Data processing for bulk imports'),
            ('Pillow', 'Image processing'),
            ('flask', 'Web framework'),
            ('flask_login', 'Authentication'),
            ('werkzeug', 'WSGI utilities')
        ]
        
        optional_packages = [
            ('sklearn', 'Machine learning predictions'),
            ('schedule', 'Task scheduling'),
            ('elasticsearch', 'Search functionality'),
            ('openpyxl', 'Excel file support'),
            ('xlrd', 'Legacy Excel support')
        ]
        
        for package, description in required_packages:
            try:
                __import__(package)
                print(f"  ✅ {package}: Available")
            except ImportError:
                self.errors.append(f"Required package {package} not found: {description}")
        
        for package, description in optional_packages:
            try:
                __import__(package)
                print(f"  ✅ {package}: Available (optional)")
            except ImportError:
                self.warnings.append(f"Optional package {package} not found: {description}")
                print(f"  ⚠️  {package}: Not available (optional)")
    
    def _validate_database_schema(self):
        """Validate database schema"""
        print("\n🏗️  Checking database schema...")

        database_url = self.config_items.get('DATABASE_URL')
        if not database_url:
            return

        try:
            with db_connection() as conn:
                cursor = conn.cursor()

                # Check required tables
                required_tables = [
                    'users', 'consumables', 'jobs', 'schedules'
                ]

                expected_tables = [
                    'inventory_alerts', 'inventory_transactions',
                    'job_consumables', 'supplier_items', 'item_templates'
                ]

                cursor.execute("""
                    SELECT table_name FROM information_schema.tables
                    WHERE table_schema = 'public'
                """)
                existing_tables = [row[0] for row in cursor.fetchall()]

                for table in required_tables:
                    if table in existing_tables:
                        print(f"  ✅ Required table: {table}")
                    else:
                        self.errors.append(f"Required table missing: {table}")

                for table in expected_tables:
                    if table in existing_tables:
                        print(f"  ✅ Inventory table: {table}")
                    else:
                        self.warnings.append(f"Inventory table missing: {table} (will be created)")
                        print(f"  ⚠️  {table}: Missing (will be created on initialization)")

                # Check consumables table structure
                if 'consumables' in existing_tables:
                    cursor.execute("""
                        SELECT column_name, data_type
                        FROM information_schema.columns
                        WHERE table_name = 'consumables'
                    """)
                    columns = {row[0]: row[1] for row in cursor.fetchall()}

                    required_columns = [
                        'id', 'name', 'category', 'quantity', 'unit'
                    ]

                    enhanced_columns = [
                        'specifications', 'internal_sku', 'supplier_sku',
                        'cost_per_unit', 'reorder_threshold'
                    ]

                    missing_required = [col for col in required_columns if col not in columns]
                    missing_enhanced = [col for col in enhanced_columns if col not in columns]

                    if missing_required:
                        self.errors.append(f"Consumables table missing required columns: {missing_required}")

                    if missing_enhanced:
                        self.warnings.append(f"Consumables table missing enhanced columns: {missing_enhanced}")
                        print(f"  ⚠️  Enhanced columns missing: {len(missing_enhanced)} (will be added)")

        except Exception as e:
            self.errors.append(f"Schema validation error: {str(e)}")
    
    def _validate_permissions(self):
        """Validate application permissions"""
        print("\n🔒 Checking permissions...")
        
        # Check if running as root (not recommended)
        if os.geteuid() == 0:
            self.warnings.append("Running as root user - consider using dedicated service account")
        
        # Check file permissions
        document_folder = self.config_items.get('DOCUMENT_FOLDER', 'documents')
        if os.path.exists(document_folder):
            folder_stat = os.stat(document_folder)
            if folder_stat.st_mode & 0o777 == 0o777:
                self.warnings.append(f"Upload folder {document_folder} has overly permissive permissions")
        
        print("  ✅ Permission check completed")
    
    def _generate_recommendations(self) -> List[str]:
        """Generate setup recommendations based on validation"""
        recommendations = []
        
        if self.errors:
            recommendations.append("🚨 Fix all errors before running the system in production")
        
        if len(self.warnings) > 5:
            recommendations.append("🔧 Consider addressing warnings for optimal performance")
        
        # Environment-specific recommendations
        if not self.config_items.get('NOTIFICATION_RECIPIENTS'):
            recommendations.append("📧 Set NOTIFICATION_RECIPIENTS for inventory alerts")
        
        if not self.config_items.get('ML_MODEL_ENABLED'):
            recommendations.append("🤖 Enable ML_MODEL_ENABLED for usage predictions")
        
        # Performance recommendations
        database_url = self.config_items.get('DATABASE_URL', '')
        if 'localhost' in database_url:
            recommendations.append("🏢 Consider using managed database service for production")
        
        # Security recommendations
        recommendations.append("🔐 Ensure DATABASE_URL credentials are secure")
        recommendations.append("🛡️  Set up regular database backups")
        recommendations.append("📊 Configure log rotation for application logs")
        
        return recommendations
    
    def _print_report(self, report: Dict[str, Any]):
        """Print validation report"""
        print("\n" + "=" * 60)
        print("📊 VALIDATION REPORT")
        print("=" * 60)
        
        if report['status'] == 'pass':
            print("✅ OVERALL STATUS: PASS")
        else:
            print("❌ OVERALL STATUS: FAIL")
        
        if report['errors']:
            print(f"\n❌ ERRORS ({len(report['errors'])}):")
            for i, error in enumerate(report['errors'], 1):
                print(f"  {i}. {error}")
        
        if report['warnings']:
            print(f"\n⚠️  WARNINGS ({len(report['warnings'])}):")
            for i, warning in enumerate(report['warnings'], 1):
                print(f"  {i}. {warning}")
        
        if report['recommendations']:
            print(f"\n💡 RECOMMENDATIONS:")
            for rec in report['recommendations']:
                print(f"  • {rec}")
        
        print("\n" + "=" * 60)

def run_setup_wizard():
    """Interactive setup wizard for first-time configuration"""
    print("🧙 Inventory System Setup Wizard")
    print("=" * 40)
    
    # Check if .env file exists
    env_file = Path('.env')
    if env_file.exists():
        print("📄 Found existing .env file")
        use_existing = input("Use existing configuration? (y/n): ").lower().strip()
        if use_existing != 'y':
            create_new_env = True
        else:
            create_new_env = False
    else:
        create_new_env = True
    
    if create_new_env:
        print("\n📝 Creating new configuration...")
        
        # Database configuration
        print("\n🗄️  Database Configuration:")
        db_host = input("Database host (localhost): ").strip() or "localhost"
        db_port = input("Database port (5432): ").strip() or "5432"
        db_name = input("Database name (inventory): ").strip() or "inventory"
        db_user = input("Database username (postgres): ").strip() or "postgres"
        db_password = input("Database password: ").strip()
        
        database_url = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
        
        # File storage
        print("\n📁 File Storage Configuration:")
        upload_folder = input("Upload folder (documents): ").strip() or "documents"
        
        # Optional features
        print("\n🔧 Optional Features:")
        enable_ml = input("Enable ML predictions? (y/n): ").lower().strip() == 'y'
        
        notification_emails = input("Notification emails (comma-separated): ").strip()
        
        # Write .env file
        env_content = f"""# Inventory System Configuration
DATABASE_URL={database_url}
DOCUMENT_FOLDER={upload_folder}
ML_MODEL_ENABLED={'true' if enable_ml else 'false'}
"""
        
        if notification_emails:
            env_content += f"NOTIFICATION_RECIPIENTS={notification_emails}\n"
        
        with open('.env', 'w') as f:
            f.write(env_content)
        
        print("✅ Configuration saved to .env file")
    
    # Run validation
    print("\n🔍 Running validation...")
    validator = ConfigValidator()
    report = validator.validate_all()
    
    if report['status'] == 'pass':
        print("\n🎉 Setup completed successfully!")
        print("You can now start the inventory system.")
    else:
        print("\n⚠️  Setup completed with issues.")
        print("Please fix the errors shown above before starting the system.")
    
    return report['status'] == 'pass'

def create_deployment_script():
    """Create deployment script for production"""
    script_content = """#!/bin/bash
# Inventory System Deployment Script

set -e  # Exit on any error

echo "🚀 Starting Inventory System Deployment"
echo "======================================"

# Check if running as root
if [ "$EUID" -eq 0 ]; then
    echo "⚠️  WARNING: Running as root. Consider using a service account."
fi

# Update system packages
echo "📦 Updating system packages..."
sudo apt-get update
sudo apt-get install -y python3 python3-pip python3-venv postgresql-client

# Create application directory
APP_DIR="/opt/inventory-system"
echo "📁 Creating application directory: $APP_DIR"
sudo mkdir -p $APP_DIR
sudo chown $USER:$USER $APP_DIR

# Create virtual environment
echo "🐍 Setting up Python virtual environment..."
cd $APP_DIR
python3 -m venv venv
source venv/bin/activate

# Install dependencies
echo "📚 Installing Python dependencies..."
pip install --upgrade pip
pip install -r requirements.txt

# Create configuration
echo "⚙️  Setting up configuration..."
if [ ! -f .env ]; then
    echo "Creating .env template..."
    cat > .env << EOF
# Database Configuration
DATABASE_URL=postgresql://username:password@localhost:5432/inventory

# File Storage
DOCUMENT_FOLDER=/var/lib/inventory/documents

# Features
ML_MODEL_ENABLED=true
NOTIFICATION_RECIPIENTS=admin@company.com

# Security
SECRET_KEY=your-secret-key-here
EOF
    echo "📝 Please edit .env with your actual configuration"
fi

# Create directories
echo "📁 Creating required directories..."
sudo mkdir -p /var/lib/inventory/documents/inventory_images
sudo mkdir -p /var/log/inventory
sudo chown -R $USER:$USER /var/lib/inventory
sudo chown -R $USER:$USER /var/log/inventory

# Set up systemd service
echo "🔧 Setting up systemd service..."
sudo tee /etc/systemd/system/inventory-system.service > /dev/null << EOF
[Unit]
Description=Inventory Management System
After=network.target postgresql.service

[Service]
Type=simple
User=$USER
Group=$USER
WorkingDirectory=$APP_DIR
Environment=PATH=$APP_DIR/venv/bin
ExecStart=$APP_DIR/venv/bin/python app.py
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

# Enable and start service
sudo systemctl daemon-reload
sudo systemctl enable inventory-system

echo "✅ Deployment completed!"
echo ""
echo "Next steps:"
echo "1. Edit $APP_DIR/.env with your configuration"
echo "2. Initialize the database: python init_db.py"
echo "3. Start the service: sudo systemctl start inventory-system"
echo "4. Check status: sudo systemctl status inventory-system"
echo "5. View logs: sudo journalctl -u inventory-system -f"
"""
    
    with open('deploy.sh', 'w') as f:
        f.write(script_content)
    
    os.chmod('deploy.sh', 0o755)
    print("✅ Created deploy.sh script")

def create_requirements_file():
    """Create requirements.txt with all dependencies"""
    requirements = """# Core Dependencies
Flask>=2.3.0
Flask-Login>=0.6.0
psycopg2-binary>=2.9.0
Werkzeug>=2.3.0

# Data Processing
pandas>=1.5.0
numpy>=1.24.0

# Image Processing
Pillow>=9.0.0

# Excel Support
openpyxl>=3.1.0
xlrd>=2.0.0

# Machine Learning (Optional)
scikit-learn>=1.3.0
joblib>=1.3.0

# Task Scheduling
schedule>=1.2.0

# Search (Optional)
elasticsearch>=8.0.0

# Development/Testing
pytest>=7.4.0
pytest-cov>=4.1.0

# Production Server
gunicorn>=21.2.0
"""
    
    with open('requirements.txt', 'w') as f:
        f.write(requirements)
    
    print("✅ Created requirements.txt file")

if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='Inventory System Configuration')
    parser.add_argument('--validate', action='store_true', help='Run configuration validation')
    parser.add_argument('--setup', action='store_true', help='Run setup wizard')
    parser.add_argument('--deploy', action='store_true', help='Create deployment files')
    
    args = parser.parse_args()
    
    if args.setup:
        success = run_setup_wizard()
        sys.exit(0 if success else 1)
    
    elif args.validate:
        validator = ConfigValidator()
        report = validator.validate_all()
        sys.exit(0 if report['status'] == 'pass' else 1)
    
    elif args.deploy:
        create_deployment_script()
        create_requirements_file()
        print("✅ Deployment files created")
    
    else:
        print("Inventory System Configuration Tool")
        print("Usage:")
        print("  --validate    Run configuration validation")
        print("  --setup       Run interactive setup wizard")
        print("  --deploy      Create deployment files")
        print("")
        print("Example: python config_validator.py --setup")