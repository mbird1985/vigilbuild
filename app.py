# app.py - Vigil Build Marketing Website
"""
Lightweight Flask app for the Vigil Build marketing website.
This serves only the public marketing pages (no authentication required).
"""

from flask import Flask, send_from_directory, redirect, url_for
import os

from routes.marketing import marketing_bp

# Get configuration from environment
SECRET_KEY = os.getenv('SECRET_KEY', 'dev-secret-key-change-in-production')
DEBUG = os.getenv('FLASK_ENV', 'development') != 'production'

app = Flask(__name__)
app.config['SECRET_KEY'] = SECRET_KEY

# Register only the marketing blueprint
app.register_blueprint(marketing_bp, url_prefix='/marketing')

@app.route('/')
def home():
    """Redirect root to marketing home page"""
    return redirect(url_for('marketing.home'))

@app.route('/favicon.ico')
def favicon():
    return send_from_directory('static', 'favicon.ico')

@app.route('/sw.js')
def serve_sw():
    return send_from_directory('static', 'sw.js', mimetype='application/javascript')

@app.route('/manifest.json')
def serve_manifest():
    return send_from_directory('static', 'manifest.json')

# Health check endpoint for DigitalOcean
@app.route('/health')
def health():
    return {'status': 'healthy'}, 200

if __name__ == "__main__":
    port = int(os.environ.get('PORT', 5000))
    host = os.environ.get('HOST', '0.0.0.0')
    print(f"Starting Vigil Build Marketing on {host}:{port}")
    app.run(host=host, port=port, debug=DEBUG)
