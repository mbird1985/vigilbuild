# app.py
from flask import Flask, request, jsonify, send_from_directory, render_template, redirect, url_for, flash, Response
from flask_login import LoginManager, login_required, current_user
from search import search
import os
from werkzeug.utils import secure_filename
from elasticsearch import Elasticsearch
from io import BytesIO, StringIO
from datetime import datetime, timedelta
from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table
from reportlab.lib.styles import getSampleStyleSheet
import csv
import requests
import re
import json
from apscheduler.schedulers.background import BackgroundScheduler
from msal import ConfidentialClientApplication
from routes.chat import chat_bp
from routes.equipment import equipment_bp
from routes.inventory import inventory_bp
from routes.schedule import schedule_bp
from routes.auth import auth_bp
from routes.documents import document_bp
from routes.reports import report_bp
from routes.system import system_bp
from routes.users import users_bp
from routes.towns import towns_bp
from routes.email import email_bp
from routes.integrations import integration_bp
from routes.jobs import jobs_bp
from routes.api import api_bp  # Make sure to import the API blueprint
from routes.pm import pm_bp
from routes.billing import billing_bp
from routes.mobile import mobile_bp
from routes.marketing import marketing_bp
import sys
from werkzeug.middleware.dispatcher import DispatcherMiddleware

from config import SECRET_KEY, UPLOAD_FOLDER, DEBUG, IS_PRODUCTION, ENVIRONMENT
from services.init_db import init_db
from services.csrf_service import init_csrf
from services.i18n_service import init_i18n

from services.automation_service import update_weather, reroute_jobs, check_equipment_health, send_automated_emails
from services.equipment_service import check_all_equipment_maintenance
from services.inventory_service import upsert_reorder_suggestions

from services.auth_service import User


if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER)

init_db()

app = Flask(__name__)
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = "auth.login"  # Use auth blueprint's login

# Use DEBUG setting from config (environment-aware)
app.config['DEBUG'] = DEBUG
app.config['TEMPLATES_AUTO_RELOAD'] = not IS_PRODUCTION  # Only auto-reload in development
app.config['ENV'] = ENVIRONMENT

# Security settings
app.config['SESSION_COOKIE_SECURE'] = IS_PRODUCTION  # HTTPS only in production
app.config['SESSION_COOKIE_HTTPONLY'] = True  # Prevent XSS access to session cookie
app.config['SESSION_COOKIE_SAMESITE'] = 'Lax'  # CSRF protection

print(f"🚀 Vigil Build starting in {ENVIRONMENT} mode (DEBUG={DEBUG})")

# Register routes with proper URL prefixes
app.register_blueprint(chat_bp, url_prefix="/chat")
app.register_blueprint(equipment_bp, url_prefix="/equipment")
app.register_blueprint(inventory_bp, url_prefix="/inventory")
app.register_blueprint(schedule_bp, url_prefix="/schedule")
app.register_blueprint(document_bp, url_prefix="/documents")
app.register_blueprint(report_bp, url_prefix="/reports")
app.register_blueprint(system_bp)  # No prefix for dashboard/system routes
app.register_blueprint(email_bp, url_prefix="/email")
app.register_blueprint(integration_bp, url_prefix="/integrations")
app.register_blueprint(users_bp, url_prefix="/users")
app.register_blueprint(towns_bp, url_prefix="/towns")
app.register_blueprint(auth_bp, url_prefix="/auth")
app.register_blueprint(jobs_bp, url_prefix="/jobs")
app.register_blueprint(api_bp)  
app.register_blueprint(pm_bp, url_prefix='/pm')
app.register_blueprint(billing_bp, url_prefix='/billing')
app.register_blueprint(mobile_bp)  # Mobile app routes (has its own /mobile prefix)
app.register_blueprint(marketing_bp, url_prefix='/marketing')  # Public marketing website

app.config["SECRET_KEY"] = SECRET_KEY
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

# Initialize CSRF protection
init_csrf(app)

# Initialize internationalization (i18n) for Spanish support
init_i18n(app)

@login_manager.user_loader
def load_user(user_id):
    try:
        return User(user_id)
    except ValueError:
        return None

@app.route("/")
@login_required
def home():
    return redirect(url_for('system.dashboard'))

@app.route("/search_page")
@login_required
def serve_search():
    return render_template('search.html')

@app.route("/upload_page")
@login_required
def serve_upload():
    return render_template('upload.html')

@app.route("/search")
@login_required
def api_search():
    """Search endpoint - requires authentication"""
    try:
        query = request.args.get("q", "")
        category = request.args.get("category", None)
        job_site = request.args.get("job_site", None)
        people = request.args.get("people", None)
        date_from = request.args.get("date_from", None)
        date_to = request.args.get("date_to", None)
        tags = request.args.get("tags", None)
        version = request.args.get("version", None)
        source = request.args.get("source", None)

        results = search(query, category, job_site, people, date_from, date_to, tags, version, source)

        # Log analytics if available
        try:
            log_analytics("search", {
                "query": query,
                "category": category,
                "job_site": job_site,
                "people": people,
                "date_from": date_from,
                "date_to": date_to,
                "tags": tags,
                "version": version,
                "source": source,
                "results_count": len(results) if results else 0,
                "user_id": current_user.id if current_user.is_authenticated else None
            })
        except Exception:
            pass  # Don't fail search if analytics logging fails

        return jsonify([{
            "title": hit["_source"].get("title", "Untitled"),
            "score": hit.get("_score", 0),
            "content": hit["_source"].get("content", "")[:200],
            "url": hit["_source"].get("metadata", {}).get("url", ""),
            "highlight": hit.get("highlight", {}),
            "id": hit.get("_id", "")
        } for hit in (results or [])])
    except Exception as e:
        app.logger.error(f"Search error: {str(e)}")
        return jsonify({"error": "Search failed", "message": str(e) if DEBUG else "An error occurred"}), 500

@app.route('/sw.js')
def serve_sw():
    return send_from_directory('static', 'sw.js')

@app.route('/manifest.json')
def serve_manifest():
    return send_from_directory('static', 'manifest.json')

"""
from services.inventory_service import initialize_inventory_system

# Add this after creating the Flask app but before running it
if __name__ == '__main__':
    if initialize_inventory_system():
        app.run(debug=True)
    else:
        print("Failed to initialize inventory system. Check logs for details.")
"""

import logging
import atexit

# Configure scheduler logging
logging.basicConfig()
logging.getLogger('apscheduler').setLevel(logging.WARNING)

def safe_job_wrapper(job_func, job_name):
    """Wrapper to catch and log errors in scheduled jobs"""
    def wrapper(*args, **kwargs):
        try:
            return job_func(*args, **kwargs)
        except Exception as e:
            app.logger.error(f"Scheduled job '{job_name}' failed: {str(e)}")
            # In production, you might want to send an alert here
    return wrapper

scheduler = BackgroundScheduler()

# Add jobs with error handling wrappers
scheduler.add_job(
    safe_job_wrapper(send_automated_emails, 'send_automated_emails'),
    'interval', days=1, next_run_time=datetime.now(), id='send_automated_emails'
)
scheduler.add_job(
    safe_job_wrapper(reroute_jobs, 'reroute_jobs'),
    'interval', hours=1, id='reroute_jobs'
)
scheduler.add_job(
    safe_job_wrapper(update_weather, 'update_weather'),
    'interval', hours=3, id='update_weather'
)
scheduler.add_job(
    safe_job_wrapper(check_equipment_health, 'check_equipment_health'),
    'interval', days=1, id='check_equipment_health'
)
scheduler.add_job(
    safe_job_wrapper(check_all_equipment_maintenance, 'check_all_equipment_maintenance'),
    'interval', days=1, id='check_all_equipment_maintenance'
)
# Nightly creation of reorder suggestions
scheduler.add_job(
    safe_job_wrapper(upsert_reorder_suggestions, 'upsert_reorder_suggestions'),
    'cron', hour=1, id='upsert_reorder_suggestions'
)

scheduler.start()

# Ensure scheduler shuts down cleanly on app exit
atexit.register(lambda: scheduler.shutdown())

if __name__ == "__main__":
    # Use environment-aware settings
    port = int(os.environ.get('PORT', 5001))
    host = os.environ.get('HOST', '127.0.0.1')

    print(f"🌐 Starting Vigil Build on {host}:{port}")
    print(f"📋 Environment: {ENVIRONMENT}")
    print(f"🔧 Debug mode: {DEBUG}")

    app.run(host=host, port=port, debug=DEBUG)