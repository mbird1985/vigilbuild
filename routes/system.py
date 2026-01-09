# routes/system.py
from flask import Blueprint, render_template, jsonify
from flask_login import login_required, current_user
from services.db import get_connection, release_connection
from services.equipment_service import get_all_equipment
from services.logging_service import log_audit
from services.elasticsearch_client import es
from datetime import datetime, timedelta
import requests
import time
# SocketIO integration can be added later when the SocketIO app is initialized

system_bp = Blueprint('system', __name__)

@system_bp.route('/dashboard')
@login_required
def dashboard():
    try:
        today = datetime.now().date()
        monday = today - timedelta(days=today.weekday())
        sunday = monday + timedelta(days=6)
        
        conn = get_connection()
        c = conn.cursor()
        
        # Jobs this week
        c.execute("""
            SELECT COUNT(*) 
            FROM schedules 
            WHERE start_date BETWEEN %s AND %s
        """, (monday, sunday))
        jobs_this_week = c.fetchone()[0]
        
        # Total days for jobs this week (since no time component)
        c.execute("""
            SELECT id, title, start_date, end_date, location, status 
            FROM schedules 
            WHERE start_date BETWEEN %s AND %s
        """, (monday, sunday))
        schedules = [
            {
                'id': row[0],
                'title': row[1],
                'start_date': row[2],
                'end_date': row[3],
                'location': row[4],
                'status': row[5]
            } for row in c.fetchall()
        ]
        total_days = sum((row['end_date'] - row['start_date']).days + 1 for row in schedules)
        
        release_connection(conn)
        
        # Equipment due for maintenance
        equipment_list = get_all_equipment()
        maintenance_due = [
            equip for equip in equipment_list
            if equip["maintenance_threshold"] and equip["hours"] >= equip["maintenance_threshold"]
        ]
        maintenance_due_count = len(maintenance_due)
        maintenance_due_labels = [equip["unique_id"] for equip in maintenance_due]
        maintenance_due_hours = [equip["hours"] for equip in maintenance_due]
        
        log_audit(es, 'view_dashboard', current_user.id, None, {'schedule_count': len(schedules)})
        return render_template(
            'dashboard.html',
            jobs_this_week=jobs_this_week,
            total_days=total_days,
            maintenance_due_count=maintenance_due_count,
            maintenance_due_labels=maintenance_due_labels,
            maintenance_due_hours=maintenance_due_hours,
            schedules=schedules,
            user=current_user
        )
    except Exception as e:
        log_audit(es, 'dashboard_error', current_user.id, None, {'error': str(e)})
        return render_template(
            'dashboard.html',
            jobs_this_week=0,
            total_days=0,
            maintenance_due_count=0,
            maintenance_due_labels=[],
            maintenance_due_hours=[],
            schedules=[],
            user=current_user,
            error=str(e)
        )

@system_bp.route("/status")
@login_required
def health_check():
    return jsonify({"status": "ok", "message": "System operational"})

@system_bp.route("/health")
def health():
    try:
        # Database check
        conn = get_connection()
        c = conn.cursor()
        c.execute("SELECT 1")
        release_connection(conn)

        # LLM check
        r = requests.post("http://127.0.0.1:11434/api/generate", json={"model": "llama3", "prompt": "ping"})
        llm_status = r.status_code == 200

        return jsonify({
            "status": "healthy",
            "database": "ok",
            "llm": "ok" if llm_status else "error"
        }), 200
    except Exception as e:
        return jsonify({"status": "error", "detail": str(e)}), 500

@system_bp.route("/ready")
def ready():
    """Simple readiness check for orchestrators"""
    try:
        # DB ping
        conn = get_connection()
        c = conn.cursor()
        c.execute("SELECT 1")
        release_connection(conn)
        return jsonify({"status": "ready"}), 200
    except Exception as e:
        return jsonify({"status": "not_ready", "detail": str(e)}), 503

@system_bp.route("/metrics")
def metrics():
    """Minimal Prometheus-style metrics (placeholder)"""
    try:
        lines = [
            "# HELP app_uptime_seconds Application uptime in seconds",
            "# TYPE app_uptime_seconds counter",
            f"app_uptime_seconds {{}} {int(time.time())}",
        ]
        return "\n".join(lines), 200, {"Content-Type": "text/plain; version=0.0.4"}
    except Exception as e:
        return (f"# error {str(e)}\n", 500, {"Content-Type": "text/plain"})