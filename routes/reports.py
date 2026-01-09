# routes/reports.py
from flask import Blueprint, render_template, request, Response, jsonify
from services.db import get_connection, release_connection
from services.logging_service import log_audit
from services.elasticsearch_client import es
from services.report_service import generate_pdf_report, generate_csv_report, parse_report_data
import plotly.express as px
from flask_login import login_required, current_user
import os
import uuid

report_bp = Blueprint('reports', __name__, url_prefix='/reports')

@report_bp.route('/')
@login_required
def reports():
    conn = get_connection()
    c = conn.cursor()
    c.execute("SELECT title, upload_date FROM documents ORDER BY upload_date DESC LIMIT 10")
    data = c.fetchall()
    c.execute("SELECT id, city_name FROM city_contacts ORDER BY city_name")
    towns = [{'id': r[0], 'name': r[1]} for r in c.fetchall()]
    release_connection(conn)
    return render_template('reports.html', data=data, towns=towns)

@report_bp.route('/generate', methods=['GET', 'POST'])
@login_required
def generate_report():
    if request.method == 'POST':
        try:
            category = request.form.get('category')
            job_site = request.form.get('job_site')
            people = request.form.get('people')
            date_from = request.form.get('date_from')
            date_to = request.form.get('date_to')
            report_format = request.form.get('format')
            # Example: Fetch data for report
            conn = get_connection()
            c = conn.cursor()
            query = "SELECT title, start_date, end_date, location FROM schedules"
            params = []
            if date_from and date_to:
                query += " WHERE start_date BETWEEN %s AND %s"
                params.extend([date_from, date_to])
            c.execute(query, params)
            data = [{'title': row[0], 'start_date': row[1], 'end_date': row[2], 'location': row[3]} for row in c.fetchall()]
            release_connection(conn)
            if category:
                data = [item for item in data if category.lower() in item.get('title', '').lower()]
            if job_site:
                data = [item for item in data if job_site.lower() in item.get('location', '').lower()]
            filename = f"report_{uuid.uuid4()}.{report_format}"
            filepath = os.path.join('reports', filename)
            os.makedirs('reports', exist_ok=True)
            if report_format == 'pdf':
                generate_pdf_report(filepath, 'Schedule Report', data)
                with open(filepath, 'rb') as f:
                    response = Response(f.read(), mimetype='application/pdf')
                    response.headers['Content-Disposition'] = f'attachment; filename={filename}'
                    log_audit(es, 'report_generated', current_user.id, None, {'type': 'pdf'})
                    return response
            elif report_format == 'csv':
                generate_csv_report(filepath, data)
                with open(filepath, 'rb') as f:
                    response = Response(f.read(), mimetype='text/csv')
                    response.headers['Content-Disposition'] = f'attachment; filename={filename}'
                    log_audit(es, 'report_generated', current_user.id, None, {'type': 'csv'})
                    return response
        except Exception as e:
            log_audit(es, 'report_error', current_user.id, None, {'error': str(e)})
            return render_template('reports.html', error=str(e))
    return render_template('reports.html')

@report_bp.route('/custom', methods=['GET', 'POST'])
@login_required
def custom_report():
    if request.method == 'POST':
        try:
            title = request.form.get('title', 'Custom Report')
            data_source = request.form.get('data_source')
            data_raw = request.form.get('data')
            report_type = request.form.get('report_type')
            if data_source == 'Manual JSON':
                data = parse_report_data(data_raw)
            else:
                conn = get_connection()
                c = conn.cursor()
                if data_source == 'Schedules':
                    c.execute("SELECT title, start_date, end_date, location FROM schedules")
                elif data_source == 'Inventory':
                    c.execute("SELECT name, quantity, location FROM consumables")
                elif data_source == 'Equipment':
                    c.execute("SELECT unique_id, equipment_type, hours FROM equipment_instances")
                data = [dict(zip([desc[0] for desc in c.description], row)) for row in c.fetchall()]
                release_connection(conn)
            filename = f"custom_report_{uuid.uuid4()}.{report_type.lower()}"
            filepath = os.path.join('reports', filename)
            os.makedirs('reports', exist_ok=True)
            if report_type == 'pdf':
                generate_pdf_report(filepath, title, data)
                with open(filepath, 'rb') as f:
                    response = Response(f.read(), mimetype='application/pdf')
                    response.headers['Content-Disposition'] = f'attachment; filename={filename}'
                    log_audit(es, 'custom_report_generated', current_user.id, None, {'type': 'pdf'})
                    return response
            elif report_type == 'csv':
                generate_csv_report(filepath, data)
                with open(filepath, 'rb') as f:
                    response = Response(f.read(), mimetype='text/csv')
                    response.headers['Content-Disposition'] = f'attachment; filename={filename}'
                    log_audit(es, 'custom_report_generated', current_user.id, None, {'type': 'csv'})
                    return response
        except Exception as e:
            log_audit(es, 'custom_report_error', current_user.id, None, {'error': str(e)})
            return render_template('report_form.html', error=str(e))
    return render_template('report_form.html')

# New route for dash
@report_bp.route("/dashboard")
def report_dashboard():
    # Placeholder until Dash app is embedded
    return render_template('report_dashboard.html')