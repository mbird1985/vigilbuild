# routes/jobs.py
from flask import Blueprint, render_template, request, flash, redirect, url_for
from flask_login import login_required, current_user
from services.jobs_service import create_job, get_job_status
from services.logging_service import log_audit
from services.elasticsearch_client import es
from services.validation_service import sanitize_string, sanitize_float, sanitize_int

jobs_bp = Blueprint('jobs', __name__, url_prefix='/jobs')

@jobs_bp.route('/')
@login_required
def jobs():
    # Town filter
    from services.db import get_connection, release_connection
    conn = get_connection()
    try:
        c = conn.cursor()
        c.execute("SELECT id, city_name FROM city_contacts ORDER BY city_name")
        towns = [{'id': r[0], 'name': r[1]} for r in c.fetchall()]
        return render_template('jobs.html', towns=towns)
    finally:
        release_connection(conn)

@jobs_bp.route('/create', methods=['GET', 'POST'])
@login_required
def create_job_route():
    if current_user.role not in ['manager', 'admin']:
        flash("Permission denied")
        return redirect(url_for('jobs.jobs'))
    if request.method == 'POST':
        try:
            form = request.form

            # Validate and sanitize inputs
            name = sanitize_string(form.get('name', ''), max_length=200)
            if not name:
                flash("Job name is required")
                return redirect(url_for('jobs.create_job_route'))

            description = sanitize_string(form.get('description', ''), max_length=2000)
            location = sanitize_string(form.get('location', ''), max_length=500)
            estimated_cost = sanitize_float(form.get('estimated_cost', 0), min_val=0, max_val=999999999)
            town_id = sanitize_int(form.get('town_id'), min_val=1) if form.get('town_id') else None

            job_id = create_job(
                name=name,
                description=description,
                estimated_cost=estimated_cost,
                location=location,
                user_id=current_user.id,
                town_id=town_id
            )
            log_audit(es, 'create_job', current_user.id, None, {'job_id': job_id, 'name': name})
            flash('Job created successfully')
            return redirect(url_for('jobs.jobs'))
        except ValueError as e:
            flash(f"Error creating job: {str(e)}")
        except Exception as e:
            flash(f"Error creating job: {str(e)}")
    # Provide towns for selection
    from services.db import get_connection, release_connection
    conn = get_connection()
    try:
        c = conn.cursor()
        c.execute("SELECT id, city_name FROM city_contacts ORDER BY city_name")
        towns = [{'id': r[0], 'name': r[1]} for r in c.fetchall()]
        return render_template('job_create.html', towns=towns)
    finally:
        release_connection(conn)

def update_job_progress(job_id, progress):
    from services.db import get_connection, release_connection
    conn = get_connection()
    try:
        c = conn.cursor()
        c.execute("UPDATE jobs SET progress = %s, updated_at = CURRENT_TIMESTAMP WHERE id = %s", (int(progress), int(job_id)))
        conn.commit()
        return True
    except Exception:
        return False
    finally:
        release_connection(conn)