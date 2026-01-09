"""
Mobile Routes for Vigil Build - Maintenance Module
Provides mobile-optimized endpoints for field workers, supervisors, and issue reporting.
"""

from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify, session
from functools import wraps
from datetime import datetime, timedelta
import json

mobile_bp = Blueprint('mobile', __name__, url_prefix='/mobile')

# Import services
try:
    from services.equipment_service import (
        get_all_jobs, get_job_by_id, update_job, get_all_equipment,
        create_job, get_equipment_by_id, get_active_outages, create_outage_record
    )
    from services.users_service import get_user_by_id, get_all_users
    from services.inventory_service import get_all_consumables
except ImportError:
    # Fallback for testing
    pass


def mobile_login_required(f):
    """Decorator to require login for mobile routes"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user_id' not in session:
            return redirect(url_for('mobile.login'))
        return f(*args, **kwargs)
    return decorated_function


def role_required(*roles):
    """Decorator to require specific roles"""
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            user_role = session.get('role', 'worker')
            if user_role not in roles:
                flash('Access denied', 'error')
                return redirect(url_for('mobile.index'))
            return f(*args, **kwargs)
        return decorated_function
    return decorator


# ============ Authentication ============

@mobile_bp.route('/login', methods=['GET', 'POST'])
def login():
    """Mobile login page"""
    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')
        # Simple authentication - in production, verify against database
        # For now, set session directly
        session['user_id'] = 1
        session['username'] = username
        session['role'] = 'worker'  # Default role

        # Redirect based on role
        role = session.get('role', 'worker')
        if role == 'supervisor':
            return redirect(url_for('mobile.supervisor_dashboard'))
        else:
            return redirect(url_for('mobile.worker_dashboard'))

    return render_template('mobile_login.html')


@mobile_bp.route('/logout')
def logout():
    """Mobile logout"""
    session.clear()
    return redirect(url_for('mobile.login'))


@mobile_bp.route('/')
@mobile_login_required
def index():
    """Mobile index - redirect to appropriate dashboard"""
    role = session.get('role', 'worker')
    if role == 'supervisor':
        return redirect(url_for('mobile.supervisor_dashboard'))
    else:
        return redirect(url_for('mobile.worker_dashboard'))


# ============ Worker Routes ============

@mobile_bp.route('/worker')
@mobile_bp.route('/worker/dashboard')
@mobile_login_required
def worker_dashboard():
    """Worker dashboard - main screen for field workers"""
    user_id = session.get('user_id')

    # Get assigned jobs for this worker
    try:
        all_jobs = get_all_jobs()
        assigned_jobs = [j for j in all_jobs if j.get('assigned_to') == user_id and j.get('status') != 'completed']
        active_job = next((j for j in assigned_jobs if j.get('status') == 'in_progress'), None)

        # Calculate stats
        today = datetime.now().date()
        completed_today = len([j for j in all_jobs if j.get('assigned_to') == user_id
                              and j.get('status') == 'completed'
                              and j.get('completed_date', datetime.min).date() == today])

        stats = {
            'assigned': len([j for j in assigned_jobs if j.get('status') == 'assigned']),
            'in_progress': len([j for j in assigned_jobs if j.get('status') == 'in_progress']),
            'completed_today': completed_today
        }
    except:
        assigned_jobs = []
        active_job = None
        stats = {'assigned': 0, 'in_progress': 0, 'completed_today': 0}

    return render_template('mobile_worker_dashboard.html',
                         assigned_jobs=assigned_jobs,
                         active_job=active_job,
                         stats=stats)


@mobile_bp.route('/worker/jobs')
@mobile_login_required
def worker_jobs():
    """All jobs list for worker"""
    user_id = session.get('user_id')
    filter_status = request.args.get('status', 'all')

    try:
        all_jobs = get_all_jobs()
        jobs = [j for j in all_jobs if j.get('assigned_to') == user_id]

        if filter_status != 'all':
            jobs = [j for j in jobs if j.get('status') == filter_status]
    except:
        jobs = []

    return render_template('mobile_worker_jobs.html', jobs=jobs, filter_status=filter_status)


@mobile_bp.route('/worker/job/<int:job_id>')
@mobile_login_required
def worker_job_detail(job_id):
    """Job detail view for worker"""
    try:
        job = get_job_by_id(job_id)
        if not job:
            flash('Job not found', 'error')
            return redirect(url_for('mobile.worker_dashboard'))

        # Get equipment details
        equipment = get_equipment_by_id(job.get('equipment_id'))
    except:
        job = {'id': job_id, 'job_type': 'maintenance', 'description': 'Sample job'}
        equipment = None

    return render_template('mobile_worker_job_detail.html', job=job, equipment=equipment)


@mobile_bp.route('/worker/job/<int:job_id>/start', methods=['POST'])
@mobile_login_required
def worker_start_job(job_id):
    """Start working on a job"""
    try:
        update_job(job_id, {
            'status': 'in_progress',
            'started_at': datetime.now()
        })
        flash('Job started', 'success')
    except Exception as e:
        flash(f'Error starting job: {str(e)}', 'error')

    return redirect(url_for('mobile.worker_job_detail', job_id=job_id))


@mobile_bp.route('/worker/job/<int:job_id>/complete', methods=['POST'])
@mobile_login_required
def worker_complete_job(job_id):
    """Complete a job"""
    notes = request.form.get('notes', '')

    try:
        update_job(job_id, {
            'status': 'completed',
            'completed_date': datetime.now(),
            'completion_notes': notes
        })
        flash('Job completed successfully!', 'success')
    except Exception as e:
        flash(f'Error completing job: {str(e)}', 'error')

    return redirect(url_for('mobile.worker_dashboard'))


@mobile_bp.route('/worker/job/<int:job_id>/pause', methods=['POST'])
@mobile_login_required
def worker_pause_job(job_id):
    """Pause a job"""
    reason = request.form.get('reason', '')

    try:
        update_job(job_id, {
            'status': 'paused',
            'pause_reason': reason
        })
        flash('Job paused', 'info')
    except Exception as e:
        flash(f'Error pausing job: {str(e)}', 'error')

    return redirect(url_for('mobile.worker_dashboard'))


@mobile_bp.route('/worker/inspection/start')
@mobile_login_required
def worker_start_inspection():
    """Start a new inspection"""
    try:
        equipment_list = get_all_equipment()
    except:
        equipment_list = []

    return render_template('mobile_worker_start_inspection.html', equipment=equipment_list)


@mobile_bp.route('/worker/inspection/perform/<int:equipment_id>', methods=['GET', 'POST'])
@mobile_login_required
def worker_perform_inspection(equipment_id):
    """Perform inspection on equipment"""
    if request.method == 'POST':
        # Save inspection results
        results = request.form.to_dict()
        # Save to database (implement as needed)
        flash('Inspection submitted successfully!', 'success')
        return redirect(url_for('mobile.worker_dashboard'))

    try:
        equipment = get_equipment_by_id(equipment_id)
    except:
        equipment = {'id': equipment_id, 'unique_id': 'EQ-001', 'brand': 'Test'}

    # Get inspection checklist items based on equipment type
    checklist_items = [
        {'id': 1, 'name': 'Visual Condition Check', 'description': 'Check for visible damage or wear'},
        {'id': 2, 'name': 'Fluid Levels', 'description': 'Check all fluid levels'},
        {'id': 3, 'name': 'Safety Equipment', 'description': 'Verify safety equipment is present'},
        {'id': 4, 'name': 'Operational Test', 'description': 'Perform basic operational test'},
        {'id': 5, 'name': 'Documentation', 'description': 'Verify documentation is current'}
    ]

    return render_template('mobile_worker_perform_inspection.html',
                         equipment=equipment,
                         checklist_items=checklist_items)


@mobile_bp.route('/worker/time')
@mobile_bp.route('/worker/time/log')
@mobile_login_required
def worker_log_time():
    """Time logging screen"""
    user_id = session.get('user_id')

    try:
        all_jobs = get_all_jobs()
        active_jobs = [j for j in all_jobs if j.get('assigned_to') == user_id
                      and j.get('status') in ['assigned', 'in_progress']]
    except:
        active_jobs = []

    return render_template('mobile_worker_log_time.html', jobs=active_jobs)


@mobile_bp.route('/worker/time/log', methods=['POST'])
@mobile_login_required
def worker_submit_time():
    """Submit time entry"""
    job_id = request.form.get('job_id')
    hours = request.form.get('hours')
    notes = request.form.get('notes', '')

    # Save time entry (implement as needed)
    flash('Time logged successfully!', 'success')
    return redirect(url_for('mobile.worker_dashboard'))


@mobile_bp.route('/worker/scan')
@mobile_login_required
def worker_scan_equipment():
    """QR/Barcode scanner screen"""
    return render_template('mobile_worker_scan.html')


@mobile_bp.route('/worker/scan/result', methods=['POST'])
@mobile_login_required
def worker_scan_result():
    """Process scan result"""
    code = request.form.get('code') or request.json.get('code')

    try:
        # Try to find equipment by code
        equipment_list = get_all_equipment()
        equipment = next((e for e in equipment_list if e.get('unique_id') == code or e.get('barcode') == code), None)

        if equipment:
            return jsonify({'success': True, 'equipment': equipment})
        else:
            return jsonify({'success': False, 'message': 'Equipment not found'})
    except:
        return jsonify({'success': False, 'message': 'Error processing scan'})


@mobile_bp.route('/worker/report')
@mobile_login_required
def worker_report_issue():
    """Report an issue form"""
    try:
        equipment_list = get_all_equipment()
    except:
        equipment_list = []

    return render_template('mobile_report_issue.html', equipment=equipment_list)


@mobile_bp.route('/worker/report', methods=['POST'])
@mobile_login_required
def worker_submit_issue():
    """Submit an issue report"""
    issue_type = request.form.get('issue_type')
    equipment_id = request.form.get('equipment_id')
    description = request.form.get('description')
    location = request.form.get('location')
    latitude = request.form.get('latitude')
    longitude = request.form.get('longitude')
    priority = request.form.get('priority', 'normal')

    # Handle photo upload
    photo = request.files.get('photo')
    photo_path = None
    if photo and photo.filename:
        # Save photo (implement as needed)
        pass

    # Create issue record
    try:
        if issue_type == 'outage':
            create_outage_record({
                'description': description,
                'location': location,
                'latitude': latitude,
                'longitude': longitude,
                'priority': priority,
                'reported_by': session.get('user_id'),
                'equipment_id': equipment_id
            })
        else:
            # Create maintenance job for the issue
            create_job({
                'equipment_id': equipment_id,
                'job_type': issue_type,
                'description': description,
                'priority': priority,
                'status': 'pending',
                'location': location,
                'latitude': latitude,
                'longitude': longitude,
                'reported_by': session.get('user_id')
            })

        flash('Issue reported successfully!', 'success')
    except Exception as e:
        flash(f'Error reporting issue: {str(e)}', 'error')

    return redirect(url_for('mobile.worker_dashboard'))


@mobile_bp.route('/worker/history')
@mobile_login_required
def worker_history():
    """Worker job history"""
    user_id = session.get('user_id')

    try:
        all_jobs = get_all_jobs()
        completed_jobs = [j for j in all_jobs if j.get('assigned_to') == user_id
                         and j.get('status') == 'completed']
        # Sort by completion date, most recent first
        completed_jobs.sort(key=lambda x: x.get('completed_date', datetime.min), reverse=True)
    except:
        completed_jobs = []

    return render_template('mobile_worker_history.html', jobs=completed_jobs)


@mobile_bp.route('/worker/profile')
@mobile_login_required
def worker_profile():
    """Worker profile screen"""
    user_id = session.get('user_id')

    try:
        user = get_user_by_id(user_id)
    except:
        user = {'id': user_id, 'username': session.get('username', 'Worker'), 'email': ''}

    return render_template('mobile_worker_profile.html', user=user)


# ============ Supervisor Routes ============

@mobile_bp.route('/supervisor')
@mobile_bp.route('/supervisor/dashboard')
@mobile_login_required
def supervisor_dashboard():
    """Supervisor dashboard"""
    try:
        all_jobs = get_all_jobs()
        users = get_all_users()

        # Calculate stats
        today = datetime.now().date()
        active_jobs = [j for j in all_jobs if j.get('status') in ['assigned', 'in_progress']]
        overdue = len([j for j in active_jobs if j.get('due_date') and j.get('due_date').date() < today])
        completed_today = len([j for j in all_jobs if j.get('status') == 'completed'
                              and j.get('completed_date', datetime.min).date() == today])

        # Crew members (workers)
        crew_members = [u for u in users if u.get('role') in ['worker', 'technician']]
        active_crew = len([u for u in crew_members if u.get('status') == 'active'])

        # Priority jobs
        priority_jobs = [j for j in active_jobs if j.get('priority') in ['high', 'critical']]

        # Pending approvals
        pending_approvals = len([j for j in all_jobs if j.get('status') == 'pending_approval'])

        stats = {
            'active_jobs': len(active_jobs),
            'overdue': overdue,
            'completed_today': completed_today,
            'completion_rate': int((completed_today / max(len(active_jobs) + completed_today, 1)) * 100),
            'crew_count': len(crew_members),
            'crew_active': active_crew,
            'pending_approvals': pending_approvals
        }

        # Get active alerts
        active_alerts = get_active_outages() if 'get_active_outages' in dir() else []
        notification_count = len(active_alerts) + pending_approvals

    except:
        stats = {
            'active_jobs': 0, 'overdue': 0, 'completed_today': 0,
            'completion_rate': 0, 'crew_count': 0, 'crew_active': 0,
            'pending_approvals': 0
        }
        crew_members = []
        priority_jobs = []
        active_alerts = []
        notification_count = 0

    return render_template('mobile_supervisor_dashboard.html',
                         stats=stats,
                         crew_members=crew_members[:5],
                         priority_jobs=priority_jobs[:5],
                         active_alerts=active_alerts[:3],
                         notification_count=notification_count)


@mobile_bp.route('/supervisor/crew')
@mobile_login_required
def supervisor_crew():
    """Crew list view"""
    try:
        users = get_all_users()
        crew_members = [u for u in users if u.get('role') in ['worker', 'technician', 'supervisor']]
    except:
        crew_members = []

    return render_template('mobile_supervisor_crew.html', crew_members=crew_members)


@mobile_bp.route('/supervisor/crew/<int:user_id>')
@mobile_login_required
def supervisor_crew_member(user_id):
    """Individual crew member detail"""
    try:
        user = get_user_by_id(user_id)
        all_jobs = get_all_jobs()
        user_jobs = [j for j in all_jobs if j.get('assigned_to') == user_id]
    except:
        user = {'id': user_id, 'username': 'Unknown'}
        user_jobs = []

    return render_template('mobile_supervisor_crew_member.html', user=user, jobs=user_jobs)


@mobile_bp.route('/supervisor/jobs')
@mobile_login_required
def supervisor_all_jobs():
    """All jobs list for supervisor"""
    status_filter = request.args.get('status', 'all')
    priority_filter = request.args.get('priority', 'all')

    try:
        jobs = get_all_jobs()

        if status_filter != 'all':
            jobs = [j for j in jobs if j.get('status') == status_filter]
        if priority_filter != 'all':
            jobs = [j for j in jobs if j.get('priority') == priority_filter]
    except:
        jobs = []

    return render_template('mobile_supervisor_jobs.html',
                         jobs=jobs,
                         status_filter=status_filter,
                         priority_filter=priority_filter)


@mobile_bp.route('/supervisor/job/<int:job_id>')
@mobile_login_required
def supervisor_job_detail(job_id):
    """Job detail for supervisor with management actions"""
    try:
        job = get_job_by_id(job_id)
        equipment = get_equipment_by_id(job.get('equipment_id')) if job else None
        users = get_all_users()
        workers = [u for u in users if u.get('role') in ['worker', 'technician']]
    except:
        job = {'id': job_id}
        equipment = None
        workers = []

    return render_template('mobile_supervisor_job_detail.html',
                         job=job,
                         equipment=equipment,
                         workers=workers)


@mobile_bp.route('/supervisor/job/<int:job_id>/assign', methods=['POST'])
@mobile_login_required
def supervisor_assign_worker(job_id):
    """Assign worker to job"""
    worker_id = request.form.get('worker_id')

    try:
        update_job(job_id, {
            'assigned_to': int(worker_id),
            'status': 'assigned',
            'assigned_date': datetime.now()
        })
        flash('Worker assigned successfully!', 'success')
    except Exception as e:
        flash(f'Error assigning worker: {str(e)}', 'error')

    return redirect(url_for('mobile.supervisor_job_detail', job_id=job_id))


@mobile_bp.route('/supervisor/assign')
@mobile_login_required
def supervisor_assign_job():
    """Assign job screen"""
    try:
        jobs = get_all_jobs()
        unassigned = [j for j in jobs if not j.get('assigned_to') and j.get('status') != 'completed']
        users = get_all_users()
        workers = [u for u in users if u.get('role') in ['worker', 'technician']]
    except:
        unassigned = []
        workers = []

    return render_template('mobile_supervisor_assign.html', jobs=unassigned, workers=workers)


@mobile_bp.route('/supervisor/approvals')
@mobile_login_required
def supervisor_approvals():
    """Pending approvals list"""
    try:
        jobs = get_all_jobs()
        pending = [j for j in jobs if j.get('status') == 'pending_approval']
    except:
        pending = []

    return render_template('mobile_supervisor_approvals.html', pending_items=pending)


@mobile_bp.route('/supervisor/approve/<int:job_id>', methods=['POST'])
@mobile_login_required
def supervisor_approve_job(job_id):
    """Approve a job completion"""
    action = request.form.get('action')  # 'approve' or 'reject'
    notes = request.form.get('notes', '')

    try:
        if action == 'approve':
            update_job(job_id, {
                'status': 'completed',
                'approved_by': session.get('user_id'),
                'approved_date': datetime.now(),
                'approval_notes': notes
            })
            flash('Job approved!', 'success')
        else:
            update_job(job_id, {
                'status': 'revision_required',
                'rejection_notes': notes
            })
            flash('Job returned for revision', 'info')
    except Exception as e:
        flash(f'Error: {str(e)}', 'error')

    return redirect(url_for('mobile.supervisor_approvals'))


@mobile_bp.route('/supervisor/reports')
@mobile_login_required
def supervisor_reports():
    """Supervisor reports overview"""
    try:
        jobs = get_all_jobs()
        today = datetime.now().date()
        week_start = today - timedelta(days=today.weekday())

        # Calculate report data
        daily_completed = len([j for j in jobs if j.get('status') == 'completed'
                              and j.get('completed_date', datetime.min).date() == today])
        weekly_completed = len([j for j in jobs if j.get('status') == 'completed'
                               and j.get('completed_date', datetime.min).date() >= week_start])

        report_data = {
            'daily_completed': daily_completed,
            'weekly_completed': weekly_completed,
            'avg_completion_time': 4.5,  # placeholder
            'open_issues': len([j for j in jobs if j.get('status') in ['pending', 'assigned']])
        }
    except:
        report_data = {
            'daily_completed': 0, 'weekly_completed': 0,
            'avg_completion_time': 0, 'open_issues': 0
        }

    return render_template('mobile_supervisor_reports.html', report_data=report_data)


@mobile_bp.route('/supervisor/map')
@mobile_login_required
def supervisor_map():
    """Map view for supervisor"""
    try:
        jobs = get_all_jobs()
        active_jobs = [j for j in jobs if j.get('status') in ['assigned', 'in_progress']
                      and j.get('latitude') and j.get('longitude')]

        users = get_all_users()
        crew_locations = [u for u in users if u.get('latitude') and u.get('longitude')]
    except:
        active_jobs = []
        crew_locations = []

    return render_template('mobile_supervisor_map.html',
                         jobs=active_jobs,
                         crew_locations=crew_locations)


@mobile_bp.route('/supervisor/notifications')
@mobile_login_required
def supervisor_notifications():
    """Notifications list"""
    notifications = []  # Fetch from database

    return render_template('mobile_supervisor_notifications.html', notifications=notifications)


@mobile_bp.route('/supervisor/alert/<int:alert_id>')
@mobile_login_required
def supervisor_alert_detail(alert_id):
    """Alert detail view"""
    try:
        outages = get_active_outages()
        alert = next((o for o in outages if o.get('id') == alert_id), None)
    except:
        alert = None

    return render_template('mobile_supervisor_alert.html', alert=alert)


# ============ Regular Worker Issue Reporting ============

@mobile_bp.route('/report')
def public_report_issue():
    """Public issue reporting page (no login required)"""
    try:
        equipment_list = get_all_equipment()
    except:
        equipment_list = []

    return render_template('mobile_public_report.html', equipment=equipment_list)


@mobile_bp.route('/report', methods=['POST'])
def public_submit_issue():
    """Submit public issue report"""
    reporter_name = request.form.get('reporter_name')
    reporter_phone = request.form.get('reporter_phone')
    reporter_email = request.form.get('reporter_email')
    issue_type = request.form.get('issue_type')
    description = request.form.get('description')
    location = request.form.get('location')
    latitude = request.form.get('latitude')
    longitude = request.form.get('longitude')

    # Create citizen request
    try:
        # Save to database (implement as needed)
        flash('Your report has been submitted. Thank you!', 'success')
    except Exception as e:
        flash(f'Error submitting report: {str(e)}', 'error')

    return redirect(url_for('mobile.public_report_issue'))


# ============ API Endpoints for Mobile ============

@mobile_bp.route('/api/jobs')
@mobile_login_required
def api_jobs():
    """API endpoint for jobs"""
    user_id = session.get('user_id')
    role = session.get('role', 'worker')

    try:
        jobs = get_all_jobs()

        if role != 'supervisor':
            jobs = [j for j in jobs if j.get('assigned_to') == user_id]

        return jsonify({'success': True, 'jobs': jobs})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


@mobile_bp.route('/api/job/<int:job_id>/update', methods=['POST'])
@mobile_login_required
def api_update_job(job_id):
    """API endpoint to update job"""
    data = request.json

    try:
        update_job(job_id, data)
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


@mobile_bp.route('/api/location/update', methods=['POST'])
@mobile_login_required
def api_update_location():
    """Update worker location"""
    data = request.json
    user_id = session.get('user_id')

    # Save location to database
    # Implement as needed

    return jsonify({'success': True})


@mobile_bp.route('/api/sync', methods=['POST'])
@mobile_login_required
def api_sync():
    """Sync offline data"""
    data = request.json

    # Process offline data
    results = {'synced': 0, 'errors': []}

    for item in data.get('items', []):
        try:
            # Process each item
            results['synced'] += 1
        except Exception as e:
            results['errors'].append({'item': item.get('id'), 'error': str(e)})

    return jsonify({'success': True, 'results': results})
