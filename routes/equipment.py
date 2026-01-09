# routes/equipment.py
from flask import Blueprint, render_template, redirect, url_for, flash, request, Response
from flask_login import login_required, current_user
from services.equipment_service import (
    get_equipment_detail, get_all_equipment, get_equipment_by_id, update_equipment, delete_equipment, add_equipment,
    get_maintenance_dashboard_counts, list_equipment_under_maintenance,
    get_equipment_maintenance_history, get_equipment_maintenance_schedule, change_equipment_status,
    create_or_update_maintenance_schedule, log_maintenance_action, attach_maintenance_document,
    list_maintenance_jobs, create_maintenance_job, update_maintenance_job,
    list_job_parts, reserve_job_part, release_job_part, commit_job_parts,
    get_jobs_for_calendar, get_workload_summary, list_maintenance_jobs_sorted, get_maintenance_job, get_my_jobs, save_maintenance_attachment,
    # Checklists & Inspections
    list_checklist_templates, get_checklist_template, create_checklist_template,
    update_checklist_template, delete_checklist_template, add_checklist_template_item,
    delete_checklist_template_item, create_inspection, get_inspection, list_inspections,
    update_inspection_item, complete_inspection, add_inspection_media,
    # Work Order Templates
    list_work_order_templates, get_work_order_template, create_work_order_template,
    add_work_order_template_step, create_job_from_template,
    # Job Steps & Signatures
    get_job_steps, complete_job_step, add_job_step, list_job_attachments,
    add_job_attachment, add_job_signature, get_job_signatures,
    # Reporting & Analytics
    get_maintenance_kpis, get_equipment_health_summary, get_maintenance_trends, export_maintenance_report,
    # Vendors
    list_vendors, get_vendor, create_vendor, update_vendor, assign_job_to_vendor,
    # Compliance
    list_equipment_certifications, add_equipment_certification, get_expiring_certifications,
    list_warranty_records, add_warranty_record,
    # Citizen Requests
    create_citizen_request, list_citizen_requests, get_citizen_request,
    convert_request_to_job, update_citizen_request_status,
    # Telematics
    record_telematics_data, get_telematics_history, get_fleet_locations,
    # Predictive
    predict_maintenance_enhanced
)
from services.users_service import get_all_certifications
from flask import jsonify
from services.db import get_connection, release_connection
from services.email_service import send_notification
import csv
import io
from datetime import datetime

equipment_bp = Blueprint("equipment", __name__)

@equipment_bp.route("/equipment")
@login_required
def equipment():
    return redirect(url_for("equipment.equipment_list"))

@equipment_bp.route("/list")
@login_required
def equipment_list():
    # Get search and sort parameters
    search_query = request.args.get('search', '').strip()
    sort_by = request.args.get('sort', 'unique_id')  # default sort by unique_id
    sort_order = request.args.get('order', 'asc')  # default ascending
    
    # Validate sort parameters
    valid_sort_fields = ['unique_id', 'equipment_type', 'brand', 'model', 'status', 'hours', 'maintenance_due']
    if sort_by not in valid_sort_fields:
        sort_by = 'unique_id'
    
    if sort_order not in ['asc', 'desc']:
        sort_order = 'asc'
    
    # Get filtered and sorted equipment
    equipment = get_all_equipment(search_query=search_query, sort_by=sort_by, sort_order=sort_order)
    
    return render_template("equipment.html", 
                         equipment=equipment, 
                         search_query=search_query,
                         current_sort=sort_by,
                         current_order=sort_order,
                         can_edit=current_user.role in ['manager', 'admin'])

@equipment_bp.route('/maintenance')
@login_required
def maintenance_landing():
    counts = get_maintenance_dashboard_counts()
    under_list = list_equipment_under_maintenance()
    # Sorting/filtering for worker view
    sort_by = request.args.get('sort', 'importance')
    assigned_to_me = request.args.get('mine') == '1'
    jobs = list_maintenance_jobs_sorted(sort_by, current_user.id if assigned_to_me else None)
    workload = get_workload_summary()
    return render_template('maintenance.html', counts=counts, under_list=under_list, jobs=jobs, workload=workload, sort_by=sort_by, assigned_to_me=assigned_to_me)

@equipment_bp.route('/maintenance/my')
@login_required
def maintenance_my_jobs():
    sort_by = request.args.get('sort', 'importance')
    jobs = get_my_jobs(current_user.id, sort_by)
    return render_template('maintenance_my.html', jobs=jobs, sort_by=sort_by)

@equipment_bp.route('/maintenance/<int:equipment_id>')
@login_required
def maintenance_detail(equipment_id):
    equipment = get_equipment_by_id(equipment_id)
    if not equipment:
        flash('Equipment not found.')
        return redirect(url_for('equipment.maintenance_landing'))
    history = get_equipment_maintenance_history(equipment_id)
    sched = get_equipment_maintenance_schedule(equipment_id)
    return render_template('maintenance_detail.html', equipment=equipment, history=history, schedule=sched)

@equipment_bp.route('/maintenance/status/<int:equipment_id>', methods=['POST'])
@login_required
def maintenance_change_status(equipment_id):
    new_status = request.form.get('status')
    try:
        change_equipment_status(equipment_id, new_status)
        flash('Status updated')
    except ValueError as e:
        flash(str(e))
    return redirect(url_for('equipment.maintenance_detail', equipment_id=equipment_id))

@equipment_bp.route('/maintenance/schedule/<int:equipment_id>', methods=['POST'])
@login_required
def maintenance_update_schedule(equipment_id):
    schedule_type = request.form.get('schedule_type')  # 'time' or 'hours'
    interval_days = request.form.get('interval_days')
    interval_hours = request.form.get('interval_hours')
    notes = request.form.get('notes')
    create_or_update_maintenance_schedule(
        equipment_id,
        schedule_type=schedule_type,
        interval_days=int(interval_days) if interval_days else None,
        interval_hours=int(interval_hours) if interval_hours else None,
        notes=notes,
        active=True,
    )
    flash('Schedule updated')
    return redirect(url_for('equipment.maintenance_detail', equipment_id=equipment_id))

@equipment_bp.route('/maintenance/log/<int:equipment_id>', methods=['POST'])
@login_required
def maintenance_log_action(equipment_id):
    action = request.form.get('action')
    details = request.form.get('details')
    status = request.form.get('status', 'completed')
    performed_by = current_user.id
    # Optional file
    if 'document' in request.files and request.files['document'].filename:
        file = request.files['document']
        from werkzeug.utils import secure_filename
        filename = secure_filename(file.filename)
        path = attach_maintenance_document('Uploads/maintenance', file, filename)
        details = (details or '') + f"\nAttachment: {path}"
    log_maintenance_action(equipment_id, action, details, status, performed_by)
    flash('Maintenance action logged')
    return redirect(url_for('equipment.maintenance_detail', equipment_id=equipment_id))

@equipment_bp.route('/maintenance/report', methods=['GET','POST'])
@login_required
def maintenance_report_issue():
    if request.method == 'GET':
        # Mobile-optimized simple form
        conn = get_connection()
        c = conn.cursor()
        c.execute("SELECT id, unique_id, brand, model FROM equipment_instances ORDER BY unique_id")
        equipment = [{'id': r[0], 'unique_id': r[1], 'brand': r[2], 'model': r[3]} for r in c.fetchall()]
        release_connection(conn)
        return render_template('maintenance_report_issue.html', equipment=equipment)
    else:
        equipment_id = int(request.form.get('equipment_id'))
        job_type = 'breakdown'
        priority = request.form.get('priority','high')
        due_date = request.form.get('due_date') or None
        notes = request.form.get('notes')
        assigned_to = None
        job_id = create_maintenance_job(equipment_id, job_type, priority, due_date, notes, assigned_to, current_user.id)
        # Optional photo attachments
        if 'photos' in request.files:
            files = request.files.getlist('photos')
            for f in files:
                if f and f.filename:
                    save_maintenance_attachment(job_id, equipment_id, f, 'incident_photo', current_user.id)
        flash(f'Breakdown reported. Job #{job_id} created.')
        return redirect(url_for('equipment.maintenance_my_jobs'))

@equipment_bp.route('/maintenance/job/create', methods=['GET','POST'])
@login_required
def maintenance_job_create():
    if request.method == 'GET':
        # Simple form page
        from services.users_service import get_all_users
        users = get_all_users() if hasattr(__import__('services.users_service'), 'get_all_users') else []
        equipment = get_all_equipment()
        return render_template('maintenance_job_create.html', users=users, equipment=equipment)
    else:
        equipment_id = int(request.form.get('equipment_id'))
        job_type = request.form.get('job_type')
        priority = request.form.get('priority','normal')
        due_date = request.form.get('due_date') or None
        notes = request.form.get('notes')
        assigned_to = request.form.get('assigned_to')
        assigned_to = int(assigned_to) if assigned_to else None
        job_id = create_maintenance_job(equipment_id, job_type, priority, due_date, notes, assigned_to, current_user.id)
        try:
            send_notification(
                subject=f"Maintenance Job #{job_id} Created",
                body=f"Job {job_id} for equipment #{equipment_id} ({job_type}, {priority}) due {due_date or '-'}.",
                recipients=["maintenance@potelco.com"],
            )
        except Exception:
            pass
        flash(f'Job {job_id} created')
        return redirect(url_for('equipment.maintenance_landing'))

@equipment_bp.route('/maintenance/job/<int:job_id>/update', methods=['POST'])
@login_required
def maintenance_job_update(job_id):
    status = request.form.get('status')
    priority = request.form.get('priority')
    due_date = request.form.get('due_date') or None
    notes = request.form.get('notes')
    assigned_to = request.form.get('assigned_to')
    assigned_to = int(assigned_to) if assigned_to else None
    update_maintenance_job(job_id, status=status, assigned_to=assigned_to, priority=priority, due_date=due_date, notes=notes)
    if status == 'completed':
        try:
            send_notification(
                subject=f"Maintenance Job #{job_id} Completed",
                body=f"Job {job_id} marked completed.",
                recipients=["maintenance@potelco.com"],
            )
        except Exception:
            pass
    flash('Job updated')
    return redirect(url_for('equipment.maintenance_landing'))

@equipment_bp.route('/maintenance/job/<int:job_id>/parts', methods=['GET','POST'])
@login_required
def maintenance_job_parts(job_id):
    if request.method == 'GET':
        parts = list_job_parts(job_id)
        return render_template('maintenance_job_parts.html', job_id=job_id, parts=parts)
    else:
        action = request.form.get('action')
        if action == 'reserve':
            reserve_job_part(job_id, int(request.form.get('consumable_id')), int(request.form.get('quantity')))
        elif action == 'release':
            release_job_part(int(request.form.get('reservation_id')))
        elif action == 'commit':
            commit_job_parts(job_id)
        return redirect(url_for('equipment.maintenance_job_parts', job_id=job_id))

@equipment_bp.route('/maintenance/calendar')
@login_required
def maintenance_calendar():
    return render_template('maintenance_calendar.html')


@equipment_bp.route('/maintenance/events')
@login_required
def maintenance_events():
    events = get_jobs_for_calendar()
    for e in events:
        e['url'] = url_for('equipment.maintenance_job_view', job_id=e['id'])
        e['allDay'] = True
    return jsonify(events)


@equipment_bp.route('/maintenance/job/<int:job_id>')
@login_required
def maintenance_job_view(job_id):
    job = get_maintenance_job(job_id)
    if not job:
        flash('Job not found')
        return redirect(url_for('equipment.maintenance_landing'))
    equipment = get_equipment_by_id(job['equipment_id'])
    # Load time logs
    conn = get_connection()
    c = conn.cursor()
    c.execute("SELECT id, user_id, hours, work_date, notes FROM maintenance_time_logs WHERE job_id = %s ORDER BY work_date DESC, id DESC", (job_id,))
    time_logs = [{ 'id': r[0], 'user_id': r[1], 'hours': float(r[2]), 'work_date': r[3], 'notes': r[4] } for r in c.fetchall()]
    release_connection(conn)
    return render_template('maintenance_detail_job.html', job=job, equipment=equipment, time_logs=time_logs)


# Customers & Vehicles (CRM)
@equipment_bp.route('/maintenance/customers')
@login_required
def maintenance_customers():
    conn = get_connection()
    c = conn.cursor()
    c.execute("SELECT id, name, company, contact_email, contact_phone FROM maintenance_customers ORDER BY created_at DESC")
    customers = [{ 'id': r[0], 'name': r[1], 'company': r[2], 'email': r[3], 'phone': r[4] } for r in c.fetchall()]
    release_connection(conn)
    return render_template('maintenance_customers.html', customers=customers)


@equipment_bp.route('/maintenance/customers/new', methods=['GET','POST'])
@login_required
def maintenance_customers_new():
    if request.method == 'GET':
        return render_template('maintenance_customers_new.html')
    name = request.form.get('name')
    company = request.form.get('company')
    email = request.form.get('email')
    phone = request.form.get('phone')
    conn = get_connection()
    c = conn.cursor()
    c.execute("INSERT INTO maintenance_customers (name, company, contact_email, contact_phone) VALUES (%s, %s, %s, %s) RETURNING id", (name, company, email, phone))
    cid = c.fetchone()[0]
    conn.commit()
    release_connection(conn)
    flash('Customer added')
    return redirect(url_for('equipment.maintenance_customer_view', customer_id=cid))


@equipment_bp.route('/maintenance/customers/<int:customer_id>')
@login_required
def maintenance_customer_view(customer_id: int):
    conn = get_connection()
    c = conn.cursor()
    c.execute("SELECT id, name, company, contact_email, contact_phone, notes FROM maintenance_customers WHERE id = %s", (customer_id,))
    row = c.fetchone()
    if not row:
        release_connection(conn)
        flash('Customer not found')
        return redirect(url_for('equipment.maintenance_customers'))
    customer = { 'id': row[0], 'name': row[1], 'company': row[2], 'email': row[3], 'phone': row[4], 'notes': row[5] }
    c.execute("SELECT id, equipment_id, vin, plate, description FROM maintenance_vehicles WHERE customer_id = %s ORDER BY id DESC", (customer_id,))
    vehicles = [{ 'id': r[0], 'equipment_id': r[1], 'vin': r[2], 'plate': r[3], 'description': r[4] } for r in c.fetchall()]
    release_connection(conn)
    return render_template('maintenance_customer_detail.html', customer=customer, vehicles=vehicles)


@equipment_bp.route('/maintenance/customers/<int:customer_id>/vehicles/add', methods=['POST'])
@login_required
def maintenance_customer_add_vehicle(customer_id: int):
    equipment_id = request.form.get('equipment_id')
    vin = request.form.get('vin')
    plate = request.form.get('plate')
    description = request.form.get('description')
    conn = get_connection()
    c = conn.cursor()
    c.execute("INSERT INTO maintenance_vehicles (customer_id, equipment_id, vin, plate, description) VALUES (%s, %s, %s, %s, %s)", (customer_id, equipment_id or None, vin, plate, description))
    conn.commit()
    release_connection(conn)
    flash('Vehicle/Asset added')
    return redirect(url_for('equipment.maintenance_customer_view', customer_id=customer_id))


# Estimates & Invoices
@equipment_bp.route('/maintenance/estimates')
@login_required
def maintenance_estimates():
    conn = get_connection()
    c = conn.cursor()
    c.execute("SELECT id, customer_id, equipment_id, total, status, created_at FROM maintenance_estimates ORDER BY created_at DESC")
    rows = c.fetchall()
    release_connection(conn)
    estimates = [{ 'id': r[0], 'customer_id': r[1], 'equipment_id': r[2], 'total': float(r[3] or 0), 'status': r[4], 'created_at': r[5] } for r in rows]
    return render_template('maintenance_estimates.html', estimates=estimates)


@equipment_bp.route('/maintenance/estimate/new', methods=['GET','POST'])
@login_required
def maintenance_estimate_new():
    if request.method == 'GET':
        conn = get_connection()
        c = conn.cursor()
        c.execute("SELECT id, name, company FROM maintenance_customers ORDER BY name")
        customers = [{ 'id': r[0], 'name': r[1], 'company': r[2] } for r in c.fetchall()]
        release_connection(conn)
        equipment = get_all_equipment()
        return render_template('maintenance_estimate_new.html', customers=customers, equipment=equipment)
    customer_id = request.form.get('customer_id')
    equipment_id = request.form.get('equipment_id')
    conn = get_connection()
    c = conn.cursor()
    c.execute("INSERT INTO maintenance_estimates (customer_id, equipment_id, subtotal, tax, total, status) VALUES (%s, %s, 0, 0, 0, 'draft') RETURNING id", (customer_id or None, equipment_id or None))
    eid = c.fetchone()[0]
    conn.commit()
    release_connection(conn)
    return redirect(url_for('equipment.maintenance_estimate_view', estimate_id=eid))


@equipment_bp.route('/maintenance/estimate/<int:estimate_id>')
@login_required
def maintenance_estimate_view(estimate_id: int):
    conn = get_connection()
    c = conn.cursor()
    c.execute("SELECT id, customer_id, equipment_id, subtotal, tax, total, status FROM maintenance_estimates WHERE id = %s", (estimate_id,))
    row = c.fetchone()
    if not row:
        release_connection(conn)
        flash('Estimate not found')
        return redirect(url_for('equipment.maintenance_estimates'))
    est = { 'id': row[0], 'customer_id': row[1], 'equipment_id': row[2], 'subtotal': float(row[3] or 0), 'tax': float(row[4] or 0), 'total': float(row[5] or 0), 'status': row[6] }
    c.execute("SELECT id, item_type, description, quantity, unit_price, total FROM maintenance_estimate_items WHERE estimate_id = %s ORDER BY id", (estimate_id,))
    items = [{ 'id': r[0], 'item_type': r[1], 'description': r[2], 'quantity': float(r[3] or 0), 'unit_price': float(r[4] or 0), 'total': float(r[5] or 0) } for r in c.fetchall()]
    release_connection(conn)
    return render_template('maintenance_estimate_detail.html', estimate=est, items=items)


@equipment_bp.route('/maintenance/estimate/<int:estimate_id>/add-item', methods=['POST'])
@login_required
def maintenance_estimate_add_item(estimate_id: int):
    item_type = request.form.get('item_type')
    description = request.form.get('description')
    quantity = float(request.form.get('quantity', 1))
    unit_price = float(request.form.get('unit_price', 0))
    total = quantity * unit_price
    conn = get_connection()
    c = conn.cursor()
    c.execute("INSERT INTO maintenance_estimate_items (estimate_id, item_type, description, quantity, unit_price, total) VALUES (%s, %s, %s, %s, %s, %s)", (estimate_id, item_type, description, quantity, unit_price, total))
    c.execute("UPDATE maintenance_estimates SET subtotal = (SELECT COALESCE(SUM(total),0) FROM maintenance_estimate_items WHERE estimate_id = %s), total = (SELECT COALESCE(SUM(total),0) FROM maintenance_estimate_items WHERE estimate_id = %s) WHERE id = %s", (estimate_id, estimate_id, estimate_id))
    conn.commit()
    release_connection(conn)
    return redirect(url_for('equipment.maintenance_estimate_view', estimate_id=estimate_id))


@equipment_bp.route('/maintenance/estimate/<int:estimate_id>/to-invoice', methods=['POST'])
@login_required
def maintenance_estimate_to_invoice(estimate_id: int):
    conn = get_connection()
    c = conn.cursor()
    c.execute("SELECT customer_id, equipment_id, total FROM maintenance_estimates WHERE id = %s", (estimate_id,))
    row = c.fetchone()
    if not row:
        release_connection(conn)
        flash('Estimate not found')
        return redirect(url_for('equipment.maintenance_estimates'))
    c.execute("INSERT INTO maintenance_invoices (customer_id, equipment_id, estimate_id, subtotal, tax, total, status) VALUES (%s, %s, %s, %s, 0, %s, 'unpaid') RETURNING id", (row[0], row[1], estimate_id, row[2], row[2]))
    inv_id = c.fetchone()[0]
    c.execute("SELECT item_type, description, quantity, unit_price, total FROM maintenance_estimate_items WHERE estimate_id = %s", (estimate_id,))
    for it in c.fetchall():
        c.execute("INSERT INTO maintenance_invoice_items (invoice_id, item_type, description, quantity, unit_price, total) VALUES (%s, %s, %s, %s, %s, %s)", (inv_id, it[0], it[1], it[2], it[3], it[4]))
    conn.commit()
    release_connection(conn)
    flash('Converted to invoice')
    return redirect(url_for('equipment.maintenance_invoice_view', invoice_id=inv_id))


@equipment_bp.route('/maintenance/invoices')
@login_required
def maintenance_invoices():
    conn = get_connection()
    c = conn.cursor()
    c.execute("SELECT id, customer_id, equipment_id, total, status, created_at FROM maintenance_invoices ORDER BY created_at DESC")
    rows = c.fetchall()
    release_connection(conn)
    invoices = [{ 'id': r[0], 'customer_id': r[1], 'equipment_id': r[2], 'total': float(r[3] or 0), 'status': r[4], 'created_at': r[5] } for r in rows]
    return render_template('maintenance_invoices.html', invoices=invoices)


@equipment_bp.route('/maintenance/invoice/<int:invoice_id>')
@login_required
def maintenance_invoice_view(invoice_id: int):
    conn = get_connection()
    c = conn.cursor()
    c.execute("SELECT id, customer_id, equipment_id, subtotal, tax, total, status FROM maintenance_invoices WHERE id = %s", (invoice_id,))
    row = c.fetchone()
    if not row:
        release_connection(conn)
        flash('Invoice not found')
        return redirect(url_for('equipment.maintenance_invoices'))
    inv = { 'id': row[0], 'customer_id': row[1], 'equipment_id': row[2], 'subtotal': float(row[3] or 0), 'tax': float(row[4] or 0), 'total': float(row[5] or 0), 'status': row[6] }
    c.execute("SELECT id, item_type, description, quantity, unit_price, total FROM maintenance_invoice_items WHERE invoice_id = %s ORDER BY id", (invoice_id,))
    items = [{ 'id': r[0], 'item_type': r[1], 'description': r[2], 'quantity': float(r[3] or 0), 'unit_price': float(r[4] or 0), 'total': float(r[5] or 0) } for r in c.fetchall()]
    release_connection(conn)
    return render_template('maintenance_invoice_detail.html', invoice=inv, items=items)


@equipment_bp.route('/maintenance/invoice/<int:invoice_id>/mark-paid', methods=['POST'])
@login_required
def maintenance_invoice_mark_paid(invoice_id: int):
    conn = get_connection()
    c = conn.cursor()
    c.execute("UPDATE maintenance_invoices SET status = 'paid' WHERE id = %s", (invoice_id,))
    conn.commit()
    release_connection(conn)
    flash('Invoice marked as paid')
    return redirect(url_for('equipment.maintenance_invoice_view', invoice_id=invoice_id))

@equipment_bp.route('/equipment_detail/<int:equipment_id>', methods=['GET'])
@login_required
def equipment_detail(equipment_id):
    equipment = get_equipment_detail(equipment_id)
    if not equipment:
        flash('Equipment not found.')
        return redirect(url_for('equipment.equipment_list'))
    return render_template('equipment_detail.html', equipment=equipment, can_edit=current_user.role in ['manager', 'admin'])

@equipment_bp.route("/add", methods=["GET", "POST"])
@login_required
def add_equipment_route():
    if request.method == "POST":
        try:
            add_equipment(request.form)
            flash("Equipment added successfully.", "success")
            return redirect(url_for("equipment.equipment_list"))
        except ValueError as e:
            flash(str(e), "error")
            # Return to form with the data to preserve user input
            certs = get_all_certifications()
            return render_template("add_equipment.html", all_certs=certs, form_data=request.form)
        except Exception as e:
            flash(f"An error occurred while adding equipment: {str(e)}", "error")
            certs = get_all_certifications()
            return render_template("add_equipment.html", all_certs=certs, form_data=request.form)

    certs = get_all_certifications()
    return render_template("add_equipment.html", all_certs=certs)

@equipment_bp.route("/edit/<int:id>", methods=["GET", "POST"])
@login_required
def edit_equipment(id):
    equipment = get_equipment_by_id(id)
    if not equipment:
        flash("Equipment not found.", "error")
        return redirect(url_for("equipment.equipment_list"))

    if request.method == "POST":
        try:
            update_equipment(id, request.form)
            flash("Equipment updated successfully.", "success")
            return redirect(url_for("equipment.equipment_list"))
        except ValueError as e:
            flash(str(e), "error")
            # Return to form with the updated data to preserve user input
            certs = get_all_certifications()
            # Create a dict from form data to display in the template
            form_equipment = {
                'id': id,
                'equipment_type': request.form.get('equipment_type'),
                'unique_id': request.form.get('unique_id'),
                'brand': request.form.get('brand'),
                'model': request.form.get('model'),
                'serial_number': request.form.get('serial_number'),
                'hours': request.form.get('hours'),
                'fuel_type': request.form.get('fuel_type'),
                'gross_weight': request.form.get('gross_weight'),
                'requires_operator': 'requires_operator' in request.form,
                'required_certification': request.form.get('required_certification'),
                'status': request.form.get('status'),
                'last_maintenance': request.form.get('last_maintenance'),
                'maintenance_threshold': request.form.get('maintenance_threshold')
            }
            return render_template("edit_equipment.html", equipment=form_equipment, all_certs=certs)
        except Exception as e:
            flash(f"An error occurred while updating equipment: {str(e)}", "error")

    certs = get_all_certifications()
    return render_template("edit_equipment.html", equipment=equipment, all_certs=certs)

@equipment_bp.route("/delete/<int:id>", methods=["POST"])
@login_required
def delete_equipment_route(id):
    delete_equipment(id)
    flash("Equipment deleted.")
    return redirect(url_for("equipment.equipment_list"))


# =============================================================================
# CHECKLIST TEMPLATES
# =============================================================================

@equipment_bp.route('/maintenance/checklists')
@login_required
def maintenance_checklists():
    templates = list_checklist_templates()
    return render_template('maintenance_checklists.html', templates=templates)


@equipment_bp.route('/maintenance/checklist/new', methods=['GET', 'POST'])
@login_required
def maintenance_checklist_new():
    if request.method == 'GET':
        equipment_types = get_equipment_types()
        return render_template('maintenance_checklist_edit.html', template=None, equipment_types=equipment_types)

    name = request.form.get('name')
    equipment_type = request.form.get('equipment_type') or None
    description = request.form.get('description')
    template_id = create_checklist_template(name, equipment_type, description)
    flash(f'Checklist template "{name}" created.')
    return redirect(url_for('equipment.maintenance_checklist_edit', template_id=template_id))


@equipment_bp.route('/maintenance/checklist/<int:template_id>')
@login_required
def maintenance_checklist_view(template_id):
    template = get_checklist_template(template_id)
    if not template:
        flash('Template not found')
        return redirect(url_for('equipment.maintenance_checklists'))
    return render_template('maintenance_checklist_view.html', template=template)


@equipment_bp.route('/maintenance/checklist/<int:template_id>/edit', methods=['GET', 'POST'])
@login_required
def maintenance_checklist_edit(template_id):
    template = get_checklist_template(template_id)
    if not template:
        flash('Template not found')
        return redirect(url_for('equipment.maintenance_checklists'))

    if request.method == 'POST':
        action = request.form.get('action')
        if action == 'update_template':
            update_checklist_template(
                template_id,
                name=request.form.get('name'),
                equipment_type=request.form.get('equipment_type') or None,
                description=request.form.get('description'),
                is_active=request.form.get('is_active') == 'on'
            )
            flash('Template updated')
        elif action == 'add_item':
            add_checklist_template_item(
                template_id,
                label=request.form.get('label'),
                category=request.form.get('category') or None,
                default_status=request.form.get('default_status', 'ok'),
                requires_photo=request.form.get('requires_photo') == 'on',
                position=int(request.form.get('position', 0))
            )
            flash('Item added')
        elif action == 'delete_item':
            item_id = int(request.form.get('item_id'))
            delete_checklist_template_item(item_id)
            flash('Item deleted')
        return redirect(url_for('equipment.maintenance_checklist_edit', template_id=template_id))

    equipment_types = get_equipment_types()
    return render_template('maintenance_checklist_edit.html', template=template, equipment_types=equipment_types)


@equipment_bp.route('/maintenance/checklist/<int:template_id>/delete', methods=['POST'])
@login_required
def maintenance_checklist_delete(template_id):
    delete_checklist_template(template_id)
    flash('Checklist template deleted')
    return redirect(url_for('equipment.maintenance_checklists'))


# =============================================================================
# INSPECTIONS
# =============================================================================

@equipment_bp.route('/maintenance/inspections')
@login_required
def maintenance_inspections_list():
    status = request.args.get('status')
    inspections = list_inspections(status=status)
    return render_template('maintenance_inspections.html', inspections=inspections, filter_status=status)


@equipment_bp.route('/maintenance/inspection/new', methods=['GET', 'POST'])
@login_required
def maintenance_inspection_new():
    if request.method == 'GET':
        equipment_list = get_all_equipment()
        templates = list_checklist_templates()
        return render_template('maintenance_inspection_new.html', equipment=equipment_list, templates=templates)

    equipment_id = int(request.form.get('equipment_id'))
    template_id = request.form.get('template_id')
    template_id = int(template_id) if template_id else None
    inspection_type = request.form.get('inspection_type', 'routine')
    notes = request.form.get('notes')

    inspection_id = create_inspection(
        equipment_id=equipment_id,
        template_id=template_id,
        inspector_id=current_user.id,
        inspection_type=inspection_type,
        notes=notes
    )
    flash(f'Inspection #{inspection_id} started')
    return redirect(url_for('equipment.maintenance_inspection_perform', inspection_id=inspection_id))


@equipment_bp.route('/maintenance/inspection/<int:inspection_id>')
@login_required
def maintenance_inspection_view(inspection_id):
    inspection = get_inspection(inspection_id)
    if not inspection:
        flash('Inspection not found')
        return redirect(url_for('equipment.maintenance_inspections_list'))
    return render_template('maintenance_inspection_detail.html', inspection=inspection)


@equipment_bp.route('/maintenance/inspection/<int:inspection_id>/perform', methods=['GET', 'POST'])
@login_required
def maintenance_inspection_perform(inspection_id):
    inspection = get_inspection(inspection_id)
    if not inspection:
        flash('Inspection not found')
        return redirect(url_for('equipment.maintenance_inspections_list'))

    if request.method == 'POST':
        action = request.form.get('action')
        if action == 'update_item':
            item_id = int(request.form.get('item_id'))
            status = request.form.get('status')
            notes = request.form.get('notes')
            update_inspection_item(item_id, status=status, notes=notes)

            # Handle photo upload
            if 'photo' in request.files and request.files['photo'].filename:
                add_inspection_media(inspection_id, item_id, request.files['photo'], 'image')
            flash('Item updated')
        elif action == 'complete':
            signature_data = request.form.get('signature_data')
            signature_name = request.form.get('signature_name', current_user.username)
            notes = request.form.get('completion_notes')
            result = complete_inspection(inspection_id, notes=notes,
                                         signature_data=signature_data, signature_name=signature_name)
            flash(f'Inspection completed with status: {result}')
            return redirect(url_for('equipment.maintenance_inspection_view', inspection_id=inspection_id))

        return redirect(url_for('equipment.maintenance_inspection_perform', inspection_id=inspection_id))

    return render_template('maintenance_inspection_perform.html', inspection=inspection)


# =============================================================================
# WORK ORDER TEMPLATES
# =============================================================================

@equipment_bp.route('/maintenance/wo-templates')
@login_required
def maintenance_wo_templates():
    templates = list_work_order_templates()
    return render_template('maintenance_wo_templates.html', templates=templates)


@equipment_bp.route('/maintenance/wo-template/new', methods=['GET', 'POST'])
@login_required
def maintenance_wo_template_new():
    if request.method == 'GET':
        equipment_types = get_equipment_types()
        return render_template('maintenance_wo_template_edit.html', template=None, equipment_types=equipment_types)

    name = request.form.get('name')
    job_type = request.form.get('job_type')
    equipment_type = request.form.get('equipment_type') or None
    description = request.form.get('description')
    estimated_hours = request.form.get('estimated_hours')
    estimated_hours = float(estimated_hours) if estimated_hours else None
    safety_notes = request.form.get('safety_notes')

    template_id = create_work_order_template(name, job_type, equipment_type, description, estimated_hours, safety_notes)
    flash(f'Work order template "{name}" created.')
    return redirect(url_for('equipment.maintenance_wo_template_edit', template_id=template_id))


@equipment_bp.route('/maintenance/wo-template/<int:template_id>/edit', methods=['GET', 'POST'])
@login_required
def maintenance_wo_template_edit(template_id):
    template = get_work_order_template(template_id)
    if not template:
        flash('Template not found')
        return redirect(url_for('equipment.maintenance_wo_templates'))

    if request.method == 'POST':
        action = request.form.get('action')
        if action == 'add_step':
            step_number = int(request.form.get('step_number', len(template.get('steps', [])) + 1))
            instruction = request.form.get('instruction')
            estimated_minutes = request.form.get('estimated_minutes')
            estimated_minutes = int(estimated_minutes) if estimated_minutes else None
            requires_signoff = request.form.get('requires_signoff') == 'on'
            add_work_order_template_step(template_id, step_number, instruction, estimated_minutes, requires_signoff)
            flash('Step added')
        return redirect(url_for('equipment.maintenance_wo_template_edit', template_id=template_id))

    equipment_types = get_equipment_types()
    return render_template('maintenance_wo_template_edit.html', template=template, equipment_types=equipment_types)


@equipment_bp.route('/maintenance/job/from-template', methods=['GET', 'POST'])
@login_required
def maintenance_job_from_template():
    if request.method == 'GET':
        templates = list_work_order_templates()
        equipment_list = get_all_equipment()
        from services.users_service import get_all_users
        users = get_all_users()
        return render_template('maintenance_job_from_template.html',
                             templates=templates, equipment=equipment_list, users=users)

    template_id = int(request.form.get('template_id'))
    equipment_id = int(request.form.get('equipment_id'))
    due_date = request.form.get('due_date') or None
    assigned_to = request.form.get('assigned_to')
    assigned_to = int(assigned_to) if assigned_to else None
    priority = request.form.get('priority', 'normal')

    job_id = create_job_from_template(template_id, equipment_id, due_date, assigned_to, priority)
    flash(f'Job #{job_id} created from template')
    return redirect(url_for('equipment.maintenance_job_view', job_id=job_id))


# =============================================================================
# ENHANCED JOB VIEW WITH STEPS & SIGNATURES
# =============================================================================

@equipment_bp.route('/maintenance/job/<int:job_id>/steps', methods=['GET', 'POST'])
@login_required
def maintenance_job_steps(job_id):
    job = get_maintenance_job(job_id)
    if not job:
        flash('Job not found')
        return redirect(url_for('equipment.maintenance_landing'))

    if request.method == 'POST':
        action = request.form.get('action')
        if action == 'complete_step':
            step_id = int(request.form.get('step_id'))
            notes = request.form.get('notes')
            signature_data = request.form.get('signature_data')
            complete_job_step(step_id, status='completed', notes=notes, signature_data=signature_data)
            flash('Step completed')
        elif action == 'add_step':
            step_number = int(request.form.get('step_number'))
            instruction = request.form.get('instruction')
            add_job_step(job_id, step_number, instruction)
            flash('Step added')
        return redirect(url_for('equipment.maintenance_job_steps', job_id=job_id))

    steps = get_job_steps(job_id)
    equipment = get_equipment_by_id(job['equipment_id'])
    return render_template('maintenance_job_steps.html', job=job, steps=steps, equipment=equipment)


@equipment_bp.route('/maintenance/job/<int:job_id>/attachments', methods=['GET', 'POST'])
@login_required
def maintenance_job_attachments(job_id):
    job = get_maintenance_job(job_id)
    if not job:
        flash('Job not found')
        return redirect(url_for('equipment.maintenance_landing'))

    if request.method == 'POST':
        if 'file' in request.files and request.files['file'].filename:
            label = request.form.get('label')
            add_job_attachment(job_id, job['equipment_id'], request.files['file'],
                             label=label, uploader_id=current_user.id)
            flash('Attachment added')
        return redirect(url_for('equipment.maintenance_job_attachments', job_id=job_id))

    attachments = list_job_attachments(job_id)
    return render_template('maintenance_job_attachments.html', job=job, attachments=attachments)


@equipment_bp.route('/maintenance/job/<int:job_id>/sign', methods=['POST'])
@login_required
def maintenance_job_sign(job_id):
    signature_type = request.form.get('signature_type')
    signature_data = request.form.get('signature_data')
    signer_name = request.form.get('signer_name', current_user.username)
    notes = request.form.get('notes')
    add_job_signature(job_id, signature_type, signature_data, signer_name, current_user.id, notes)
    flash(f'{signature_type.title()} signature recorded')
    return redirect(url_for('equipment.maintenance_job_view', job_id=job_id))


# =============================================================================
# REPORTING & ANALYTICS
# =============================================================================

@equipment_bp.route('/maintenance/reports')
@login_required
def maintenance_reports():
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    kpis = get_maintenance_kpis(start_date, end_date)
    trends = get_maintenance_trends(12)
    health = get_equipment_health_summary()
    expiring_certs = get_expiring_certifications(30)
    return render_template('maintenance_reports.html',
                          kpis=kpis, trends=trends, health=health,
                          expiring_certs=expiring_certs,
                          start_date=start_date, end_date=end_date)


@equipment_bp.route('/maintenance/reports/export')
@login_required
def maintenance_reports_export():
    start_date = request.args.get('start_date', '')
    end_date = request.args.get('end_date', '')
    format_type = request.args.get('format', 'csv')

    data = export_maintenance_report(start_date, end_date, format_type)

    if format_type == 'csv':
        output = io.StringIO()
        writer = csv.writer(output)

        # KPIs section
        writer.writerow(['Maintenance Report'])
        writer.writerow(['Period', f'{start_date} to {end_date}'])
        writer.writerow([])
        writer.writerow(['Key Performance Indicators'])
        writer.writerow(['Total Jobs', data['kpis']['total_jobs']])
        writer.writerow(['Completed Jobs', data['kpis']['completed_jobs']])
        writer.writerow(['Completion Rate', f"{data['kpis']['completion_rate']}%"])
        writer.writerow(['Planned Maintenance %', f"{data['kpis']['planned_maintenance_pct']}%"])
        writer.writerow(['Avg Completion Hours', data['kpis']['avg_completion_hours']])
        writer.writerow(['Overdue Jobs', data['kpis']['overdue_count']])
        writer.writerow(['Total Labor Hours', data['kpis']['total_labor_hours']])
        writer.writerow(['Total Parts Cost', f"${data['kpis']['total_parts_cost']}"])
        writer.writerow([])

        # Equipment Health
        writer.writerow(['Equipment Health Summary'])
        writer.writerow(['Equipment ID', 'Brand', 'Model', 'Status', 'Health Score', 'Recent Breakdowns'])
        for eq in data['equipment_health'][:20]:
            writer.writerow([eq['unique_id'], eq['brand'], eq['model'],
                           eq['status'], eq['health_score'], eq['recent_breakdowns']])

        output.seek(0)
        return Response(
            output.getvalue(),
            mimetype='text/csv',
            headers={'Content-Disposition': f'attachment;filename=maintenance_report_{datetime.now().strftime("%Y%m%d")}.csv'}
        )

    return jsonify(data)


@equipment_bp.route('/maintenance/reports/api/kpis')
@login_required
def maintenance_api_kpis():
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    return jsonify(get_maintenance_kpis(start_date, end_date))


@equipment_bp.route('/maintenance/reports/api/trends')
@login_required
def maintenance_api_trends():
    months = int(request.args.get('months', 12))
    return jsonify(get_maintenance_trends(months))


@equipment_bp.route('/maintenance/reports/api/health')
@login_required
def maintenance_api_health():
    return jsonify(get_equipment_health_summary())


# =============================================================================
# VENDORS & CONTRACTORS
# =============================================================================

@equipment_bp.route('/maintenance/vendors')
@login_required
def maintenance_vendors():
    vendor_type = request.args.get('type')
    vendors = list_vendors(vendor_type)
    return render_template('maintenance_vendors.html', vendors=vendors, filter_type=vendor_type)


@equipment_bp.route('/maintenance/vendor/new', methods=['GET', 'POST'])
@login_required
def maintenance_vendor_new():
    if request.method == 'GET':
        return render_template('maintenance_vendor_edit.html', vendor=None)

    vendor_id = create_vendor(
        name=request.form.get('name'),
        vendor_type=request.form.get('vendor_type'),
        contact_name=request.form.get('contact_name'),
        contact_email=request.form.get('contact_email'),
        contact_phone=request.form.get('contact_phone'),
        address=request.form.get('address'),
        specialties=request.form.get('specialties'),
        notes=request.form.get('notes')
    )
    flash('Vendor created')
    return redirect(url_for('equipment.maintenance_vendor_view', vendor_id=vendor_id))


@equipment_bp.route('/maintenance/vendor/<int:vendor_id>')
@login_required
def maintenance_vendor_view(vendor_id):
    vendor = get_vendor(vendor_id)
    if not vendor:
        flash('Vendor not found')
        return redirect(url_for('equipment.maintenance_vendors'))
    return render_template('maintenance_vendor_detail.html', vendor=vendor)


@equipment_bp.route('/maintenance/vendor/<int:vendor_id>/edit', methods=['GET', 'POST'])
@login_required
def maintenance_vendor_edit(vendor_id):
    vendor = get_vendor(vendor_id)
    if not vendor:
        flash('Vendor not found')
        return redirect(url_for('equipment.maintenance_vendors'))

    if request.method == 'POST':
        update_vendor(vendor_id,
            name=request.form.get('name'),
            vendor_type=request.form.get('vendor_type'),
            contact_name=request.form.get('contact_name'),
            contact_email=request.form.get('contact_email'),
            contact_phone=request.form.get('contact_phone'),
            address=request.form.get('address'),
            specialties=request.form.get('specialties'),
            notes=request.form.get('notes'),
            insurance_expiry=request.form.get('insurance_expiry') or None,
            contract_expiry=request.form.get('contract_expiry') or None
        )
        flash('Vendor updated')
        return redirect(url_for('equipment.maintenance_vendor_view', vendor_id=vendor_id))

    return render_template('maintenance_vendor_edit.html', vendor=vendor)


@equipment_bp.route('/maintenance/job/<int:job_id>/assign-vendor', methods=['POST'])
@login_required
def maintenance_job_assign_vendor(job_id):
    vendor_id = int(request.form.get('vendor_id'))
    estimated_cost = request.form.get('estimated_cost')
    estimated_cost = float(estimated_cost) if estimated_cost else None
    notes = request.form.get('notes')
    assign_job_to_vendor(job_id, vendor_id, estimated_cost, notes)
    flash('Job assigned to vendor')
    return redirect(url_for('equipment.maintenance_job_view', job_id=job_id))


# =============================================================================
# COMPLIANCE - CERTIFICATIONS & WARRANTIES
# =============================================================================

@equipment_bp.route('/maintenance/compliance')
@login_required
def maintenance_compliance():
    certifications = list_equipment_certifications()
    warranties = list_warranty_records()
    expiring = get_expiring_certifications(30)
    return render_template('maintenance_compliance.html',
                          certifications=certifications, warranties=warranties, expiring=expiring)


@equipment_bp.route('/maintenance/compliance/certification/add', methods=['POST'])
@login_required
def maintenance_add_certification():
    equipment_id = int(request.form.get('equipment_id'))
    add_equipment_certification(
        equipment_id=equipment_id,
        certification_type=request.form.get('certification_type'),
        certification_number=request.form.get('certification_number'),
        issued_date=request.form.get('issued_date') or None,
        expiry_date=request.form.get('expiry_date') or None,
        notes=request.form.get('notes')
    )
    flash('Certification added')
    return redirect(url_for('equipment.maintenance_compliance'))


@equipment_bp.route('/maintenance/compliance/warranty/add', methods=['POST'])
@login_required
def maintenance_add_warranty():
    equipment_id = int(request.form.get('equipment_id'))
    add_warranty_record(
        equipment_id=equipment_id,
        warranty_type=request.form.get('warranty_type'),
        provider=request.form.get('provider'),
        start_date=request.form.get('start_date'),
        end_date=request.form.get('end_date'),
        coverage_details=request.form.get('coverage_details'),
        notes=request.form.get('notes')
    )
    flash('Warranty record added')
    return redirect(url_for('equipment.maintenance_compliance'))


# =============================================================================
# CITIZEN/311 REQUEST PORTAL
# =============================================================================

@equipment_bp.route('/maintenance/citizen-requests')
@login_required
def maintenance_citizen_requests():
    status = request.args.get('status')
    request_type = request.args.get('type')
    requests = list_citizen_requests(status=status, request_type=request_type)
    return render_template('maintenance_citizen_requests.html', requests=requests,
                          filter_status=status, filter_type=request_type)


@equipment_bp.route('/maintenance/citizen-request/<int:request_id>')
@login_required
def maintenance_citizen_request_view(request_id):
    req = get_citizen_request(request_id)
    if not req:
        flash('Request not found')
        return redirect(url_for('equipment.maintenance_citizen_requests'))
    return render_template('maintenance_citizen_request_detail.html', request=req)


@equipment_bp.route('/maintenance/citizen-request/<int:request_id>/convert', methods=['POST'])
@login_required
def maintenance_citizen_request_convert(request_id):
    equipment_id = request.form.get('equipment_id')
    equipment_id = int(equipment_id) if equipment_id else None
    priority = request.form.get('priority', 'normal')
    assigned_to = request.form.get('assigned_to')
    assigned_to = int(assigned_to) if assigned_to else None

    job_id = convert_request_to_job(request_id, equipment_id, priority, assigned_to)
    flash(f'Request converted to Job #{job_id}')
    return redirect(url_for('equipment.maintenance_job_view', job_id=job_id))


@equipment_bp.route('/maintenance/citizen-request/<int:request_id>/status', methods=['POST'])
@login_required
def maintenance_citizen_request_status(request_id):
    status = request.form.get('status')
    resolution_notes = request.form.get('resolution_notes')
    update_citizen_request_status(request_id, status, resolution_notes)
    flash('Request status updated')
    return redirect(url_for('equipment.maintenance_citizen_request_view', request_id=request_id))


# Public portal for citizen submissions (no login required)
@equipment_bp.route('/public/report-issue', methods=['GET', 'POST'])
def public_report_issue():
    if request.method == 'GET':
        return render_template('public_report_issue.html')

    request_type = request.form.get('request_type')
    description = request.form.get('description')
    location = request.form.get('location')
    latitude = request.form.get('latitude')
    longitude = request.form.get('longitude')
    reporter_name = request.form.get('reporter_name')
    reporter_email = request.form.get('reporter_email')
    reporter_phone = request.form.get('reporter_phone')

    # Handle photo upload
    photo_path = None
    if 'photo' in request.files and request.files['photo'].filename:
        from werkzeug.utils import secure_filename
        import os
        file = request.files['photo']
        filename = secure_filename(file.filename)
        upload_dir = 'Uploads/citizen_requests'
        os.makedirs(upload_dir, exist_ok=True)
        photo_path = os.path.join(upload_dir, f"{datetime.now().strftime('%Y%m%d%H%M%S')}_{filename}")
        file.save(photo_path)

    request_id = create_citizen_request(
        request_type=request_type,
        description=description,
        location=location,
        latitude=float(latitude) if latitude else None,
        longitude=float(longitude) if longitude else None,
        reporter_name=reporter_name,
        reporter_email=reporter_email,
        reporter_phone=reporter_phone,
        photo_path=photo_path
    )

    flash(f'Your request has been submitted. Reference number: #{request_id}')
    return render_template('public_report_issue_success.html', request_id=request_id)


# =============================================================================
# TELEMATICS & FLEET MAP
# =============================================================================

@equipment_bp.route('/maintenance/fleet-map')
@login_required
def maintenance_fleet_map():
    locations = get_fleet_locations()
    return render_template('maintenance_fleet_map.html', locations=locations)


@equipment_bp.route('/maintenance/telematics/<int:equipment_id>')
@login_required
def maintenance_telematics(equipment_id):
    equipment = get_equipment_by_id(equipment_id)
    if not equipment:
        flash('Equipment not found')
        return redirect(url_for('equipment.equipment_list'))

    hours = int(request.args.get('hours', 24))
    data_type = request.args.get('type')
    history = get_telematics_history(equipment_id, data_type=data_type, hours=hours)
    prediction = predict_maintenance_enhanced(equipment_id)

    return render_template('maintenance_telematics.html',
                          equipment=equipment, history=history, prediction=prediction)


@equipment_bp.route('/maintenance/api/telematics', methods=['POST'])
def maintenance_api_telematics_receive():
    """API endpoint for receiving telematics data from IoT devices."""
    data = request.get_json()
    if not data:
        return jsonify({'error': 'No data provided'}), 400

    equipment_id = data.get('equipment_id')
    data_type = data.get('data_type')
    value = data.get('value')

    if not all([equipment_id, data_type, value is not None]):
        return jsonify({'error': 'Missing required fields'}), 400

    record_id = record_telematics_data(
        equipment_id=int(equipment_id),
        data_type=data_type,
        value=float(value),
        unit=data.get('unit'),
        source=data.get('source'),
        raw_data=data.get('raw_data')
    )

    return jsonify({'success': True, 'record_id': record_id})


@equipment_bp.route('/maintenance/api/fleet-locations')
@login_required
def maintenance_api_fleet_locations():
    return jsonify(get_fleet_locations())


@equipment_bp.route('/maintenance/api/sync-providers')
@login_required
def maintenance_api_sync_providers():
    """Sync all active telematics providers."""
    from services.telematics_providers import sync_all_providers

    try:
        results = sync_all_providers()
        total_synced = sum(results.values())
        return jsonify({
            'success': True,
            'results': results,
            'total_synced': total_synced
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# =============================================================================
# GPS / TELEMATICS PROVIDER CONFIGURATION
# =============================================================================

@equipment_bp.route('/maintenance/telematics/providers')
@login_required
def maintenance_telematics_providers():
    """List and manage telematics/GPS providers."""
    from services.telematics_providers import get_telematics_providers
    providers = get_telematics_providers(active_only=False)
    return render_template('maintenance_telematics_providers.html', providers=providers)


@equipment_bp.route('/maintenance/telematics/providers/new', methods=['GET', 'POST'])
@login_required
def maintenance_telematics_provider_new():
    """Add a new telematics provider."""
    from services.telematics_providers import save_telematics_provider, get_telematics_provider

    if request.method == 'POST':
        name = request.form.get('name')
        provider_type = request.form.get('provider_type')
        is_active = request.form.get('is_active') == 'on'

        # Build config based on provider type
        config = {}
        if provider_type == 'samsara':
            config = {
                'api_key': request.form.get('api_key'),
                'base_url': request.form.get('base_url', 'https://api.samsara.com')
            }
        elif provider_type == 'geotab':
            config = {
                'username': request.form.get('username'),
                'password': request.form.get('password'),
                'database': request.form.get('database'),
                'base_url': request.form.get('base_url', 'https://my.geotab.com/apiv1')
            }
        elif provider_type == 'verizon_connect':
            config = {
                'api_key': request.form.get('api_key'),
                'account_id': request.form.get('account_id'),
                'base_url': request.form.get('base_url', 'https://fim.api.verizonconnect.com/api')
            }
        elif provider_type == 'generic':
            config = {
                'api_key': request.form.get('api_key'),
                'base_url': request.form.get('base_url'),
                'auth_type': request.form.get('auth_type', 'bearer'),
                'auth_header': request.form.get('auth_header', 'Authorization'),
                'vehicles_endpoint': request.form.get('vehicles_endpoint', '/vehicles'),
                'locations_endpoint': request.form.get('locations_endpoint', '/locations')
            }

        try:
            # Test connection before saving
            provider_instance = get_telematics_provider(provider_type, config)
            if provider_instance and provider_instance.authenticate():
                provider_id = save_telematics_provider(name, provider_type, config, is_active)
                flash(f'Provider "{name}" added and connection verified!', 'success')
                return redirect(url_for('equipment.maintenance_telematics_providers'))
            else:
                flash('Could not connect to provider. Please check your credentials.', 'error')
        except Exception as e:
            flash(f'Error adding provider: {str(e)}', 'error')

    return render_template('maintenance_telematics_provider_form.html', provider=None)


@equipment_bp.route('/maintenance/telematics/providers/<int:provider_id>/edit', methods=['GET', 'POST'])
@login_required
def maintenance_telematics_provider_edit(provider_id):
    """Edit an existing telematics provider."""
    from services.telematics_providers import get_telematics_providers, save_telematics_provider, get_telematics_provider

    # Get existing provider
    providers = get_telematics_providers(active_only=False)
    provider = next((p for p in providers if p['id'] == provider_id), None)
    if not provider:
        flash('Provider not found', 'error')
        return redirect(url_for('equipment.maintenance_telematics_providers'))

    if request.method == 'POST':
        name = request.form.get('name')
        provider_type = request.form.get('provider_type')
        is_active = request.form.get('is_active') == 'on'

        # Build config
        config = {}
        if provider_type == 'samsara':
            config = {
                'api_key': request.form.get('api_key'),
                'base_url': request.form.get('base_url', 'https://api.samsara.com')
            }
        elif provider_type == 'geotab':
            config = {
                'username': request.form.get('username'),
                'password': request.form.get('password'),
                'database': request.form.get('database'),
                'base_url': request.form.get('base_url', 'https://my.geotab.com/apiv1')
            }
        elif provider_type == 'verizon_connect':
            config = {
                'api_key': request.form.get('api_key'),
                'account_id': request.form.get('account_id'),
                'base_url': request.form.get('base_url', 'https://fim.api.verizonconnect.com/api')
            }
        elif provider_type == 'generic':
            config = {
                'api_key': request.form.get('api_key'),
                'base_url': request.form.get('base_url'),
                'auth_type': request.form.get('auth_type', 'bearer'),
                'auth_header': request.form.get('auth_header', 'Authorization'),
                'vehicles_endpoint': request.form.get('vehicles_endpoint', '/vehicles'),
                'locations_endpoint': request.form.get('locations_endpoint', '/locations')
            }

        try:
            save_telematics_provider(name, provider_type, config, is_active)
            flash(f'Provider "{name}" updated!', 'success')
            return redirect(url_for('equipment.maintenance_telematics_providers'))
        except Exception as e:
            flash(f'Error updating provider: {str(e)}', 'error')

    return render_template('maintenance_telematics_provider_form.html', provider=provider)


@equipment_bp.route('/maintenance/telematics/providers/<int:provider_id>/test')
@login_required
def maintenance_telematics_provider_test(provider_id):
    """Test connection to a telematics provider."""
    from services.telematics_providers import get_telematics_providers, get_telematics_provider

    providers = get_telematics_providers(active_only=False)
    provider = next((p for p in providers if p['id'] == provider_id), None)
    if not provider:
        return jsonify({'success': False, 'error': 'Provider not found'})

    try:
        instance = get_telematics_provider(provider['provider_type'], provider['config'])
        if instance and instance.authenticate():
            vehicles = instance.get_vehicles()
            return jsonify({
                'success': True,
                'message': f'Connected successfully! Found {len(vehicles)} vehicles.'
            })
        return jsonify({'success': False, 'error': 'Authentication failed'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


@equipment_bp.route('/maintenance/telematics/providers/<int:provider_id>/sync', methods=['POST'])
@login_required
def maintenance_telematics_provider_sync(provider_id):
    """Manually trigger a sync for a provider."""
    from services.telematics_providers import sync_provider_locations

    try:
        count = sync_provider_locations(provider_id)
        flash(f'Synced {count} equipment locations from provider', 'success')
    except Exception as e:
        flash(f'Sync error: {str(e)}', 'error')

    return redirect(url_for('equipment.maintenance_telematics_providers'))


@equipment_bp.route('/maintenance/telematics/providers/<int:provider_id>/vehicles')
@login_required
def maintenance_telematics_provider_vehicles(provider_id):
    """Get vehicles from a provider (for AJAX linking)."""
    from services.telematics_providers import get_telematics_providers, get_telematics_provider

    providers = get_telematics_providers(active_only=False)
    provider = next((p for p in providers if p['id'] == provider_id), None)
    if not provider:
        return jsonify({'error': 'Provider not found'}), 404

    try:
        instance = get_telematics_provider(provider['provider_type'], provider['config'])
        if instance:
            vehicles = instance.get_vehicles()
            return jsonify({'vehicles': vehicles})
        return jsonify({'error': 'Could not connect to provider'}), 500
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@equipment_bp.route('/maintenance/telematics/providers/<int:provider_id>/link', methods=['GET', 'POST'])
@login_required
def maintenance_telematics_provider_link(provider_id):
    """Link equipment to provider vehicles."""
    from services.telematics_providers import get_telematics_providers, get_telematics_provider, link_equipment_to_provider

    providers = get_telematics_providers(active_only=False)
    provider = next((p for p in providers if p['id'] == provider_id), None)
    if not provider:
        flash('Provider not found', 'error')
        return redirect(url_for('equipment.maintenance_telematics_providers'))

    # Get local equipment
    conn = get_connection()
    c = conn.cursor()
    c.execute("""
        SELECT ei.id, ei.unique_id, ei.equipment_type, etl.external_id
        FROM equipment_instances ei
        LEFT JOIN equipment_telematics_links etl ON ei.id = etl.equipment_id AND etl.provider_id = %s
        ORDER BY ei.equipment_type, ei.unique_id
    """, (provider_id,))
    equipment = [{'id': r[0], 'unique_id': r[1], 'type': r[2], 'linked_to': r[3]} for r in c.fetchall()]
    release_connection(conn)

    # Get provider vehicles
    try:
        instance = get_telematics_provider(provider['provider_type'], provider['config'])
        vehicles = instance.get_vehicles() if instance else []
    except:
        vehicles = []

    if request.method == 'POST':
        # Process link submissions
        links_made = 0
        for eq in equipment:
            external_id = request.form.get(f'link_{eq["id"]}')
            if external_id:
                if link_equipment_to_provider(eq['id'], provider_id, external_id):
                    links_made += 1

        flash(f'Updated {links_made} equipment links', 'success')
        return redirect(url_for('equipment.maintenance_telematics_providers'))

    return render_template('maintenance_telematics_link.html',
                          provider=provider, equipment=equipment, vehicles=vehicles)


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def get_equipment_types():
    """Get distinct equipment types from database."""
    conn = get_connection()
    c = conn.cursor()
    c.execute("SELECT DISTINCT equipment_type FROM equipment_instances WHERE equipment_type IS NOT NULL ORDER BY equipment_type")
    types = [r[0] for r in c.fetchall()]
    release_connection(conn)
    return types


# Time logging route (referenced in maintenance_detail_job.html)
@equipment_bp.route('/maintenance/job/<int:job_id>/log-time', methods=['POST'])
@login_required
def maintenance_job_log_time(job_id):
    work_date = request.form.get('work_date')
    hours = float(request.form.get('hours', 0))
    notes = request.form.get('notes')

    conn = get_connection()
    c = conn.cursor()
    c.execute("""
        INSERT INTO maintenance_time_logs (job_id, user_id, hours, work_date, notes)
        VALUES (%s, %s, %s, %s, %s)
    """, (job_id, current_user.id, hours, work_date, notes))
    conn.commit()
    release_connection(conn)

    flash('Time logged')
    return redirect(url_for('equipment.maintenance_job_view', job_id=job_id))