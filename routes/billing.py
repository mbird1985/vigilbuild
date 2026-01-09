from flask import Blueprint, render_template, request, jsonify, flash, redirect, url_for
from services.db import get_connection, release_connection
from services.logging_service import log_audit
from services.elasticsearch_client import es
from flask_login import login_required, current_user
import openpyxl

billing_bp = Blueprint('billing', __name__, url_prefix='/billing')

@billing_bp.route('/job_codes')
@login_required
def job_codes():
    conn = get_connection()
    c = conn.cursor()
    c.execute('SELECT id, service_unit, core_code, category FROM service_units ORDER BY category, service_unit')
    codes = [dict(zip(['id', 'service_unit', 'core_code', 'category'], r)) for r in c.fetchall()]
    release_connection(conn)
    return render_template('billing/job_codes.html', codes=codes)

@billing_bp.route('/job_codes/create', methods=['GET', 'POST'])
@login_required
def create_job_code():
    if request.method == 'POST':
        if current_user.role not in ['admin', 'manager']:
            flash('Permission denied.', 'error')
            return redirect(url_for('billing.job_codes'))
        
        data = request.form
        conn = get_connection()
        c = conn.cursor()
        try:
            c.execute('''
                INSERT INTO service_units (
                    source_file, service_unit, core_code, service_master, unit_of_measure, 
                    definition, clarification, sap_service_master_, task_code_description, 
                    unnamed_2, description, uom, explanation, om_contract_service_units, 
                    core_service_master, non_core_code, non_core_service_master, work_type, 
                    definitions, category
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
            ''', (
                data.get('source_file', ''), data.get('service_unit', ''), data.get('core_code', ''), 
                data.get('service_master', ''), data.get('unit_of_measure', ''), data.get('definition', ''), 
                data.get('clarification', ''), data.get('sap_service_master_', ''), data.get('task_code_description', ''), 
                data.get('unnamed_2', ''), data.get('description', ''), data.get('uom', ''), 
                data.get('explanation', ''), data.get('om_contract_service_units', ''), data.get('core_service_master', ''), 
                data.get('non_core_code', ''), data.get('non_core_service_master', ''), data.get('work_type', ''), 
                data.get('definitions', ''), data.get('category', '')
            ))
            conn.commit()
            flash('Job code created successfully.', 'success')
            return redirect(url_for('billing.job_codes'))
        except Exception as e:
            conn.rollback()
            flash(f'Error creating job code: {str(e)}', 'error')
        finally:
            release_connection(conn)
    
    return render_template('billing/create_job_code.html')

@billing_bp.route('/job_codes/<int:id>')
@login_required
def view_job_code(id):
    conn = get_connection()
    c = conn.cursor()
    c.execute('''
        SELECT id, source_file, service_unit, core_code, service_master, unit_of_measure, 
               definition, clarification, sap_service_master_, task_code_description, 
               unnamed_2, description, uom, explanation, om_contract_service_units, 
               core_service_master, non_core_code, non_core_service_master, work_type, 
               definitions, category, created_at
        FROM service_units WHERE id = %s
    ''', (id,))
    result = c.fetchone()
    if not result:
        flash('Job code not found.', 'error')
        release_connection(conn)
        return redirect(url_for('billing.job_codes'))
    
    columns = ['id', 'source_file', 'service_unit', 'core_code', 'service_master', 'unit_of_measure', 
               'definition', 'clarification', 'sap_service_master_', 'task_code_description', 
               'unnamed_2', 'description', 'uom', 'explanation', 'om_contract_service_units', 
               'core_service_master', 'non_core_code', 'non_core_service_master', 'work_type', 
               'definitions', 'category', 'created_at']
    job_code = dict(zip(columns, result))
    release_connection(conn)
    return render_template('billing/view_job_code.html', job_code=job_code)

@billing_bp.route('/job_codes/<int:id>/edit', methods=['GET', 'POST'])
@login_required
def edit_job_code(id):
    if current_user.role not in ['admin', 'manager']:
        flash('Permission denied.', 'error')
        return redirect(url_for('billing.view_job_code', id=id))
    
    conn = get_connection()
    c = conn.cursor()
    
    if request.method == 'POST':
        data = request.form
        try:
            c.execute('''
                UPDATE service_units SET 
                    source_file=%s, service_unit=%s, core_code=%s, service_master=%s, unit_of_measure=%s, 
                    definition=%s, clarification=%s, sap_service_master_=%s, task_code_description=%s, 
                    unnamed_2=%s, description=%s, uom=%s, explanation=%s, om_contract_service_units=%s, 
                    core_service_master=%s, non_core_code=%s, non_core_service_master=%s, work_type=%s, 
                    definitions=%s, category=%s
                WHERE id = %s
            ''', (
                data.get('source_file', ''), data.get('service_unit', ''), data.get('core_code', ''), 
                data.get('service_master', ''), data.get('unit_of_measure', ''), data.get('definition', ''), 
                data.get('clarification', ''), data.get('sap_service_master_', ''), data.get('task_code_description', ''), 
                data.get('unnamed_2', ''), data.get('description', ''), data.get('uom', ''), 
                data.get('explanation', ''), data.get('om_contract_service_units', ''), data.get('core_service_master', ''), 
                data.get('non_core_code', ''), data.get('non_core_service_master', ''), data.get('work_type', ''), 
                data.get('definitions', ''), data.get('category', ''), id
            ))
            conn.commit()
            flash('Job code updated successfully.', 'success')
            return redirect(url_for('billing.view_job_code', id=id))
        except Exception as e:
            conn.rollback()
            flash(f'Error updating job code: {str(e)}', 'error')
        finally:
            release_connection(conn)
    
    # GET: Load job code data
    c.execute('''
        SELECT id, source_file, service_unit, core_code, service_master, unit_of_measure, 
               definition, clarification, sap_service_master_, task_code_description, 
               unnamed_2, description, uom, explanation, om_contract_service_units, 
               core_service_master, non_core_code, non_core_service_master, work_type, 
               definitions, category
        FROM service_units WHERE id = %s
    ''', (id,))
    result = c.fetchone()
    if not result:
        flash('Job code not found.', 'error')
        release_connection(conn)
        return redirect(url_for('billing.job_codes'))
    
    columns = ['id', 'source_file', 'service_unit', 'core_code', 'service_master', 'unit_of_measure', 
               'definition', 'clarification', 'sap_service_master_', 'task_code_description', 
               'unnamed_2', 'description', 'uom', 'explanation', 'om_contract_service_units', 
               'core_service_master', 'non_core_code', 'non_core_service_master', 'work_type', 
               'definitions', 'category']
    job_code = dict(zip(columns, result))
    release_connection(conn)
    return render_template('billing/edit_job_code.html', job_code=job_code)

@billing_bp.route('/job_codes/<int:id>/delete', methods=['POST'])
@login_required
def delete_job_code(id):
    if current_user.role not in ['admin', 'manager']:
        flash('Permission denied.', 'error')
        return redirect(url_for('billing.view_job_code', id=id))
    
    conn = get_connection()
    c = conn.cursor()
    try:
        c.execute('DELETE FROM service_units WHERE id = %s', (id,))
        conn.commit()
        flash('Job code deleted successfully.', 'success')
    except Exception as e:
        conn.rollback()
        flash(f'Error deleting job code: {str(e)}', 'error')
    finally:
        release_connection(conn)
    return redirect(url_for('billing.job_codes'))

@billing_bp.route('/invoices')
@login_required
def invoices():
    conn = get_connection()
    c = conn.cursor()
    c.execute('SELECT id, invoice_number, title, status, total, created_at FROM invoices ORDER BY created_at DESC')
    invs = [dict(zip(['id', 'invoice_number', 'title', 'status', 'total', 'created_at'], r)) for r in c.fetchall()]
    release_connection(conn)
    return render_template('billing/invoices.html', invoices=invs)

@billing_bp.route('/invoices/create', methods=['GET', 'POST'])
@login_required
def create_invoice():
    if request.method == 'POST':
        data = request.form
        job_id = data.get('job_id')
        # Gather lines, etc.
        # ...
        flash('Invoice created.', 'success')
        return redirect(url_for('billing.invoices'))
    
    # GET: Show form with job select, service unit picker, etc.
    conn = get_connection()
    c = conn.cursor()
    c.execute('SELECT id, name FROM jobs ORDER BY name')
    jobs = [dict(zip(['id', 'name'], r)) for r in c.fetchall()]
    c.execute('SELECT id, service_unit FROM service_units ORDER BY service_unit')
    units = [dict(zip(['id', 'service_unit'], r)) for r in c.fetchall()]
    release_connection(conn)
    return render_template('billing/create_invoice.html', jobs=jobs, service_units=units)
