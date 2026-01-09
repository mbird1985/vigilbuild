# routes/users.py
from flask import Blueprint, render_template, redirect, url_for, flash, request
from flask_login import login_required, current_user
from services.users_service import get_all_users, get_user_by_id, add_user, update_user, delete_user, get_all_certifications, resolve_certifications
from services.logging_service import log_audit
from services.elasticsearch_client import es
from services.db import get_connection, release_connection
from services.validation_service import (
    validate_email, validate_username, validate_phone,
    sanitize_string, sanitize_int, validate_required, validate_form_data
)
import json
from werkzeug.security import generate_password_hash

# Allowed roles for validation
ALLOWED_ROLES = ['admin', 'manager', 'user', 'viewer', 'technician', 'operator']

users_bp = Blueprint("users", __name__, url_prefix="/users")

@users_bp.route("/")
@login_required
def user_list():
    users = get_all_users()
    log_audit(es, 'view_users', current_user.id, None, {'user_count': len(users)})
    return render_template("users.html", users=users)

@users_bp.route("/add", methods=["GET", "POST"])
@login_required
def add_user_route():
    if current_user.role not in ['admin', 'manager']:
        flash("Permission denied")
        return redirect(url_for("users.user_list"))
    if request.method == "POST":
        # Validate and sanitize inputs
        valid, username = validate_username(request.form.get("username", ""))
        if not valid:
            flash(username)  # username contains error message
            certs = get_all_certifications()
            return render_template("add_user.html", all_certs=certs)

        full_name = sanitize_string(request.form.get("full_name", ""), max_length=100)
        if not full_name:
            flash("Full name is required")
            certs = get_all_certifications()
            return render_template("add_user.html", all_certs=certs)

        valid, email = validate_email(request.form.get("email", ""))
        if not valid:
            flash(email)  # email contains error message
            certs = get_all_certifications()
            return render_template("add_user.html", all_certs=certs)

        job_title = sanitize_string(request.form.get("job_title", ""), max_length=100)
        role = sanitize_string(request.form.get("role", ""), max_length=50)
        if role and role not in ALLOWED_ROLES:
            flash(f"Invalid role. Allowed roles: {', '.join(ALLOWED_ROLES)}")
            certs = get_all_certifications()
            return render_template("add_user.html", all_certs=certs)

        password = request.form.get("password", "")
        if len(password) < 6:
            flash("Password must be at least 6 characters")
            certs = get_all_certifications()
            return render_template("add_user.html", all_certs=certs)

        certifications = request.form.getlist("certifications")
        cert_ids = resolve_certifications(certifications)

        try:
            add_user(username, full_name, email, job_title, role, password, cert_ids, current_user.id)
            flash("User added successfully")
            return redirect(url_for("users.user_list"))
        except Exception as e:
            flash(f"Error adding user: {str(e)}")
    certs = get_all_certifications()
    return render_template("add_user.html", all_certs=certs)

@users_bp.route("/edit/<int:id>", methods=["GET", "POST"])
@login_required
def edit_user(id):
    # Validate ID
    id = sanitize_int(id, min_val=1)
    if id <= 0:
        flash("Invalid user ID")
        return redirect(url_for("users.user_list"))

    if current_user.role not in ['admin', 'manager']:
        flash("Permission denied")
        return redirect(url_for("users.user_list"))
    user = get_user_by_id(id)
    if not user:
        flash("User not found")
        return redirect(url_for("users.user_list"))
    if request.method == "POST":
        # Validate and sanitize inputs
        valid, username = validate_username(request.form.get("username", ""))
        if not valid:
            flash(username)
            certs = get_all_certifications()
            return render_template("edit_user.html", user=user, all_certs=certs)

        full_name = sanitize_string(request.form.get("full_name", ""), max_length=100)
        if not full_name:
            flash("Full name is required")
            certs = get_all_certifications()
            return render_template("edit_user.html", user=user, all_certs=certs)

        valid, email = validate_email(request.form.get("email", ""))
        if not valid:
            flash(email)
            certs = get_all_certifications()
            return render_template("edit_user.html", user=user, all_certs=certs)

        job_title = sanitize_string(request.form.get("job_title", ""), max_length=100)
        role = sanitize_string(request.form.get("role", ""), max_length=50)
        if role and role not in ALLOWED_ROLES:
            flash(f"Invalid role. Allowed roles: {', '.join(ALLOWED_ROLES)}")
            certs = get_all_certifications()
            return render_template("edit_user.html", user=user, all_certs=certs)

        certifications = request.form.getlist("certifications")
        cert_ids = resolve_certifications(certifications)
        try:
            update_user(id, username, full_name, email, job_title, role, cert_ids, current_user.id)
            flash("User updated successfully")
            return redirect(url_for("users.user_list"))
        except Exception as e:
            flash(f"Error updating user: {str(e)}")
    certs = get_all_certifications()
    user_certs = get_user_certifications(id)
    user_cert_names = [cert['name'] for cert in user_certs]
    return render_template("edit_user.html", user=user, all_certs=certs, user_cert_names=user_cert_names)

@users_bp.route("/delete/<int:id>", methods=["POST"])
@login_required
def delete_user_route(id):
    if current_user.role not in ['admin', 'manager']:
        flash("Permission denied")
        return redirect(url_for("users.user_list"))
    try:
        delete_user(id)
        flash("User deleted successfully")
        return redirect(url_for("users.user_list"))
    except Exception as e:
        flash(f"Error deleting user: {str(e)}")
        return redirect(url_for("users.user_list"))

@users_bp.route('/roles')
@login_required
def roles():
    if current_user.role != 'admin':
        flash('Permission denied.', 'error')
        return redirect(url_for('system.dashboard'))
    
    conn = get_connection()
    c = conn.cursor()
    
    # Get all roles
    c.execute('SELECT id, name, description FROM roles ORDER BY name')
    roles = [{'id': r[0], 'name': r[1], 'description': r[2]} for r in c.fetchall()]
    
    # Get all users with their roles (unique, no repetition)
    c.execute('''
        SELECT u.id, u.full_name, u.job_title,
               array_remove(array_agg(DISTINCT r.name), NULL) AS roles
        FROM users u
        LEFT JOIN user_roles ur ON ur.user_id = u.id
        LEFT JOIN roles r ON r.id = ur.role_id
        GROUP BY u.id, u.full_name, u.job_title
        ORDER BY u.full_name
    ''')
    users = [{'id': r[0], 'full_name': r[1], 'job_title': r[2], 'roles': r[3] or []} for r in c.fetchall()]
    
    # Get permissions for each role
    permissions = {}
    for role in roles:
        c.execute('SELECT permission FROM permissions WHERE role_id = %s', (role['id'],))
        permissions[role['id']] = [p[0] for p in c.fetchall()]
    
    release_connection(conn)
    return render_template('users/roles.html', roles=roles, users=users, permissions=permissions)

@users_bp.route('/roles/create', methods=['POST'])
@login_required
def create_role():
    if current_user.role != 'admin':
        flash('Permission denied.', 'error')
        return redirect(url_for('users.roles'))
    
    name = request.form.get('name')
    description = request.form.get('description')
    perms = request.form.getlist('permissions')
    
    if not name:
        flash('Role name required.', 'error')
        return redirect(url_for('users.roles'))
    
    conn = get_connection()
    c = conn.cursor()
    try:
        c.execute('INSERT INTO roles (name, description) VALUES (%s, %s) RETURNING id', (name, description))
        role_id = c.fetchone()[0]
        
        for perm in perms:
            c.execute('INSERT INTO permissions (role_id, permission) VALUES (%s, %s)', (role_id, perm))
        
        conn.commit()
        flash('Role created successfully.', 'success')
    except Exception as e:
        conn.rollback()
        flash(f'Error creating role: {str(e)}', 'error')
    finally:
        release_connection(conn)
    
    return redirect(url_for('users.roles'))

@users_bp.route('/roles/assign', methods=['POST'])
@login_required
def assign_role():
    if current_user.role != 'admin':
        flash('Permission denied.', 'error')
        return redirect(url_for('users.roles'))
    
    user_id = request.form.get('user_id')
    role_id = request.form.get('role_id')
    
    conn = get_connection()
    c = conn.cursor()
    try:
        c.execute('INSERT INTO user_roles (user_id, role_id) VALUES (%s, %s) ON CONFLICT DO NOTHING', (user_id, role_id))
        conn.commit()
        flash('Role assigned.', 'success')
    except Exception as e:
        conn.rollback()
        flash(f'Error assigning role: {str(e)}', 'error')
    finally:
        release_connection(conn)
    
    return redirect(url_for('users.roles'))

@users_bp.route('/people')
@login_required
def people():
    conn = get_connection()
    c = conn.cursor()
    c.execute('''
        SELECT DISTINCT id, full_name, email, job_title, role, certifications
        FROM users
        ORDER BY full_name
    ''')
    rows = c.fetchall()
    people = []
    for r in rows:
        certs = r[5]
        try:
            if isinstance(certs, str):
                import json as _json
                certs = _json.loads(certs) if certs else []
        except Exception:
            certs = []
        if certs is None:
            certs = []
        people.append({'id': r[0], 'full_name': r[1], 'email': r[2], 'job_title': r[3], 'role': r[4], 'certifications': certs})
    release_connection(conn)
    return render_template('users/people.html', people=people)

@users_bp.route('/people/<int:id>')
@login_required
def person_detail(id):
    conn = get_connection()
    c = conn.cursor()
    c.execute('''
        SELECT id, username, full_name, email, job_title, role, certifications,
               address, home_phone, cell_phone, work_phone
        FROM users
        WHERE id = %s
    ''', (id,))
    person = c.fetchone()
    if not person:
        flash('Person not found.', 'error')
        return redirect(url_for('users.people'))
    
    person_dict = {
        'id': person[0],
        'username': person[1],
        'full_name': person[2],
        'email': person[3],
        'job_title': person[4],
        'role': person[5],
        'certifications': person[6],
        'address': person[7],
        'home_phone': person[8],
        'cell_phone': person[9],
        'work_phone': person[10]
    }
    release_connection(conn)
    return render_template('users/person_detail.html', person=person_dict)

@users_bp.route('/people/add', methods=['GET', 'POST'])
@login_required
def add_person():
    if current_user.role not in ['admin', 'manager']:
        flash('Permission denied.', 'error')
        return redirect(url_for('users.people'))
    
    if request.method == 'POST':
        # Collect all fields
        username = request.form.get('username')
        full_name = request.form.get('full_name')
        email = request.form.get('email')
        job_title = request.form.get('job_title')
        role = request.form.get('role')
        password = request.form.get('password')  # Hash in service
        certifications = request.form.getlist('certifications')
        address = request.form.get('address')
        home_phone = request.form.get('home_phone')
        cell_phone = request.form.get('cell_phone')
        work_phone = request.form.get('work_phone')
        
        conn = get_connection()
        c = conn.cursor()
        try:
            c.execute('''
                INSERT INTO users (username, full_name, email, job_title, role, password, certifications,
                                   address, home_phone, cell_phone, work_phone)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            ''', (username, full_name, email, job_title, role, generate_password_hash(password),
                  json.dumps(certifications), address, home_phone, cell_phone, work_phone))
            new_user_id = c.fetchone()[0]

            # Map primary role to user_roles if exists
            if role:
                c.execute('SELECT id FROM roles WHERE name=%s', (role,))
                row = c.fetchone()
                if row:
                    c.execute('INSERT INTO user_roles (user_id, role_id) VALUES (%s, %s) ON CONFLICT DO NOTHING', (new_user_id, row[0]))

            conn.commit()
            log_audit(es, 'add_person', current_user.id, new_user_id, {'username': username, 'role': role})
            flash('Person added successfully.', 'success')
            return redirect(url_for('users.people'))
        except Exception as e:
            conn.rollback()
            flash(f'Error adding person: {str(e)}', 'error')
        finally:
            release_connection(conn)
    
    # For GET, show form with certification options, roles, etc.
    conn = get_connection()
    c = conn.cursor()
    c.execute('SELECT name FROM certifications ORDER BY name')
    certs = [r[0] for r in c.fetchall()]
    c.execute('SELECT name FROM roles ORDER BY name')
    roles = [r[0] for r in c.fetchall()]
    release_connection(conn)
    return render_template('users/add_person.html', certs=certs, roles=roles)

@users_bp.route('/people/edit/<int:id>', methods=['GET', 'POST'])
@login_required
def edit_person(id):
    if current_user.role not in ['admin', 'manager']:
        flash('Permission denied.', 'error')
        return redirect(url_for('users.people'))
    
    conn = get_connection()
    c = conn.cursor()
    
    if request.method == 'POST':
        # Similar to add, but UPDATE
        full_name = request.form.get('full_name')
        email = request.form.get('email')
        job_title = request.form.get('job_title')
        role = request.form.get('role')
        certifications = request.form.getlist('certifications')
        address = request.form.get('address')
        home_phone = request.form.get('home_phone')
        cell_phone = request.form.get('cell_phone')
        work_phone = request.form.get('work_phone')
        
        try:
            c.execute('''
                UPDATE users SET full_name=%s, email=%s, job_title=%s, role=%s, certifications=%s,
                                 address=%s, home_phone=%s, cell_phone=%s, work_phone=%s
                WHERE id=%s
            ''', (full_name, email, job_title, role, json.dumps(certifications),
                  address, home_phone, cell_phone, work_phone, id))

            # Sync user_roles to selected role
            c.execute('DELETE FROM user_roles WHERE user_id=%s', (id,))
            if role:
                c.execute('SELECT id FROM roles WHERE name=%s', (role,))
                row = c.fetchone()
                if row:
                    c.execute('INSERT INTO user_roles (user_id, role_id) VALUES (%s, %s) ON CONFLICT DO NOTHING', (id, row[0]))

            conn.commit()
            log_audit(es, 'edit_person', current_user.id, id, {'role': role})
            flash('Person updated successfully.', 'success')
            return redirect(url_for('users.people'))
        except Exception as e:
            conn.rollback()
            flash(f'Error updating person: {str(e)}', 'error')
        finally:
            release_connection(conn)
    
    # GET: Load person data with explicit column order
    c.execute('''
        SELECT id, username, full_name, email, job_title, role, certifications,
               address, home_phone, cell_phone, work_phone
        FROM users WHERE id=%s
    ''', (id,))
    person = c.fetchone()
    if not person:
        flash('Person not found.', 'error')
        return redirect(url_for('users.people'))
    
    c.execute('SELECT name FROM certifications ORDER BY name')
    certs = [r[0] for r in c.fetchall()]
    c.execute('SELECT name FROM roles ORDER BY name')
    roles = [r[0] for r in c.fetchall()]
    release_connection(conn)
    
    # Normalize certifications to a list
    certs_value = person[6]
    try:
        if isinstance(certs_value, str):
            import json as _json
            certs_value = _json.loads(certs_value) if certs_value else []
    except Exception:
        certs_value = []
    if certs_value is None:
        certs_value = []

    person_dict = {
        'id': person[0],
        'username': person[1],
        'full_name': person[2],
        'email': person[3],
        'job_title': person[4],
        'role': person[5],
        'certifications': certs_value,
        'address': person[7],
        'home_phone': person[8],
        'cell_phone': person[9],
        'work_phone': person[10]
    }
    return render_template('users/edit_person.html', person=person_dict, certs=certs, roles=roles)

@users_bp.route('/people/delete/<int:id>', methods=['POST'])
@login_required
def delete_person(id):
    if current_user.role not in ['admin', 'manager']:
        flash('Permission denied.', 'error')
        return redirect(url_for('users.people'))
    
    conn = get_connection()
    c = conn.cursor()
    try:
        c.execute('DELETE FROM users WHERE id=%s', (id,))
        conn.commit()
        flash('Person deleted successfully.', 'success')
    except Exception as e:
        conn.rollback()
        flash(f'Error deleting person: {str(e)}', 'error')
    finally:
        release_connection(conn)
    
    return redirect(url_for('users.people'))

@users_bp.route('/certifications', methods=['GET', 'POST'])
@login_required
def certifications():
    if current_user.role not in ['admin', 'manager']:
        flash('Permission denied.', 'error')
        return redirect(url_for('users.people'))
    
    if request.method == 'POST':
        name = request.form.get('name')
        description = request.form.get('description')
        issuing_company = request.form.get('issuing_company')
        validity_term_years = request.form.get('validity_term_years')
        requirements_text = request.form.get('requirements_text')
        
        try:
            add_certification(name, description, issuing_company, validity_term_years, requirements_text)
            flash('Certification added.', 'success')
        except Exception as e:
            flash(f'Error adding certification: {str(e)}', 'error')
    
    certs = get_all_certifications()
    return render_template('users/certifications.html', certs=certs)

@users_bp.route('/certifications/edit/<int:id>', methods=['GET', 'POST'])
@login_required
def edit_certification(id):
    if current_user.role not in ['admin', 'manager']:
        flash('Permission denied.', 'error')
        return redirect(url_for('users.certifications'))
    
    cert = next((c for c in get_all_certifications() if c['id'] == id), None)
    if not cert:
        flash('Certification not found.', 'error')
        return redirect(url_for('users.certifications'))
    
    if request.method == 'POST':
        name = request.form.get('name')
        description = request.form.get('description')
        issuing_company = request.form.get('issuing_company')
        validity_term_years = request.form.get('validity_term_years')
        requirements_text = request.form.get('requirements_text')
        
        try:
            update_certification(id, name, description, issuing_company, validity_term_years, requirements_text)
            flash('Certification updated.', 'success')
            return redirect(url_for('users.certifications'))
        except Exception as e:
            flash(f'Error updating certification: {str(e)}', 'error')
    
    return render_template('users/edit_certification.html', cert=cert)

@users_bp.route('/certifications/delete/<int:id>', methods=['POST'])
@login_required
def delete_certification_route(id):
    if current_user.role not in ['admin', 'manager']:
        flash('Permission denied.', 'error')
        return redirect(url_for('users.certifications'))
    
    try:
        delete_certification(id)
        flash('Certification deleted.', 'success')
    except Exception as e:
        flash(f'Error deleting certification: {str(e)}', 'error')
    
    return redirect(url_for('users.certifications'))