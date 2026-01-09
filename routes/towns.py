# routes/towns.py
from flask import Blueprint, render_template, request, jsonify, flash, redirect, url_for
from flask_login import login_required, current_user
from services.db import get_connection, release_connection
from services.logging_service import log_audit
from werkzeug.exceptions import BadRequest
from services.elasticsearch_client import es
from services.validation_service import sanitize_string, sanitize_int, validate_email, validate_phone

towns_bp = Blueprint('towns', __name__, url_prefix='/towns')

@towns_bp.route('/')
@login_required
def towns():
    conn = get_connection()
    c = conn.cursor()
    # Safety net: ensure new columns/tables exist
    try:
        c.execute("ALTER TABLE city_contacts ADD COLUMN IF NOT EXISTS phone TEXT")
        c.execute("ALTER TABLE city_contacts ADD COLUMN IF NOT EXISTS address TEXT")
        c.execute('''CREATE TABLE IF NOT EXISTS town_contacts (
            id SERIAL PRIMARY KEY,
            town_id INTEGER NOT NULL,
            role TEXT NOT NULL,
            name TEXT NOT NULL,
            email TEXT,
            phone TEXT,
            address TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )''')
    except Exception:
        pass
    c.execute("""
        SELECT DISTINCT ON (LOWER(city_name)) id, city_name, email, contact_name, phone, address
        FROM city_contacts
        WHERE TRIM(city_name) <> ''
        ORDER BY LOWER(city_name), id DESC
    """)
    towns = []
    for row in c.fetchall():
        towns.append({'id': row[0], 'name': row[1], 'email': row[2], 'contact_name': row[3], 'phone': row[4], 'address': row[5]})
    release_connection(conn)
    return render_template('towns.html', towns=towns)

@towns_bp.route('/add', methods=['GET', 'POST'])
@login_required
def add_town():
    if request.method == 'POST':
        # Validate and sanitize inputs
        city_name = sanitize_string(request.form.get('city_name', ''), max_length=200)
        if not city_name:
            flash('Town name is required.')
            return redirect(url_for('towns.add_town'))

        valid, email = validate_email(request.form.get('email', ''))
        if not valid and request.form.get('email', '').strip():
            flash(f'Invalid email: {email}')
            return redirect(url_for('towns.add_town'))

        contact_name = sanitize_string(request.form.get('contact_name', ''), max_length=100)

        valid, phone = validate_phone(request.form.get('phone', ''))
        if not valid and request.form.get('phone', '').strip():
            flash(f'Invalid phone: {phone}')
            return redirect(url_for('towns.add_town'))

        address = sanitize_string(request.form.get('address', ''), max_length=500)
        try:
            conn = get_connection()
            c = conn.cursor()
            # Ensure columns exist before insert
            try:
                c.execute("ALTER TABLE city_contacts ADD COLUMN IF NOT EXISTS phone TEXT")
                c.execute("ALTER TABLE city_contacts ADD COLUMN IF NOT EXISTS address TEXT")
            except Exception:
                pass
            c.execute(
                """
                INSERT INTO city_contacts (city_name, email, contact_name, phone, address)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (city_name) DO UPDATE SET 
                    email = EXCLUDED.email,
                    contact_name = EXCLUDED.contact_name,
                    phone = EXCLUDED.phone,
                    address = EXCLUDED.address
                """,
                (city_name, email or None, contact_name or None, phone or None, address or None)
            )
            conn.commit()
            release_connection(conn)
            log_audit(es, 'town_added', current_user.id, None, {'city_name': city_name})
            flash('Town added successfully.')
            return redirect(url_for('towns.towns'))
        except Exception as e:
            log_audit(es, 'town_add_error', current_user.id, None, {'error': str(e)})
            flash(f'Error adding town: {str(e)}')
    return render_template('add_town.html')

@towns_bp.route('/edit/<int:id>', methods=['GET', 'POST'])
@login_required
def edit_town(id):
    conn = get_connection()
    c = conn.cursor()
    try:
        c.execute("ALTER TABLE city_contacts ADD COLUMN IF NOT EXISTS phone TEXT")
        c.execute("ALTER TABLE city_contacts ADD COLUMN IF NOT EXISTS address TEXT")
    except Exception:
        pass
    c.execute("SELECT id, city_name, email, contact_name, phone, address FROM city_contacts WHERE id = %s", (id,))
    town = c.fetchone()
    release_connection(conn)
    if not town:
        flash('Town not found.')
        return redirect(url_for('towns.towns'))
    town_data = {'id': town[0], 'name': town[1], 'email': town[2], 'contact_name': town[3], 'phone': town[4], 'address': town[5]}
    if request.method == 'POST':
        city_name = (request.form.get('city_name') or '').strip()
        email = (request.form.get('email') or '').strip()
        contact_name = (request.form.get('contact_name') or '').strip()
        phone = (request.form.get('phone') or '').strip()
        address = (request.form.get('address') or '').strip()
        if not city_name:
            flash('Town name is required.')
            return redirect(url_for('towns.edit_town', id=id))
        try:
            conn = get_connection()
            c = conn.cursor()
            c.execute(
                "UPDATE city_contacts SET city_name = %s, email = %s, contact_name = %s, phone = %s, address = %s WHERE id = %s",
                (city_name, email or None, contact_name or None, phone or None, address or None, id)
            )
            conn.commit()
            release_connection(conn)
            log_audit(es, 'town_updated', current_user.id, None, {'city_name': city_name})
            flash('Town updated successfully.')
            return redirect(url_for('towns.towns'))
        except Exception as e:
            log_audit(es, 'town_update_error', current_user.id, None, {'error': str(e)})
            flash(f'Error updating town: {str(e)}')
    return render_template('edit_town.html', town=town_data)

@towns_bp.route('/contacts/<int:town_id>')
@login_required
def list_town_contacts(town_id):
    conn = get_connection()
    c = conn.cursor()
    # Safety net: ensure contacts table exists
    try:
        c.execute('''CREATE TABLE IF NOT EXISTS town_contacts (
            id SERIAL PRIMARY KEY,
            town_id INTEGER NOT NULL,
            role TEXT NOT NULL,
            name TEXT NOT NULL,
            email TEXT,
            phone TEXT,
            address TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )''')
    except Exception:
        pass
    c.execute("SELECT id, role, name, email, phone, address FROM town_contacts WHERE town_id = %s ORDER BY role, name", (town_id,))
    contacts = [
        {'id': r[0], 'role': r[1], 'name': r[2], 'email': r[3], 'phone': r[4], 'address': r[5]}
        for r in c.fetchall()
    ]
    release_connection(conn)
    return render_template('town_contacts.html', town_id=town_id, contacts=contacts)

@towns_bp.route('/contacts/<int:town_id>/add', methods=['POST'])
@login_required
def add_town_contact(town_id):
    role = (request.form.get('role') or '').strip()
    name = (request.form.get('name') or '').strip()
    email = (request.form.get('email') or '').strip()
    phone = (request.form.get('phone') or '').strip()
    address = (request.form.get('address') or '').strip()
    if not (role and name):
        flash('Role and name are required.')
        return redirect(url_for('towns.list_town_contacts', town_id=town_id))
    conn = get_connection()
    c = conn.cursor()
    c.execute(
        "INSERT INTO town_contacts (town_id, role, name, email, phone, address) VALUES (%s, %s, %s, %s, %s, %s)",
        (town_id, role, name, email or None, phone or None, address or None)
    )
    conn.commit()
    release_connection(conn)
    flash('Contact added.')
    return redirect(url_for('towns.list_town_contacts', town_id=town_id))

@towns_bp.route('/contacts/<int:town_id>/delete/<int:contact_id>', methods=['POST'])
@login_required
def delete_town_contact(town_id, contact_id):
    conn = get_connection()
    c = conn.cursor()
    c.execute("DELETE FROM town_contacts WHERE id = %s AND town_id = %s", (contact_id, town_id))
    conn.commit()
    release_connection(conn)
    flash('Contact deleted.')
    return redirect(url_for('towns.list_town_contacts', town_id=town_id))

@towns_bp.route('/delete/<int:id>')
@login_required
def delete_town(id):
    try:
        conn = get_connection()
        c = conn.cursor()
        c.execute("SELECT city_name FROM city_contacts WHERE id = %s", (id,))
        city_name = c.fetchone()
        if city_name:
            c.execute("DELETE FROM city_contacts WHERE id = %s", (id,))
            conn.commit()
            log_audit(es, 'town_deleted', current_user.id, None, {'city_name': city_name[0]})
            flash('Town deleted successfully.')
        else:
            flash('Town not found.')
        release_connection(conn)
    except Exception as e:
        log_audit(es, 'town_delete_error', current_user.id, None, {'error': str(e)})
        flash(f'Error deleting town: {str(e)}')
    return redirect(url_for('towns.towns'))