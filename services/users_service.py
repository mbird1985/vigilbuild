# services/users_service.py
from services.db import get_connection, release_connection
from services.logging_service import log_audit
from services.elasticsearch_client import es
from werkzeug.security import generate_password_hash
from datetime import datetime
import json

def get_all_users():
    conn = get_connection()
    c = conn.cursor()
    c.execute('''
        SELECT id, username, full_name, email, job_title, role, certifications,
               address, home_phone, cell_phone, work_phone
        FROM users
        ORDER BY full_name
    ''')
    users = [dict(zip([col[0] for col in c.description], row)) for row in c.fetchall()]
    release_connection(conn)
    return users

def get_user_by_id(user_id):
    conn = get_connection()
    c = conn.cursor()
    c.execute('''
        SELECT id, username, full_name, email, job_title, role, certifications,
               address, home_phone, cell_phone, work_phone
        FROM users
        WHERE id = %s
    ''', (user_id,))
    user = dict(zip([col[0] for col in c.description], c.fetchone() or ()))
    release_connection(conn)
    return user if user else None

def add_user(username, full_name, email, job_title, role, password, certifications, address, home_phone, cell_phone, work_phone, created_by):
    conn = get_connection()
    c = conn.cursor()
    hashed_pw = generate_password_hash(password)
    try:
        c.execute('''
            INSERT INTO users (username, full_name, email, job_title, role, password, certifications,
                               address, home_phone, cell_phone, work_phone)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        ''', (username, full_name, email, job_title, role, hashed_pw, json.dumps(certifications),
              address, home_phone, cell_phone, work_phone))
        user_id = c.fetchone()[0]
        conn.commit()
        log_audit(es, 'add_user', created_by, user_id, {'username': username})
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        release_connection(conn)

def update_user(user_id, username, full_name, email, job_title, role, certifications, address, home_phone, cell_phone, work_phone, updated_by):
    conn = get_connection()
    c = conn.cursor()
    try:
        c.execute('''
            UPDATE users SET username=%s, full_name=%s, email=%s, job_title=%s, role=%s, certifications=%s,
                             address=%s, home_phone=%s, cell_phone=%s, work_phone=%s, updated_at=NOW()
            WHERE id=%s
        ''', (username, full_name, email, job_title, role, json.dumps(certifications),
              address, home_phone, cell_phone, work_phone, user_id))
        conn.commit()
        log_audit(es, 'update_user', updated_by, user_id, {'username': username})
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        release_connection(conn)

def delete_user(user_id):
    conn = get_connection()
    c = conn.cursor()
    try:
        c.execute('DELETE FROM users WHERE id=%s', (user_id,))
        conn.commit()
        log_audit(es, 'delete_user', None, user_id, {})
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        release_connection(conn)

def get_user_certifications(user_id):
    conn = get_connection()
    c = conn.cursor()
    c.execute("SELECT c.id, c.name FROM certifications c JOIN user_certifications uc ON c.id = uc.cert_id WHERE uc.user_id = %s", (user_id,))
    certs = [{"id": row[0], "name": row[1]} for row in c.fetchall()]
    release_connection(conn)
    return certs

def resolve_certifications(cert_names):
    conn = get_connection()
    c = conn.cursor()
    cert_ids = []
    for name in cert_names or []:
        name = name.strip().lower()
        if not name:
            continue
        c.execute("SELECT id FROM certifications WHERE lower(name) = %s", (name,))
        result = c.fetchone()
        if result:
            cert_ids.append(result[0])
        else:
            c.execute("INSERT INTO certifications (name) VALUES (%s) RETURNING id", (name,))
            cert_ids.append(c.fetchone()[0])
    conn.commit()
    release_connection(conn)
    return cert_ids

def get_all_certifications():
    conn = get_connection()
    c = conn.cursor()
    c.execute('''
        SELECT id, name, description, issuing_company, validity_term_years, requirements_text
        FROM certifications
        ORDER BY name
    ''')
    certs = [dict(zip(['id', 'name', 'description', 'issuing_company', 'validity_term_years', 'requirements_text'], r)) for r in c.fetchall()]
    release_connection(conn)
    return certs

def add_certification(name, description, issuing_company, validity_term_years, requirements_text):
    conn = get_connection()
    c = conn.cursor()
    try:
        c.execute('''
            INSERT INTO certifications (name, description, issuing_company, validity_term_years, requirements_text)
            VALUES (%s, %s, %s, %s, %s)
        ''', (name, description, issuing_company, validity_term_years, requirements_text))
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        release_connection(conn)

def update_certification(cert_id, name, description, issuing_company, validity_term_years, requirements_text):
    conn = get_connection()
    c = conn.cursor()
    try:
        c.execute('''
            UPDATE certifications SET name=%s, description=%s, issuing_company=%s, 
                                      validity_term_years=%s, requirements_text=%s
            WHERE id=%s
        ''', (name, description, issuing_company, validity_term_years, requirements_text, cert_id))
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        release_connection(conn)

def delete_certification(cert_id):
    conn = get_connection()
    c = conn.cursor()
    try:
        c.execute('DELETE FROM certifications WHERE id=%s', (cert_id,))
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        release_connection(conn)

def get_all_roles():
    conn = get_connection()
    c = conn.cursor()
    c.execute('SELECT id, name, description FROM roles ORDER BY name')
    roles = [dict(zip(['id', 'name', 'description'], r)) for r in c.fetchall()]
    release_connection(conn)
    return roles

def request_password_reset(email):
    # Generate token, save to DB, send email
    pass

def reset_password(token, new_password):
    # Validate token, update password
    pass