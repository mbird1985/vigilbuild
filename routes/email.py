# routes/email.py
from flask import Blueprint, request, render_template, redirect, url_for, flash
from flask_login import login_required, current_user
from datetime import datetime
from services.db import get_connection, release_connection
from services.logging_service import log_audit
from services.elasticsearch_client import es
from services.email_service import send_notification

email_bp = Blueprint("email", __name__, url_prefix="/email")

@email_bp.route("/email_templates", methods=["GET", "POST"])
@login_required
def email_templates():
    if not current_user.is_admin():
        flash('Admin access required.')
        return redirect(url_for('system.dashboard'))  # Updated redirect to dashboard

    conn = get_connection()  # Use PostgreSQL
    c = conn.cursor()

    if request.method == 'POST':
        form = request.form

        if 'add_template' in form:
            c.execute("""
                INSERT INTO email_templates (subject, body, cc, bcc, outlook_enabled, is_html, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (
                form.get('subject'), form.get('body'),
                form.get('cc'), form.get('bcc'),
                form.get('outlook_enabled') == 'on',
                form.get('is_html') == 'on',
                datetime.now()
            ))
            flash(f"Template '{form.get('subject')[:30]}...' added.")

        elif 'update_template' in form:
            c.execute("""
                UPDATE email_templates
                SET subject=%s, body=%s, cc=%s, bcc=%s, outlook_enabled=%s, is_html=%s
                WHERE id=%s
            """, (
                form.get('subject'), form.get('body'),
                form.get('cc'), form.get('bcc'),
                form.get('outlook_enabled') == 'on',
                form.get('is_html') == 'on',
                form.get('template_id')
            ))
            flash(f"Template updated.")

        elif 'delete_template' in form:
            c.execute("SELECT subject FROM email_templates WHERE id = %s", (form.get('template_id'),))
            result = c.fetchone()
            template_name = result[0] if result else 'Unknown'
            c.execute("DELETE FROM email_templates WHERE id = %s", (form.get('template_id'),))
            c.execute("DELETE FROM automation_rules WHERE template_id = %s", (form.get('template_id'),))
            flash(f"Template deleted.")

        elif 'add_rule' in form:
            c.execute("""
                INSERT INTO automation_rules
                (template_id, trigger_type, trigger_value, recipient_type, recipient_value, created_by, created_at, active)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                form.get('template_id'), form.get('trigger_type'), form.get('trigger_value'),
                form.get('recipient_type'), form.get('recipient_value') or None,
                current_user.id,
                datetime.now(),
                form.get('active') == 'on'
            ))
            flash("Automation rule added.")

        elif 'update_rule' in form:
            c.execute("""
                UPDATE automation_rules
                SET template_id=%s, trigger_type=%s, trigger_value=%s, recipient_type=%s, recipient_value=%s, active=%s
                WHERE id=%s
            """, (
                form.get('template_id'), form.get('trigger_type'), form.get('trigger_value'),
                form.get('recipient_type'), form.get('recipient_value') or None,
                form.get('active') == 'on',
                form.get('rule_id')
            ))
            flash("Automation rule updated.")

        elif 'delete_rule' in form:
            c.execute("DELETE FROM automation_rules WHERE id = %s", (form.get('rule_id'),))
            flash("Automation rule deleted.")

        elif 'preview_template' in form:
            c.execute("SELECT subject, body, cc, bcc, is_html FROM email_templates WHERE id = %s", (form.get('template_id'),))
            template = c.fetchone()
            if template:
                subject, body, cc, bcc, is_html = template
                dummy_data = {
                    "location": "Sample Town",
                    "start_date": "2025-04-01",
                    "end_date": "2025-04-02",
                    "start_time": "08:00",
                    "end_time": "17:00",
                    "description": "Sample maintenance work",
                    "job_number": "J12345",
                    "job_name": "Sample Job",
                    "resource_name": "Sample Truck"
                }
                preview = {
                    "subject": subject.format(**dummy_data),
                    "body": body.format(**dummy_data),
                    "cc": cc,
                    "bcc": bcc,
                    "is_html": is_html
                }
        conn.commit()

    # Load templates and rules (filter out blanks)
    templates = []
    rules = []
    try:
        c.execute("""
            SELECT id, subject, body, cc, bcc, outlook_enabled, is_html, last_used, created_at
            FROM email_templates
            WHERE COALESCE(TRIM(subject),'') <> '' AND COALESCE(TRIM(body),'') <> ''
            ORDER BY created_at DESC
        """)
        template_rows = c.fetchall()
        for row in template_rows:
            templates.append({
                'id': row[0],
                'name': row[1][:50] if row[1] else 'Untitled',  # Use subject as name
                'subject': row[1],
                'body': row[2],
                'cc': row[3],
                'bcc': row[4],
                'outlook_enabled': row[5],
                'is_html': row[6],
                'last_used': row[7],
                'created_by': None,
                'timestamp': row[8]
            })
    except Exception as e:
        import logging
        logging.error(f"Error fetching email templates: {str(e)}")

    try:
        c.execute("""
            SELECT id, template_id, trigger_type, trigger_value, recipient_type, recipient_value, active, created_by, created_at
            FROM automation_rules
            WHERE COALESCE(TRIM(trigger_type),'') <> ''
            ORDER BY created_at DESC
        """)
        rule_rows = c.fetchall()
        for row in rule_rows:
            rules.append({
                'id': row[0],
                'template_id': row[1],
                'trigger_type': row[2],
                'trigger_value': row[3],
                'recipient_type': row[4],
                'recipient_value': row[5],
                'active': row[6],
                'created_by': row[7],
                'timestamp': row[8]
            })
    except Exception as e:
        import logging
        logging.error(f"Error fetching automation rules: {str(e)}")

    release_connection(conn)

    edit_template_id = request.args.get('edit_template_id')
    edit_rule_id = request.args.get('edit_rule_id')

    edit_template = None
    if edit_template_id:
        try:
            conn = get_connection()
            c = conn.cursor()
            c.execute("SELECT id, subject, body, cc, bcc, outlook_enabled, is_html FROM email_templates WHERE id = %s", (edit_template_id,))
            row = c.fetchone()
            if row:
                edit_template = {
                    'id': row[0],
                    'name': row[1][:50] if row[1] else 'Untitled',  # Use subject as name
                    'subject': row[1],
                    'body': row[2],
                    'cc': row[3],
                    'bcc': row[4],
                    'outlook_enabled': row[5],
                    'is_html': row[6]
                }
            release_connection(conn)
        except Exception:
            pass

    edit_rule = None
    if edit_rule_id:
        try:
            conn = get_connection()
            c = conn.cursor()
            c.execute("SELECT id, template_id, trigger_type, trigger_value, recipient_type, recipient_value, active FROM automation_rules WHERE id = %s", (edit_rule_id,))
            row = c.fetchone()
            if row:
                edit_rule = {
                    'id': row[0],
                    'template_id': row[1],
                    'trigger_type': row[2],
                    'trigger_value': row[3],
                    'recipient_type': row[4],
                    'recipient_value': row[5],
                    'active': row[6]
                }
            release_connection(conn)
        except Exception:
            pass

    return render_template("email_templates.html",
                           templates=templates,
                           rules=rules,
                           edit_template=edit_template,
                           edit_rule=edit_rule)
