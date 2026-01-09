# services/email_service.py
import smtplib
from email.mime.text import MIMEText
from datetime import datetime
from config import SMTP_USER, SMTP_PASS, SMTP_SERVER, SMTP_PORT, OUTLOOK_CLIENT_ID, OUTLOOK_CLIENT_SECRET, OUTLOOK_AUTHORITY, OUTLOOK_REDIRECT_URI
from services.elasticsearch_client import es
from services.logging_service import log_audit
from services.db import get_connection, release_connection
import requests
import msal
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from jinja2 import Template
import os
from email import encoders

def get_email_templates():
    conn = get_connection()
    c = conn.cursor()
    c.execute("SELECT * FROM email_templates")
    templates = c.fetchall()
    release_connection(conn)
    return templates

def get_automation_rules(template_id=None):
    conn = get_connection()
    c = conn.cursor()
    if template_id:
        c.execute("SELECT * FROM automation_rules WHERE template_id = %s", (template_id,))
    else:
        c.execute("SELECT * FROM automation_rules")
    rules = c.fetchall()
    release_connection(conn)
    return rules

def get_template_by_id(template_id):
    conn = get_connection()
    c = conn.cursor()
    c.execute("SELECT * FROM email_templates WHERE id = %s", (template_id,))
    template = c.fetchone()
    release_connection(conn)
    return template

def get_rule_by_id(rule_id):
    conn = get_connection()
    c = conn.cursor()
    c.execute("SELECT * FROM automation_rules WHERE id = %s", (rule_id,))
    rule = c.fetchone()
    release_connection(conn)
    return rule

def get_template_preview(template_id, sample_data=None):
    template = get_template_by_id(template_id)
    if not template:
        return None
    _, name, subject, body, cc, bcc, *_rest, is_html = template
    if sample_data is None:
        sample_data = {
            "location": "Example Town", "start_date": "2025-04-01", "end_date": "2025-04-01",
            "start_time": "08:00", "end_time": "17:00", "description": "Example job",
            "job_number": "J0001", "job_name": "Sample Job", "resource_name": "BT001"
        }
    return {
        "subject": subject.format(**sample_data),
        "body": body.format(**sample_data),
        "cc": cc,
        "bcc": bcc,
        "is_html": bool(is_html)
    }

def send_notification(subject, body, recipients, cc=None, bcc=None, is_html=False, attachments=None, template_vars=None):
    if template_vars:
        template = Template(body)
        body = template.render(**template_vars)

    msg = MIMEMultipart() if attachments else MIMEText(body, "html" if is_html else "plain")
    msg["Subject"] = subject
    msg["From"] = SMTP_USER
    msg["To"] = ", ".join(recipients)
    if cc:
        msg["Cc"] = ", ".join(cc)
    all_recipients = recipients + (cc if cc else []) + (bcc if bcc else [])

    if attachments:
        for file_path in attachments or []:
            with open(file_path, "rb") as f:
                part = MIMEBase('application', 'octet-stream')
                part.set_payload(f.read())
            encoders.encode_base64(part)
            part.add_header('Content-Disposition', f'attachment; filename={os.path.basename(file_path)}')
            msg.attach(part)
        msg.attach(MIMEText(body, "html" if is_html else "plain"))

    try:
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(SMTP_USER, SMTP_PASS)
            server.sendmail(SMTP_USER, all_recipients, msg.as_string())
        print(f"[Email] Notification sent to: {all_recipients}")
        log_audit(es, "send_notification", "system", None, {"subject": subject, "recipients": all_recipients})
    except Exception as e:
        print(f"[Email] Failed to send notification: {e}")
        log_audit(es, "send_notification_failed", "system", None, {"subject": subject, "recipients": all_recipients, "error": str(e)})
    return True

def send_outlook_email(user_id, subject, body, to_list, cc_list=None, bcc_list=None):
    app = msal.ConfidentialClientApplication(
        OUTLOOK_CLIENT_ID, authority=OUTLOOK_AUTHORITY,
        client_credential=OUTLOOK_CLIENT_SECRET
    )
    token = app.acquire_token_silent(["https://graph.microsoft.com/.default"], account=None)
    if not token:
        token = app.acquire_token_for_client(scopes=["https://graph.microsoft.com/.default"])
    url = "https://graph.microsoft.com/v1.0/me/sendMail"
    message = {
        "message": {
            "subject": subject,
            "body": {"contentType": "HTML", "content": body},
            "toRecipients": [{"emailAddress": {"address": addr}} for addr in to_list],
            "ccRecipients": [{"emailAddress": {"address": addr}} for addr in cc_list or []],
            "bccRecipients": [{"emailAddress": {"address": addr}} for addr in bcc_list or []],
        }
    }
    headers = {"Authorization": f"Bearer {token['access_token']}", "Content-Type": "application/json"}
    response = requests.post(url, json=message, headers=headers)
    if response.status_code == 202:
        print(f"[Outlook] Email sent via Microsoft Graph API.")
        log_audit(es, "send_outlook_email", user_id, None, {"subject": subject, "to": to_list})
        return True
    else:
        print(f"[Outlook] Email failed: {response.status_code}, {response.text}")
        log_audit(es, "send_outlook_email_failed", user_id, None, {"subject": subject, "to": to_list, "error": response.text})
        return False