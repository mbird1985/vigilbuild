# services/automation_service.py
from services.db import get_connection, release_connection
from services.logging_service import log_audit
from services.elasticsearch_client import es
from services.email_service import send_notification, send_outlook_email
from services.equipment_service import get_all_equipment
from services.job_utils import needs_rerouting
from services.ollama_llm import generate_response
from config import WEATHER_API_KEY, NOTIFICATION_RECIPIENTS, SMTP_USER, SMTP_PASS, SMTP_SERVER, SMTP_PORT
from datetime import datetime, timedelta
import requests
from email.mime.text import MIMEText
import smtplib
import time

def notify_city_contacts(job_id, location):
    conn = get_connection()
    c = conn.cursor()
    c.execute("SELECT email, city_name FROM city_contacts WHERE city_name = %s", (location,))
    contacts = c.fetchall()
    release_connection(conn)

    if not contacts:
        log_audit(es, "notify_city_contacts_error", None, job_id, {"error": f"No contacts found for {location}"})
        return

    subject = f"Potelco Job Notification for {location}"
    body = f"New job scheduled in {location} (Job ID: {job_id}). Please review permit requirements."
    msg = MIMEText(body)
    msg['Subject'] = subject
    msg['From'] = SMTP_USER
    msg['To'] = ", ".join([contact[0] for contact in contacts] + NOTIFICATION_RECIPIENTS)

    try:
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            for attempt in range(3):
                try:
                    server.starttls()
                    server.login(SMTP_USER, SMTP_PASS)
                    server.send_message(msg)
                    break
                except Exception as retry_err:
                    if attempt == 2:
                        raise
                    time.sleep(1)  # Simple backoff
        log_audit(es, "notify_city_contacts", None, job_id, {"location": location, "contacts": len(contacts)})
    except Exception as e:
        log_audit(es, "notify_city_contacts_error", None, job_id, {"error": str(e)})

def reroute_jobs():
    conn = get_connection()
    c = conn.cursor()
    today = datetime.now().date().strftime("%Y-%m-%d")
    c.execute("SELECT * FROM schedules WHERE start_date = %s AND status = 'scheduled'", (today,))
    jobs = c.fetchall()
    for job in jobs:
        if needs_rerouting(job):
            new_start = (datetime.strptime(job[2], "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
            new_end = (datetime.strptime(job[3], "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
            c.execute("UPDATE schedules SET start_date = %s, end_date = %s, status = 'rescheduled' WHERE id = %s",
                      (new_start, new_end, job[0]))
            log_audit(es, "reroute", "system", job[0], {"reason": "weather/equipment"})
    conn.commit()
    release_connection(conn)

def check_all_equipment_maintenance():
    equipments = get_all_equipment()
    for equipment in equipments:
        if equipment["hours"] >= equipment["maintenance_threshold"] and equipment["maintenance_threshold"]:
            subject = f"Equipment Maintenance Alert: {equipment['unique_id']}"
            body = f"Equipment '{equipment['unique_id']}' has reached {equipment['hours']} hours (threshold: {equipment['maintenance_threshold']}). Last maintenance: {equipment['last_maintenance'] or 'Never'}."
            send_notification(subject, body, NOTIFICATION_RECIPIENTS)
            log_audit(es, "maintenance_alert", None, None, {"equipment_id": equipment["id"], "unique_id": equipment["unique_id"], "hours": equipment["hours"]})

def update_weather(location="Site A"):
    url = f"http://api.openweathermap.org/data/2.5/weather?q={location}&appid={WEATHER_API_KEY}&units=metric"
    try:
        response = requests.get(url).json()
        weather = {
            "date": datetime.now().strftime("%Y-%m-%d"),
            "location": location,
            "temp": response["main"]["temp"],
            "wind_speed": response["wind"]["speed"],
            "precipitation": response["rain"]["1h"] if "rain" in response else 0,
            "timestamp": datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        }
        conn = get_connection()
        c = conn.cursor()
        c.execute("INSERT INTO weather (date, location, temp, wind_speed, precipitation, timestamp) VALUES (%s, %s, %s, %s, %s, %s)",
                  (weather["date"], weather["location"], weather["temp"], weather["wind_speed"], weather["precipitation"], weather["timestamp"]))
        conn.commit()
        release_connection(conn)
    except Exception as e:
        log_audit(es, "weather_fetch_failed", None, None, {"error": str(e)})

def check_equipment_health():
    conn = get_connection()
    c = conn.cursor()
    c.execute("SELECT id, unique_id, hours, maintenance_threshold, last_maintenance FROM equipment_instances")
    equipment = c.fetchall()
    for eq in equipment:
        if eq[2] >= eq[3]:
            send_notification(
                f"Equipment Maintenance Needed: {eq[1]}",
                f"Equipment '{eq[1]}' has reached {eq[2]} hours of use (threshold: {eq[3]}). Last maintenance: {eq[4] or 'Never'}.",
                NOTIFICATION_RECIPIENTS
            )
            log_audit(es, "maintenance_alert", None, None, {"usage_hours": eq[2], "unique_id": eq[1]})

def send_automated_emails():
    conn = get_connection()
    c = conn.cursor()
    today = datetime.now().date()
    now = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    
    c.execute("SELECT * FROM automation_rules WHERE active = true")
    rules = c.fetchall()
    
    for rule in rules:
        rule_id, template_id, trigger_type, trigger_value, recipient_type, recipient_value, created_by, timestamp, active = rule
        
        c.execute("SELECT subject, body, outlook_enabled, cc, bcc, is_html FROM email_templates WHERE id = %s", (template_id,))
        template = c.fetchone()
        if not template:
            continue
        subject_template, body_template, outlook_enabled, cc, bcc, is_html = template
        
        c.execute("SELECT city_name, email FROM city_contacts")
        city_emails = {row[0]: row[1] for row in c.fetchall()}
        c.execute("SELECT id, email FROM users WHERE role = 'manager'")
        managers = {row[0]: row[1] for row in c.fetchall()}

        if trigger_type == "days_before":
            days_before = int(trigger_value)
            trigger_date = today + timedelta(days=days_before)
            trigger_date_str = trigger_date.strftime("%Y-%m-%d")
            c.execute("SELECT * FROM schedules WHERE start_date <= %s AND end_date >= %s AND status = 'scheduled'", (trigger_date_str, trigger_date_str))
        elif trigger_type == "job_status":
            c.execute("SELECT * FROM schedules WHERE status = %s", (trigger_value,))
        
        jobs = c.fetchall()
        
        for job in jobs:
            job_id, title, start_date, end_date, description, user_id, location, resource_name, status = job
            if not description:
                description = "No description provided"
            if not location:
                location = "TBD"
            
            variables = {
                "location": location,
                "start_date": start_date,
                "end_date": end_date,
                "description": description,
                "job_name": title,
                "resource_name": resource_name or "None"
            }
            
            subject = subject_template.format(**variables)
            if len(description) > 200:
                prompt = f"Summarize this job description for an email: {description}"
                ai_summary = generate_response(prompt)
                body = body_template.format(**variables).replace("{description}", ai_summary)
            else:
                body = body_template.format(**variables)
            
            if recipient_type == "location_city":
                recipient = city_emails.get(location, NOTIFICATION_RECIPIENTS[0])
            elif recipient_type == "manager":
                recipient = managers.get(user_id, NOTIFICATION_RECIPIENTS[0])
            else:
                recipient = recipient_value or NOTIFICATION_RECIPIENTS[0]

            sent = False
            if outlook_enabled and send_outlook_email(user_id, subject, body, [recipient], cc=cc, bcc=bcc, is_html=is_html):
                sent = True
            elif send_notification(subject, body, [recipient], cc=cc, bcc=bcc, is_html=is_html):
                sent = True
            
            c.execute("UPDATE email_templates SET last_used = %s WHERE id = %s", (now, template_id))
            c.execute("INSERT INTO email_logs (template_id, job_id, recipient, subject, sent_timestamp, status) VALUES (%s, %s, %s, %s, %s, %s)",
                      (template_id, job_id, recipient, subject, now, "sent" if sent else "failed"))
            log_audit(es, "email_sent", "system", job_id, {"rule_id": rule_id, "recipient": recipient, "status": "sent" if sent else "failed"})

    conn.commit()
    release_connection(conn)