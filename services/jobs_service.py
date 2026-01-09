# services/jobs_service.py
from services.db import get_connection, release_connection
from services.logging_service import log_audit
from services.email_service import send_notification
from services.schedule_service import check_availability
from datetime import datetime
import pulp

def get_job_status(job_name):
    conn = get_connection()
    c = conn.cursor()
    c.execute("SELECT status FROM jobs WHERE name = %s", (job_name,))
    result = c.fetchone()
    release_connection(conn)
    return result[0] if result else None

def create_job(name, description, estimated_cost, location, user_id, town_id=None):
    conn = get_connection()
    try:
        c = conn.cursor()
        c.execute(
            """
            INSERT INTO jobs (name, description, estimated_cost, location, town_id, status, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING id
            """,
            (name, description, estimated_cost, location, town_id, "pending", datetime.now()),
        )
        job_id = c.fetchone()[0]
        conn.commit()
        log_audit(es, "create_job", user_id, None, {"job_id": job_id, "name": name})
        if town_id:
            send_town_notification(job_id, town_id, user_id)
        elif location:
            send_city_notification(job_id, location, user_id)
        return job_id
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        release_connection(conn)

def send_city_notification(job_id, location, user_id):
    conn = get_connection()
    c = conn.cursor()
    c.execute("SELECT email FROM city_contacts WHERE city_name = %s", (location,))
    email = c.fetchone()
    release_connection(conn)
    if email:
        subject = f"Notification: Work Scheduled in {location}"
        body = f"Job {job_id} scheduled at {location}. Contact for details."
        send_notification(subject, body, [email[0]])
        log_audit(es, "city_notification", user_id, None, {"job_id": job_id, "location": location})

def send_town_notification(job_id, town_id, user_id):
    conn = get_connection()
    c = conn.cursor()
    c.execute("SELECT city_name, email FROM city_contacts WHERE id = %s", (town_id,))
    row = c.fetchone()
    release_connection(conn)
    if row and row[1]:
        city_name, email = row
        subject = f"Notification: Work Scheduled in {city_name}"
        body = f"Job {job_id} scheduled in {city_name}."
        send_notification(subject, body, [email])
        log_audit(es, "town_notification", user_id, None, {"job_id": job_id, "town_id": town_id})

def optimize_job_cost(job_id):
    prob = pulp.LpProblem("JobCostOptimization", pulp.LpMinimize)
    # Placeholder: Add variables/constraints
    prob.solve()
    return prob.objective.value() if prob.status == pulp.LpStatusOptimal else None