# services/schedule_service.py
from services.db import get_connection, release_connection
from services.logging_service import log_audit
from services.elasticsearch_client import es
from services.automation_service import notify_city_contacts
from services.email_service import send_notification
from config import WEATHER_API_KEY, NOTIFICATION_RECIPIENTS
from ortools.constraint_solver import pywrapcp
from datetime import datetime
from datetime import timedelta
import requests
import logging

logging.basicConfig(filename='scheduling.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

def check_conflicts(start_time, end_time, user_id, equipment_id):
    conn = get_connection()
    c = conn.cursor()
    conflicts = []
    if user_id:
        c.execute("SELECT id, title, start_date, end_date FROM schedules WHERE user_id = %s AND ((start_date <= %s AND end_date >= %s) OR (start_date <= %s AND end_date >= %s))",
                  (user_id, end_time, start_time, start_time, end_time))
        user_conflicts = [f"User conflict with schedule {row[0]}: {row[1]} ({row[2]} to {row[3]})" for row in c.fetchall()]
        conflicts.extend(user_conflicts)
    if equipment_id:
        c.execute("SELECT id, title, start_date, end_date FROM schedules WHERE resource_name = %s AND ((start_date <= %s AND end_date >= %s) OR (start_date <= %s AND end_date >= %s))",
                  (equipment_id, end_time, start_time, start_time, end_time))
        equip_conflicts = [f"Equipment conflict with schedule {row[0]}: {row[1]} ({row[2]} to {row[3]})" for row in c.fetchall()]
        conflicts.extend(equip_conflicts)
    release_connection(conn)
    if conflicts:
        logging.info(f"Conflicts detected for start={start_time}, end={end_time}, user_id={user_id}, equipment_id={equipment_id}: {conflicts}")
    return conflicts

def add_schedule(title, start_date, end_date, description, user_id, location=None, equipment_id=None):
    conn = get_connection()
    conn.autocommit = False
    try:
        c = conn.cursor()
        # Check requires_operator
        if equipment_id:
            c.execute("SELECT requires_operator FROM equipment_instances WHERE id = %s", (equipment_id,))
            result = c.fetchone()
            requires_operator = bool(result[0]) if result else False
            if requires_operator and not user_id:
                raise ValueError("Equipment requires an operator but no user_id provided")
        conflicts = check_conflicts(start_date, end_date, user_id, equipment_id)
        if conflicts:
            prompt = f"Suggest alternatives for scheduling conflict: {conflicts}"
            suggestions = generate_response(prompt)
            raise ValueError(f"Conflicts: {conflicts}. Suggestions: {suggestions}")
        weather = fetch_weather(location, start_date)
        if weather['wind_speed'] > 15:
            send_notification("Weather Alert", f"Bad weather for {title} at {location}", NOTIFICATION_RECIPIENTS)
        c.execute(
            "INSERT INTO schedules (title, start_date, end_date, description, user_id, location, resource_name, status) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s) RETURNING id",
            (title, start_date, end_date, description, user_id, location, equipment_id, "scheduled")
        )
        schedule_id = c.fetchone()[0]
        conn.commit()
        log_audit(es, "add_schedule", user_id, schedule_id, {"title": title, "location": location})
        if location:
            notify_city_contacts(schedule_id, location)
        return schedule_id
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        release_connection(conn)

def fetch_weather(location, date):
    url = f"https://api.openweathermap.org/data/2.5/forecast?q={location}&appid={WEATHER_API_KEY}&units=metric"
    try:
        response = requests.get(url).json()
        return {'wind_speed': response['list'][0]['wind']['speed']}
    except Exception as e:
        log_audit(es, "weather_fetch_failed", None, None, {"error": str(e)})
        return {'wind_speed': 0}

def optimize_schedule(start_date, end_date, resource_id):
    solver = pywrapcp.Solver("OptimizeSchedule")
    # Placeholder: Implement OR-Tools logic
    return "Optimized slot found"

def get_all_schedules():
    conn = get_connection()
    c = conn.cursor()
    c.execute("SELECT id, title, start_date, end_date, description, location, user_id, status, resource_name FROM schedules")
    schedules = [
        {
            "id": row[0],
            "title": row[1],
            "start_date": row[2],
            "end_date": row[3],
            "description": row[4],
            "location": row[5],
            "user_id": row[6],
            "status": row[7],
            "resource_name": row[8]
        } for row in c.fetchall()
    ]
    release_connection(conn)
    return schedules

def list_schedule_by_town(town_id: int):
    """List schedule rows for jobs in a specific town (best-effort join on location name if town_id not set)."""
    conn = get_connection()
    c = conn.cursor()
    try:
        c.execute(
            """
            SELECT s.id, s.title, s.start_date, s.end_date, s.description, s.location
            FROM schedules s
            JOIN jobs j ON j.id = s.id
            WHERE j.town_id = %s
            ORDER BY s.start_date DESC
            """,
            (town_id,),
        )
        return [
            {'id': r[0], 'title': r[1], 'start_date': r[2], 'end_date': r[3], 'description': r[4], 'location': r[5]}
            for r in c.fetchall()
        ]
    finally:
        release_connection(conn)

def get_schedule_event(event_id):
    conn = get_connection()
    c = conn.cursor()
    c.execute("SELECT id, title, start_date, end_date, description, location, user_id, status, resource_name FROM schedules WHERE id = %s", (event_id,))
    result = c.fetchone()
    release_connection(conn)
    if result:
        return {
            "id": result[0],
            "title": result[1],
            "start_date": result[2],
            "end_date": result[3],
            "description": result[4],
            "location": result[5],
            "user_id": result[6],
            "status": result[7],
            "resource_name": result[8]
        }
    return None

def update_schedule(schedule_id, title, start_date, end_date, description, user_id, location, equipment_id):
    conn = get_connection()
    conn.autocommit = False
    try:
        c = conn.cursor()
        c.execute(
            "UPDATE schedules SET title=%s, start_date=%s, end_date=%s, description=%s, location=%s, resource_name=%s, status=%s WHERE id=%s AND user_id=%s",
            (title, start_date, end_date, description, location, equipment_id, "scheduled", schedule_id, user_id)
        )
        conn.commit()
        log_audit(es, "update_schedule", user_id, schedule_id, {"title": title})
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        release_connection(conn)

def delete_schedule(schedule_id, user_id):
    conn = get_connection()
    conn.autocommit = False
    try:
        c = conn.cursor()
        c.execute("SELECT title FROM schedules WHERE id = %s", (schedule_id,))
        result = c.fetchone()
        if result:
            title = result[0]
            c.execute("DELETE FROM schedules WHERE id = %s", (schedule_id,))
            conn.commit()
            log_audit(es, "delete_schedule", user_id, schedule_id, {"title": title})
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        release_connection(conn)

def schedule_equipment_by_name(equipment_name, start_date, end_date, title, description, user_id):
    conn = get_connection()
    conn.autocommit = False
    try:
        c = conn.cursor()
        c.execute("SELECT id, unique_id FROM equipment_instances WHERE unique_id LIKE %s", (f"%{equipment_name}%",))
        matches = c.fetchall()
        if not matches:
            return f"No equipment found matching '{equipment_name}'."
        for equipment_id, equipment_label in matches:
            c.execute("SELECT 1 FROM schedules WHERE resource_name = %s AND NOT (end_date <= %s OR start_date >= %s)",
                      (equipment_id, start_date, end_date))
            if not c.fetchone():
                c.execute(
                    "INSERT INTO schedules (title, start_date, end_date, description, user_id, resource_name, status) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s)",
                    (title, start_date, end_date, description, user_id, equipment_id, "scheduled")
                )
                conn.commit()
                return f"{equipment_label} scheduled from {start_date} to {end_date}."
        return f"All units of '{equipment_name}' are booked."
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        release_connection(conn)

def get_schedule_summary(days_ahead=1):
    conn = get_connection()
    c = conn.cursor()
    today = datetime.now().date()
    future = today + timedelta(days=days_ahead)
    c.execute(
        "SELECT s.start_date, s.end_date, s.title, e.unique_id "
        "FROM schedules s "
        "LEFT JOIN equipment_instances e ON s.resource_name = e.id "
        "WHERE s.start_date BETWEEN %s AND %s "
        "ORDER BY s.start_date",
        (today.isoformat(), future.isoformat())
    )
    rows = c.fetchall()
    release_connection(conn)
    return [f"{start_date} - {end_date} | {title} ({equip or 'No equipment'})" for start_date, end_date, title, equip in rows] or ["No jobs scheduled."]

def get_schedule_status(schedule_job):
    conn = get_connection()
    c = conn.cursor()
    c.execute("SELECT status FROM schedules WHERE title = %s", (schedule_job,))
    result = c.fetchone()
    release_connection(conn)
    return result[0] if result else None

def check_availability(resource_name, start_date, end_date, cursor):
    cursor.execute(
        "SELECT title, start_date, end_date "
        "FROM schedules "
        "WHERE resource_name = %s AND ("
        "(start_date <= %s AND end_date >= %s) OR "
        "(start_date <= %s AND end_date >= %s) OR "
        "(start_date >= %s AND end_date <= %s)"
        ")",
        (resource_name, end_date, start_date, end_date, start_date, start_date, end_date)
    )
    conflict = cursor.fetchone()
    if conflict:
        title, s_date, e_date = conflict
        return False, f"{resource_name} is booked for '{title}' from {s_date} to {e_date}."
    return True, ""

def get_inventory_quantity(item_name):
    conn = get_connection()
    c = conn.cursor()
    c.execute("SELECT quantity FROM consumables WHERE name = %s", (item_name,))
    result = c.fetchone()
    release_connection(conn)
    return result[0] if result else None