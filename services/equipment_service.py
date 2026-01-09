# services/equipment_service.py
from datetime import datetime, timedelta
from config import NOTIFICATION_RECIPIENTS
from services.db import db_connection, db_transaction, get_connection, release_connection
from services.logging_service import log_audit
from services.email_service import send_notification
# New: For predictive (simple linear regression example)
from sklearn.linear_model import LinearRegression
import numpy as np
from flask_login import current_user
import os

def get_equipment_status(equipment_filter=None):
    with db_connection() as conn:
        c = conn.cursor()
        if equipment_filter:
            c.execute("SELECT unique_id, status FROM equipment_instances WHERE unique_id LIKE %s", (f"%{equipment_filter}%",))
        else:
            c.execute("SELECT unique_id, status FROM equipment_instances")
        rows = c.fetchall()
        return [f"{unique_id}: {status}" for unique_id, status in rows] or ["No equipment found."]

def create_or_update_maintenance_schedule(equipment_id: int, schedule_type: str, interval_days: int = None, interval_hours: int = None, notes: str = None, active: bool = True):
    with db_transaction() as conn:
        c = conn.cursor()
        # Try update first; if none updated, insert
        c.execute(
            """
            UPDATE equipment_maintenance_schedule
            SET interval_days = COALESCE(%s, interval_days),
                interval_hours = COALESCE(%s, interval_hours),
                notes = COALESCE(%s, notes),
                active = %s
            WHERE equipment_id = %s AND schedule_type = %s
            """,
            (interval_days, interval_hours, notes, active, equipment_id, schedule_type),
        )
        if c.rowcount == 0:
            c.execute(
                """
                INSERT INTO equipment_maintenance_schedule (equipment_id, schedule_type, interval_days, interval_hours, last_performed, next_due, notes, active)
                VALUES (%s, %s, %s, %s, NULL, NULL, %s, %s)
                """,
                (equipment_id, schedule_type, interval_days, interval_hours, notes, active),
            )

def log_maintenance_action(equipment_id: int, action: str, details: str = None, status: str = 'completed', performed_by: int = None, document_path: str = None):
    """Log a maintenance action; updates equipment last_maintenance and schedules next_due when completed."""
    with db_transaction() as conn:
        c = conn.cursor()
        c.execute(
            """
            INSERT INTO equipment_maintenance (equipment_id, action, details, status, performed_by, performed_at)
            VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
            RETURNING id
            """,
            (equipment_id, action, details, status, performed_by),
        )
        maint_id = c.fetchone()[0]

        if status == 'completed':
            c.execute("UPDATE equipment_instances SET last_maintenance = CURRENT_TIMESTAMP WHERE id = %s", (equipment_id,))
            # Update schedules: time-based gets next_due by interval, hours-based left for telemetry
            c.execute(
                """
                UPDATE equipment_maintenance_schedule
                SET last_performed = CURRENT_TIMESTAMP,
                    next_due = CASE
                        WHEN schedule_type = 'time' AND interval_days IS NOT NULL THEN CURRENT_TIMESTAMP + (interval_days || ' days')::interval
                        ELSE next_due
                    END
                WHERE equipment_id = %s AND active = TRUE
                """,
                (equipment_id,),
            )
        return maint_id

def attach_maintenance_document(upload_root: str, file_storage, filename_safe: str) -> str:
    os.makedirs(upload_root, exist_ok=True)
    target_path = os.path.join(upload_root, filename_safe)
    file_storage.save(target_path)
    return target_path

def recalc_hours_based_next_due(equipment_id: int, current_hours: int):
    """Recalculate hours-based schedules next_due as a timestamp approximation using avg daily hours (8h/day)."""
    with db_transaction() as conn:
        c = conn.cursor()
        c.execute(
            """
            SELECT id, interval_hours FROM equipment_maintenance_schedule
            WHERE equipment_id = %s AND active = TRUE AND interval_hours IS NOT NULL AND schedule_type = 'hours'
            """,
            (equipment_id,),
        )
        schedules = c.fetchall()
        if not schedules:
            return 0
        updated = 0
        for sched_id, interval_hours in schedules:
            c.execute(
                """
                UPDATE equipment_maintenance_schedule
                SET next_due = CURRENT_TIMESTAMP + make_interval(days => %s)
                WHERE id = %s
                """,
                (max(1, int((interval_hours or 0) / 8)), sched_id),
            )
            updated += 1
        return updated

def get_maintenance_dashboard_counts():
    """Return total under maintenance, due now, due soon (<=14 days)."""
    with db_connection() as conn:
        c = conn.cursor()
        # Safety net: ensure tables exist
        c.execute('''CREATE TABLE IF NOT EXISTS equipment_maintenance_schedule (
            id SERIAL PRIMARY KEY,
            equipment_id INTEGER NOT NULL,
            schedule_type TEXT NOT NULL,
            interval_days INTEGER,
            interval_hours INTEGER,
            last_performed TIMESTAMP,
            next_due TIMESTAMP,
            notes TEXT,
            active BOOLEAN DEFAULT TRUE
        )''')
        conn.commit()
        # Under maintenance count
        c.execute("SELECT COUNT(*) FROM equipment_instances WHERE status = 'under maintenance'")
        under_maint = c.fetchone()[0]

        # Due now (over threshold or schedule next_due <= today)
        c.execute("""
            SELECT COUNT(*) FROM equipment_instances ei
            LEFT JOIN equipment_maintenance_schedule ems ON ems.equipment_id = ei.id AND ems.active = TRUE
            WHERE (ei.maintenance_threshold IS NOT NULL AND ei.hours >= ei.maintenance_threshold)
               OR (ems.next_due IS NOT NULL AND ems.next_due::date <= CURRENT_DATE)
        """)
        due_now = c.fetchone()[0]

        # Due soon within 14 days
        c.execute("""
            SELECT COUNT(*) FROM equipment_maintenance_schedule
            WHERE active = TRUE AND next_due IS NOT NULL AND next_due::date > CURRENT_DATE AND next_due::date <= CURRENT_DATE + INTERVAL '14 days'
        """)
        due_soon = c.fetchone()[0]
        return {"under_maintenance": under_maint, "due_now": due_now, "due_soon": due_soon}

def list_equipment_under_maintenance():
    with db_connection() as conn:
        c = conn.cursor()
        c.execute("""
            SELECT id, unique_id, equipment_type, brand, model, status, last_maintenance
            FROM equipment_instances
            WHERE status = 'under maintenance'
            ORDER BY last_maintenance DESC NULLS LAST
        """)
        return [
            {
                'id': r[0], 'unique_id': r[1], 'equipment_type': r[2], 'brand': r[3], 'model': r[4], 'status': r[5], 'last_maintenance': r[6]
            } for r in c.fetchall()
        ]

def list_maintenance_jobs(filter_status: str = None):
    with db_connection() as conn:
        c = conn.cursor()
        # Safety net: ensure jobs table exists
        c.execute('''CREATE TABLE IF NOT EXISTS equipment_maintenance_jobs (
            id SERIAL PRIMARY KEY,
            equipment_id INTEGER NOT NULL,
            job_type TEXT NOT NULL,
            priority TEXT DEFAULT 'normal',
            status TEXT DEFAULT 'new',
            assigned_to INTEGER,
            due_date DATE,
            notes TEXT,
            created_by INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )''')
        conn.commit()
        if filter_status:
            c.execute(
                """
                SELECT id, equipment_id, job_type, priority, status, assigned_to, due_date, notes, created_at
                FROM equipment_maintenance_jobs
                WHERE status = %s
                ORDER BY priority DESC, due_date ASC NULLS LAST, created_at DESC
                """,
                (filter_status,),
            )
        else:
            c.execute(
                """
                SELECT id, equipment_id, job_type, priority, status, assigned_to, due_date, notes, created_at
                FROM equipment_maintenance_jobs
                ORDER BY priority DESC, due_date ASC NULLS LAST, created_at DESC
                """
            )
        rows = c.fetchall()
        return [
            {
                'id': r[0], 'equipment_id': r[1], 'job_type': r[2], 'priority': r[3], 'status': r[4],
                'assigned_to': r[5], 'due_date': r[6], 'notes': r[7], 'created_at': r[8]
            } for r in rows
        ]

def list_maintenance_jobs_sorted(sort_by: str, assignee: int = None):
    sort_map = {
        'importance': "CASE priority WHEN 'critical' THEN 1 WHEN 'high' THEN 2 WHEN 'normal' THEN 3 ELSE 4 END",
        'received': 'created_at DESC',
        'due': 'due_date ASC NULLS LAST',
        'assigned_to_me': 'assigned_to'
    }
    order_clause = sort_map.get(sort_by, sort_map['importance'])
    with db_connection() as conn:
        c = conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS equipment_maintenance_jobs (
            id SERIAL PRIMARY KEY,
            equipment_id INTEGER NOT NULL,
            job_type TEXT NOT NULL,
            priority TEXT DEFAULT 'normal',
            status TEXT DEFAULT 'new',
            assigned_to INTEGER,
            due_date DATE,
            notes TEXT,
            created_by INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )''')
        conn.commit()
        if assignee:
            c.execute(f"""
                SELECT id, equipment_id, job_type, priority, status, assigned_to, due_date, notes, created_at
                FROM equipment_maintenance_jobs
                WHERE assigned_to = %s
                ORDER BY {order_clause}
            """, (assignee,))
        else:
            c.execute(f"""
                SELECT id, equipment_id, job_type, priority, status, assigned_to, due_date, notes, created_at
                FROM equipment_maintenance_jobs
                ORDER BY {order_clause}
            """)
        rows = c.fetchall()
        return [
            {'id': r[0], 'equipment_id': r[1], 'job_type': r[2], 'priority': r[3], 'status': r[4], 'assigned_to': r[5], 'due_date': r[6], 'notes': r[7], 'created_at': r[8]}
            for r in rows
        ]

def get_maintenance_job(job_id: int):
    with db_connection() as conn:
        c = conn.cursor()
        c.execute("""
            SELECT id, equipment_id, job_type, priority, status, assigned_to, due_date, notes, created_at
            FROM equipment_maintenance_jobs WHERE id = %s
        """, (job_id,))
        r = c.fetchone()
        if not r:
            return None
        return {'id': r[0], 'equipment_id': r[1], 'job_type': r[2], 'priority': r[3], 'status': r[4], 'assigned_to': r[5], 'due_date': r[6], 'notes': r[7], 'created_at': r[8]}

def get_my_jobs(user_id: int, sort_by: str = 'importance'):
    return list_maintenance_jobs_sorted(sort_by, user_id)

def save_maintenance_attachment(job_id: int, equipment_id: int, file_storage, label: str, uploader_id: int):
    from werkzeug.utils import secure_filename
    filename = secure_filename(file_storage.filename)
    path = attach_maintenance_document('Uploads/maintenance', file_storage, filename)
    with db_transaction() as conn:
        c = conn.cursor()
        c.execute(
            """
            INSERT INTO maintenance_attachments (job_id, equipment_id, path, label, uploaded_by)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (job_id, equipment_id, path, label, uploader_id),
        )
        return path

def create_maintenance_job(equipment_id: int, job_type: str, priority: str, due_date: str = None, notes: str = None, assigned_to: int = None, created_by: int = None):
    with db_transaction() as conn:
        c = conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS equipment_maintenance_jobs (
            id SERIAL PRIMARY KEY,
            equipment_id INTEGER NOT NULL,
            job_type TEXT NOT NULL,
            priority TEXT DEFAULT 'normal',
            status TEXT DEFAULT 'new',
            assigned_to INTEGER,
            due_date DATE,
            notes TEXT,
            created_by INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )''')
        c.execute(
            """
            INSERT INTO equipment_maintenance_jobs (equipment_id, job_type, priority, status, assigned_to, due_date, notes, created_by)
            VALUES (%s, %s, %s, 'new', %s, %s, %s, %s)
            RETURNING id
            """,
            (equipment_id, job_type, priority, assigned_to, due_date, notes, created_by),
        )
        job_id = c.fetchone()[0]
        return job_id

def update_maintenance_job(job_id: int, status: str = None, assigned_to: int = None, priority: str = None, due_date: str = None, notes: str = None):
    with db_transaction() as conn:
        c = conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS equipment_maintenance_jobs (
            id SERIAL PRIMARY KEY,
            equipment_id INTEGER NOT NULL,
            job_type TEXT NOT NULL,
            priority TEXT DEFAULT 'normal',
            status TEXT DEFAULT 'new',
            assigned_to INTEGER,
            due_date DATE,
            notes TEXT,
            created_by INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )''')
        sets = []
        params = []
        if status:
            sets.append("status = %s")
            params.append(status)
        if assigned_to is not None:
            sets.append("assigned_to = %s")
            params.append(assigned_to)
        if priority:
            sets.append("priority = %s")
            params.append(priority)
        if due_date:
            sets.append("due_date = %s")
            params.append(due_date)
        if notes is not None:
            sets.append("notes = %s")
            params.append(notes)
        if not sets:
            return False
        params.append(job_id)
        # Add ack/resolution timestamps
        if status == 'in_progress':
            sets.append("acknowledged_at = COALESCE(acknowledged_at, CURRENT_TIMESTAMP)")
        if status == 'completed':
            sets.append("resolved_at = CURRENT_TIMESTAMP")
        c.execute(f"UPDATE equipment_maintenance_jobs SET {', '.join(sets)}, updated_at = CURRENT_TIMESTAMP WHERE id = %s", params)
        return True

# Parts reservation utilities
def list_job_parts(job_id: int):
    with db_connection() as conn:
        c = conn.cursor()
        c.execute(
            """
            SELECT mp.id, mp.consumable_id, c.name, mp.quantity, mp.committed, mp.reserved_at
            FROM maintenance_job_parts mp
            JOIN consumables c ON c.id = mp.consumable_id
            WHERE mp.job_id = %s
            ORDER BY mp.reserved_at DESC
            """,
            (job_id,),
        )
        rows = c.fetchall()
        return [
            {
                'id': r[0], 'consumable_id': r[1], 'name': r[2], 'quantity': r[3], 'committed': r[4], 'reserved_at': r[5]
            } for r in rows
        ]

def reserve_job_part(job_id: int, consumable_id: int, quantity: int):
    if quantity <= 0:
        raise ValueError("Quantity must be positive")
    with db_transaction() as conn:
        c = conn.cursor()
        # Optional: check availability (soft reservation)
        c.execute("SELECT quantity FROM consumables WHERE id = %s", (consumable_id,))
        row = c.fetchone()
        if not row:
            raise ValueError("Consumable not found")
        c.execute(
            """
            INSERT INTO maintenance_job_parts (job_id, consumable_id, quantity, committed)
            VALUES (%s, %s, %s, FALSE)
            RETURNING id
            """,
            (job_id, consumable_id, quantity),
        )
        rid = c.fetchone()[0]
        return rid

def release_job_part(reservation_id: int):
    with db_transaction() as conn:
        c = conn.cursor()
        c.execute("DELETE FROM maintenance_job_parts WHERE id = %s AND committed = FALSE", (reservation_id,))
        deleted = c.rowcount
        return deleted > 0

def commit_job_parts(job_id: int):
    """Deduct reserved parts from inventory and mark committed."""
    with db_transaction() as conn:
        c = conn.cursor()
        # Get uncommitted reservations
        c.execute(
            "SELECT id, consumable_id, quantity FROM maintenance_job_parts WHERE job_id = %s AND committed = FALSE",
            (job_id,),
        )
        reservations = c.fetchall()
        for rid, consumable_id, qty in reservations:
            # Deduct inventory
            c.execute("SELECT quantity FROM consumables WHERE id = %s", (consumable_id,))
            row = c.fetchone()
            current_qty = row[0] if row else 0
            new_qty = max(0, (current_qty or 0) - qty)
            c.execute("UPDATE consumables SET quantity = %s WHERE id = %s", (new_qty, consumable_id))
            # Record transaction
            try:
                c.execute(
                    """
                    INSERT INTO inventory_transactions (consumable_id, transaction_type, quantity_change, quantity_before, quantity_after, notes, performed_by)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """,
                    (consumable_id, 'maintenance_commit', -qty, current_qty, new_qty, f'Committed to maintenance job {job_id}', getattr(current_user, 'id', None)),
                )
            except Exception:
                pass
            # Mark committed
            c.execute("UPDATE maintenance_job_parts SET committed = TRUE WHERE id = %s", (rid,))
        return len(reservations)

# Calendar and workload helpers
def get_jobs_for_calendar():
    with db_connection() as conn:
        c = conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS equipment_maintenance_jobs (
            id SERIAL PRIMARY KEY,
            equipment_id INTEGER NOT NULL,
            job_type TEXT NOT NULL,
            priority TEXT DEFAULT 'normal',
            status TEXT DEFAULT 'new',
            assigned_to INTEGER,
            due_date DATE,
            notes TEXT,
            created_by INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )''')
        conn.commit()
        c.execute(
            """
            SELECT id, equipment_id, job_type, priority, status, due_date
            FROM equipment_maintenance_jobs
            WHERE due_date IS NOT NULL
            """
        )
        rows = c.fetchall()
        events = []
        for jid, eqid, jtype, prio, status, due in rows:
            color = '#2563eb' if jtype == 'maintenance' else '#dc2626'
            events.append({
                'id': jid,
                'title': f"{jtype.title()} #{eqid} ({prio})",
                'start': due.isoformat(),
                'color': color
            })
        return events

def get_workload_summary():
    with db_connection() as conn:
        c = conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS equipment_maintenance_jobs (
            id SERIAL PRIMARY KEY,
            equipment_id INTEGER NOT NULL,
            job_type TEXT NOT NULL,
            priority TEXT DEFAULT 'normal',
            status TEXT DEFAULT 'new',
            assigned_to INTEGER,
            due_date DATE,
            notes TEXT,
            created_by INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )''')
        conn.commit()
        c.execute(
            """
            SELECT COALESCE(assigned_to, 0) as assignee, COUNT(*),
                   SUM(CASE WHEN status = 'new' THEN 1 ELSE 0 END) as new_cnt,
                   SUM(CASE WHEN status = 'in_progress' THEN 1 ELSE 0 END) as in_prog_cnt
            FROM equipment_maintenance_jobs
            GROUP BY assignee
            ORDER BY COUNT(*) DESC
            """
        )
        rows = c.fetchall()
        return [
            {'assigned_to': r[0], 'total': r[1], 'new': r[2], 'in_progress': r[3]}
            for r in rows
        ]

def get_overdue_jobs():
    with db_connection() as conn:
        c = conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS equipment_maintenance_jobs (
            id SERIAL PRIMARY KEY,
            equipment_id INTEGER NOT NULL,
            job_type TEXT NOT NULL,
            priority TEXT DEFAULT 'normal',
            status TEXT DEFAULT 'new',
            assigned_to INTEGER,
            due_date DATE,
            notes TEXT,
            created_by INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )''')
        conn.commit()
        c.execute(
            """
            SELECT id, equipment_id, job_type, priority, status, due_date
            FROM equipment_maintenance_jobs
            WHERE due_date IS NOT NULL AND due_date < CURRENT_DATE AND status NOT IN ('completed','cancelled')
            ORDER BY due_date ASC
            """
        )
        return [
            {'id': r[0], 'equipment_id': r[1], 'job_type': r[2], 'priority': r[3], 'status': r[4], 'due_date': r[5]}
            for r in c.fetchall()
        ]

def send_overdue_notifications():
    overdue = get_overdue_jobs()
    if not overdue:
        return 0
    lines = [f"Job #{j['id']} for equipment #{j['equipment_id']} ({j['job_type']}) was due {j['due_date']} (status: {j['status']})" for j in overdue]
    body = "Overdue Maintenance Jobs:\n\n" + "\n".join(lines)
    try:
        send_notification("Overdue Maintenance Jobs", body, [os.environ.get('MAINT_NOTIF_EMAIL','maintenance@example.com')])
    except Exception:
        pass
    return len(overdue)

def get_equipment_maintenance_history(equipment_id: int):
    with db_connection() as conn:
        c = conn.cursor()
        c.execute("""
            SELECT id, action, details, status, performed_by, performed_at
            FROM equipment_maintenance
            WHERE equipment_id = %s
            ORDER BY performed_at DESC
        """, (equipment_id,))
        rows = c.fetchall()
        return [
            {
                'id': r[0], 'action': r[1], 'details': r[2], 'status': r[3], 'performed_by': r[4], 'performed_at': r[5]
            } for r in rows
        ]

def get_equipment_maintenance_schedule(equipment_id: int):
    with db_connection() as conn:
        c = conn.cursor()
        c.execute("""
            SELECT id, schedule_type, interval_days, interval_hours, last_performed, next_due, notes, active
            FROM equipment_maintenance_schedule
            WHERE equipment_id = %s
            ORDER BY next_due ASC NULLS LAST
        """, (equipment_id,))
        rows = c.fetchall()
        return [
            {
                'id': r[0], 'schedule_type': r[1], 'interval_days': r[2], 'interval_hours': r[3],
                'last_performed': r[4], 'next_due': r[5], 'notes': r[6], 'active': r[7]
            } for r in rows
        ]

def change_equipment_status(equipment_id: int, new_status: str):
    """Update status; block non-available in job checkout logic elsewhere."""
    valid_status = {'active', 'available', 'under maintenance', 'out'}
    if new_status not in valid_status:
        raise ValueError("Invalid status")
    with db_transaction() as conn:
        c = conn.cursor()
        c.execute("UPDATE equipment_instances SET status = %s WHERE id = %s", (new_status, equipment_id))

def check_unique_id_exists(unique_id, exclude_id=None):
    """Check if a unique_id already exists in the database"""
    with db_connection() as conn:
        c = conn.cursor()

        if exclude_id:
            # When editing, exclude the current equipment from the check
            c.execute("SELECT id FROM equipment_instances WHERE unique_id = %s AND id != %s", (unique_id, exclude_id))
        else:
            # When adding new equipment, check if unique_id exists
            c.execute("SELECT id FROM equipment_instances WHERE unique_id = %s", (unique_id,))

        result = c.fetchone()
        return result is not None

def add_equipment(form_data):
    """Add new equipment from form data"""
    # Extract unique_id first to check for duplicates
    unique_id = form_data.get('unique_id', '').strip()

    if not unique_id:
        raise ValueError("Equipment ID (unique_id) is required")

    # Check if unique_id already exists
    if check_unique_id_exists(unique_id):
        raise ValueError(f"Equipment ID '{unique_id}' already exists in the database. Please use a different ID.")

    with db_transaction() as conn:
        c = conn.cursor()
        # Extract form data
        equipment_type = form_data.get('equipment_type')
        brand = form_data.get('brand')
        model = form_data.get('model')
        serial_number = form_data.get('serial_number')
        hours = float(form_data.get('hours', 0))
        fuel_type = form_data.get('fuel_type')
        gross_weight = float(form_data.get('gross_weight', 0)) if form_data.get('gross_weight') else None
        requires_operator = 'requires_operator' in form_data
        required_certification = form_data.get('required_certification')
        status = form_data.get('status', 'available')
        last_maintenance = form_data.get('last_maintenance') if form_data.get('last_maintenance') else None
        maintenance_threshold = int(form_data.get('maintenance_threshold', 1000)) if form_data.get('maintenance_threshold') else None

        c.execute("""INSERT INTO equipment_instances
                     (equipment_type, unique_id, brand, model, serial_number, hours, fuel_type, gross_weight,
                      requires_operator, required_certification, status, last_maintenance, maintenance_threshold)
                     VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id""",
                  (equipment_type, unique_id, brand, model, serial_number, hours, fuel_type, gross_weight,
                   requires_operator, required_certification, status, last_maintenance, maintenance_threshold))
        equipment_id = c.fetchone()[0]

        if current_user.is_authenticated:
            log_audit(current_user.id, "add_equipment", {"equipment_id": equipment_id, "unique_id": unique_id})

        return equipment_id

def update_equipment(equipment_id, form_data):
    """Update equipment from form data"""
    # Extract unique_id first to check for duplicates
    unique_id = form_data.get('unique_id', '').strip()

    if not unique_id:
        raise ValueError("Equipment ID (unique_id) is required")

    # Check if unique_id already exists (excluding current equipment)
    if check_unique_id_exists(unique_id, exclude_id=equipment_id):
        raise ValueError(f"Equipment ID '{unique_id}' already exists in the database. Please use a different ID.")

    with db_transaction() as conn:
        c = conn.cursor()
        # Extract form data
        equipment_type = form_data.get('equipment_type')
        brand = form_data.get('brand')
        model = form_data.get('model')
        serial_number = form_data.get('serial_number')
        hours = float(form_data.get('hours', 0))
        fuel_type = form_data.get('fuel_type')
        gross_weight = float(form_data.get('gross_weight', 0)) if form_data.get('gross_weight') else None
        requires_operator = 'requires_operator' in form_data
        required_certification = form_data.get('required_certification')
        status = form_data.get('status', 'available')
        last_maintenance = form_data.get('last_maintenance') if form_data.get('last_maintenance') else None
        maintenance_threshold = int(form_data.get('maintenance_threshold', 1000)) if form_data.get('maintenance_threshold') else None

        c.execute("""UPDATE equipment_instances SET
                     equipment_type=%s, unique_id=%s, brand=%s, model=%s, serial_number=%s, hours=%s,
                     fuel_type=%s, gross_weight=%s, requires_operator=%s, required_certification=%s,
                     status=%s, last_maintenance=%s, maintenance_threshold=%s
                     WHERE id=%s""",
                  (equipment_type, unique_id, brand, model, serial_number, hours, fuel_type, gross_weight,
                   requires_operator, required_certification, status, last_maintenance, maintenance_threshold, equipment_id))

        if current_user.is_authenticated:
            log_audit(current_user.id, "update_equipment", {"equipment_id": equipment_id, "unique_id": unique_id})

    check_maintenance_alert(equipment_id)

def check_maintenance_alert(equipment_id):
    equipment = get_equipment_detail(equipment_id)
    if equipment and equipment["hours"] >= equipment.get("maintenance_threshold", 1000):
        subject = f"Equipment Maintenance Alert: {equipment['unique_id']}"
        body = f"Equipment '{equipment['unique_id']}' has reached {equipment['hours']} hours (threshold: {equipment.get('maintenance_threshold', 1000)}). Last maintenance: {equipment.get('last_maintenance', 'Never')}."
        # New: Predictive - estimate next failure
        predicted_hours = predict_next_maintenance(equipment_id)
        body += f" Predicted next maintenance at {predicted_hours} hours."
        send_notification(subject, body, NOTIFICATION_RECIPIENTS)
        
        if current_user.is_authenticated:
            log_audit(current_user.id, "maintenance_alert", {
                "equipment_id": equipment_id, 
                "unique_id": equipment["unique_id"], 
                "hours": equipment["hours"]
            })

def predict_next_maintenance(equipment_id):
    # Fetch historical (dummy example)
    historical = np.array([100, 200, 300]).reshape(-1, 1)  # Hours over time
    failures = np.array([1, 2, 3])  # Dummy failures
    model = LinearRegression().fit(historical, failures)
    equipment = get_equipment_detail(equipment_id)
    return model.predict(np.array([[equipment["hours"] + 100]]))[0]  # Predict +100 hours

def check_all_equipment_maintenance():
    """Check maintenance status for all equipment and trigger alerts if needed."""
    equipment_list = get_all_equipment()
    for equipment in equipment_list:
        if equipment["hours"] >= equipment.get("maintenance_threshold", 1000):
            subject = f"Equipment Maintenance Alert: {equipment['unique_id']}"
            body = f"Equipment '{equipment['unique_id']}' has reached {equipment['hours']} hours (threshold: {equipment.get('maintenance_threshold', 1000)}). Last maintenance: {equipment.get('last_maintenance', 'Never')}."
            send_notification(subject, body, NOTIFICATION_RECIPIENTS)
            log_audit(None, "maintenance_alert", {
                "equipment_id": equipment["id"],
                "unique_id": equipment["unique_id"],
                "hours": equipment["hours"]
            })

def calculate_maintenance_due_date(equipment):
    """Calculate when maintenance is due based on hours and threshold"""
    if not equipment.get('maintenance_threshold') or not equipment.get('hours'):
        return None
    
    hours_remaining = equipment['maintenance_threshold'] - equipment['hours']
    if hours_remaining <= 0:
        return "OVERDUE"
    
    # Estimate based on average usage (assume 8 hours per day, 5 days per week)
    avg_hours_per_day = 8
    avg_days_per_week = 5
    estimated_days = (hours_remaining / avg_hours_per_day) * (7 / avg_days_per_week)
    
    if estimated_days <= 0:
        return "OVERDUE"
    
    due_date = datetime.now() + timedelta(days=estimated_days)
    return due_date.strftime('%Y-%m-%d')

def get_equipment_detail(equipment_id):
    with db_connection() as conn:
        c = conn.cursor()
        c.execute("""SELECT id, equipment_type, unique_id, brand, model, serial_number, hours, fuel_type,
                     gross_weight, requires_operator, required_certification, status, last_maintenance, maintenance_threshold
                     FROM equipment_instances WHERE id = %s""", (equipment_id,))
        equipment = c.fetchone()

        if not equipment:
            return None

        equipment_dict = {
            "id": equipment[0], "equipment_type": equipment[1], "unique_id": equipment[2], "brand": equipment[3],
            "model": equipment[4], "serial_number": equipment[5], "hours": equipment[6], "fuel_type": equipment[7],
            "gross_weight": equipment[8], "requires_operator": equipment[9], "required_certification": equipment[10],
            "status": equipment[11], "last_maintenance": equipment[12], "maintenance_threshold": equipment[13]
        }

        # Add calculated maintenance due date
        equipment_dict["maintenance_due"] = calculate_maintenance_due_date(equipment_dict)

        return equipment_dict

def get_all_equipment(search_query=None, sort_by='unique_id', sort_order='asc', limit=100, offset=0):
    """Get all equipment with optional search and sorting"""
    with db_connection() as conn:
        c = conn.cursor()

        # Build the base query with DISTINCT to prevent duplicates
        base_query = """SELECT DISTINCT id, equipment_type, unique_id, brand, model, serial_number, hours, fuel_type,
                        gross_weight, requires_operator, required_certification, status, last_maintenance, maintenance_threshold
                        FROM equipment_instances"""

        params = []
        where_conditions = []

        # Add search conditions
        if search_query:
            search_conditions = [
                "unique_id ILIKE %s",
                "equipment_type ILIKE %s",
                "brand ILIKE %s",
                "model ILIKE %s",
                "serial_number ILIKE %s",
                "status ILIKE %s"
            ]
            where_conditions.append(f"({' OR '.join(search_conditions)})")
            search_param = f"%{search_query}%"
            params.extend([search_param] * len(search_conditions))

        # Add WHERE clause if we have conditions
        if where_conditions:
            base_query += " WHERE " + " AND ".join(where_conditions)

        # Add sorting
        sort_mapping = {
            'unique_id': 'unique_id',
            'equipment_type': 'equipment_type',
            'brand': 'brand',
            'model': 'model',
            'status': 'status',
            'hours': 'hours',
            'maintenance_due': 'maintenance_threshold - hours'  # Sort by hours remaining
        }

        if sort_by in sort_mapping:
            sort_column = sort_mapping[sort_by]
            sort_direction = 'DESC' if sort_order == 'desc' else 'ASC'
            base_query += f" ORDER BY {sort_column} {sort_direction}"

        # Add pagination
        base_query += " LIMIT %s OFFSET %s"
        params.append(limit)
        params.append(offset)

        c.execute(base_query, params)
        rows = c.fetchall()

        # Convert to list of dictionaries and add calculated fields
        equipment_list = []
        seen_ids = set()  # Track unique equipment IDs to prevent duplicates

        for row in rows:
            equipment_id = row[0]

            # Skip if we've already seen this equipment ID
            if equipment_id in seen_ids:
                continue
            seen_ids.add(equipment_id)

            equipment_dict = {
                "id": row[0], "equipment_type": row[1], "unique_id": row[2], "brand": row[3], "model": row[4],
                "serial_number": row[5], "hours": row[6], "fuel_type": row[7], "gross_weight": row[8],
                "requires_operator": bool(row[9]), "required_certification": row[10], "status": row[11],
                "last_maintenance": row[12], "maintenance_threshold": row[13]
            }

            # Add calculated maintenance due date
            equipment_dict["maintenance_due"] = calculate_maintenance_due_date(equipment_dict)

            equipment_list.append(equipment_dict)

        return equipment_list

def get_equipment_by_id(equipment_id):
    return get_equipment_detail(equipment_id)

def delete_equipment(equipment_id):
    with db_transaction() as conn:
        c = conn.cursor()
        c.execute("DELETE FROM equipment_instances WHERE id = %s", (equipment_id,))

    if current_user.is_authenticated:
        log_audit(current_user.id, "delete_equipment", {"equipment_id": equipment_id})


# =============================================================================
# INSPECTION CHECKLISTS - Template Management
# =============================================================================

def list_checklist_templates(equipment_type: str = None):
    """List all checklist templates, optionally filtered by equipment type."""
    with db_connection() as conn:
        c = conn.cursor()
        if equipment_type:
            c.execute("""
                SELECT id, name, equipment_type, description, is_active, created_at
                FROM maintenance_checklist_templates
                WHERE equipment_type = %s OR equipment_type IS NULL
                ORDER BY name
            """, (equipment_type,))
        else:
            c.execute("""
                SELECT id, name, equipment_type, description, is_active, created_at
                FROM maintenance_checklist_templates
                ORDER BY name
            """)
        return [
            {'id': r[0], 'name': r[1], 'equipment_type': r[2], 'description': r[3],
             'is_active': r[4], 'created_at': r[5]}
            for r in c.fetchall()
        ]


def get_checklist_template(template_id: int):
    """Get a single checklist template with its items."""
    with db_connection() as conn:
        c = conn.cursor()
        c.execute("""
            SELECT id, name, equipment_type, description, is_active, created_at
            FROM maintenance_checklist_templates WHERE id = %s
        """, (template_id,))
        row = c.fetchone()
        if not row:
            return None
        template = {
            'id': row[0], 'name': row[1], 'equipment_type': row[2],
            'description': row[3], 'is_active': row[4], 'created_at': row[5]
        }
        c.execute("""
            SELECT id, label, category, default_status, requires_photo, position
            FROM maintenance_checklist_template_items
            WHERE template_id = %s ORDER BY position, id
        """, (template_id,))
        template['items'] = [
            {'id': r[0], 'label': r[1], 'category': r[2], 'default_status': r[3],
             'requires_photo': r[4], 'position': r[5]}
            for r in c.fetchall()
        ]
        return template


def create_checklist_template(name: str, equipment_type: str = None, description: str = None):
    """Create a new checklist template."""
    with db_transaction() as conn:
        c = conn.cursor()
        c.execute("""
            INSERT INTO maintenance_checklist_templates (name, equipment_type, description, is_active)
            VALUES (%s, %s, %s, TRUE) RETURNING id
        """, (name, equipment_type, description))
        template_id = c.fetchone()[0]
        if current_user.is_authenticated:
            log_audit(current_user.id, "create_checklist_template", {"template_id": template_id, "name": name})
        return template_id


def update_checklist_template(template_id: int, name: str = None, equipment_type: str = None,
                               description: str = None, is_active: bool = None):
    """Update a checklist template."""
    with db_transaction() as conn:
        c = conn.cursor()
        updates = []
        params = []
        if name is not None:
            updates.append("name = %s")
            params.append(name)
        if equipment_type is not None:
            updates.append("equipment_type = %s")
            params.append(equipment_type if equipment_type else None)
        if description is not None:
            updates.append("description = %s")
            params.append(description)
        if is_active is not None:
            updates.append("is_active = %s")
            params.append(is_active)
        if updates:
            params.append(template_id)
            c.execute(f"UPDATE maintenance_checklist_templates SET {', '.join(updates)} WHERE id = %s", params)
        return True


def delete_checklist_template(template_id: int):
    """Delete a checklist template and its items."""
    with db_transaction() as conn:
        c = conn.cursor()
        c.execute("DELETE FROM maintenance_checklist_templates WHERE id = %s", (template_id,))
        if current_user.is_authenticated:
            log_audit(current_user.id, "delete_checklist_template", {"template_id": template_id})
        return True


def add_checklist_template_item(template_id: int, label: str, category: str = None,
                                 default_status: str = 'ok', requires_photo: bool = False, position: int = 0):
    """Add an item to a checklist template."""
    with db_transaction() as conn:
        c = conn.cursor()
        c.execute("""
            INSERT INTO maintenance_checklist_template_items
            (template_id, label, category, default_status, requires_photo, position)
            VALUES (%s, %s, %s, %s, %s, %s) RETURNING id
        """, (template_id, label, category, default_status, requires_photo, position))
        return c.fetchone()[0]


def update_checklist_template_item(item_id: int, label: str = None, category: str = None,
                                    default_status: str = None, requires_photo: bool = None, position: int = None):
    """Update a checklist template item."""
    with db_transaction() as conn:
        c = conn.cursor()
        updates = []
        params = []
        if label is not None:
            updates.append("label = %s")
            params.append(label)
        if category is not None:
            updates.append("category = %s")
            params.append(category)
        if default_status is not None:
            updates.append("default_status = %s")
            params.append(default_status)
        if requires_photo is not None:
            updates.append("requires_photo = %s")
            params.append(requires_photo)
        if position is not None:
            updates.append("position = %s")
            params.append(position)
        if updates:
            params.append(item_id)
            c.execute(f"UPDATE maintenance_checklist_template_items SET {', '.join(updates)} WHERE id = %s", params)
        return True


def delete_checklist_template_item(item_id: int):
    """Delete a checklist template item."""
    with db_transaction() as conn:
        c = conn.cursor()
        c.execute("DELETE FROM maintenance_checklist_template_items WHERE id = %s", (item_id,))
        return True


# =============================================================================
# INSPECTIONS - Performing Inspections
# =============================================================================

def create_inspection(equipment_id: int, template_id: int = None, inspector_id: int = None,
                      job_id: int = None, inspection_type: str = 'routine', notes: str = None):
    """Create a new inspection, optionally from a template."""
    with db_transaction() as conn:
        c = conn.cursor()
        c.execute("""
            INSERT INTO maintenance_inspections
            (equipment_id, template_id, inspector_id, job_id, inspection_type, status, notes)
            VALUES (%s, %s, %s, %s, %s, 'in_progress', %s) RETURNING id
        """, (equipment_id, template_id, inspector_id, job_id, inspection_type, notes))
        inspection_id = c.fetchone()[0]

        # If template provided, copy template items to inspection items
        if template_id:
            c.execute("""
                INSERT INTO maintenance_inspection_items (inspection_id, label, category, status, requires_photo, position)
                SELECT %s, label, category, default_status, requires_photo, position
                FROM maintenance_checklist_template_items
                WHERE template_id = %s
                ORDER BY position, id
            """, (inspection_id, template_id))

        if current_user.is_authenticated:
            log_audit(current_user.id, "create_inspection", {"inspection_id": inspection_id, "equipment_id": equipment_id})
        return inspection_id


def get_inspection(inspection_id: int):
    """Get an inspection with its items and media."""
    with db_connection() as conn:
        c = conn.cursor()
        c.execute("""
            SELECT i.id, i.equipment_id, i.template_id, i.inspector_id, i.job_id,
                   i.inspection_type, i.status, i.notes, i.created_at, i.completed_at,
                   e.unique_id, e.brand, e.model, u.username
            FROM maintenance_inspections i
            LEFT JOIN equipment_instances e ON e.id = i.equipment_id
            LEFT JOIN users u ON u.id = i.inspector_id
            WHERE i.id = %s
        """, (inspection_id,))
        row = c.fetchone()
        if not row:
            return None
        inspection = {
            'id': row[0], 'equipment_id': row[1], 'template_id': row[2],
            'inspector_id': row[3], 'job_id': row[4], 'inspection_type': row[5],
            'status': row[6], 'notes': row[7], 'created_at': row[8], 'completed_at': row[9],
            'equipment_unique_id': row[10], 'equipment_brand': row[11],
            'equipment_model': row[12], 'inspector_name': row[13]
        }

        # Get inspection items
        c.execute("""
            SELECT id, label, category, status, notes, requires_photo, position
            FROM maintenance_inspection_items
            WHERE inspection_id = %s ORDER BY position, id
        """, (inspection_id,))
        inspection['items'] = [
            {'id': r[0], 'label': r[1], 'category': r[2], 'status': r[3],
             'notes': r[4], 'requires_photo': r[5], 'position': r[6]}
            for r in c.fetchall()
        ]

        # Get media
        c.execute("""
            SELECT id, item_id, media_path, media_type, caption, uploaded_at
            FROM maintenance_inspection_media
            WHERE inspection_id = %s ORDER BY uploaded_at
        """, (inspection_id,))
        inspection['media'] = [
            {'id': r[0], 'item_id': r[1], 'media_path': r[2], 'media_type': r[3],
             'caption': r[4], 'uploaded_at': r[5]}
            for r in c.fetchall()
        ]
        return inspection


def list_inspections(equipment_id: int = None, status: str = None, limit: int = 50):
    """List inspections with optional filters."""
    with db_connection() as conn:
        c = conn.cursor()
        query = """
            SELECT i.id, i.equipment_id, i.inspection_type, i.status, i.created_at, i.completed_at,
                   e.unique_id, e.brand, e.model, u.username,
                   (SELECT COUNT(*) FROM maintenance_inspection_items WHERE inspection_id = i.id AND status = 'fail') as fail_count
            FROM maintenance_inspections i
            LEFT JOIN equipment_instances e ON e.id = i.equipment_id
            LEFT JOIN users u ON u.id = i.inspector_id
            WHERE 1=1
        """
        params = []
        if equipment_id:
            query += " AND i.equipment_id = %s"
            params.append(equipment_id)
        if status:
            query += " AND i.status = %s"
            params.append(status)
        query += " ORDER BY i.created_at DESC LIMIT %s"
        params.append(limit)
        c.execute(query, params)
        return [
            {'id': r[0], 'equipment_id': r[1], 'inspection_type': r[2], 'status': r[3],
             'created_at': r[4], 'completed_at': r[5], 'equipment_unique_id': r[6],
             'equipment_brand': r[7], 'equipment_model': r[8], 'inspector_name': r[9],
             'fail_count': r[10]}
            for r in c.fetchall()
        ]


def update_inspection_item(item_id: int, status: str = None, notes: str = None):
    """Update an inspection item's status and notes."""
    with db_transaction() as conn:
        c = conn.cursor()
        updates = []
        params = []
        if status is not None:
            updates.append("status = %s")
            params.append(status)
        if notes is not None:
            updates.append("notes = %s")
            params.append(notes)
        if updates:
            params.append(item_id)
            c.execute(f"UPDATE maintenance_inspection_items SET {', '.join(updates)} WHERE id = %s", params)
        return True


def complete_inspection(inspection_id: int, overall_status: str = None, notes: str = None,
                        signature_data: str = None, signature_name: str = None):
    """Complete an inspection with optional e-signature."""
    with db_transaction() as conn:
        c = conn.cursor()
        # Determine overall status based on items if not provided
        if not overall_status:
            c.execute("""
                SELECT COUNT(*) FROM maintenance_inspection_items
                WHERE inspection_id = %s AND status = 'fail'
            """, (inspection_id,))
            fail_count = c.fetchone()[0]
            overall_status = 'failed' if fail_count > 0 else 'passed'

        c.execute("""
            UPDATE maintenance_inspections
            SET status = %s, notes = COALESCE(%s, notes), completed_at = CURRENT_TIMESTAMP,
                signature_data = %s, signature_name = %s
            WHERE id = %s
        """, (overall_status, notes, signature_data, signature_name, inspection_id))

        # If inspection failed, optionally create a maintenance job
        if overall_status == 'failed':
            c.execute("SELECT equipment_id FROM maintenance_inspections WHERE id = %s", (inspection_id,))
            equipment_id = c.fetchone()[0]
            # Auto-create job for failed inspection
            c.execute("""
                INSERT INTO equipment_maintenance_jobs
                (equipment_id, job_type, priority, status, notes, created_by)
                VALUES (%s, 'inspection_failure', 'high', 'new', %s, %s)
                RETURNING id
            """, (equipment_id, f"Auto-created from failed inspection #{inspection_id}",
                  current_user.id if current_user.is_authenticated else None))

        if current_user.is_authenticated:
            log_audit(current_user.id, "complete_inspection", {"inspection_id": inspection_id, "status": overall_status})
        return overall_status


def add_inspection_media(inspection_id: int, item_id: int = None, file_storage=None,
                         media_type: str = 'image', caption: str = None):
    """Add media (photo/video) to an inspection."""
    from werkzeug.utils import secure_filename
    import os

    if not file_storage or not file_storage.filename:
        return None

    filename = secure_filename(file_storage.filename)
    upload_dir = f'Uploads/inspections/{inspection_id}'
    os.makedirs(upload_dir, exist_ok=True)
    filepath = os.path.join(upload_dir, filename)
    file_storage.save(filepath)

    with db_transaction() as conn:
        c = conn.cursor()
        c.execute("""
            INSERT INTO maintenance_inspection_media
            (inspection_id, item_id, media_path, media_type, caption)
            VALUES (%s, %s, %s, %s, %s) RETURNING id
        """, (inspection_id, item_id, filepath, media_type, caption))
        return c.fetchone()[0]


# =============================================================================
# WORK ORDER TEMPLATES & PROCEDURES
# =============================================================================

def list_work_order_templates():
    """List all work order templates."""
    with db_connection() as conn:
        c = conn.cursor()
        c.execute("""
            SELECT id, name, job_type, equipment_type, description, estimated_hours,
                   is_active, created_at
            FROM maintenance_work_order_templates
            ORDER BY name
        """)
        return [
            {'id': r[0], 'name': r[1], 'job_type': r[2], 'equipment_type': r[3],
             'description': r[4], 'estimated_hours': r[5], 'is_active': r[6], 'created_at': r[7]}
            for r in c.fetchall()
        ]


def get_work_order_template(template_id: int):
    """Get a work order template with its steps and required parts."""
    with db_connection() as conn:
        c = conn.cursor()
        c.execute("""
            SELECT id, name, job_type, equipment_type, description, estimated_hours,
                   safety_notes, is_active, created_at
            FROM maintenance_work_order_templates WHERE id = %s
        """, (template_id,))
        row = c.fetchone()
        if not row:
            return None
        template = {
            'id': row[0], 'name': row[1], 'job_type': row[2], 'equipment_type': row[3],
            'description': row[4], 'estimated_hours': row[5], 'safety_notes': row[6],
            'is_active': row[7], 'created_at': row[8]
        }

        # Get procedure steps
        c.execute("""
            SELECT id, step_number, instruction, estimated_minutes, requires_signoff
            FROM maintenance_work_order_template_steps
            WHERE template_id = %s ORDER BY step_number
        """, (template_id,))
        template['steps'] = [
            {'id': r[0], 'step_number': r[1], 'instruction': r[2],
             'estimated_minutes': r[3], 'requires_signoff': r[4]}
            for r in c.fetchall()
        ]

        # Get required parts
        c.execute("""
            SELECT id, consumable_id, quantity, notes
            FROM maintenance_work_order_template_parts
            WHERE template_id = %s
        """, (template_id,))
        template['parts'] = [
            {'id': r[0], 'consumable_id': r[1], 'quantity': r[2], 'notes': r[3]}
            for r in c.fetchall()
        ]
        return template


def create_work_order_template(name: str, job_type: str, equipment_type: str = None,
                                description: str = None, estimated_hours: float = None,
                                safety_notes: str = None):
    """Create a new work order template."""
    with db_transaction() as conn:
        c = conn.cursor()
        c.execute("""
            INSERT INTO maintenance_work_order_templates
            (name, job_type, equipment_type, description, estimated_hours, safety_notes, is_active)
            VALUES (%s, %s, %s, %s, %s, %s, TRUE) RETURNING id
        """, (name, job_type, equipment_type, description, estimated_hours, safety_notes))
        template_id = c.fetchone()[0]
        if current_user.is_authenticated:
            log_audit(current_user.id, "create_wo_template", {"template_id": template_id, "name": name})
        return template_id


def add_work_order_template_step(template_id: int, step_number: int, instruction: str,
                                  estimated_minutes: int = None, requires_signoff: bool = False):
    """Add a step to a work order template."""
    with db_transaction() as conn:
        c = conn.cursor()
        c.execute("""
            INSERT INTO maintenance_work_order_template_steps
            (template_id, step_number, instruction, estimated_minutes, requires_signoff)
            VALUES (%s, %s, %s, %s, %s) RETURNING id
        """, (template_id, step_number, instruction, estimated_minutes, requires_signoff))
        return c.fetchone()[0]


def create_job_from_template(template_id: int, equipment_id: int, due_date: str = None,
                              assigned_to: int = None, priority: str = 'normal'):
    """Create a maintenance job from a work order template."""
    template = get_work_order_template(template_id)
    if not template:
        raise ValueError("Template not found")

    with db_transaction() as conn:
        c = conn.cursor()
        # Create the job
        c.execute("""
            INSERT INTO equipment_maintenance_jobs
            (equipment_id, job_type, priority, status, assigned_to, due_date, notes,
             template_id, estimated_hours, created_by)
            VALUES (%s, %s, %s, 'new', %s, %s, %s, %s, %s, %s)
            RETURNING id
        """, (equipment_id, template['job_type'], priority, assigned_to, due_date,
              template['description'], template_id, template['estimated_hours'],
              current_user.id if current_user.is_authenticated else None))
        job_id = c.fetchone()[0]

        # Copy procedure steps
        for step in template.get('steps', []):
            c.execute("""
                INSERT INTO maintenance_job_steps
                (job_id, step_number, instruction, estimated_minutes, requires_signoff, status)
                VALUES (%s, %s, %s, %s, %s, 'pending')
            """, (job_id, step['step_number'], step['instruction'],
                  step['estimated_minutes'], step['requires_signoff']))

        # Reserve required parts
        for part in template.get('parts', []):
            try:
                reserve_job_part(job_id, part['consumable_id'], part['quantity'])
            except Exception:
                pass  # Part may not be available

        return job_id


# =============================================================================
# JOB PROCEDURES & STEP TRACKING
# =============================================================================

def get_job_steps(job_id: int):
    """Get procedure steps for a job."""
    with db_connection() as conn:
        c = conn.cursor()
        c.execute("""
            SELECT id, step_number, instruction, estimated_minutes, requires_signoff,
                   status, completed_at, completed_by, signature_data, notes
            FROM maintenance_job_steps
            WHERE job_id = %s ORDER BY step_number
        """, (job_id,))
        return [
            {'id': r[0], 'step_number': r[1], 'instruction': r[2], 'estimated_minutes': r[3],
             'requires_signoff': r[4], 'status': r[5], 'completed_at': r[6],
             'completed_by': r[7], 'signature_data': r[8], 'notes': r[9]}
            for r in c.fetchall()
        ]


def complete_job_step(step_id: int, status: str = 'completed', notes: str = None,
                       signature_data: str = None):
    """Mark a job step as completed."""
    with db_transaction() as conn:
        c = conn.cursor()
        c.execute("""
            UPDATE maintenance_job_steps
            SET status = %s, notes = %s, signature_data = %s,
                completed_at = CURRENT_TIMESTAMP, completed_by = %s
            WHERE id = %s
        """, (status, notes, signature_data,
              current_user.id if current_user.is_authenticated else None, step_id))
        return True


def add_job_step(job_id: int, step_number: int, instruction: str,
                  estimated_minutes: int = None, requires_signoff: bool = False):
    """Add an ad-hoc step to an existing job."""
    with db_transaction() as conn:
        c = conn.cursor()
        c.execute("""
            INSERT INTO maintenance_job_steps
            (job_id, step_number, instruction, estimated_minutes, requires_signoff, status)
            VALUES (%s, %s, %s, %s, %s, 'pending') RETURNING id
        """, (job_id, step_number, instruction, estimated_minutes, requires_signoff))
        return c.fetchone()[0]


# =============================================================================
# JOB ATTACHMENTS & DOCUMENTS
# =============================================================================

def list_job_attachments(job_id: int):
    """List all attachments for a job."""
    with db_connection() as conn:
        c = conn.cursor()
        c.execute("""
            SELECT id, path, label, file_type, uploaded_by, uploaded_at
            FROM maintenance_attachments
            WHERE job_id = %s ORDER BY uploaded_at DESC
        """, (job_id,))
        return [
            {'id': r[0], 'path': r[1], 'label': r[2], 'file_type': r[3],
             'uploaded_by': r[4], 'uploaded_at': r[5]}
            for r in c.fetchall()
        ]


def add_job_attachment(job_id: int, equipment_id: int, file_storage, label: str = None,
                        file_type: str = None, uploader_id: int = None):
    """Add an attachment to a job."""
    from werkzeug.utils import secure_filename
    import os

    if not file_storage or not file_storage.filename:
        return None

    filename = secure_filename(file_storage.filename)
    upload_dir = f'Uploads/maintenance/jobs/{job_id}'
    os.makedirs(upload_dir, exist_ok=True)
    filepath = os.path.join(upload_dir, filename)
    file_storage.save(filepath)

    if not file_type:
        ext = filename.rsplit('.', 1)[-1].lower() if '.' in filename else ''
        if ext in ['jpg', 'jpeg', 'png', 'gif']:
            file_type = 'image'
        elif ext in ['pdf']:
            file_type = 'pdf'
        elif ext in ['doc', 'docx']:
            file_type = 'document'
        else:
            file_type = 'other'

    with db_transaction() as conn:
        c = conn.cursor()
        c.execute("""
            INSERT INTO maintenance_attachments
            (job_id, equipment_id, path, label, file_type, uploaded_by)
            VALUES (%s, %s, %s, %s, %s, %s) RETURNING id
        """, (job_id, equipment_id, filepath, label or filename, file_type, uploader_id))
        return c.fetchone()[0]


# =============================================================================
# E-SIGNATURES
# =============================================================================

def add_job_signature(job_id: int, signature_type: str, signature_data: str,
                       signer_name: str, signer_id: int = None, notes: str = None):
    """Add an e-signature to a job (start, completion, approval, etc.)."""
    with db_transaction() as conn:
        c = conn.cursor()
        c.execute("""
            INSERT INTO maintenance_job_signatures
            (job_id, signature_type, signature_data, signer_name, signer_id, notes)
            VALUES (%s, %s, %s, %s, %s, %s) RETURNING id
        """, (job_id, signature_type, signature_data, signer_name, signer_id, notes))
        return c.fetchone()[0]


def get_job_signatures(job_id: int):
    """Get all signatures for a job."""
    with db_connection() as conn:
        c = conn.cursor()
        c.execute("""
            SELECT id, signature_type, signer_name, signer_id, signed_at, notes
            FROM maintenance_job_signatures
            WHERE job_id = %s ORDER BY signed_at
        """, (job_id,))
        return [
            {'id': r[0], 'signature_type': r[1], 'signer_name': r[2],
             'signer_id': r[3], 'signed_at': r[4], 'notes': r[5]}
            for r in c.fetchall()
        ]


# =============================================================================
# REPORTING & ANALYTICS
# =============================================================================

def get_maintenance_kpis(start_date: str = None, end_date: str = None):
    """Calculate key maintenance KPIs."""
    with db_connection() as conn:
        c = conn.cursor()

        # Date filter clause
        date_clause = ""
        params = []
        if start_date and end_date:
            date_clause = "AND created_at BETWEEN %s AND %s"
            params = [start_date, end_date]

        # Total jobs
        c.execute(f"SELECT COUNT(*) FROM equipment_maintenance_jobs WHERE 1=1 {date_clause}", params)
        total_jobs = c.fetchone()[0]

        # Completed jobs
        c.execute(f"SELECT COUNT(*) FROM equipment_maintenance_jobs WHERE status = 'completed' {date_clause}", params)
        completed_jobs = c.fetchone()[0]

        # Jobs by type
        c.execute(f"""
            SELECT job_type, COUNT(*) FROM equipment_maintenance_jobs
            WHERE 1=1 {date_clause} GROUP BY job_type
        """, params)
        jobs_by_type = {r[0]: r[1] for r in c.fetchall()}

        # Planned vs reactive (breakdown = reactive)
        planned = jobs_by_type.get('maintenance', 0) + jobs_by_type.get('preventive', 0)
        reactive = jobs_by_type.get('breakdown', 0) + jobs_by_type.get('emergency', 0)
        total_categorized = planned + reactive
        planned_pct = round((planned / total_categorized * 100), 1) if total_categorized > 0 else 0

        # Average time to complete (MTTR approximation)
        c.execute(f"""
            SELECT AVG(EXTRACT(EPOCH FROM (resolved_at - created_at)) / 3600)
            FROM equipment_maintenance_jobs
            WHERE status = 'completed' AND resolved_at IS NOT NULL {date_clause}
        """, params)
        avg_completion_hours = c.fetchone()[0] or 0

        # Overdue jobs
        c.execute("""
            SELECT COUNT(*) FROM equipment_maintenance_jobs
            WHERE status NOT IN ('completed', 'cancelled')
            AND due_date < CURRENT_DATE
        """)
        overdue_count = c.fetchone()[0]

        # Jobs by priority
        c.execute(f"""
            SELECT priority, COUNT(*) FROM equipment_maintenance_jobs
            WHERE 1=1 {date_clause} GROUP BY priority
        """, params)
        jobs_by_priority = {r[0]: r[1] for r in c.fetchall()}

        # Equipment with most issues
        c.execute(f"""
            SELECT e.unique_id, e.brand, e.model, COUNT(j.id) as job_count
            FROM equipment_maintenance_jobs j
            JOIN equipment_instances e ON e.id = j.equipment_id
            WHERE j.job_type = 'breakdown' {date_clause.replace('created_at', 'j.created_at')}
            GROUP BY e.id, e.unique_id, e.brand, e.model
            ORDER BY job_count DESC LIMIT 10
        """, params)
        top_breakdown_equipment = [
            {'unique_id': r[0], 'brand': r[1], 'model': r[2], 'breakdown_count': r[3]}
            for r in c.fetchall()
        ]

        # Technician workload
        c.execute(f"""
            SELECT u.username, COUNT(j.id) as total,
                   SUM(CASE WHEN j.status = 'completed' THEN 1 ELSE 0 END) as completed
            FROM equipment_maintenance_jobs j
            LEFT JOIN users u ON u.id = j.assigned_to
            WHERE j.assigned_to IS NOT NULL {date_clause.replace('created_at', 'j.created_at')}
            GROUP BY u.id, u.username
            ORDER BY total DESC
        """, params)
        technician_stats = [
            {'username': r[0] or 'Unassigned', 'total': r[1], 'completed': r[2]}
            for r in c.fetchall()
        ]

        # Labor hours logged
        c.execute(f"""
            SELECT COALESCE(SUM(hours), 0) FROM maintenance_time_logs
            WHERE 1=1 {date_clause.replace('created_at', 'work_date')}
        """, params)
        total_labor_hours = float(c.fetchone()[0] or 0)

        # Parts cost (from committed parts)
        c.execute(f"""
            SELECT COALESCE(SUM(mp.quantity * COALESCE(c.unit_cost, 0)), 0)
            FROM maintenance_job_parts mp
            JOIN consumables c ON c.id = mp.consumable_id
            JOIN equipment_maintenance_jobs j ON j.id = mp.job_id
            WHERE mp.committed = TRUE {date_clause.replace('created_at', 'j.created_at')}
        """, params)
        total_parts_cost = float(c.fetchone()[0] or 0)

        return {
            'total_jobs': total_jobs,
            'completed_jobs': completed_jobs,
            'completion_rate': round((completed_jobs / total_jobs * 100), 1) if total_jobs > 0 else 0,
            'planned_maintenance_pct': planned_pct,
            'reactive_maintenance_pct': round(100 - planned_pct, 1) if planned_pct else 0,
            'avg_completion_hours': round(avg_completion_hours, 1),
            'overdue_count': overdue_count,
            'jobs_by_type': jobs_by_type,
            'jobs_by_priority': jobs_by_priority,
            'top_breakdown_equipment': top_breakdown_equipment,
            'technician_stats': technician_stats,
            'total_labor_hours': round(total_labor_hours, 1),
            'total_parts_cost': round(total_parts_cost, 2)
        }


def get_equipment_health_summary():
    """Get health status for all equipment."""
    with db_connection() as conn:
        c = conn.cursor()
        c.execute("""
            SELECT
                e.id, e.unique_id, e.brand, e.model, e.equipment_type, e.status,
                e.hours, e.maintenance_threshold, e.last_maintenance,
                (SELECT COUNT(*) FROM equipment_maintenance_jobs j
                 WHERE j.equipment_id = e.id AND j.job_type = 'breakdown'
                 AND j.created_at > CURRENT_DATE - INTERVAL '90 days') as recent_breakdowns,
                (SELECT MAX(created_at) FROM equipment_maintenance_jobs j
                 WHERE j.equipment_id = e.id AND j.job_type = 'breakdown') as last_breakdown
            FROM equipment_instances e
            ORDER BY recent_breakdowns DESC, e.unique_id
        """)
        equipment_list = []
        for r in c.fetchall():
            hours = r[6] or 0
            threshold = r[7] or 1000
            hours_remaining = threshold - hours
            recent_breakdowns = r[9] or 0

            # Calculate health score (0-100)
            health_score = 100
            if hours_remaining <= 0:
                health_score -= 40
            elif hours_remaining < threshold * 0.1:
                health_score -= 20
            health_score -= min(30, recent_breakdowns * 10)
            if r[5] == 'under maintenance':
                health_score -= 10
            health_score = max(0, health_score)

            # Determine status color
            if health_score >= 70:
                health_status = 'good'
            elif health_score >= 40:
                health_status = 'warning'
            else:
                health_status = 'critical'

            equipment_list.append({
                'id': r[0], 'unique_id': r[1], 'brand': r[2], 'model': r[3],
                'equipment_type': r[4], 'status': r[5], 'hours': hours,
                'maintenance_threshold': threshold, 'hours_remaining': hours_remaining,
                'last_maintenance': r[8], 'recent_breakdowns': recent_breakdowns,
                'last_breakdown': r[10], 'health_score': health_score,
                'health_status': health_status
            })
        return equipment_list


def get_maintenance_trends(months: int = 12):
    """Get monthly maintenance trends."""
    with db_connection() as conn:
        c = conn.cursor()
        c.execute("""
            SELECT
                DATE_TRUNC('month', created_at) as month,
                COUNT(*) as total_jobs,
                SUM(CASE WHEN job_type = 'breakdown' THEN 1 ELSE 0 END) as breakdowns,
                SUM(CASE WHEN job_type IN ('maintenance', 'preventive') THEN 1 ELSE 0 END) as planned,
                SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed
            FROM equipment_maintenance_jobs
            WHERE created_at >= CURRENT_DATE - INTERVAL '%s months'
            GROUP BY DATE_TRUNC('month', created_at)
            ORDER BY month
        """, (months,))
        return [
            {'month': r[0].strftime('%Y-%m') if r[0] else '', 'total': r[1],
             'breakdowns': r[2], 'planned': r[3], 'completed': r[4]}
            for r in c.fetchall()
        ]


def export_maintenance_report(start_date: str, end_date: str, format: str = 'csv'):
    """Generate exportable maintenance report data."""
    kpis = get_maintenance_kpis(start_date, end_date)
    trends = get_maintenance_trends(12)
    health = get_equipment_health_summary()

    return {
        'period': {'start': start_date, 'end': end_date},
        'kpis': kpis,
        'trends': trends,
        'equipment_health': health
    }


# =============================================================================
# VENDOR & CONTRACTOR MANAGEMENT
# =============================================================================

def list_vendors(vendor_type: str = None):
    """List all vendors/contractors."""
    with db_connection() as conn:
        c = conn.cursor()
        query = """
            SELECT id, name, vendor_type, contact_name, contact_email, contact_phone,
                   address, specialties, rating, is_active, created_at
            FROM maintenance_vendors
            WHERE 1=1
        """
        params = []
        if vendor_type:
            query += " AND vendor_type = %s"
            params.append(vendor_type)
        query += " ORDER BY name"
        c.execute(query, params)
        return [
            {'id': r[0], 'name': r[1], 'vendor_type': r[2], 'contact_name': r[3],
             'contact_email': r[4], 'contact_phone': r[5], 'address': r[6],
             'specialties': r[7], 'rating': r[8], 'is_active': r[9], 'created_at': r[10]}
            for r in c.fetchall()
        ]


def get_vendor(vendor_id: int):
    """Get a vendor with their job history."""
    with db_connection() as conn:
        c = conn.cursor()
        c.execute("""
            SELECT id, name, vendor_type, contact_name, contact_email, contact_phone,
                   address, specialties, rating, notes, is_active, created_at,
                   insurance_expiry, contract_expiry
            FROM maintenance_vendors WHERE id = %s
        """, (vendor_id,))
        row = c.fetchone()
        if not row:
            return None
        vendor = {
            'id': row[0], 'name': row[1], 'vendor_type': row[2], 'contact_name': row[3],
            'contact_email': row[4], 'contact_phone': row[5], 'address': row[6],
            'specialties': row[7], 'rating': row[8], 'notes': row[9], 'is_active': row[10],
            'created_at': row[11], 'insurance_expiry': row[12], 'contract_expiry': row[13]
        }

        # Get job history
        c.execute("""
            SELECT j.id, j.equipment_id, j.job_type, j.status, j.created_at,
                   e.unique_id, vj.cost, vj.completed_at
            FROM maintenance_vendor_jobs vj
            JOIN equipment_maintenance_jobs j ON j.id = vj.job_id
            LEFT JOIN equipment_instances e ON e.id = j.equipment_id
            WHERE vj.vendor_id = %s
            ORDER BY vj.created_at DESC LIMIT 20
        """, (vendor_id,))
        vendor['jobs'] = [
            {'job_id': r[0], 'equipment_id': r[1], 'job_type': r[2], 'status': r[3],
             'created_at': r[4], 'equipment_unique_id': r[5], 'cost': r[6], 'completed_at': r[7]}
            for r in c.fetchall()
        ]
        return vendor


def create_vendor(name: str, vendor_type: str, contact_name: str = None,
                   contact_email: str = None, contact_phone: str = None,
                   address: str = None, specialties: str = None, notes: str = None):
    """Create a new vendor."""
    with db_transaction() as conn:
        c = conn.cursor()
        c.execute("""
            INSERT INTO maintenance_vendors
            (name, vendor_type, contact_name, contact_email, contact_phone,
             address, specialties, notes, is_active)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, TRUE) RETURNING id
        """, (name, vendor_type, contact_name, contact_email, contact_phone,
              address, specialties, notes))
        vendor_id = c.fetchone()[0]
        if current_user.is_authenticated:
            log_audit(current_user.id, "create_vendor", {"vendor_id": vendor_id, "name": name})
        return vendor_id


def update_vendor(vendor_id: int, **kwargs):
    """Update a vendor."""
    allowed_fields = ['name', 'vendor_type', 'contact_name', 'contact_email',
                      'contact_phone', 'address', 'specialties', 'notes',
                      'is_active', 'rating', 'insurance_expiry', 'contract_expiry']
    with db_transaction() as conn:
        c = conn.cursor()
        updates = []
        params = []
        for field in allowed_fields:
            if field in kwargs:
                updates.append(f"{field} = %s")
                params.append(kwargs[field])
        if updates:
            params.append(vendor_id)
            c.execute(f"UPDATE maintenance_vendors SET {', '.join(updates)} WHERE id = %s", params)
        return True


def assign_job_to_vendor(job_id: int, vendor_id: int, estimated_cost: float = None, notes: str = None):
    """Assign a maintenance job to an external vendor."""
    with db_transaction() as conn:
        c = conn.cursor()
        c.execute("""
            INSERT INTO maintenance_vendor_jobs
            (job_id, vendor_id, estimated_cost, notes, status)
            VALUES (%s, %s, %s, %s, 'assigned') RETURNING id
        """, (job_id, vendor_id, estimated_cost, notes))
        assignment_id = c.fetchone()[0]

        # Update job to reflect vendor assignment
        c.execute("""
            UPDATE equipment_maintenance_jobs
            SET vendor_id = %s, notes = COALESCE(notes, '') || %s
            WHERE id = %s
        """, (vendor_id, f'\nAssigned to vendor #{vendor_id}', job_id))

        return assignment_id


# =============================================================================
# COMPLIANCE & CERTIFICATIONS
# =============================================================================

def list_equipment_certifications(equipment_id: int = None):
    """List certifications/compliance records for equipment."""
    with db_connection() as conn:
        c = conn.cursor()
        query = """
            SELECT ec.id, ec.equipment_id, ec.certification_type, ec.certification_number,
                   ec.issued_date, ec.expiry_date, ec.status, ec.notes,
                   e.unique_id, e.brand, e.model
            FROM equipment_certifications ec
            JOIN equipment_instances e ON e.id = ec.equipment_id
            WHERE 1=1
        """
        params = []
        if equipment_id:
            query += " AND ec.equipment_id = %s"
            params.append(equipment_id)
        query += " ORDER BY ec.expiry_date ASC"
        c.execute(query, params)
        return [
            {'id': r[0], 'equipment_id': r[1], 'certification_type': r[2],
             'certification_number': r[3], 'issued_date': r[4], 'expiry_date': r[5],
             'status': r[6], 'notes': r[7], 'equipment_unique_id': r[8],
             'equipment_brand': r[9], 'equipment_model': r[10]}
            for r in c.fetchall()
        ]


def add_equipment_certification(equipment_id: int, certification_type: str,
                                 certification_number: str = None, issued_date: str = None,
                                 expiry_date: str = None, notes: str = None):
    """Add a certification record for equipment."""
    with db_transaction() as conn:
        c = conn.cursor()
        c.execute("""
            INSERT INTO equipment_certifications
            (equipment_id, certification_type, certification_number, issued_date, expiry_date, status, notes)
            VALUES (%s, %s, %s, %s, %s, 'valid', %s) RETURNING id
        """, (equipment_id, certification_type, certification_number, issued_date, expiry_date, notes))
        return c.fetchone()[0]


def get_expiring_certifications(days_ahead: int = 30):
    """Get certifications expiring within specified days."""
    with db_connection() as conn:
        c = conn.cursor()
        c.execute("""
            SELECT ec.id, ec.equipment_id, ec.certification_type, ec.expiry_date,
                   e.unique_id, e.brand, e.model,
                   (ec.expiry_date - CURRENT_DATE) as days_until_expiry
            FROM equipment_certifications ec
            JOIN equipment_instances e ON e.id = ec.equipment_id
            WHERE ec.status = 'valid'
            AND ec.expiry_date BETWEEN CURRENT_DATE AND CURRENT_DATE + INTERVAL '%s days'
            ORDER BY ec.expiry_date
        """, (days_ahead,))
        return [
            {'id': r[0], 'equipment_id': r[1], 'certification_type': r[2],
             'expiry_date': r[3], 'equipment_unique_id': r[4], 'equipment_brand': r[5],
             'equipment_model': r[6], 'days_until_expiry': r[7]}
            for r in c.fetchall()
        ]


def list_warranty_records(equipment_id: int = None, active_only: bool = True):
    """List warranty records for equipment."""
    with db_connection() as conn:
        c = conn.cursor()
        query = """
            SELECT w.id, w.equipment_id, w.warranty_type, w.provider,
                   w.start_date, w.end_date, w.coverage_details, w.notes,
                   e.unique_id, e.brand, e.model
            FROM equipment_warranties w
            JOIN equipment_instances e ON e.id = w.equipment_id
            WHERE 1=1
        """
        params = []
        if equipment_id:
            query += " AND w.equipment_id = %s"
            params.append(equipment_id)
        if active_only:
            query += " AND w.end_date >= CURRENT_DATE"
        query += " ORDER BY w.end_date ASC"
        c.execute(query, params)
        return [
            {'id': r[0], 'equipment_id': r[1], 'warranty_type': r[2], 'provider': r[3],
             'start_date': r[4], 'end_date': r[5], 'coverage_details': r[6], 'notes': r[7],
             'equipment_unique_id': r[8], 'equipment_brand': r[9], 'equipment_model': r[10]}
            for r in c.fetchall()
        ]


def add_warranty_record(equipment_id: int, warranty_type: str, provider: str,
                         start_date: str, end_date: str, coverage_details: str = None, notes: str = None):
    """Add a warranty record for equipment."""
    with db_transaction() as conn:
        c = conn.cursor()
        c.execute("""
            INSERT INTO equipment_warranties
            (equipment_id, warranty_type, provider, start_date, end_date, coverage_details, notes)
            VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING id
        """, (equipment_id, warranty_type, provider, start_date, end_date, coverage_details, notes))
        return c.fetchone()[0]


# =============================================================================
# 311/CITIZEN REQUEST PORTAL
# =============================================================================

def create_citizen_request(request_type: str, description: str, location: str = None,
                            latitude: float = None, longitude: float = None,
                            reporter_name: str = None, reporter_email: str = None,
                            reporter_phone: str = None, photo_path: str = None):
    """Create a citizen service request (311-style)."""
    with db_transaction() as conn:
        c = conn.cursor()
        c.execute("""
            INSERT INTO citizen_requests
            (request_type, description, location, latitude, longitude,
             reporter_name, reporter_email, reporter_phone, photo_path, status)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'new')
            RETURNING id
        """, (request_type, description, location, latitude, longitude,
              reporter_name, reporter_email, reporter_phone, photo_path))
        request_id = c.fetchone()[0]
        return request_id


def list_citizen_requests(status: str = None, request_type: str = None, limit: int = 50):
    """List citizen requests with optional filters."""
    with db_connection() as conn:
        c = conn.cursor()
        query = """
            SELECT id, request_type, description, location, status,
                   reporter_name, created_at, job_id
            FROM citizen_requests
            WHERE 1=1
        """
        params = []
        if status:
            query += " AND status = %s"
            params.append(status)
        if request_type:
            query += " AND request_type = %s"
            params.append(request_type)
        query += " ORDER BY created_at DESC LIMIT %s"
        params.append(limit)
        c.execute(query, params)
        return [
            {'id': r[0], 'request_type': r[1], 'description': r[2], 'location': r[3],
             'status': r[4], 'reporter_name': r[5], 'created_at': r[6], 'job_id': r[7]}
            for r in c.fetchall()
        ]


def get_citizen_request(request_id: int):
    """Get a citizen request with full details."""
    with db_connection() as conn:
        c = conn.cursor()
        c.execute("""
            SELECT id, request_type, description, location, latitude, longitude,
                   reporter_name, reporter_email, reporter_phone, photo_path,
                   status, job_id, resolution_notes, created_at, resolved_at
            FROM citizen_requests WHERE id = %s
        """, (request_id,))
        row = c.fetchone()
        if not row:
            return None
        return {
            'id': row[0], 'request_type': row[1], 'description': row[2],
            'location': row[3], 'latitude': row[4], 'longitude': row[5],
            'reporter_name': row[6], 'reporter_email': row[7], 'reporter_phone': row[8],
            'photo_path': row[9], 'status': row[10], 'job_id': row[11],
            'resolution_notes': row[12], 'created_at': row[13], 'resolved_at': row[14]
        }


def convert_request_to_job(request_id: int, equipment_id: int = None, priority: str = 'normal',
                            assigned_to: int = None):
    """Convert a citizen request to a maintenance job."""
    request = get_citizen_request(request_id)
    if not request:
        raise ValueError("Request not found")

    with db_transaction() as conn:
        c = conn.cursor()
        # Create maintenance job
        notes = f"From Citizen Request #{request_id}\nLocation: {request['location']}\n{request['description']}"
        c.execute("""
            INSERT INTO equipment_maintenance_jobs
            (equipment_id, job_type, priority, status, assigned_to, notes, created_by)
            VALUES (%s, 'citizen_request', %s, 'new', %s, %s, %s)
            RETURNING id
        """, (equipment_id, priority, assigned_to, notes,
              current_user.id if current_user.is_authenticated else None))
        job_id = c.fetchone()[0]

        # Link request to job
        c.execute("""
            UPDATE citizen_requests
            SET job_id = %s, status = 'assigned'
            WHERE id = %s
        """, (job_id, request_id))

        return job_id


def update_citizen_request_status(request_id: int, status: str, resolution_notes: str = None):
    """Update citizen request status."""
    with db_transaction() as conn:
        c = conn.cursor()
        if status == 'resolved':
            c.execute("""
                UPDATE citizen_requests
                SET status = %s, resolution_notes = %s, resolved_at = CURRENT_TIMESTAMP
                WHERE id = %s
            """, (status, resolution_notes, request_id))
        else:
            c.execute("""
                UPDATE citizen_requests
                SET status = %s, resolution_notes = COALESCE(%s, resolution_notes)
                WHERE id = %s
            """, (status, resolution_notes, request_id))
        return True


# =============================================================================
# TELEMATICS & IOT INTEGRATION
# =============================================================================

def record_telematics_data(equipment_id: int, data_type: str, value: float,
                            unit: str = None, source: str = None, raw_data: dict = None):
    """Record telematics data from IoT devices."""
    import json
    with db_transaction() as conn:
        c = conn.cursor()
        c.execute("""
            INSERT INTO equipment_telematics
            (equipment_id, data_type, value, unit, source, raw_data)
            VALUES (%s, %s, %s, %s, %s, %s) RETURNING id
        """, (equipment_id, data_type, value, unit, source,
              json.dumps(raw_data) if raw_data else None))
        record_id = c.fetchone()[0]

        # Update equipment hours if data_type is 'engine_hours'
        if data_type == 'engine_hours':
            c.execute("""
                UPDATE equipment_instances SET hours = %s WHERE id = %s
            """, (value, equipment_id))
            # Recalculate hours-based maintenance schedules
            recalc_hours_based_next_due(equipment_id, int(value))

        # Check for alerts
        check_telematics_alerts(equipment_id, data_type, value)

        return record_id


def get_telematics_history(equipment_id: int, data_type: str = None,
                           hours: int = 24, limit: int = 100):
    """Get recent telematics data for equipment."""
    with db_connection() as conn:
        c = conn.cursor()
        query = """
            SELECT id, data_type, value, unit, source, recorded_at
            FROM equipment_telematics
            WHERE equipment_id = %s
            AND recorded_at > CURRENT_TIMESTAMP - INTERVAL '%s hours'
        """
        params = [equipment_id, hours]
        if data_type:
            query += " AND data_type = %s"
            params.append(data_type)
        query += " ORDER BY recorded_at DESC LIMIT %s"
        params.append(limit)
        c.execute(query, params)
        return [
            {'id': r[0], 'data_type': r[1], 'value': float(r[2]),
             'unit': r[3], 'source': r[4], 'recorded_at': r[5]}
            for r in c.fetchall()
        ]


def get_equipment_location(equipment_id: int):
    """Get latest GPS location for equipment."""
    with db_connection() as conn:
        c = conn.cursor()
        c.execute("""
            SELECT latitude, longitude, recorded_at
            FROM equipment_telematics
            WHERE equipment_id = %s AND data_type = 'gps_location'
            ORDER BY recorded_at DESC LIMIT 1
        """, (equipment_id,))
        row = c.fetchone()
        if row:
            return {'latitude': row[0], 'longitude': row[1], 'recorded_at': row[2]}
        return None


def get_fleet_locations():
    """Get latest locations for all equipment with GPS."""
    with db_connection() as conn:
        c = conn.cursor()
        c.execute("""
            SELECT DISTINCT ON (e.id)
                e.id, e.unique_id, e.brand, e.model, e.status,
                t.value as location_data, t.recorded_at
            FROM equipment_instances e
            LEFT JOIN equipment_telematics t ON t.equipment_id = e.id AND t.data_type = 'gps_location'
            ORDER BY e.id, t.recorded_at DESC
        """)
        return [
            {'id': r[0], 'unique_id': r[1], 'brand': r[2], 'model': r[3],
             'status': r[4], 'location_data': r[5], 'recorded_at': r[6]}
            for r in c.fetchall()
        ]


def check_telematics_alerts(equipment_id: int, data_type: str, value: float):
    """Check if telematics data triggers any alerts."""
    with db_connection() as conn:
        c = conn.cursor()
        # Get alert thresholds
        c.execute("""
            SELECT id, alert_type, threshold_min, threshold_max, alert_message
            FROM telematics_alert_rules
            WHERE equipment_type IS NULL OR equipment_type = (
                SELECT equipment_type FROM equipment_instances WHERE id = %s
            )
            AND data_type = %s AND is_active = TRUE
        """, (equipment_id, data_type))

        for rule in c.fetchall():
            rule_id, alert_type, threshold_min, threshold_max, message = rule
            triggered = False
            if threshold_min is not None and value < threshold_min:
                triggered = True
            if threshold_max is not None and value > threshold_max:
                triggered = True

            if triggered:
                # Create alert
                c.execute("""
                    INSERT INTO telematics_alerts
                    (equipment_id, rule_id, data_type, value, alert_message, status)
                    VALUES (%s, %s, %s, %s, %s, 'new')
                """, (equipment_id, rule_id, data_type, value, message))

                # Optionally create maintenance job for critical alerts
                if alert_type == 'critical':
                    equipment = get_equipment_detail(equipment_id)
                    create_maintenance_job(
                        equipment_id, 'breakdown', 'critical',
                        notes=f"Auto-created from telematics alert: {message}\nValue: {value}"
                    )


def create_geofence(name: str, coordinates: list, equipment_ids: list = None, alert_on_exit: bool = True):
    """Create a geofence zone."""
    import json
    with db_transaction() as conn:
        c = conn.cursor()
        c.execute("""
            INSERT INTO geofences
            (name, coordinates, alert_on_exit, is_active)
            VALUES (%s, %s, %s, TRUE) RETURNING id
        """, (name, json.dumps(coordinates), alert_on_exit))
        geofence_id = c.fetchone()[0]

        # Assign equipment to geofence
        if equipment_ids:
            for eq_id in equipment_ids:
                c.execute("""
                    INSERT INTO geofence_equipment (geofence_id, equipment_id)
                    VALUES (%s, %s)
                """, (geofence_id, eq_id))

        return geofence_id


# =============================================================================
# PREDICTIVE MAINTENANCE (Enhanced)
# =============================================================================

def get_equipment_failure_history(equipment_id: int = None, equipment_type: str = None):
    """Get failure history for predictive analysis."""
    with db_connection() as conn:
        c = conn.cursor()
        query = """
            SELECT e.id, e.equipment_type, e.brand, e.model,
                   j.created_at, j.job_type,
                   (SELECT hours FROM equipment_telematics
                    WHERE equipment_id = e.id AND data_type = 'engine_hours'
                    AND recorded_at <= j.created_at
                    ORDER BY recorded_at DESC LIMIT 1) as hours_at_failure
            FROM equipment_maintenance_jobs j
            JOIN equipment_instances e ON e.id = j.equipment_id
            WHERE j.job_type = 'breakdown'
        """
        params = []
        if equipment_id:
            query += " AND e.id = %s"
            params.append(equipment_id)
        if equipment_type:
            query += " AND e.equipment_type = %s"
            params.append(equipment_type)
        query += " ORDER BY j.created_at"
        c.execute(query, params)
        return [
            {'equipment_id': r[0], 'equipment_type': r[1], 'brand': r[2], 'model': r[3],
             'failure_date': r[4], 'job_type': r[5], 'hours_at_failure': r[6]}
            for r in c.fetchall()
        ]


def predict_maintenance_enhanced(equipment_id: int):
    """Enhanced predictive maintenance using historical data."""
    equipment = get_equipment_detail(equipment_id)
    if not equipment:
        return None

    # Get failure history for this equipment type
    failures = get_equipment_failure_history(equipment_type=equipment['equipment_type'])

    if len(failures) < 3:
        # Not enough data, use simple threshold-based prediction
        hours_remaining = (equipment.get('maintenance_threshold', 1000) or 1000) - (equipment.get('hours', 0) or 0)
        days_estimate = max(1, hours_remaining / 8)  # Assume 8 hours/day usage
        return {
            'equipment_id': equipment_id,
            'prediction_type': 'threshold_based',
            'hours_until_maintenance': max(0, hours_remaining),
            'estimated_days': round(days_estimate),
            'confidence': 'low',
            'recommendation': 'Schedule maintenance before threshold'
        }

    # Use historical failures to calculate MTBF
    hours_at_failures = [f['hours_at_failure'] for f in failures if f['hours_at_failure']]
    if hours_at_failures:
        avg_hours_between_failures = sum(hours_at_failures) / len(hours_at_failures)
        current_hours = equipment.get('hours', 0) or 0
        hours_since_last_maint = current_hours  # Simplified

        predicted_failure_hours = avg_hours_between_failures
        hours_until_predicted = max(0, predicted_failure_hours - (current_hours % predicted_failure_hours))

        # Determine risk level
        risk_ratio = current_hours / predicted_failure_hours if predicted_failure_hours > 0 else 0
        if risk_ratio > 0.9:
            risk_level = 'high'
            confidence = 'medium'
        elif risk_ratio > 0.7:
            risk_level = 'medium'
            confidence = 'medium'
        else:
            risk_level = 'low'
            confidence = 'high'

        return {
            'equipment_id': equipment_id,
            'prediction_type': 'historical_mtbf',
            'mtbf_hours': round(avg_hours_between_failures),
            'current_hours': current_hours,
            'hours_until_predicted_maintenance': round(hours_until_predicted),
            'estimated_days': round(hours_until_predicted / 8),
            'risk_level': risk_level,
            'confidence': confidence,
            'data_points': len(hours_at_failures),
            'recommendation': f"Based on {len(hours_at_failures)} historical failures, "
                            f"maintenance recommended within {round(hours_until_predicted)} hours"
        }

    return {
        'equipment_id': equipment_id,
        'prediction_type': 'insufficient_data',
        'confidence': 'very_low',
        'recommendation': 'Continue regular maintenance schedule'
    }


# ============================================================================
# INFRASTRUCTURE-SPECIFIC FEATURES - Utilities, Roads, Municipalities
# ============================================================================

def create_utility_asset(asset_type: str, asset_data: dict) -> int:
    """Create a utility infrastructure asset (poles, transformers, lines, etc.)"""
    conn = get_connection()
    c = conn.cursor()
    try:
        c.execute("""
            INSERT INTO utility_assets (
                asset_type, asset_id, location_description,
                latitude, longitude, installation_date,
                manufacturer, model, voltage_class, capacity,
                condition_rating, last_inspection_date,
                circuit_id, feeder_id, pole_class, material,
                status, notes, created_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
            RETURNING id
        """, (
            asset_type,
            asset_data.get('asset_id'),
            asset_data.get('location_description'),
            asset_data.get('latitude'),
            asset_data.get('longitude'),
            asset_data.get('installation_date'),
            asset_data.get('manufacturer'),
            asset_data.get('model'),
            asset_data.get('voltage_class'),
            asset_data.get('capacity'),
            asset_data.get('condition_rating', 'good'),
            asset_data.get('last_inspection_date'),
            asset_data.get('circuit_id'),
            asset_data.get('feeder_id'),
            asset_data.get('pole_class'),
            asset_data.get('material'),
            asset_data.get('status', 'in_service'),
            asset_data.get('notes')
        ))
        asset_id = c.fetchone()[0]
        conn.commit()
        return asset_id
    except Exception as e:
        conn.rollback()
        raise
    finally:
        release_connection(conn)


def get_utility_assets(asset_type: str = None, circuit_id: str = None, condition: str = None) -> list:
    """Get utility assets with optional filtering"""
    conn = get_connection()
    c = conn.cursor()
    try:
        query = """
            SELECT id, asset_type, asset_id, location_description, latitude, longitude,
                   installation_date, voltage_class, capacity, condition_rating,
                   last_inspection_date, circuit_id, feeder_id, status
            FROM utility_assets WHERE 1=1
        """
        params = []
        if asset_type:
            query += " AND asset_type = %s"
            params.append(asset_type)
        if circuit_id:
            query += " AND circuit_id = %s"
            params.append(circuit_id)
        if condition:
            query += " AND condition_rating = %s"
            params.append(condition)
        query += " ORDER BY asset_type, asset_id"

        c.execute(query, params)
        assets = []
        for row in c.fetchall():
            assets.append({
                'id': row[0], 'asset_type': row[1], 'asset_id': row[2],
                'location_description': row[3], 'latitude': row[4], 'longitude': row[5],
                'installation_date': row[6], 'voltage_class': row[7], 'capacity': row[8],
                'condition_rating': row[9], 'last_inspection_date': row[10],
                'circuit_id': row[11], 'feeder_id': row[12], 'status': row[13]
            })
        return assets
    finally:
        release_connection(conn)


def create_outage_record(outage_data: dict) -> int:
    """Record a utility outage event"""
    conn = get_connection()
    c = conn.cursor()
    try:
        c.execute("""
            INSERT INTO utility_outages (
                outage_type, affected_area, affected_customers, cause,
                start_time, estimated_restore, actual_restore,
                circuit_id, feeder_id, crew_assigned,
                status, weather_related, notes, created_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
            RETURNING id
        """, (
            outage_data.get('outage_type', 'unplanned'),
            outage_data.get('affected_area'),
            outage_data.get('affected_customers'),
            outage_data.get('cause'),
            outage_data.get('start_time', datetime.now()),
            outage_data.get('estimated_restore'),
            outage_data.get('actual_restore'),
            outage_data.get('circuit_id'),
            outage_data.get('feeder_id'),
            outage_data.get('crew_assigned'),
            outage_data.get('status', 'active'),
            outage_data.get('weather_related', False),
            outage_data.get('notes')
        ))
        outage_id = c.fetchone()[0]
        conn.commit()
        return outage_id
    except Exception as e:
        conn.rollback()
        raise
    finally:
        release_connection(conn)


def get_active_outages() -> list:
    """Get all active utility outages"""
    conn = get_connection()
    c = conn.cursor()
    try:
        c.execute("""
            SELECT id, outage_type, affected_area, affected_customers, cause,
                   start_time, estimated_restore, circuit_id, crew_assigned, status
            FROM utility_outages
            WHERE status IN ('active', 'crew_en_route', 'crew_on_site')
            ORDER BY start_time DESC
        """)
        outages = []
        for row in c.fetchall():
            outages.append({
                'id': row[0], 'outage_type': row[1], 'affected_area': row[2],
                'affected_customers': row[3], 'cause': row[4],
                'start_time': row[5], 'estimated_restore': row[6],
                'circuit_id': row[7], 'crew_assigned': row[8], 'status': row[9]
            })
        return outages
    finally:
        release_connection(conn)


def create_road_segment(segment_data: dict) -> int:
    """Create a road segment for road maintenance tracking"""
    conn = get_connection()
    c = conn.cursor()
    try:
        c.execute("""
            INSERT INTO road_segments (
                segment_id, road_name, road_type, start_point, end_point,
                length_miles, lane_count, surface_type, speed_limit,
                pci_rating, last_inspection_date, last_resurfacing_date,
                jurisdiction, maintenance_district, traffic_volume,
                status, notes, created_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
            RETURNING id
        """, (
            segment_data.get('segment_id'),
            segment_data.get('road_name'),
            segment_data.get('road_type', 'local'),
            segment_data.get('start_point'),
            segment_data.get('end_point'),
            segment_data.get('length_miles'),
            segment_data.get('lane_count'),
            segment_data.get('surface_type', 'asphalt'),
            segment_data.get('speed_limit'),
            segment_data.get('pci_rating'),
            segment_data.get('last_inspection_date'),
            segment_data.get('last_resurfacing_date'),
            segment_data.get('jurisdiction'),
            segment_data.get('maintenance_district'),
            segment_data.get('traffic_volume'),
            segment_data.get('status', 'good'),
            segment_data.get('notes')
        ))
        segment_id = c.fetchone()[0]
        conn.commit()
        return segment_id
    except Exception as e:
        conn.rollback()
        raise
    finally:
        release_connection(conn)


def get_road_segments(road_type: str = None, condition: str = None, district: str = None) -> list:
    """Get road segments with optional filtering"""
    conn = get_connection()
    c = conn.cursor()
    try:
        query = """
            SELECT id, segment_id, road_name, road_type, length_miles,
                   lane_count, surface_type, pci_rating, last_inspection_date,
                   last_resurfacing_date, maintenance_district, status
            FROM road_segments WHERE 1=1
        """
        params = []
        if road_type:
            query += " AND road_type = %s"
            params.append(road_type)
        if condition:
            query += " AND status = %s"
            params.append(condition)
        if district:
            query += " AND maintenance_district = %s"
            params.append(district)
        query += " ORDER BY road_name, segment_id"

        c.execute(query, params)
        segments = []
        for row in c.fetchall():
            segments.append({
                'id': row[0], 'segment_id': row[1], 'road_name': row[2],
                'road_type': row[3], 'length_miles': row[4], 'lane_count': row[5],
                'surface_type': row[6], 'pci_rating': row[7],
                'last_inspection_date': row[8], 'last_resurfacing_date': row[9],
                'maintenance_district': row[10], 'status': row[11]
            })
        return segments
    finally:
        release_connection(conn)


def create_pothole_report(report_data: dict) -> int:
    """Create a pothole or road defect report"""
    conn = get_connection()
    c = conn.cursor()
    try:
        c.execute("""
            INSERT INTO road_defects (
                defect_type, road_segment_id, location_description,
                latitude, longitude, severity, dimensions,
                reported_by, reporter_contact, photo_path,
                status, priority, job_id, notes, created_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
            RETURNING id
        """, (
            report_data.get('defect_type', 'pothole'),
            report_data.get('road_segment_id'),
            report_data.get('location_description'),
            report_data.get('latitude'),
            report_data.get('longitude'),
            report_data.get('severity', 'medium'),
            report_data.get('dimensions'),
            report_data.get('reported_by'),
            report_data.get('reporter_contact'),
            report_data.get('photo_path'),
            report_data.get('status', 'reported'),
            report_data.get('priority', 'normal'),
            report_data.get('job_id'),
            report_data.get('notes')
        ))
        defect_id = c.fetchone()[0]
        conn.commit()
        return defect_id
    except Exception as e:
        conn.rollback()
        raise
    finally:
        release_connection(conn)


def get_municipal_assets(asset_category: str = None) -> list:
    """Get municipal infrastructure assets (signs, lights, hydrants, etc.)"""
    conn = get_connection()
    c = conn.cursor()
    try:
        query = """
            SELECT id, asset_category, asset_type, asset_id, location_description,
                   latitude, longitude, installation_date, condition_rating,
                   last_maintenance_date, next_maintenance_due, status
            FROM municipal_assets WHERE 1=1
        """
        params = []
        if asset_category:
            query += " AND asset_category = %s"
            params.append(asset_category)
        query += " ORDER BY asset_category, asset_type, asset_id"

        c.execute(query, params)
        assets = []
        for row in c.fetchall():
            assets.append({
                'id': row[0], 'asset_category': row[1], 'asset_type': row[2],
                'asset_id': row[3], 'location_description': row[4],
                'latitude': row[5], 'longitude': row[6],
                'installation_date': row[7], 'condition_rating': row[8],
                'last_maintenance_date': row[9], 'next_maintenance_due': row[10],
                'status': row[11]
            })
        return assets
    finally:
        release_connection(conn)


def get_infrastructure_dashboard() -> dict:
    """Get infrastructure overview dashboard data"""
    conn = get_connection()
    c = conn.cursor()
    try:
        stats = {}

        # Utility stats
        try:
            c.execute("SELECT COUNT(*) FROM utility_assets")
            stats['utility_assets'] = c.fetchone()[0]
            c.execute("SELECT COUNT(*) FROM utility_outages WHERE status IN ('active', 'crew_en_route', 'crew_on_site')")
            stats['active_outages'] = c.fetchone()[0]
            c.execute("SELECT SUM(affected_customers) FROM utility_outages WHERE status = 'active'")
            stats['customers_affected'] = c.fetchone()[0] or 0
        except:
            stats['utility_assets'] = 0
            stats['active_outages'] = 0
            stats['customers_affected'] = 0

        # Road stats
        try:
            c.execute("SELECT COUNT(*), COALESCE(SUM(length_miles), 0) FROM road_segments")
            row = c.fetchone()
            stats['road_segments'] = row[0]
            stats['total_road_miles'] = float(row[1])
            c.execute("SELECT COUNT(*) FROM road_defects WHERE status IN ('reported', 'scheduled')")
            stats['open_defects'] = c.fetchone()[0]
        except:
            stats['road_segments'] = 0
            stats['total_road_miles'] = 0
            stats['open_defects'] = 0

        # Municipal assets
        try:
            c.execute("SELECT asset_category, COUNT(*) FROM municipal_assets GROUP BY asset_category")
            stats['municipal_by_category'] = {row[0]: row[1] for row in c.fetchall()}
        except:
            stats['municipal_by_category'] = {}

        return stats
    finally:
        release_connection(conn)