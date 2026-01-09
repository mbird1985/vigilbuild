# routes/schedule.py
from flask import Blueprint, render_template, redirect, url_for, flash, jsonify, request
from flask_login import login_required, current_user
from services.schedule_service import get_schedule_event, get_all_schedules, add_schedule, update_schedule, delete_schedule, list_schedule_by_town
from services.equipment_service import get_all_equipment
from services.users_service import get_all_users
from services.db import get_connection, release_connection
from services.logging_service import log_audit
from services.elasticsearch_client import es
from datetime import datetime
import logging

logging.basicConfig(filename='schedule.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

schedule_bp = Blueprint("schedule", __name__, url_prefix="/schedule")

@schedule_bp.route("/")
@login_required
def schedule():
    try:
        town_id = request.args.get('town_id')
        if town_id:
            schedules = list_schedule_by_town(int(town_id))
        else:
            schedules = get_all_schedules()
        equipment = get_all_equipment()
        can_edit = current_user.role in ['manager', 'admin']
        # Town filter options
        conn = get_connection()
        c = conn.cursor()
        c.execute("SELECT id, city_name FROM city_contacts ORDER BY city_name")
        towns = [{'id': r[0], 'name': r[1]} for r in c.fetchall()]
        release_connection(conn)
        return render_template('schedule.html', schedules=schedules, equipment=equipment, can_edit=can_edit, towns=towns, current_town_id=town_id)
    except Exception as e:
        flash(f"Error loading schedule: {str(e)}")
        return redirect(url_for('system.dashboard'))

@schedule_bp.route("/schedule_detail/<int:event_id>", methods=['GET', 'POST'])
@login_required
def schedule_detail(event_id):
    try:
        event = get_schedule_event(event_id)
        if not event:
            flash('Event not found.')
            return redirect(url_for('schedule.schedule'))
        conn = get_connection()
        c = conn.cursor()
        c.execute("SELECT id, resource_type, resource_id, quantity FROM job_resources WHERE schedule_id = %s", (event_id,))
        resources = [
            {
                "id": row[0],
                "resource_type": row[1],
                "resource_id": row[2],
                "quantity": row[3],
                "name": get_resource_name(row[1], row[2])
            } for row in c.fetchall()
        ]
        equipment = get_all_equipment()
        people = get_all_users()
        c.execute("SELECT id, name, quantity FROM consumables")
        consumables = [{"id": row[0], "name": row[1], "stock": row[2]} for row in c.fetchall()]
        release_connection(conn)
        people = [{"id": p["id"], "name": p["username"], "job_title": p.get("job_title", "N/A")} for p in people]
        if request.method == 'POST':
            if "delete" in request.form:
                delete_schedule(event_id, current_user.id)
                flash("Schedule deleted")
                return redirect(url_for('schedule.schedule'))
            if "add_resource" in request.form:
                resource_type = request.form.get("resource_type")
                resource_id = request.form.get("resource_id")
                quantity = int(request.form.get("quantity", 1))
                if resource_type not in ["equipment", "person", "consumable"]:
                    flash("Invalid resource type")
                    return redirect(url_for('schedule.schedule_detail', event_id=event_id))
                conn = get_connection()
                c = conn.cursor()
                if resource_type == "equipment":
                    c.execute("SELECT 1 FROM equipment_instances WHERE id = %s", (resource_id,))
                elif resource_type == "person":
                    c.execute("SELECT 1 FROM users WHERE id = %s", (resource_id,))
                elif resource_type == "consumable":
                    c.execute("SELECT 1 FROM consumables WHERE id = %s", (resource_id,))
                if not c.fetchone():
                    flash(f"Invalid {resource_type} ID")
                    release_connection(conn)
                    return redirect(url_for('schedule.schedule_detail', event_id=event_id))
                c.execute("INSERT INTO job_resources (schedule_id, resource_type, resource_id, quantity) VALUES (%s, %s, %s, %s)",
                          (event_id, resource_type, resource_id, quantity))
                conn.commit()
                release_connection(conn)
                flash("Resource added")
            if "remove_resource" in request.form:
                resource_id = request.form.get("resource_id")
                conn = get_connection()
                c = conn.cursor()
                c.execute("SELECT 1 FROM job_resources WHERE id = %s", (resource_id,))
                if not c.fetchone():
                    flash("Resource not found")
                    release_connection(conn)
                    return redirect(url_for('schedule.schedule_detail', event_id=event_id))
                c.execute("DELETE FROM job_resources WHERE id = %s", (resource_id,))
                conn.commit()
                release_connection(conn)
                flash("Resource removed")
        return render_template(
            'schedule_detail.html',
            event=event,
            resources=resources,
            equipment=equipment,
            people=people,
            consumables=consumables,
            can_edit=current_user.role in ['manager', 'admin']
        )
    except Exception as e:
        flash(f"Error loading schedule details: {str(e)}")
        return redirect(url_for('schedule.schedule'))

def get_resource_name(resource_type, resource_id):
    try:
        conn = get_connection()
        c = conn.cursor()
        if resource_type == "equipment":
            c.execute("SELECT unique_id FROM equipment_instances WHERE id = %s", (resource_id,))
        elif resource_type == "person":
            c.execute("SELECT username FROM users WHERE id = %s", (resource_id,))
        elif resource_type == "consumable":
            c.execute("SELECT name FROM consumables WHERE id = %s", (resource_id,))
        else:
            return "Unknown"
        result = c.fetchone()
        release_connection(conn)
        return result[0] if result else "Unknown"
    except Exception:
        return "Unknown"

@schedule_bp.route("/create_schedule", methods=["POST"])
@login_required
def create_schedule():
    if current_user.role not in ['manager', 'admin']:
        flash("Permission denied")
        return redirect(url_for('schedule.schedule'))
    try:
        form = request.form
        required_fields = ["title", "start_date", "end_date"]
        for field in required_fields:
            if field not in form:
                raise ValueError(f"Missing required field: {field}")
        schedule_id = add_schedule(
            title=form["title"],
            start_date=form["start_date"],
            end_date=form["end_date"],
            description=form.get("description", ""),
            user_id=current_user.id,
            location=form.get("location", ""),
            equipment_id=form.get("equipment_id")
        )
        log_audit(es, "schedule_create", current_user.id, schedule_id, {"title": form["title"]})
        flash("Job added successfully")
        return redirect(url_for('schedule.schedule'))
    except ValueError as e:
        flash(f"Error adding job: {str(e)}")
        return redirect(url_for('schedule.schedule'))
    except Exception as e:
        flash(f"Error adding job: {str(e)}")
        return redirect(url_for('schedule.schedule'))

@schedule_bp.route('/events')
@login_required
def events():
    filter_type = request.args.get('filter', '')
    board_id = request.args.get('board_id')
    # Modify get_jobs_for_calendar to accept filter and board_id, query pm_items if board_id provided
    events = get_jobs_for_calendar(filter_type, board_id)
    return jsonify(events)

@schedule_bp.route('/event/update', methods=['POST'])
@login_required
def update_event():
    # Handle drag-drop update from FullCalendar
    try:
        data = request.get_json(silent=True) or request.form
        event_id_raw = data.get('id') or data.get('event_id')
        start = data.get('start') or data.get('start_date')
        end = data.get('end') or data.get('end_date')

        if not event_id_raw or not start:
            return jsonify({"error": "Missing required fields (id, start)"}), 400

        try:
            event_id = int(event_id_raw)
        except ValueError:
            return jsonify({"error": "Invalid event id"}), 400

        conn = get_connection()
        c = conn.cursor()

        if end:
            c.execute(
                "UPDATE schedules SET start_date = %s, end_date = %s, updated_at = CURRENT_TIMESTAMP WHERE id = %s",
                (start, end, event_id)
            )
        else:
            c.execute(
                "UPDATE schedules SET start_date = %s, updated_at = CURRENT_TIMESTAMP WHERE id = %s",
                (start, event_id)
            )
        conn.commit()
        try:
            release_connection(conn)
        except Exception:
            pass

        log_audit(es, 'schedule_drag_update', current_user.id, event_id, {"start": start, "end": end})
        return jsonify({"status": "ok"})
    except Exception as e:
        logging.error(f"update_event error: {str(e)}")
        return jsonify({"error": str(e)}), 500

@schedule_bp.route("/update/<int:schedule_id>", methods=["POST"])
@login_required
def update_schedule_route(schedule_id):
    if current_user.role not in ['manager', 'admin']:
        flash("Permission denied")
        return jsonify({"error": "Permission denied"}), 403
    try:
        form = request.form
        required_fields = ["title", "start_date", "end_date"]
        for field in required_fields:
            if field not in form:
                raise ValueError(f"Missing required field: {field}")
        update_schedule(
            schedule_id=schedule_id,
            title=form["title"],
            start_date=form["start_date"],
            end_date=form["end_date"],
            description=form.get("description", ""),
            user_id=current_user.id,
            location=form.get("location", ""),
            equipment_id=form.get("equipment_id")
        )
        log_audit(es, "schedule_update", current_user.id, schedule_id, {"title": form["title"]})
        flash("Schedule updated successfully")
        return redirect(url_for('schedule.schedule'))
    except ValueError as e:
        flash(f"Error updating schedule: {str(e)}")
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        flash(f"Error updating schedule: {str(e)}")
        return jsonify({"error": f"Update failed: {str(e)}"}), 500

@schedule_bp.route("/delete/<int:schedule_id>", methods=["POST"])
@login_required
def delete_schedule_route(schedule_id):
    if current_user.role not in ['manager', 'admin']:
        flash("Permission denied")
        return jsonify({"error": "Permission denied"}), 403
    try:
        delete_schedule(schedule_id, current_user.id)
        log_audit(es, "schedule_delete", current_user.id, schedule_id)
        flash("Schedule deleted successfully")
        return redirect(url_for('schedule.schedule'))
    except Exception as e:
        flash(f"Error deleting schedule: {str(e)}")
        return jsonify({"error": f"Deletion failed: {str(e)}"}), 500