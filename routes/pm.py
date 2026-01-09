from flask import Blueprint, render_template, request, jsonify, redirect, url_for, flash
import json
from flask_login import login_required, current_user
from services.db import get_connection, release_connection
from services.integration_service import (
    IntegrationManager,
    get_powerbi_datasets,
    get_salesforce_accounts,
)
from datetime import datetime, timedelta
import plotly.express as px

pm_bp = Blueprint("pm", __name__, url_prefix="/pm")


def _ensure_pm_tables():
    """Ensure PM tables exist by delegating to init_db's create_pm_tables."""
    from services.init_db import create_pm_tables
    try:
        create_pm_tables()
    except Exception:
        # Fallback soft-fail: ignore if already created or DB not reachable
        pass


@pm_bp.route("/")
@pm_bp.route("/dashboard")
@login_required
def dashboard():
    _ensure_pm_tables()
    # Load user's widgets
    conn = get_connection()
    c = conn.cursor()
    c.execute(
        """
        SELECT id, widget_type, config, position, width, height
        FROM pm_dashboard_widgets
        WHERE user_id = %s
        ORDER BY position ASC, id ASC
        """,
        (current_user.id,),
    )
    rows = c.fetchall()
    release_connection(conn)

    widgets = [
        {
            "id": r[0],
            "widget_type": r[1],
            "config": r[2],
            "position": r[3],
            "width": r[4],
            "height": r[5],
        }
        for r in rows
    ]

    # Integration status for quick hints
    integration_status = IntegrationManager(current_user.id)
    status = {
        "powerbi": integration_status.is_connected("powerbi"),
        "salesforce": integration_status.is_connected("salesforce"),
    }

    return render_template("pm/dashboard.html", widgets=widgets, status=status)


@pm_bp.route("/boards")
@login_required
def boards():
    _ensure_pm_tables()
    conn = get_connection()
    c = conn.cursor()
    # List boards where user is owner or a member
    c.execute(
        """
        SELECT b.id, b.name, b.is_private, b.is_shareable
        FROM pm_boards b
        LEFT JOIN pm_board_members m ON m.board_id = b.id AND m.user_id = %s
        WHERE b.owner_id = %s OR m.user_id = %s
        ORDER BY b.created_at DESC
        """,
        (current_user.id, current_user.id, current_user.id),
    )
    boards = [
        {"id": r[0], "name": r[1], "is_private": r[2], "is_shareable": r[3]}
        for r in c.fetchall()
    ]
    release_connection(conn)
    return render_template("pm/boards.html", boards=boards)


@pm_bp.route("/boards/create", methods=["POST"]) 
@login_required
def create_board():
    _ensure_pm_tables()
    data = request.get_json() or {}
    name = data.get("name")
    is_private = bool(data.get("is_private", False))
    is_shareable = bool(data.get("is_shareable", False))
    template_key = data.get("template_key")
    if not name:
        return jsonify({"error": "name required"}), 400
    conn = get_connection()
    c = conn.cursor()
    c.execute(
        """
        INSERT INTO pm_boards (name, is_private, is_shareable, owner_id, template_key)
        VALUES (%s, %s, %s, %s, %s)
        RETURNING id
        """,
        (name, is_private, is_shareable, current_user.id, template_key),
    )
    board_id = c.fetchone()[0]
    # Create default columns
    try:
        default_columns = [
            ("Status", "status", 0, {"options": ["To Do", "In Progress", "Blocked", "Done"]}),
            ("Assignee", "people", 1, {}),
            ("Due Date", "date", 2, {}),
        ]
        for name, col_type, pos, cfg in default_columns:
            c.execute(
                "INSERT INTO pm_columns (board_id, name, type, position, config) VALUES (%s, %s, %s, %s, %s)",
                (board_id, name, col_type, pos, json.dumps(cfg)),
            )
    except Exception:
        pass
    conn.commit()
    release_connection(conn)
    return jsonify({"id": board_id}), 201


@pm_bp.route("/boards/<int:board_id>")
@login_required
def board_detail(board_id: int):
    _ensure_pm_tables()
    view = request.args.get("view", "kanban")
    conn = get_connection()
    c = conn.cursor()
    # Load board
    c.execute("SELECT id, name FROM pm_boards WHERE id = %s", (board_id,))
    row = c.fetchone()
    if not row:
        release_connection(conn)
        return render_template("error.html", message="Board not found"), 404
    board = {"id": row[0], "name": row[1]}
    # Load columns and items
    c.execute("SELECT id, name, type, position FROM pm_columns WHERE board_id = %s ORDER BY position ASC, id ASC", (board_id,))
    columns = [{"id": r[0], "name": r[1], "type": r[2], "position": r[3]} for r in c.fetchall()]
    c.execute("SELECT id, name, status, due_date, assignee_id, position FROM pm_items WHERE board_id = %s ORDER BY position ASC, id ASC", (board_id,))
    items = [{"id": r[0], "name": r[1], "status": r[2], "due_date": r[3], "assignee_id": r[4], "position": r[5]} for r in c.fetchall()]
    release_connection(conn)
    # Optional: compute critical path and workload when needed
    cp_data = None
    workload_data = None
    if view == "gantt":
        cp_data = _compute_critical_path(board_id)
    if view == "workload":
        workload_data = _compute_workload_capacity(board_id)

    template_map = {
        "kanban": "pm/views/kanban.html",
        "timeline": "pm/views/timeline.html",
        "gantt": "pm/views/gantt.html",
        "calendar": "pm/views/calendar.html",
        "map": "pm/views/map.html",
        "workload": "pm/views/workload.html",
        "chart": "pm/views/chart.html",
        "files": "pm/views/files.html",
        "forms": "pm/views/forms.html",
    }
    tpl = template_map.get(view, template_map["kanban"])
    return render_template(tpl, board=board, columns=columns, items=items, active_view=view, cp_data=cp_data, workload_data=workload_data)


def _compute_critical_path(board_id: int):
    conn = get_connection()
    c = conn.cursor()
    c.execute(
        "SELECT id, name, start_date, end_date FROM pm_items WHERE board_id = %s",
        (board_id,),
    )
    items = {}
    min_date = None
    for r in c.fetchall():
        item_id = r[0]
        name = r[1]
        start = r[2]
        end = r[3]
        duration = 1
        if start and end and end >= start:
            duration = (end - start).days + 1
        items[item_id] = {
            "id": item_id,
            "name": name,
            "start": start,
            "end": end,
            "duration": max(1, duration),
        }
        if start and (min_date is None or start < min_date):
            min_date = start

    if min_date is None:
        min_date = datetime.now().date()

    # Load dependencies
    c.execute(
        "SELECT predecessor_id, successor_id, type FROM pm_dependencies WHERE board_id = %s",
        (board_id,),
    )
    preds = {i: set() for i in items.keys()}
    succs = {i: set() for i in items.keys()}
    for pre, suc, _t in c.fetchall():
        if pre in items and suc in items:
            preds[suc].add(pre)
            succs[pre].add(suc)
    release_connection(conn)

    # Topological order (Kahn)
    in_deg = {i: len(preds[i]) for i in items}
    queue = [i for i, d in in_deg.items() if d == 0]
    order = []
    while queue:
        n = queue.pop(0)
        order.append(n)
        for s in succs[n]:
            in_deg[s] -= 1
            if in_deg[s] == 0:
                queue.append(s)

    # Forward pass
    ES = {i: 0 for i in items}
    EF = {i: items[i]["duration"] for i in items}
    for n in order:
        if preds[n]:
            ES[n] = max(EF[p] for p in preds[n])
            EF[n] = ES[n] + items[n]["duration"]

    project_duration = max(EF.values()) if EF else 0

    # Backward pass
    LF = {i: project_duration for i in items}
    LS = {i: LF[i] - items[i]["duration"] for i in items}
    for n in reversed(order):
        if succs[n]:
            LF[n] = min(LS[s] for s in succs[n])
            LS[n] = LF[n] - items[n]["duration"]

    slack = {i: LS[i] - ES[i] for i in items}
    critical = {i for i, s in slack.items() if s == 0}

    # Build scheduled dates (use existing when provided, else compute)
    scheduled = []
    for i, data in items.items():
        s_date = data["start"] if data["start"] else (min_date + timedelta(days=ES[i]))
        e_date = data["end"] if data["end"] else (s_date + timedelta(days=data["duration"] - 1))
        scheduled.append({
            "id": i,
            "name": data["name"],
            "start": s_date,
            "end": e_date,
            "critical": i in critical,
            "es": ES[i],
            "ef": EF[i],
            "ls": LS[i],
            "lf": LF[i],
            "slack": slack[i],
        })

    return {
        "project_duration_days": project_duration,
        "tasks": scheduled,
    }


def _compute_workload_capacity(board_id: int):
    conn = get_connection()
    c = conn.cursor()
    # Load items with assignee and estimated hours
    c.execute(
        """
        SELECT i.assignee_id, COALESCE(i.estimated_hours, 0)
        FROM pm_items i
        WHERE i.board_id = %s
        """,
        (board_id,),
    )
    hours_by_user = {}
    for uid, hrs in c.fetchall():
        if not uid:
            continue
        hours_by_user[uid] = hours_by_user.get(uid, 0) + float(hrs or 0)

    # Capacity
    c.execute("SELECT user_id, weekly_capacity_hours FROM pm_user_capacity")
    capacity = {row[0]: float(row[1]) for row in c.fetchall()}

    # Load usernames for display
    c.execute("SELECT id, username FROM users")
    id_to_name = {row[0]: row[1] for row in c.fetchall()}
    release_connection(conn)

    rows = []
    for uid, assigned in hours_by_user.items():
        cap = capacity.get(uid, 40.0)
        utilization = assigned / cap if cap > 0 else 0
        rows.append({
            "user_id": uid,
            "username": id_to_name.get(uid, str(uid)),
            "assigned_hours": round(assigned, 2),
            "weekly_capacity_hours": cap,
            "utilization": round(utilization, 2),
            "overallocated": utilization > 1.0,
        })
    # Include users with capacity but no assignments
    for uid, cap in capacity.items():
        if uid not in hours_by_user:
            rows.append({
                "user_id": uid,
                "username": id_to_name.get(uid, str(uid)),
                "assigned_hours": 0.0,
                "weekly_capacity_hours": cap,
                "utilization": 0.0,
                "overallocated": False,
            })

    # Sort by utilization desc
    rows.sort(key=lambda r: r["utilization"], reverse=True)
    return {"rows": rows}


# Board APIs: columns/items/dependencies/time logs/files
@pm_bp.route("/boards/<int:board_id>/columns", methods=["POST"]) 
@login_required
def create_column(board_id: int):
    data = request.get_json() or {}
    name = data.get("name")
    col_type = data.get("type", "text")
    position = int(data.get("position", 0))
    config = data.get("config")
    if not name:
        return jsonify({"error": "name required"}), 400
    conn = get_connection()
    c = conn.cursor()
    c.execute(
        """
        INSERT INTO pm_columns (board_id, name, type, position, config)
        VALUES (%s, %s, %s, %s, %s)
        RETURNING id
        """,
        (board_id, name, col_type, position, json.dumps(config) if isinstance(config, dict) else config),
    )
    col_id = c.fetchone()[0]
    conn.commit()
    release_connection(conn)
    return jsonify({"id": col_id}), 201

@pm_bp.route('/boards/<int:board_id>/columns', methods=['POST'])
@login_required
def add_column(board_id):
    data = request.get_json()
    name = data.get('name')
    type_ = data.get('type')
    config = data.get('config')
    position = data.get('position', 0)
    is_restricted = data.get('is_restricted', False)

    conn = get_connection()
    c = conn.cursor()
    c.execute('''INSERT INTO pm_columns (board_id, name, type, config, position, is_restricted)
                 VALUES (%s, %s, %s, %s, %s, %s) RETURNING id''',
              (board_id, name, type_, json.dumps(config), position, is_restricted))
    column_id = c.fetchone()[0]
    conn.commit()
    release_connection(conn)
    return jsonify({'id': column_id}), 201

@pm_bp.route('/columns/<int:column_id>', methods=['PUT'])
@login_required
def edit_column(column_id):
    data = request.get_json()
    name = data.get('name')
    type_ = data.get('type')
    config = data.get('config')
    position = data.get('position')
    is_restricted = data.get('is_restricted')

    conn = get_connection()
    c = conn.cursor()
    c.execute('''UPDATE pm_columns SET name=%s, type=%s, config=%s, position=%s, is_restricted=%s
                 WHERE id=%s''',
              (name, type_, json.dumps(config), position, is_restricted, column_id))
    conn.commit()
    release_connection(conn)
    return jsonify({'message': 'Column updated'}), 200


@pm_bp.route("/boards/<int:board_id>/items", methods=["POST"]) 
@login_required
def create_item(board_id: int):
    data = request.get_json() or {}
    name = data.get("name")
    status = data.get("status")
    due_date = data.get("due_date")
    assignee_id = data.get("assignee_id")
    if not name:
        return jsonify({"error": "name required"}), 400
    conn = get_connection()
    c = conn.cursor()
    c.execute("SELECT COALESCE(MAX(position), 0) + 1 FROM pm_items WHERE board_id = %s", (board_id,))
    next_pos = c.fetchone()[0]
    c.execute(
        """
        INSERT INTO pm_items (board_id, name, status, due_date, assignee_id, position)
        VALUES (%s, %s, %s, %s, %s, %s)
        RETURNING id
        """,
        (board_id, name, status, due_date, assignee_id, next_pos),
    )
    item_id = c.fetchone()[0]
    conn.commit()
    release_connection(conn)
    return jsonify({"id": item_id}), 201

@pm_bp.route('/boards/<int:board_id>/items', methods=['POST'])
@login_required
def add_item(board_id):
    data = request.get_json()
    name = data.get('name')
    status = data.get('status')
    start_date = data.get('start_date')
    end_date = data.get('end_date')
    due_date = data.get('due_date')
    assignee_id = data.get('assignee_id')
    position = data.get('position', 0)
    estimated_hours = data.get('estimated_hours')

    conn = get_connection()
    c = conn.cursor()
    c.execute('''INSERT INTO pm_items (board_id, name, status, start_date, end_date, due_date, assignee_id, position, estimated_hours)
                 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id''',
              (board_id, name, status, start_date, end_date, due_date, assignee_id, position, estimated_hours))
    item_id = c.fetchone()[0]
    conn.commit()
    release_connection(conn)
    return jsonify({'id': item_id}), 201

@pm_bp.route('/items/<int:item_id>', methods=['PUT'])
@login_required
def update_item(item_id):
    data = request.get_json()
    name = data.get('name')
    status = data.get('status')
    start_date = data.get('start_date')
    end_date = data.get('end_date')
    due_date = data.get('due_date')
    assignee_id = data.get('assignee_id')
    position = data.get('position')
    estimated_hours = data.get('estimated_hours')

    conn = get_connection()
    c = conn.cursor()
    c.execute('''UPDATE pm_items SET name=%s, status=%s, start_date=%s, end_date=%s, due_date=%s, assignee_id=%s, position=%s, estimated_hours=%s
                 WHERE id=%s''',
              (name, status, start_date, end_date, due_date, assignee_id, position, estimated_hours, item_id))
    conn.commit()
    release_connection(conn)
    return jsonify({'message': 'Item updated'}), 200


@pm_bp.route("/items/<int:item_id>/values", methods=["POST"]) 
@login_required
def set_item_value(item_id: int):
    data = request.get_json() or {}
    column_id = data.get("column_id")
    value = data.get("value")
    if not column_id:
        return jsonify({"error": "column_id required"}), 400
    conn = get_connection()
    c = conn.cursor()
    c.execute(
        """
        INSERT INTO pm_item_values (item_id, column_id, value)
        VALUES (%s, %s, %s)
        ON CONFLICT (item_id, column_id) DO UPDATE SET value = EXCLUDED.value
        """,
        (item_id, column_id, json.dumps(value) if isinstance(value, dict) else value),
    )
    conn.commit()
    release_connection(conn)
    return jsonify({"status": "ok"})


@pm_bp.route("/items/<int:item_id>/status", methods=["POST"]) 
@login_required
def set_item_status(item_id: int):
    data = request.get_json() or {}
    status = data.get("status")
    if status is None:
        return jsonify({"error": "status required"}), 400
    conn = get_connection()
    c = conn.cursor()
    c.execute("UPDATE pm_items SET status = %s WHERE id = %s", (status, item_id))
    conn.commit()
    release_connection(conn)
    return jsonify({"status": "ok"})


@pm_bp.route("/dependencies", methods=["POST"]) 
@login_required
def add_dependency():
    data = request.get_json() or {}
    board_id = data.get("board_id")
    predecessor_id = data.get("predecessor_id")
    successor_id = data.get("successor_id")
    dep_type = data.get("type", "FS")
    if not all([board_id, predecessor_id, successor_id]):
        return jsonify({"error": "board_id, predecessor_id, successor_id required"}), 400
    conn = get_connection()
    c = conn.cursor()
    c.execute(
        """
        INSERT INTO pm_dependencies (board_id, predecessor_id, successor_id, type)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (predecessor_id, successor_id) DO NOTHING
        """,
        (board_id, predecessor_id, successor_id, dep_type),
    )
    conn.commit()
    release_connection(conn)
    return jsonify({"status": "ok"})


@pm_bp.route("/items/<int:item_id>/time", methods=["POST"]) 
@login_required
def log_time(item_id: int):
    data = request.get_json() or {}
    hours = data.get("hours")
    work_date = data.get("work_date")
    notes = data.get("notes")
    if hours is None:
        return jsonify({"error": "hours required"}), 400
    conn = get_connection()
    c = conn.cursor()
    c.execute(
        """
        INSERT INTO pm_time_logs (item_id, user_id, hours, work_date, notes)
        VALUES (%s, %s, %s, %s, %s)
        RETURNING id
        """,
        (item_id, current_user.id, hours, work_date, notes),
    )
    time_id = c.fetchone()[0]
    conn.commit()
    release_connection(conn)
    return jsonify({"id": time_id}), 201


# Automations
@pm_bp.route("/boards/<int:board_id>/automations", methods=["GET", "POST"]) 
@login_required
def automations(board_id: int):
    _ensure_pm_tables()
    conn = get_connection()
    c = conn.cursor()
    if request.method == "POST":
        data = request.get_json() or {}
        name = data.get("name")
        trigger = data.get("trigger", {})
        actions = data.get("actions", [])
        monthly_limit = int(data.get("monthly_limit", 250))
        if not name:
            return jsonify({"error": "name required"}), 400
        c.execute(
            """
            INSERT INTO pm_automations (board_id, name, trigger, actions, monthly_limit)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING id
            """,
            (board_id, name, json.dumps(trigger), json.dumps(actions), monthly_limit),
        )
        automation_id = c.fetchone()[0]
        conn.commit()
        release_connection(conn)
        return jsonify({"id": automation_id}), 201
    c.execute("SELECT id, name, trigger, actions, active, monthly_limit, used_this_month FROM pm_automations WHERE board_id = %s ORDER BY id DESC", (board_id,))
    autos = [
        {
            "id": r[0],
            "name": r[1],
            "trigger": r[2],
            "actions": r[3],
            "active": r[4],
            "monthly_limit": r[5],
            "used_this_month": r[6],
        }
        for r in c.fetchall()
    ]
    release_connection(conn)
    return render_template("pm/automations.html", board_id=board_id, automations=autos)


# Forms
@pm_bp.route("/boards/<int:board_id>/forms", methods=["GET", "POST"]) 
@login_required
def forms_editor(board_id: int):
    _ensure_pm_tables()
    conn = get_connection()
    c = conn.cursor()
    if request.method == "POST":
        data = request.get_json() or {}
        name = data.get("name")
        definition = data.get("definition", {})
        if not name:
            return jsonify({"error": "name required"}), 400
        c.execute(
            """
            INSERT INTO pm_forms (board_id, name, definition)
            VALUES (%s, %s, %s)
            RETURNING id
            """,
            (board_id, name, json.dumps(definition)),
        )
        form_id = c.fetchone()[0]
        conn.commit()
        release_connection(conn)
        return jsonify({"id": form_id}), 201
    c.execute("SELECT id, name, definition FROM pm_forms WHERE board_id = %s ORDER BY id DESC", (board_id,))
    forms = [{"id": r[0], "name": r[1], "definition": r[2]} for r in c.fetchall()]
    release_connection(conn)
    return render_template("pm/forms_editor.html", board_id=board_id, forms=forms)

@pm_bp.route('/boards/<int:board_id>/forms', methods=['POST'])
@login_required
def create_form(board_id):
    data = request.get_json()
    name = data.get('name')
    fields = data.get('fields', [])

    conn = get_connection()
    c = conn.cursor()
    c.execute('''INSERT INTO pm_forms (board_id, name, fields)
                 VALUES (%s, %s, %s) RETURNING id''',
              (board_id, name, json.dumps(fields)))
    form_id = c.fetchone()[0]
    conn.commit()
    release_connection(conn)
    return jsonify({'id': form_id}), 201


@pm_bp.route("/forms/<int:form_id>/submit", methods=["POST"]) 
def submit_form(form_id: int):
    # Public endpoint: create an item according to form definition
    conn = get_connection()
    c = conn.cursor()
    c.execute("SELECT board_id, definition FROM pm_forms WHERE id = %s", (form_id,))
    row = c.fetchone()
    if not row:
        release_connection(conn)
        return jsonify({"error": "form not found"}), 404
    board_id, definition = row[0], row[1]
    payload = request.get_json() or {}
    name = payload.get("name") or payload.get("title") or "Form Submission"
    # Create item
    c.execute(
        "INSERT INTO pm_items (board_id, name, status) VALUES (%s, %s, %s) RETURNING id",
        (board_id, name, payload.get("status")),
    )
    item_id = c.fetchone()[0]
    # Map fields to columns if defined
    try:
        fields = (definition or {}).get("fields", [])
        for field in fields:
            column_id = field.get("column_id")
            key = field.get("key")
            if column_id and key and key in payload:
                c.execute(
                    """
                    INSERT INTO pm_item_values (item_id, column_id, value)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (item_id, column_id) DO UPDATE SET value = EXCLUDED.value
                    """,
                    (item_id, column_id, json.dumps(payload[key]) if isinstance(payload[key], dict) else payload[key]),
                )
    except Exception:
        pass
    conn.commit()
    release_connection(conn)
    return jsonify({"item_id": item_id}), 201


@pm_bp.route("/forms/<int:form_id>", methods=["GET"]) 
def render_form(form_id: int):
    # Public simple renderer
    conn = get_connection()
    c = conn.cursor()
    c.execute("SELECT name, definition FROM pm_forms WHERE id = %s", (form_id,))
    row = c.fetchone()
    release_connection(conn)
    if not row:
        return render_template("error.html", message="Form not found"), 404
    form = {"name": row[0], "definition": row[1]}
    return render_template("pm/form_public.html", form=form, form_id=form_id)


@pm_bp.route("/bidding")
@login_required
def bidding():
    _ensure_pm_tables()
    # Load available templates
    conn = get_connection()
    c = conn.cursor()
    c.execute("SELECT id, name, description FROM pm_bid_templates ORDER BY name ASC")
    templates = [{"id": r[0], "name": r[1], "description": r[2]} for r in c.fetchall()]
    release_connection(conn)
    return render_template("pm/bidding.html", templates=templates)


@pm_bp.route("/job-templates")
@login_required
def job_templates():
    _ensure_pm_tables()
    conn = get_connection()
    c = conn.cursor()
    c.execute("SELECT id, name, description, items FROM pm_bid_templates ORDER BY name ASC")
    templates = [
        {"id": r[0], "name": r[1], "description": r[2], "items": r[3]}
        for r in c.fetchall()
    ]
    release_connection(conn)
    return render_template("pm/job_templates.html", templates=templates)


@pm_bp.route("/imports")
@login_required
def imports():
    return render_template("pm/imports.html")


@pm_bp.route("/powerbi")
@login_required
def powerbi_list():
    datasets = get_powerbi_datasets(current_user.id)
    return render_template("pm/powerbi.html", datasets=datasets or [])


@pm_bp.route("/salesforce")
@login_required
def salesforce_list():
    accounts = get_salesforce_accounts(current_user.id, limit=50)
    return render_template("pm/salesforce.html", accounts=accounts or [])


@pm_bp.route("/schedule")
@login_required
def schedule_view():
    # Simple schedules summary for PM context
    conn = get_connection()
    c = conn.cursor()
    c.execute(
        """
        SELECT id, title, start_date, end_date, location, status
        FROM schedules
        ORDER BY start_date DESC
        LIMIT 100
        """
    )
    schedules = [
        {
            "id": r[0],
            "title": r[1],
            "start_date": r[2],
            "end_date": r[3],
            "location": r[4],
            "status": r[5],
        }
        for r in c.fetchall()
    ]
    release_connection(conn)
    return render_template("pm/schedule.html", schedules=schedules)


# APIs for dashboard customization
@pm_bp.route("/api/widgets", methods=["GET"]) 
@login_required
def get_widgets():
    conn = get_connection()
    c = conn.cursor()
    c.execute(
        """
        SELECT id, widget_type, config, position, width, height
        FROM pm_dashboard_widgets
        WHERE user_id = %s
        ORDER BY position ASC, id ASC
        """,
        (current_user.id,),
    )
    result = [
        {
            "id": r[0],
            "widget_type": r[1],
            "config": r[2],
            "position": r[3],
            "width": r[4],
            "height": r[5],
        }
        for r in c.fetchall()
    ]
    release_connection(conn)
    return jsonify(result)


@pm_bp.route("/api/widgets", methods=["POST"]) 
@login_required
def add_widget():
    payload = request.get_json() or {}
    widget_type = payload.get("widget_type")
    config = payload.get("config")
    position = int(payload.get("position", 0))
    width = int(payload.get("width", 6))
    height = int(payload.get("height", 3))

    if not widget_type:
        return jsonify({"error": "widget_type required"}), 400

    conn = get_connection()
    c = conn.cursor()
    c.execute(
        """
        INSERT INTO pm_dashboard_widgets (user_id, widget_type, config, position, width, height)
        VALUES (%s, %s, %s, %s, %s, %s)
        RETURNING id
        """,
        (current_user.id, widget_type, json.dumps(config) if isinstance(config, dict) else config, position, width, height),
    )
    new_id = c.fetchone()[0]
    conn.commit()
    release_connection(conn)
    return jsonify({"id": new_id}), 201


@pm_bp.route("/api/widgets/<int:widget_id>", methods=["DELETE"]) 
@login_required
def delete_widget(widget_id: int):
    conn = get_connection()
    c = conn.cursor()
    c.execute(
        "DELETE FROM pm_dashboard_widgets WHERE id = %s AND user_id = %s",
        (widget_id, current_user.id),
    )
    conn.commit()
    release_connection(conn)
    return jsonify({"status": "deleted"})


@pm_bp.route("/api/widgets/reorder", methods=["POST"]) 
@login_required
def reorder_widgets():
    order = request.get_json() or []  # [{id, position}]
    conn = get_connection()
    c = conn.cursor()
    for item in order:
        try:
            c.execute(
                "UPDATE pm_dashboard_widgets SET position = %s WHERE id = %s AND user_id = %s",
                (int(item.get("position", 0)), int(item.get("id")), current_user.id),
            )
        except Exception:
            continue
    conn.commit()
    release_connection(conn)
    return jsonify({"status": "ok"})


@pm_bp.route("/api/templates", methods=["POST"]) 
@login_required
def create_template():
    data = request.get_json() or {}
    name = data.get("name")
    description = data.get("description")
    items = data.get("items", [])
    markup = data.get("default_markup", 0)
    if not name:
        return jsonify({"error": "name required"}), 400

    conn = get_connection()
    c = conn.cursor()
    c.execute(
        """
        INSERT INTO pm_bid_templates (name, description, items, default_markup)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (name) DO UPDATE SET description = EXCLUDED.description, items = EXCLUDED.items, default_markup = EXCLUDED.default_markup
        RETURNING id
        """,
        (name, description, json.dumps(items), markup),
    )
    template_id = c.fetchone()[0]
    conn.commit()
    release_connection(conn)
    return jsonify({"id": template_id}), 201


@pm_bp.route("/risks", methods=["GET", "POST"]) 
@login_required
def risks():
    _ensure_pm_tables()
    conn = get_connection()
    c = conn.cursor()
    if request.method == "POST":
        title = request.form.get("title")
        probability = request.form.get("probability")
        impact = request.form.get("impact")
        mitigation = request.form.get("mitigation")
        try:
            c.execute(
                """
                INSERT INTO pm_risks (title, probability, impact, mitigation, owner_id)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (title, int(probability or 3), int(impact or 3), mitigation, current_user.id),
            )
            conn.commit()
            flash("Risk added")
        except Exception as e:
            conn.rollback()
            flash(f"Failed to add risk: {e}")
    c.execute(
        """
        SELECT id, title, probability, impact, mitigation, status, created_at
        FROM pm_risks
        ORDER BY created_at DESC
        """
    )
    risks = [
        {
            "id": r[0],
            "title": r[1],
            "probability": r[2],
            "impact": r[3],
            "mitigation": r[4],
            "status": r[5],
            "created_at": r[6],
        }
        for r in c.fetchall()
    ]
    release_connection(conn)
    return render_template("pm/risks.html", risks=risks)


@pm_bp.route("/risks/<int:risk_id>/delete", methods=["POST"]) 
@login_required
def delete_risk(risk_id: int):
    conn = get_connection()
    c = conn.cursor()
    c.execute("DELETE FROM pm_risks WHERE id = %s", (risk_id,))
    conn.commit()
    release_connection(conn)
    flash("Risk deleted")
    return redirect(url_for("pm.risks"))


# For multi-user Kanban, ensure board access checks
def check_board_access(board_id):
    # Implement logic to check if user can view/edit board
    return True  # Placeholder


