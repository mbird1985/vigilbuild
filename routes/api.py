# routes/api.py
from flask import Blueprint, jsonify, request
from flask_login import login_required, current_user
try:
    from flask_jwt_extended import jwt_required
except Exception:
    # Fallback no-op decorator if JWT isn't installed/configured
    def jwt_required(fn=None, *dargs, **dkwargs):
        def decorator(f):
            return f
        return decorator if fn is None else decorator(fn)
from services.inventory_service import (
    deduct_quantity_on_assignment,
    get_inventory_item,
    generate_reorder_report,
)
from services.document_service import get_document_by_id
from services.vector_indexer import model  # SentenceTransformer for tags
from services.tools import route_action
from services.integration_service import (
    IntegrationManager, 
    fetch_outlook_events, 
    send_outlook_email,
    get_powerbi_datasets,
    get_salesforce_accounts,
    call_custom_api
)
from services.inventory_service import upsert_reorder_suggestions
from services.elasticsearch_client import es
from services.logging_service import log_audit

api_bp = Blueprint("api", __name__, url_prefix="/api")

@api_bp.route("/inventory/v1/reorder/suggest", methods=["POST"])
@login_required
def suggest_reorders():
    """Return system-generated reorder suggestions with supplier and cost info"""
    suggestions = generate_reorder_report()
    try:
        log_audit(es, 'inventory_reorder_suggest_view', current_user.id, None, {'count': len(suggestions)})
    except Exception:
        pass
    return jsonify({"items": suggestions})

@api_bp.route("/inventory/v1/reorder/approve", methods=["POST"])
@login_required
def approve_reorder():
    """Approve a reorder request; supports Idempotency-Key for dedupe"""
    # Role check
    try:
        if not hasattr(current_user, 'has_role') or not current_user.has_role('admin', 'inventory.approver'):
            return jsonify({"error": "Forbidden"}), 403
    except Exception:
        return jsonify({"error": "Forbidden"}), 403
    data = request.get_json() or {}
    request_id = data.get("request_id")
    approved_quantity = data.get("approved_quantity")
    supplier_name = data.get("supplier_name")
    notes = data.get("notes")

    if not all([request_id, approved_quantity]):
        return jsonify({"error": "Missing required fields"}), 400

    # Idempotency
    idempotency_key = request.headers.get("Idempotency-Key")

    from services.db import get_connection, release_connection
    conn = get_connection()
    c = conn.cursor()

    try:
        if idempotency_key:
            c.execute("SELECT id FROM reorder_requests WHERE idempotency_key = %s", (idempotency_key,))
            existing = c.fetchone()
            if existing:
                # Already processed
                return jsonify({"message": "Already processed", "request_id": existing[0]}), 200

        # Verify request
        c.execute("SELECT id, consumable_id, suggested_quantity, status FROM reorder_requests WHERE id = %s", (request_id,))
        row = c.fetchone()
        if not row:
            return jsonify({"error": "Request not found"}), 404
        if row[3] not in ("pending", "suggested"):
            return jsonify({"error": f"Request not in approvable state: {row[3]}"}), 400

        # Create approval record
        c.execute(
            """
            INSERT INTO reorder_approvals (request_id, approved_by, approved_quantity, approval_notes)
            VALUES (%s, %s, %s, %s)
            RETURNING id
            """,
            (request_id, current_user.id, int(approved_quantity), notes),
        )
        approval_id = c.fetchone()[0]

        # Mark request approved and set idempotency key if provided
        c.execute(
            "UPDATE reorder_requests SET status = 'approved', updated_at = CURRENT_TIMESTAMP, idempotency_key = COALESCE(idempotency_key, %s) WHERE id = %s",
            (idempotency_key, request_id),
        )

        # Dispatch via email (stub for vendor adapters)
        from services.email_service import send_notification
        c.execute("""
            SELECT c.name, c.unit, COALESCE(si.supplier_name, %s), COALESCE(si.supplier_sku, c.supplier_sku)
            FROM consumables c
            LEFT JOIN supplier_items si ON si.consumable_id = c.id AND si.preferred = TRUE
            WHERE c.id = %s
        """, (supplier_name, row[1]))
        item_row = c.fetchone()
        item_name, unit, resolved_supplier, supplier_sku = item_row

        subject = f"PO Approval - {item_name}"
        body = f"Approved reorder for {approved_quantity} {unit} of {item_name}. Supplier: {resolved_supplier}. SKU: {supplier_sku}."
        send_notification(subject, body, ["procurement@potelco.com"])  # replace with real distribution list

        # Track purchase order stub
        c.execute(
            """
            INSERT INTO purchase_orders (request_id, supplier_name, payload, dispatch_channel)
            VALUES (%s, %s, %s, %s)
            RETURNING id
            """,
            (request_id, resolved_supplier, None, 'email'),
        )
        po_id = c.fetchone()[0]

        conn.commit()
        try:
            log_audit(es, 'inventory_reorder_approved', current_user.id, None, {
                'request_id': request_id,
                'approval_id': approval_id,
                'po_id': po_id,
                'approved_quantity': approved_quantity,
                'supplier': resolved_supplier,
            })
        except Exception:
            pass
        return jsonify({"approval_id": approval_id, "po_id": po_id})
    except Exception as e:
        conn.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        release_connection(conn)

@api_bp.route("/inventory/v1/reorder/queue", methods=["GET"])
@login_required
def get_reorder_queue():
    # Role check (view queue restricted to approvers)
    try:
        if not hasattr(current_user, 'has_role') or not current_user.has_role('admin', 'inventory.approver'):
            return jsonify({"error": "Forbidden"}), 403
    except Exception:
        return jsonify({"error": "Forbidden"}), 403
    from services.db import get_connection, release_connection
    conn = get_connection()
    c = conn.cursor()
    try:
        c.execute(
            """
            SELECT rr.id, c.name, rr.suggested_quantity, rr.unit, rr.status, rr.estimated_cost, rr.lead_time_days
            FROM reorder_requests rr
            JOIN consumables c ON c.id = rr.consumable_id
            WHERE rr.status IN ('pending','suggested','approved')
            ORDER BY rr.created_at DESC
            """
        )
        items = [
            {
                "request_id": r[0],
                "name": r[1],
                "suggested_quantity": r[2],
                "unit": r[3],
                "status": r[4],
                "estimated_cost": float(r[5]) if r[5] is not None else None,
                "lead_time_days": r[6],
            }
            for r in c.fetchall()
        ]
        try:
            log_audit(es, 'inventory_reorder_queue_view', current_user.id, None, {'count': len(items)})
        except Exception:
            pass
        return jsonify({"items": items})
    finally:
        release_connection(conn)

@api_bp.route("/inventory/v1/reorder/suggest/run", methods=["POST"])
@login_required
def run_reorder_suggestion_job():
    try:
        if not hasattr(current_user, 'has_role') or not current_user.has_role('admin', 'inventory.approver'):
            return jsonify({"error": "Forbidden"}), 403
    except Exception:
        return jsonify({"error": "Forbidden"}), 403
    created = upsert_reorder_suggestions(created_by=current_user.id)
    try:
        log_audit(es, 'inventory_reorder_suggest_run', current_user.id, None, {'created': created})
    except Exception:
        pass
    return jsonify({"created": created})

@api_bp.route("/documents/suggest_tags", methods=["POST"])
@login_required
def suggest_tags():
    data = request.get_json()
    title = data.get("title")
    if not title:
        return jsonify({"message": "Missing title"}), 400
    # Use SentenceTransformer for tag suggestion
    prompt = f"Suggest 3-5 tags for a document titled: {title}"
    embedding = model.encode([prompt])
    tags = ["construction", "safety", "utility"]  # Mock; replace with LLM call
    return jsonify({"tags": tags})

@api_bp.route("/report/data/<source>", methods=["GET"])
@login_required
def get_report_data(source):
    from services.db import get_connection, release_connection
    conn = get_connection()
    c = conn.cursor()
    if source == "schedules":
        c.execute("SELECT * FROM schedules LIMIT 10")
        data = [{"id": row[0], "job": row[7], "start": f"{row[3]} {row[5]}"} for row in c.fetchall()]
    elif source == "inventory":
        c.execute("SELECT * FROM consumables LIMIT 10")
        data = [{"name": row[1], "quantity": row[3], "unit": row[6]} for row in c.fetchall()]
    elif source == "equipment":
        c.execute("SELECT * FROM equipment_instances LIMIT 10")
        data = [{"unique_id": row[2], "status": row[11], "hours": row[6]} for row in c.fetchall()]
    else:
        data = []
    release_connection(conn)
    return jsonify(data)

@api_bp.route("/report/chart", methods=["GET"])
@login_required
def get_chart_data():
    # Mock data; replace with real DB query
    return jsonify({"labels": ["Item A", "Item B"], "values": [10, 20]})

@api_bp.route("/action/<action_name>", methods=["POST"])
@login_required
def handle_action(action_name):
    return route_action(action_name, request.get_json())

# Integration API endpoints
@api_bp.route("/integrations/status", methods=["GET"])
@login_required
def get_integrations_status():
    """Get status of all integrations for current user"""
    integration_mgr = IntegrationManager(current_user.id)
    
    status = {
        'outlook': integration_mgr.is_connected('outlook'),
        'powerbi': integration_mgr.is_connected('powerbi'),
        'salesforce': integration_mgr.is_connected('salesforce'),
        'custom_systems': []
    }
    
    # Get custom integrations
    from services.db import get_connection, release_connection
    conn = get_connection()
    c = conn.cursor()
    c.execute("""
        SELECT system_type, system_name FROM integrations 
        WHERE user_id = %s AND system_type NOT IN ('outlook', 'powerbi', 'salesforce')
        AND enabled = TRUE
    """, (current_user.id,))
    
    for row in c.fetchall():
        status['custom_systems'].append({
            'system_type': row[0],
            'system_name': row[1]
        })
    
    release_connection(conn)
    return jsonify(status)

@api_bp.route("/integrations/outlook/events", methods=["GET"])
@login_required
def get_outlook_events():
    """Get Outlook calendar events"""
    limit = request.args.get('limit', 10, type=int)
    events = fetch_outlook_events(current_user.id, limit)
    return jsonify(events)

@api_bp.route("/integrations/outlook/send_email", methods=["POST"])
@login_required
def send_email_via_outlook():
    """Send email via Outlook"""
    data = request.get_json()
    to_email = data.get('to_email')
    subject = data.get('subject')
    body = data.get('body')
    is_html = data.get('is_html', False)
    
    if not all([to_email, subject, body]):
        return jsonify({"error": "Missing required fields"}), 400
    
    success, message = send_outlook_email(current_user.id, to_email, subject, body, is_html)
    
    if success:
        return jsonify({"message": message})
    else:
        return jsonify({"error": message}), 400

@api_bp.route("/integrations/powerbi/datasets", methods=["GET"])
@login_required
def get_power_bi_datasets():
    """Get Power BI datasets"""
    datasets = get_powerbi_datasets(current_user.id)
    return jsonify(datasets)

@api_bp.route("/integrations/salesforce/accounts", methods=["GET"])
@login_required
def get_sf_accounts():
    """Get Salesforce accounts"""
    limit = request.args.get('limit', 10, type=int)
    accounts = get_salesforce_accounts(current_user.id, limit)
    return jsonify(accounts)

@api_bp.route("/integrations/custom/<system_type>/<path:endpoint>", methods=["GET", "POST", "PUT", "DELETE"])
@login_required
def call_custom_integration(system_type, endpoint):
    """Make API call to custom integration"""
    method = request.method
    data = request.get_json() if request.method in ['POST', 'PUT'] else None
    
    result = call_custom_api(current_user.id, system_type, endpoint, method, data)
    
    if result is not None:
        return jsonify(result)
    else:
        return jsonify({"error": "Failed to call custom API or integration not found"}), 400

@api_bp.route("/integrations/test/<system_type>", methods=["POST"])
@login_required
def test_integration(system_type):
    """Test integration connection"""
    integration_mgr = IntegrationManager(current_user.id)
    config = integration_mgr.get_integration(system_type)
    
    if not config:
        return jsonify({"error": "Integration not found"}), 404
    
    # Import the test function from integrations route
    from routes.integrations import test_integration_connection
    success, message = test_integration_connection(system_type, config)
    
    return jsonify({
        "success": success,
        "message": message
    })

@api_bp.route("/equipment/check_unique_id", methods=["POST"])
@login_required
def check_equipment_unique_id():
    """Check if an equipment unique_id already exists"""
    data = request.get_json()
    unique_id = data.get("unique_id", "").strip()
    exclude_id = data.get("exclude_id")  # For edit operations
    
    if not unique_id:
        return jsonify({"exists": False, "message": "No unique_id provided"})
    
    try:
        from services.equipment_service import check_unique_id_exists
        exists = check_unique_id_exists(unique_id, exclude_id)
        return jsonify({"exists": exists})
    except Exception as e:
        return jsonify({"exists": False, "error": str(e)}), 500

@api_bp.route("/integrations/sync/<system_type>", methods=["POST"])
@login_required
def sync_integration_data(system_type):
    """Sync data from integration system"""
    integration_mgr = IntegrationManager(current_user.id)
    
    if not integration_mgr.is_connected(system_type):
        return jsonify({"error": "Integration not connected"}), 400
    
    try:
        if system_type == 'outlook':
            # Sync calendar events
            events = fetch_outlook_events(current_user.id, 50)
            # Here you could save events to your database
            return jsonify({"message": f"Synced {len(events)} events from Outlook"})
        
        elif system_type == 'salesforce':
            # Sync accounts
            accounts = get_salesforce_accounts(current_user.id, 100)
            # Here you could save accounts to your database
            return jsonify({"message": f"Synced {len(accounts)} accounts from Salesforce"})
        
        elif system_type == 'powerbi':
            # Sync datasets info
            datasets = get_powerbi_datasets(current_user.id)
            return jsonify({"message": f"Found {len(datasets)} datasets in Power BI"})
        
        else:
            return jsonify({"error": "Sync not supported for this integration type"}), 400
    
    except Exception as e:
        return jsonify({"error": f"Sync failed: {str(e)}"}), 500