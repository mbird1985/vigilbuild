# services/tools.py
from services.report_service import generate_pdf_report
from services.inventory_service import get_inventory_summary, deduct_quantity_on_assignment
from services.schedule_service import schedule_equipment_by_name, get_schedule_summary
from services.equipment_service import get_equipment_status
from services.jobs_service import create_job, optimize_job_cost
from services.logging_service import log_audit
import os
from werkzeug.utils import secure_filename
from flask import jsonify
from datetime import datetime

TOOLS = {}

def register_tool(name):
    def wrapper(func):
        TOOLS[name] = func
        return func
    return wrapper

@register_tool("generate_report")
def handle_generate_report(params):
    title = params.get("title", "AI Report")
    data = params.get("data", [])
    report_type = params.get("format", "pdf")
    safe_title = secure_filename(title.replace(' ', '_'))
    filename = f"static/reports/{safe_title}.{report_type}"
    os.makedirs("static/reports", exist_ok=True)
    if report_type == "pdf":
        generate_pdf_report(filename, title, data)
        log_audit(None, "generate_report", {"title": title, "filename": filename})
        return {"message": f"Report '{title}' generated.", "download_url": f"/{filename}"}
    return {"message": "Only PDF reports are supported right now."}

@register_tool("get_inventory_summary")
def handle_inventory_summary(_params):
    summary = get_inventory_summary()
    return {"message": "Inventory Summary:\n" + "\n".join(summary)}

@register_tool("schedule_equipment")
def handle_schedule_equipment(params):
    equipment = params.get("equipment")
    start = params.get("start")
    end = params.get("end")
    job = params.get("job", f"Scheduled via assistant for {equipment}")
    if not (equipment and start and end):
        return {"message": "Missing required fields: equipment, start, end."}
    message = schedule_equipment_by_name(equipment, start[:10], start[11:16], end[:10], end[11:16], job)
    log_audit(None, "schedule_equipment", {"equipment": equipment, "start": start, "end": end})
    return {"message": message}

@register_tool("get_schedule_summary")
def handle_schedule_summary(params):
    days = params.get("days", 1)
    try:
        days = int(days)
    except ValueError:
        days = 1
    summary = get_schedule_summary(days_ahead=days)
    return {"message": "Upcoming Schedule:\n" + "\n".join(summary)}

@register_tool("get_equipment_status")
def handle_equipment_status(params):
    keyword = params.get("filter")
    status_list = get_equipment_status(keyword)
    return {"message": "Equipment Status:\n" + "\n".join(status_list)}

@register_tool("create_job")
def handle_create_job(params):
    name = params.get("name")
    description = params.get("description")
    estimated_cost = params.get("estimated_cost", 0.0)
    location = params.get("location")
    user_id = params.get("user_id", "system")
    if not (name and location):
        return {"message": "Missing required fields: name, location."}
    job_id = create_job(name, description, estimated_cost, location, user_id)
    return {"message": f"Job {name} created with ID {job_id}."}

@register_tool("optimize_job_cost")
def handle_optimize_job_cost(params):
    job_id = params.get("job_id")
    if not job_id:
        return {"message": "Missing required field: job_id."}
    cost = optimize_job_cost(job_id)
    return {"message": f"Optimized cost for job {job_id}: ${cost:.2f}"}

@register_tool("deduct_inventory")
def handle_deduct_inventory(params):
    item_id = params.get("item_id")
    quantity = params.get("quantity", 1)
    try:
        quantity = int(quantity)
    except Exception:
        quantity = 1
    user_id = params.get("user_id", "system")
    if not item_id or quantity < 1:
        return {"message": "Missing or invalid fields: item_id, quantity."}
    deduct_quantity_on_assignment(item_id, quantity, user_id)
    return {"message": f"Deducted {quantity} from item {item_id}."}

@register_tool("get_weather")
def handle_get_weather(params):
    location = params.get("location")
    date = params.get("date", datetime.now().strftime("%Y-%m-%d"))
    if not location:
        return {"message": "Location required."}
    from services.weather import fetch_weather
    weather = fetch_weather(location, date)
    return {"weather": weather}

def route_action(action_name, params):
    handler = TOOLS.get(action_name)
    if handler:
        result = handler(params)
        return jsonify(result)  # Ensure JSON response
    return jsonify({"message": f"No handler found for action '{action_name}'"})