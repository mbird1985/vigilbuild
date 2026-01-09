# services/logging_service.py
from datetime import datetime
from services.db import db_connection
import logging
import shutil
import os

# Configure fallback logging
logging.basicConfig(filename='audit_fallback.log', level=logging.WARNING,
                    format='%(asctime)s - %(levelname)s - %(message)s')

def log_analytics(es, event_type, data):
    try:
        if not es.indices.exists(index="analytics"):
            es.indices.create(index="analytics")
        es.index(index="analytics", body={
            "event_type": event_type,
            "data": data,
            "timestamp": datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        })
        es.indices.refresh(index="analytics")
    except Exception as e:
        logging.error(f"Failed to log analytics event '{event_type}': {str(e)}")
        print(f"Analytics logging failed: {str(e)}")

def log_audit(es, action, user_id, document_id=None, document_title=None, details=None):
    # Check fallback log size
    if os.path.exists('audit_fallback.log') and os.path.getsize('audit_fallback.log') > 10*1024*1024:
        shutil.copy('audit_fallback.log', f'audit_fallback_{datetime.now().strftime("%Y%m%d")}.log')
        open('audit_fallback.log', 'w').close()
    try:
        if not es.indices.exists(index="audit"):
            es.indices.create(index="audit")
        audit_entry = {
            "action": action,
            "user_id": user_id,
            "document_id": document_id,
            "document_title": document_title,
            "details": details or {},
            "timestamp": datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        }
        es.index(index="audit", body=audit_entry)
        es.indices.refresh(index="audit")
        print(f"Audit logged: {audit_entry}")
    except Exception as e:
        logging.error(f"Failed to log audit action '{action}' for user {user_id}: {str(e)}")
        print(f"Audit logging failed: {str(e)}")

def get_audit_logs():
    with db_connection() as conn:
        c = conn.cursor()
        c.execute("SELECT * FROM audit_log ORDER BY timestamp DESC")
        logs = c.fetchall()
        return logs