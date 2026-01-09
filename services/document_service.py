# services/document_service.py
from services.db import get_connection, release_connection
from datetime import datetime
from config import UPLOAD_FOLDER
import os
from services.document_loader import load_and_chunk_documents
from services.vector_indexer import build_vector_index
from services.logging_service import log_audit
from services.elasticsearch_client import es  # Import es here
from werkzeug.utils import secure_filename

ALLOWED_EXTENSIONS = {'pdf', 'doc', 'docx', 'txt', 'png', 'jpg', 'jpeg'}

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def extract_text(file_path):
    filename = os.path.basename(file_path)
    try:
        if filename.lower().endswith('.pdf'):
            chunks = parse_pdf(file_path, filename)
        elif filename.lower().endswith('.docx'):
            chunks = parse_docx(file_path, filename)
        elif filename.lower().endswith('.txt'):
            chunks = parse_txt(file_path, filename)
        else:
            raise ValueError(f"Unsupported file type: {filename}")
        return ' '.join([chunk['text'] for chunk in chunks])
    except Exception as e:
        log_audit(es, "extract_text_error", None, None, {"filename": filename, "error": str(e)})
        raise

def save_uploaded_document(file, user_id, tags=None):
    filename = secure_filename(file.filename)
    file_path = os.path.join(UPLOAD_FOLDER, filename)
    file.save(file_path)
    conn = get_connection()
    c = conn.cursor()
    c.execute("INSERT INTO documents (filename, uploader_id, upload_date, tags) VALUES (%s, %s, %s, %s)",
              (filename, user_id, datetime.now(), ','.join(tags or [])))
    conn.commit()
    release_connection(conn)
    log_audit(es, "document_upload", user_id, None, {"filename": filename})
    load_and_chunk_documents()
    build_vector_index()
    return filename

def get_all_documents():
    conn = get_connection()
    c = conn.cursor()
    docs = []
    try:
        # First try simple query to check what columns exist
        c.execute("SELECT id, filename, upload_date FROM documents ORDER BY upload_date DESC")
        rows = c.fetchall()
        for row in rows:
            docs.append({
                'id': row[0],
                'title': row[1] if row[1] else 'Untitled',
                'category': 'Uncategorized',
                'version': '1.0',
                'filename': row[1],
                'url': f'/documents/download/{row[1]}' if row[1] else '#',
                'upload_date': row[2]
            })
    except Exception as e:
        # If documents table doesn't exist or other error, return empty list
        import logging
        logging.error(f"Error fetching documents: {str(e)}")
    finally:
        release_connection(conn)
    return docs

def get_document_by_id(doc_id):
    conn = get_connection()
    c = conn.cursor()
    c.execute("SELECT * FROM documents WHERE id = %s", (doc_id,))
    doc = c.fetchone()
    release_connection(conn)
    return doc

def update_document(doc_id, new_title, user_id):
    conn = get_connection()
    c = conn.cursor()
    c.execute("UPDATE documents SET title = %s WHERE id = %s", (new_title, doc_id))
    conn.commit()
    release_connection(conn)
    log_audit(es, "document_update", user_id, None, {"doc_id": doc_id, "new_title": new_title})

def delete_document(doc_id):
    conn = get_connection()
    c = conn.cursor()
    c.execute("SELECT filename FROM documents WHERE id = %s", (doc_id,))
    filename = c.fetchone()[0]
    file_path = os.path.join(UPLOAD_FOLDER, filename)
    if os.path.exists(file_path):
        os.remove(file_path)
    c.execute("DELETE FROM documents WHERE id = %s", (doc_id,))
    conn.commit()
    release_connection(conn)
    log_audit(es, "document_delete", None, None, {"doc_id": doc_id})
    build_vector_index()

def get_document_versions(title):
    conn = get_connection()
    c = conn.cursor()
    c.execute("SELECT * FROM documents WHERE title = %s ORDER BY upload_date DESC", (title,))
    versions = c.fetchall()
    release_connection(conn)
    return versions

def index_document(file_path, filename, user_id):
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
        es.index(index='documents', body={'filename': filename, 'content': content, 'user_id': user_id, 'timestamp': datetime.now().isoformat()})
        log_audit(es, "document_index", user_id, None, {"filename": filename})
    except Exception as e:
        log_audit(es, "document_index_error", user_id, None, {"filename": filename, "error": str(e)})