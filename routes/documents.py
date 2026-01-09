from flask import Blueprint, request, redirect, url_for, render_template, flash, send_from_directory, abort, current_app
from flask_login import login_required, current_user
from services.document_service import (
    save_uploaded_document, get_all_documents, get_document_by_id,
    update_document, delete_document, get_document_versions, allowed_file, index_document
)
from werkzeug.utils import secure_filename
from config import DOCUMENT_FOLDER, UPLOAD_FOLDER, MAX_IMAGE_SIZE
import os
# New: For AI tagging
from services.ollama_llm import generate_response  # Assume LLM for auto-tags

document_bp = Blueprint("documents", __name__, url_prefix="/documents")


def is_safe_path(basedir, path):
    """Verify that the resolved path is within the expected directory (prevent path traversal)"""
    # Resolve the absolute path
    abs_basedir = os.path.abspath(basedir)
    abs_path = os.path.abspath(os.path.join(basedir, path))
    # Check that the resolved path starts with the base directory
    return abs_path.startswith(abs_basedir)

@document_bp.route("/")
@login_required
def document_list():
    documents = get_all_documents()
    return render_template("documents.html", documents=documents)

@document_bp.route('/upload', methods=['POST'])
@login_required
def upload_document():
    if 'file' not in request.files:
        flash('No file provided.')
        return redirect(url_for('documents.document_list'))

    file = request.files['file']

    if not file or not file.filename:
        flash('No file selected.')
        return redirect(url_for('documents.document_list'))

    if not allowed_file(file.filename):
        flash('Invalid file type. Allowed types: pdf, docx, txt')
        return redirect(url_for('documents.document_list'))

    # Secure the filename to prevent path traversal attacks
    filename = secure_filename(file.filename)

    # Prevent empty filename after sanitization
    if not filename:
        flash('Invalid filename.')
        return redirect(url_for('documents.document_list'))

    # Check file size (limit to MAX_IMAGE_SIZE as a reasonable default)
    file.seek(0, os.SEEK_END)
    file_size = file.tell()
    file.seek(0)  # Reset file pointer

    if file_size > MAX_IMAGE_SIZE:
        flash(f'File too large. Maximum size is {MAX_IMAGE_SIZE // (1024*1024)}MB.')
        return redirect(url_for('documents.document_list'))

    # Verify the path is safe
    file_path = os.path.join(UPLOAD_FOLDER, filename)
    if not is_safe_path(UPLOAD_FOLDER, filename):
        flash('Invalid file path.')
        current_app.logger.warning(f"Path traversal attempt by user {current_user.id}: {filename}")
        return redirect(url_for('documents.document_list'))

    try:
        file.save(file_path)
        # New: AI auto-tagging
        tags = auto_generate_tags(file_path)
        # Save as new document; versioning can be implemented later
        save_uploaded_document(file, user_id=current_user.id, tags=tags)
        index_document(file_path, filename, current_user.id)
        flash('Document uploaded and indexed successfully.')
    except Exception as e:
        current_app.logger.error(f"Document upload failed: {str(e)}")
        flash('Failed to upload document. Please try again.')

    return redirect(url_for('documents.document_list'))

def auto_generate_tags(file_path):
    with open(file_path, 'r') as f:
        content = f.read(500)  # First 500 chars
    prompt = f"Generate 3-5 tags for this document content: {content}"
    response = generate_response(prompt)
    return response.split(', ')  # Simple parse

@document_bp.route("/uploads/<filename>")
@login_required
def uploaded_file(filename):
    # Secure the filename to prevent path traversal
    safe_filename = secure_filename(filename)

    if not safe_filename or safe_filename != filename:
        current_app.logger.warning(f"Path traversal attempt in uploads by user {current_user.id}: {filename}")
        abort(404)

    # Verify the file exists and path is safe
    if not is_safe_path(UPLOAD_FOLDER, safe_filename):
        abort(404)

    file_path = os.path.join(UPLOAD_FOLDER, safe_filename)
    if not os.path.exists(file_path):
        abort(404)

    return send_from_directory(UPLOAD_FOLDER, safe_filename, as_attachment=False)

@document_bp.route("/edit/<int:id>", methods=["GET", "POST"])
@login_required
def edit_document(id):
    document = get_document_by_id(id)
    if not document:
        flash("Document not found.")
        return redirect(url_for("documents.document_list"))
    if request.method == "POST":
        new_title = request.form.get("title")
        update_document(id, new_title, current_user.id)
        flash("Document updated.")
        return redirect(url_for("documents.document_list"))
    return render_template("edit_document.html", document=document)

@document_bp.route("/delete/<int:id>")
@login_required
def delete_document_route(id):
    delete_document(id)
    flash("Document deleted.")
    return redirect(url_for("documents.document_list"))

@document_bp.route("/versions/<title>")
@login_required
def document_versions(title):
    versions = get_document_versions(title)
    return render_template("versions.html", versions=versions, title=title)

@document_bp.route("/view/<filename>")
@login_required
def view_document(filename):
    # Secure the filename to prevent path traversal
    safe_filename = secure_filename(filename)

    if not safe_filename or safe_filename != filename:
        current_app.logger.warning(f"Path traversal attempt in view by user {current_user.id}: {filename}")
        flash("Invalid document path.")
        return redirect(url_for("documents.document_list"))

    # Verify path is within allowed directory
    if not is_safe_path(DOCUMENT_FOLDER, safe_filename):
        flash("Invalid document path.")
        return redirect(url_for("documents.document_list"))

    safe_path = os.path.join(DOCUMENT_FOLDER, safe_filename)
    if not os.path.exists(safe_path):
        flash("Document not found.")
        return redirect(url_for("documents.document_list"))

    # Display inline in browser
    return send_from_directory(DOCUMENT_FOLDER, safe_filename, as_attachment=False)