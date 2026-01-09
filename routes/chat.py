# routes/chat.py
from flask import Blueprint, request, jsonify, render_template
from flask_login import login_required, current_user
from services.rag_chat import generate_rag_response
from services.logging_service import log_audit
from services.elasticsearch_client import es
from services.db import get_connection

chat_bp = Blueprint("chat", __name__, url_prefix="/chat")

@chat_bp.route('/', methods=['GET'])
@login_required
def ask():
    return render_template('chat.html')

@chat_bp.route('/ask', methods=['POST'])
@login_required
def ask_question():
    try:
        query = request.form.get('query') or request.json.get('query')
        if not query:
            return jsonify({'error': 'No query provided'}), 400
        response = generate_rag_response(query)
        log_audit(es, 'chat_query', current_user.id, None, {'query': query, 'response': response[:200]})
        conn = get_connection()
        # Save query and response to chat_history table
        return jsonify({'response': response})
    except Exception as e:
        log_audit(es, 'chat_error', current_user.id, None, {'error': str(e)})
        return jsonify({'error': str(e)}), 500