# services/rag_chat.py
import re
from flask import session
from elasticsearch import Elasticsearch
from config import ES_HOST, ES_USER, ES_PASS
from services.ollama_llm import generate_response
from services.jobs_service import get_job_status
from services.inventory_service import get_inventory_quantity
from services.logging_service import log_audit
from services.elasticsearch_client import es

def search_knowledge_base(query, top_k=3):
    res = es.search(index='knowledge_base', body={
        'query': {
            'match': {
                'text': query
            }
        },
        'size': top_k
    })
    return [hit['_source']['text'] for hit in res['hits']['hits']]

def classify_intent(query):
    if re.search(r'status of job (\w+)', query, re.IGNORECASE):
        return 'job_status', re.search(r'status of job (\w+)', query, re.IGNORECASE).group(1)
    elif re.search(r'how many (\w+) are in inventory', query, re.IGNORECASE):
        return 'inventory_quantity', re.search(r'how many (\w+) are in inventory', query, re.IGNORECASE).group(1)
    else:
        return 'general', None

def generate_rag_response(query):
    intent, param = classify_intent(query)
    if intent == 'job_status':
        job_name = param
        status = get_job_status(job_name)
        if status:
            return f"The status of job {job_name} is {status}."
        else:
            return f"Job {job_name} not found."
    elif intent == 'inventory_quantity':
        item_name = param
        quantity = get_inventory_quantity(item_name)
        if quantity is not None:
            return f"There are {quantity} {item_name} in inventory."
        else:
            return f"{item_name} not found in inventory."
    else:
        context = search_knowledge_base(query)
        context_str = '\n'.join(context)
        history = session.get('chat_history', [])[-5:]
        context_str += "\nHistory: " + "\n".join([f"Q: {h['q']}\nA: {h['a']}" for h in history])
        prompt = f"Based on the following information:\n{context_str}\n\nAnswer the question: {query}"
        response = generate_response(prompt)
        log_audit(es, 'rag_query', None, None, {'query': query, 'response': response[:200]})
        history.append({'q': query, 'a': response})
        session['chat_history'] = history
        return response
