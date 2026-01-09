# services/ollama_llm.py
import requests
from config import OLLAMA_HOST
from services.logging_service import log_audit
from services.elasticsearch_client import es
import logging
import json

# Redis is optional - gracefully handle when not available
redis = None
try:
    from redis import Redis
    redis = Redis(socket_connect_timeout=1)
    redis.ping()  # Test connection
except Exception:
    redis = None
    logging.info("Redis not available - LLM caching disabled")

logging.basicConfig(filename='ollama_fallback.log', level=logging.WARNING,
                    format='%(asctime)s - %(levelname)s - %(message)s')

SYSTEM_PROMPT = """
You are an assistant for Potelco, a Quanta Services company specializing in power distribution. Respond with structured JSON, adhering to OSHA safety regulations, utility industry standards, and Quanta's operational needs (e.g., scheduling, inventory, safety alerts). Use precise terminology for construction and utility work.

Examples:
User: Schedule a bucket truck for Monday at 9AM
{
  "action": "schedule_equipment",
  "equipment": "bucket truck",
  "start": "2025-08-04 09:00",
  "end": "2025-08-04 17:00",
  "job": "Power line maintenance",
  "safety_check": "OSHA-compliant operator required"
}
User: Generate a safety report
{
  "action": "generate_report",
  "format": "pdf",
  "title": "Safety Compliance Report",
  "data": [
    {"incident": "Near miss", "date": "2025-07-30", "compliance": "OSHA 1910.269"}
  ]
}
Always return a valid JSON object.
"""

def call_ollama(user_prompt: str, model: str = "llama3", stream: bool = False) -> str:
    try:
        full_prompt = f"{SYSTEM_PROMPT}\n\nUser: {user_prompt}\nAssistant:"
        response = requests.post(
            f"{OLLAMA_HOST}/api/generate",
            json={"model": model, "prompt": full_prompt, "stream": stream},
            timeout=30
        )
        response.raise_for_status()
        result = response.json().get("response", "").strip()
        json.loads(result)  # Validate JSON
        return result
    except Exception as e:
        log_audit(es, "ollama_error", None, None, {"prompt": user_prompt, "error": str(e)})
        return json.dumps({"error": f"LLM error: {str(e)}"})

def generate_response(prompt, model="llama3.2"):
    cache_key = f"llm:{hash(prompt)}"

    # Try to get from cache if Redis is available
    if redis:
        try:
            cached = redis.get(cache_key)
            if cached:
                return cached.decode()
        except Exception:
            pass  # Redis error, continue without cache

    try:
        response = requests.post(
            f"{OLLAMA_HOST}/api/generate",
            json={"model": model, "prompt": prompt, "stream": False},
            timeout=30
        )
        response.raise_for_status()
        result = response.json().get("response", "No response from LLM")

        # Cache result if Redis is available
        if redis:
            try:
                redis.setex(cache_key, 3600, result)
            except Exception:
                pass  # Redis error, continue without caching

        return result
    except Exception as e:
        logging.error(f"Ollama LLM error: {str(e)}")
        log_audit(es, "ollama_error", None, None, {"prompt": prompt, "error": str(e)})
        return "I apologize, but I'm having trouble connecting to the AI service. Please try again later."