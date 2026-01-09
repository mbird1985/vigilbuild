# services/elasticsearch_client.py
from elasticsearch import Elasticsearch
from config import ES_HOST, ES_USER, ES_PASS
import os

# Enforce .env usage
if not all([ES_HOST, ES_USER, ES_PASS]):
    raise ValueError("Elasticsearch credentials missing in config")

# Create shared ES client with connection pooling
es = Elasticsearch(
    ES_HOST,
    basic_auth=(ES_USER, ES_PASS)
)

# Test connection on import
try:
    if not es.ping():
        print(f"Elasticsearch not reachable at {ES_HOST}")
        raise ConnectionError("Elasticsearch not reachable")
    print(f"Elasticsearch connected successfully at {ES_HOST}")
except Exception as e:
    print(f"Elasticsearch initialization error: {str(e)}")
    raise