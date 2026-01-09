# services/vector_indexer.py
import faiss
import numpy as np
import pickle
from sentence_transformers import SentenceTransformer
from services.document_loader import load_and_chunk_documents
from services.logging_service import log_audit
from services.elasticsearch_client import es
import os
from config import S3_BUCKET, S3_ACCESS_KEY, S3_SECRET_KEY
from redis import Redis

INDEX_FILE = "vector_index/faiss.index"
META_FILE = "vector_index/meta.pkl"
INDEX_VERSION = "v1"

redis_client = Redis(host='localhost', port=6379, db=0)
model = SentenceTransformer("all-MiniLM-L12-v2", device="cuda")  # GPU for embeddings, faster model

def build_vector_index():
    print("🔄 Loading and chunking documents...")
    chunks = load_and_chunk_documents()

    texts = [chunk["text"] for chunk in chunks]
    embeddings = model.encode(texts, show_progress_bar=True, convert_to_numpy=True, device="cuda", batch_size=128)

    if faiss.get_num_gpus() > 0:
        res = faiss.StandardGpuResources()
        index = faiss.GpuIndexFlatL2(res, embeddings.shape[1])
    else:
        index = faiss.IndexFlatL2(embeddings.shape[1])
    index.add(embeddings)

    try:
        os.makedirs("vector_index", exist_ok=True)
        faiss.write_index(index, f"{INDEX_FILE}.{INDEX_VERSION}")
        with open(f"{META_FILE}.{INDEX_VERSION}", "wb") as f:
            pickle.dump(chunks, f)
        if es:
            for i, chunk in enumerate(chunks):
                es.index(index="document_chunks", id=f"{chunk['source']}_{i}", body=chunk)
        print(f"✅ Indexed {len(chunks)} chunks.")
        log_audit(es, "build_vector_index", None, None, {"chunk_count": len(chunks), "version": INDEX_VERSION})
        if S3_ACCESS_KEY and S3_SECRET_KEY and S3_BUCKET:
            upload_to_s3(f"{INDEX_FILE}.{INDEX_VERSION}", S3_BUCKET)
            upload_to_s3(f"{META_FILE}.{INDEX_VERSION}", S3_BUCKET)
    except Exception as e:
        log_audit(es, "build_vector_index_error", None, None, {"error": str(e)})
        raise

def semantic_search(query, top_k=5):
    cache_key = f"search:{query}"
    cached = redis_client.get(cache_key)
    if cached:
        return pickle.loads(cached)

    try:
        index = faiss.read_index(f"{INDEX_FILE}.{INDEX_VERSION}")
        with open(f"{META_FILE}.{INDEX_VERSION}", "rb") as f:
            metadata = pickle.load(f)
    except Exception as e:
        log_audit(es, "load_index_error", None, None, {"error": str(e)})
        return []

    query_vec = model.encode([query], convert_to_numpy=True, device="cuda", batch_size=128)
    D, I = index.search(query_vec, top_k)
    results = [metadata[i] for i in I[0] if i < len(metadata)]
    redis_client.setex(cache_key, 3600, pickle.dumps(results))  # Cache for 1 hour
    log_audit(es, "semantic_search", None, None, {"query": query, "results": len(results)})
    return results

def upload_to_s3(file_path, bucket):
    try:
        import boto3
        if not (S3_ACCESS_KEY and S3_SECRET_KEY and bucket):
            log_audit(es, "s3_skip", None, None, {"reason": "AWS credentials not set"})
            return
        s3 = boto3.client("s3", aws_access_key_id=S3_ACCESS_KEY, aws_secret_access_key=S3_SECRET_KEY)
        s3.upload_file(file_path, bucket, os.path.basename(file_path))
        log_audit(es, "s3_upload", None, None, {"file": file_path, "bucket": bucket})
    except Exception as e:
        log_audit(es, "s3_upload_error", None, None, {"file": file_path, "error": str(e)})