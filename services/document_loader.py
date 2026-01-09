# services/document_loader.py
import os
import fitz  # PyMuPDF
import pytesseract
from docx import Document
from config import DOCUMENT_FOLDER
from services.logging_service import log_audit
from services.elasticsearch_client import es  # Import es here
import asyncio

pytesseract.pytesseract.tesseract_cmd = r'C:\Program Files\Tesseract-OCR\tesseract.exe'

CHUNK_SIZE = 500

async def load_and_chunk_documents():
    all_chunks = []
    for filename in os.listdir(DOCUMENT_FOLDER):
        filepath = os.path.join(DOCUMENT_FOLDER, filename)
        if os.path.getsize(filepath) > 10 * 1024 * 1024:  # 10MB limit
            continue  # or raise error
        if not filename.lower().endswith((".pdf", ".txt", ".docx")):
            continue
        try:
            if filename.lower().endswith(".pdf"):
                chunks = parse_pdf(filepath, filename)
            elif filename.lower().endswith(".docx"):
                chunks = parse_docx(filepath, filename)
            else:
                chunks = parse_txt(filepath, filename)
            all_chunks.extend(chunks)
            log_audit(es, "document_load", None, None, {"filename": filename, "chunks": len(chunks)})
        except Exception as e:
            log_audit(es, "document_load_error", None, None, {"filename": filename, "error": str(e)})
    return all_chunks

def parse_pdf(filepath, source):
    doc = fitz.open(filepath)
    chunks = []
    for page_number, page in enumerate(doc, start=1):
        text = page.get_text("text")
        if not text:
            pix = page.get_pixmap()
            text = pytesseract.image_to_string(pix)
        page_chunks = chunk_text(text, CHUNK_SIZE)
        for i, chunk in enumerate(page_chunks):
            chunks.append({
                "text": chunk,
                "source": source,
                "page": page_number,
                "chunk_id": f"{source}_p{page_number}_c{i}"
            })
    return chunks

def parse_docx(filepath, source):
    doc = Document(filepath)
    text = "\n".join([para.text for para in doc.paragraphs if para.text.strip()])
    chunks = chunk_text(text, CHUNK_SIZE)
    return [
        {
            "text": chunk,
            "source": source,
            "page": None,
            "chunk_id": f"{source}_c{i}"
        }
        for i, chunk in enumerate(chunks)
    ]

def parse_txt(filepath, source):
    with open(filepath, "r", encoding="utf-8", errors="ignore") as f:
        text = f.read()
    chunks = chunk_text(text, CHUNK_SIZE)
    return [
        {
            "text": chunk,
            "source": source,
            "page": None,
            "chunk_id": f"{source}_c{i}"
        }
        for i, chunk in enumerate(chunks)
    ]

def chunk_text(text, size):
    paragraphs = [p.strip() for p in text.split("\n\n") if p.strip()]
    chunks = []
    for para in paragraphs:
        while len(para) > size:
            split_at = para.rfind(" ", 0, size)
            if split_at == -1:
                split_at = size
            chunks.append(para[:split_at].strip())
            para = para[split_at:].strip()
        if para:
            chunks.append(para)
    return chunks
