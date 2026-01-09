# services/report_service.py
from reportlab.lib.pagesizes import letter
from reportlab.lib.units import inch  # Added for margins
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, Image
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib import colors  # For table styling
import csv
import json
from services.logging_service import log_audit
from services.db import get_connection, release_connection
from services.elasticsearch_client import es
import openpyxl

def generate_pdf_report(filepath, title, data, chart_path=None):
    try:
        styles = getSampleStyleSheet()
        styles['Title'].alignment = 1  # Center title
        doc = SimpleDocTemplate(filepath, pagesize=letter, rightMargin=inch*0.5, leftMargin=inch*0.5, topMargin=inch, bottomMargin=inch*0.5)
        elements = [Paragraph(title, styles['Title']), Spacer(1, 12)]
        if data:
            keys = list(data[0].keys())
            table_data = [keys] + [list(d.values()) for d in data]
            table = Table(table_data, colWidths=[doc.width/len(keys)] * len(keys))
            table.setStyle([
                ('BACKGROUND', (0,0), (-1,0), colors.grey),
                ('TEXTCOLOR', (0,0), (-1,0), colors.whitesmoke),
                ('ALIGN', (0,0), (-1,-1), 'CENTER'),
                ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold'),
                ('BOTTOMPADDING', (0,0), (-1,0), 12),
                ('BACKGROUND', (0,1), (-1,-1), colors.beige),
            ])
            elements.append(table)
        if chart_path:
            elements.append(Spacer(1, 12))
            elements.append(Image(chart_path, width=6*inch, height=3*inch))
        doc.build(elements)
        log_audit(es, None, "generate_pdf_report", {"filepath": filepath, "title": title})
    except Exception as e:
        log_audit(es, None, "generate_pdf_report_error", {"filepath": filepath, "error": str(e)})
        raise

def generate_csv_report(filepath, data):
    if not data:
        return
    try:
        with open(filepath, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=data[0].keys())
            writer.writeheader()
            writer.writerows(data)
        log_audit(es, None, "generate_csv_report", {"filepath": filepath})
    except Exception as e:
        log_audit(es, None, "generate_csv_report_error", {"filepath": filepath, "error": str(e)})
        raise

def parse_report_data(data_raw):
    try:
        return json.loads(data_raw)
    except json.JSONDecodeError:
        return []

def generate_filtered_report(report_type, date_range):
    conn = get_connection()
    c = conn.cursor()
    if report_type == "summary":
        start_date = datetime.now() - timedelta(days=int(date_range[:-1]))
        c.execute("SELECT * FROM schedules WHERE start_date >= %s", (start_date,))
        data = c.fetchall()
    # Expand for other types
    release_connection(conn)
    return data

def generate_filtered_pdf_report(filepath, title, data, filter_key, filter_value):
    filtered_data = [item for item in data if item.get(filter_key) == filter_value]
    try:
        styles = getSampleStyleSheet()
        doc = SimpleDocTemplate(filepath, pagesize=letter)
        elements = [Paragraph(f"{title} (Filtered by {filter_key}={filter_value})", styles['Title']), Spacer(1, 12)]
        if filtered_data:
            keys = list(filtered_data[0].keys())
            table_data = [keys] + [list(d.values()) for d in filtered_data]
            table = Table(table_data)
            elements.append(table)
        else:
            elements.append(Paragraph("No data matches the filter.", styles['Normal']))
        doc.build(elements)
        log_audit(None, es, "generate_filtered_report", {"filepath": filepath, "title": title, "filter_key": filter_key})
    except Exception as e:
        log_audit(None, es, "generate_filtered_report_error", {"filepath": filepath, "error": str(e)})
        raise

def generate_excel_report(filepath, data):
    wb = openpyxl.Workbook()
    ws = wb.active
    if data:
        headers = list(data[0].keys())
        ws.append(headers)
        for row in data:
            ws.append(list(row.values()))
    wb.save(filepath)