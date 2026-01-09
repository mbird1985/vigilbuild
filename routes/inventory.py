# routes/inventory.py
from flask import Blueprint, render_template, redirect, url_for, flash, request, jsonify, send_file, Response
from flask_login import login_required, current_user
from werkzeug.utils import secure_filename
import json
import os
import csv
import io
import logging
import sys
from datetime import datetime, timedelta
from config import UPLOAD_FOLDER
from services.db import db_connection, db_transaction, get_connection, release_connection
from typing import Optional, Dict
from services.logging_service import log_audit
from services.inventory_service import (
    get_enhanced_inventory_item, get_all_consumables, add_consumable_with_grouping,
    create_enhanced_consumable, merge_with_existing_item, update_consumable,
    delete_consumable, get_inventory_stats, get_item_templates,
    get_categories_and_base_names, find_similar_items, generate_supply_list_for_job,
    SupplierManagement, InventoryAnalytics, bulk_import_inventory,
    generate_reorder_report, get_inventory_value_report, track_job_consumable_usage,
    upload_item_image, EnhancedInventoryNameGenerator, allowed_file, create_inventory_alert,
    # New enhanced classes for metrics, suppliers, orders
    InventoryMetrics, EnhancedSupplierManagement, PriceHistoryTracker,
    PurchaseOrderManager, InventoryTransferManager, SupplierAPIIntegration
)

inventory_bp = Blueprint("inventory", __name__)

# Constants
ALLOWED_IMAGE_EXTENSIONS = {'png', 'jpg', 'jpeg', 'gif'}
ALLOWED_DOC_EXTENSIONS = {'pdf', 'doc', 'docx', 'xls', 'xlsx'}

def allowed_file(filename, allowed_extensions):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in allowed_extensions

def get_recent_inventory_alerts():
    """Get recent inventory alerts for display"""
    try:
        with db_connection() as conn:
            c = conn.cursor()

            # Check if inventory_alerts table exists
            c.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_schema = 'public'
                    AND table_name = 'inventory_alerts'
                )
            """)
            table_exists = c.fetchone()[0]

            if not table_exists:
                return []

            # Get unacknowledged alerts from last 7 days
            c.execute("""
                SELECT ia.id, ia.alert_type, ia.alert_level, ia.message,
                       ia.created_at, c.name, c.internal_sku
                FROM inventory_alerts ia
                JOIN consumables c ON ia.consumable_id = c.id
                WHERE ia.acknowledged = FALSE
                AND ia.created_at > CURRENT_DATE - INTERVAL '7 days'
                AND c.active = TRUE
                ORDER BY
                    CASE ia.alert_level
                        WHEN 'critical' THEN 1
                        WHEN 'warning' THEN 2
                        ELSE 3
                    END,
                    ia.created_at DESC
                LIMIT 10
            """)

            alerts = []
            for row in c.fetchall():
                alerts.append({
                    'id': row[0],
                    'type': row[1],
                    'level': row[2],
                    'message': row[3],
                    'created_at': row[4],
                    'item_name': row[5],
                    'item_sku': row[6]
                })

            return alerts
    except Exception as e:
        logging.error(f"Error getting inventory alerts: {str(e)}")
        return []

def get_template_for_item(category: str, base_name: str) -> Optional[Dict]:
    """Get template for specific category and base name"""
    try:
        with db_connection() as conn:
            c = conn.cursor()
            c.execute("""
                SELECT required_specs, optional_specs, default_unit, naming_pattern
                FROM item_templates
                WHERE category = %s AND base_name = %s
            """, (category, base_name))
            result = c.fetchone()

            if result:
                return {
                    'required_specs': json.loads(result[0]) if result[0] else {},
                    'optional_specs': json.loads(result[1]) if result[1] else {},
                    'default_unit': result[2],
                    'naming_pattern': result[3]
                }
            return None
    except Exception as e:
        logging.error(f"Error getting template: {str(e)}")
        return None

def calculate_usage_statistics(item_id):
    """Calculate usage statistics for an inventory item"""
    try:
        with db_connection() as conn:
            c = conn.cursor()

            # Check if job_consumables table exists
            c.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_schema = 'public'
                    AND table_name = 'job_consumables'
                )
            """)
            table_exists = c.fetchone()[0]

            if not table_exists:
                return {
                    'avg_monthly_usage': 0,
                    'total_used': 0,
                    'days_until_reorder': None,
                    'last_used_date': None
                }

            # Calculate average monthly usage over last 6 months
            c.execute("""
                SELECT
                    COALESCE(AVG(monthly_usage), 0) as avg_monthly,
                    COALESCE(SUM(total_usage), 0) as total_used
                FROM (
                    SELECT
                        DATE_TRUNC('month', date_used) as month,
                        SUM(quantity_used) as monthly_usage,
                        SUM(quantity_used) as total_usage
                    FROM job_consumables
                    WHERE consumable_id = %s
                    AND date_used >= CURRENT_DATE - INTERVAL '6 months'
                    GROUP BY DATE_TRUNC('month', date_used)
                ) as monthly_stats
            """, (item_id,))

            result = c.fetchone()
            avg_monthly = float(result[0]) if result[0] else 0
            total_used = float(result[1]) if result[1] else 0

            # Get current quantity and reorder threshold
            c.execute("""
                SELECT quantity, reorder_threshold, minimum_stock_level
                FROM consumables
                WHERE id = %s
            """, (item_id,))

            item_data = c.fetchone()
            days_until_reorder = None

            if item_data and avg_monthly > 0:
                current_qty = item_data[0]
                threshold = item_data[1] or item_data[2] or 0

                # Calculate days until reorder needed
                if threshold > 0 and current_qty > threshold:
                    daily_usage = avg_monthly / 30
                    if daily_usage > 0:
                        days_until_reorder = max(0, (current_qty - threshold) / daily_usage)

            # Get last used date
            c.execute("""
                SELECT MAX(date_used)
                FROM job_consumables
                WHERE consumable_id = %s
            """, (item_id,))

            last_used_row = c.fetchone()
            last_used = last_used_row[0] if last_used_row else None

            return {
                'avg_monthly_usage': round(avg_monthly, 2),
                'total_used': round(total_used, 2),
                'days_until_reorder': round(days_until_reorder) if days_until_reorder else None,
                'last_used_date': last_used
            }

    except Exception as e:
        logging.error(f"Error calculating usage statistics for item {item_id}: {str(e)}")
        return {
            'avg_monthly_usage': 0,
            'total_used': 0,
            'days_until_reorder': None,
            'last_used_date': None
        }

# Add ML prediction integration to inventory routes
def generate_supply_list_for_job(job_id: int, requirements: dict) -> dict:
    """Generate supply list based on job requirements using ML predictions"""
    try:
        from services.ml_prediction_service import get_job_usage_prediction
        
        # Get job details
        conn = get_connection()
        c = conn.cursor()
        
        c.execute("""
            SELECT job_type, square_footage, crew_size, location, description
            FROM jobs WHERE id = %s
        """, (job_id,))
        
        job_data = c.fetchone()
        if not job_data:
            return {'error': 'Job not found'}
        
        job_specs = {
            'job_type': job_data[0],
            'square_footage': job_data[1] or requirements.get('square_footage', 1000),
            'crew_size': job_data[2] or requirements.get('crew_size', 3),
            'location': job_data[3] or requirements.get('location', 'unknown'),
            'duration_days': requirements.get('duration_days', 5)
        }
        
        # Get ML predictions
        predictions = get_job_usage_prediction(job_specs)
        
        # Get current inventory levels for predicted items
        if predictions.get('predictions'):
            predicted_items = []
            for prediction in predictions['predictions']:
                c.execute("""
                    SELECT id, name, quantity, unit, location, cost_per_unit
                    FROM consumables
                    WHERE category = %s AND base_name = %s AND active = TRUE
                    ORDER BY quantity DESC
                    LIMIT 5
                """, (prediction['category'], prediction['base_name']))
                
                inventory_items = []
                for inv_row in c.fetchall():
                    inventory_items.append({
                        'id': inv_row[0],
                        'name': inv_row[1],
                        'current_stock': inv_row[2],
                        'unit': inv_row[3],
                        'location': inv_row[4],
                        'cost_per_unit': float(inv_row[5]) if inv_row[5] else None
                    })
                
                predicted_items.append({
                    'category': prediction['category'],
                    'base_name': prediction['base_name'],
                    'predicted_quantity': prediction['predicted_quantity'],
                    'confidence': prediction['confidence'],
                    'available_items': inventory_items,
                    'sufficient_stock': any(item['current_stock'] >= prediction['predicted_quantity'] for item in inventory_items),
                    'total_cost': sum(item['cost_per_unit'] * prediction['predicted_quantity'] 
                                    for item in inventory_items if item['cost_per_unit']) if inventory_items else None
                })
            
            predictions['predicted_items'] = predicted_items
        
        release_connection(conn)
        return predictions
        
    except Exception as e:
        logging.error(f"Error generating supply list: {str(e)}")
        return {'error': str(e), 'predictions': []}

# Add route for ML predictions
@inventory_bp.route("/predict_usage/<int:job_id>", methods=["GET", "POST"])
@login_required
def predict_job_usage(job_id):
    """Predict consumable usage for a job using ML"""
    if request.method == "GET":
        # Get job details for form
        conn = get_connection()
        c = conn.cursor()
        c.execute("SELECT id, name, job_type, square_footage, crew_size, location FROM jobs WHERE id = %s", (job_id,))
        job = c.fetchone()
        release_connection(conn)
        
        if not job:
            flash("Job not found.", 'error')
            return redirect(url_for('main.dashboard'))
        
        return render_template("predict_job_usage.html", job={
            'id': job[0], 'name': job[1], 'job_type': job[2], 
            'square_footage': job[3], 'crew_size': job[4], 'location': job[5]
        })
    
    elif request.method == "POST":
        requirements = {
            'square_footage': int(request.form.get('square_footage', 1000)),
            'crew_size': int(request.form.get('crew_size', 3)),
            'duration_days': int(request.form.get('duration_days', 5)),
            'location': request.form.get('location', 'unknown')
        }
        
        try:
            predictions = generate_supply_list_for_job(job_id, requirements)
            
            if 'error' in predictions:
                flash(f"Prediction error: {predictions['error']}", 'error')
                return redirect(url_for('inventory.predict_job_usage', job_id=job_id))
            
            return render_template("job_supply_predictions.html", 
                                 predictions=predictions, 
                                 job_id=job_id,
                                 requirements=requirements)
        
        except Exception as e:
            logging.error(f"Error in predict_job_usage: {str(e)}")
            flash(f"An error occurred: {str(e)}", 'error')
            return redirect(url_for('inventory.predict_job_usage', job_id=job_id))

# Add advanced search functionality
@inventory_bp.route("/api/search", methods=["GET"])
@login_required
def api_inventory_search():
    """Advanced inventory search API"""
    query = request.args.get('q', '').strip()
    category = request.args.get('category', '')
    location = request.args.get('location', '')
    limit = min(int(request.args.get('limit', 20)), 100)
    
    if len(query) < 2:
        return jsonify({'results': []})
    
    try:
        conn = get_connection()
        c = conn.cursor()
        
        # Build dynamic query
        where_conditions = ["active = TRUE"]
        params = []
        
        # Text search across multiple fields
        search_conditions = [
            "name ILIKE %s",
            "supplier ILIKE %s",
            "manufacturer ILIKE %s",
            "supplier_sku ILIKE %s",
            "internal_sku ILIKE %s",
            "part_number ILIKE %s"
        ]
        where_conditions.append(f"({' OR '.join(search_conditions)})")
        search_param = f"%{query}%"
        params.extend([search_param] * len(search_conditions))
        
        if category:
            where_conditions.append("category = %s")
            params.append(category)
            
        if location:
            where_conditions.append("location ILIKE %s")
            params.append(f"%{location}%")
        
        query_sql = f"""
            SELECT id, name, category, supplier, quantity, unit, location, 
                   cost_per_unit, internal_sku, image_path,
                   CASE 
                       WHEN quantity = 0 THEN 'out_of_stock'
                       WHEN quantity <= COALESCE(minimum_stock_level, reorder_threshold, 0) THEN 'low_stock'
                       ELSE 'in_stock'
                   END as stock_status
            FROM consumables
            WHERE {' AND '.join(where_conditions)}
            ORDER BY 
                CASE 
                    WHEN name ILIKE %s THEN 1
                    WHEN supplier ILIKE %s THEN 2
                    ELSE 3
                END,
                name
            LIMIT %s
        """
        
        params.extend([f"%{query}%", f"%{query}%", limit])
        c.execute(query_sql, params)
        
        results = []
        for row in c.fetchall():
            results.append({
                'id': row[0],
                'name': row[1],
                'category': row[2],
                'supplier': row[3],
                'quantity': row[4],
                'unit': row[5],
                'location': row[6],
                'cost_per_unit': float(row[7]) if row[7] else None,
                'internal_sku': row[8],
                'image_path': row[9],
                'stock_status': row[10],
                'url': url_for('inventory.inventory_detail', item_id=row[0])
            })
        
        release_connection(conn)
        return jsonify({'results': results, 'total': len(results)})
        
    except Exception as e:
        logging.error(f"Search API error: {str(e)}")
        return jsonify({'error': 'Search failed', 'results': []}), 500

# Add barcode scanning support
@inventory_bp.route("/scan", methods=["GET", "POST"])
@login_required
def barcode_scan():
    """Handle barcode scanning for inventory lookup"""
    if request.method == "GET":
        return render_template("barcode_scanner.html")
    
    elif request.method == "POST":
        barcode = request.form.get('barcode', '').strip()
        
        if not barcode:
            return jsonify({'error': 'No barcode provided'}), 400
        
        try:
            conn = get_connection()
            c = conn.cursor()
            
            # Search by barcode, internal SKU, or supplier SKU
            c.execute("""
                SELECT id, name, quantity, unit, location, stock_status
                FROM (
                    SELECT id, name, quantity, unit, location,
                           CASE 
                               WHEN quantity = 0 THEN 'out_of_stock'
                               WHEN quantity <= COALESCE(minimum_stock_level, reorder_threshold, 0) THEN 'low_stock'
                               ELSE 'in_stock'
                           END as stock_status
                    FROM consumables
                    WHERE active = TRUE
                    AND (barcode = %s OR internal_sku = %s OR supplier_sku = %s)
                ) subq
                LIMIT 1
            """, (barcode, barcode, barcode))
            
            result = c.fetchone()
            release_connection(conn)
            
            if result:
                return jsonify({
                    'found': True,
                    'item': {
                        'id': result[0],
                        'name': result[1],
                        'quantity': result[2],
                        'unit': result[3],
                        'location': result[4],
                        'stock_status': result[5],
                        'url': url_for('inventory.inventory_detail', item_id=result[0])
                    }
                })
            else:
                return jsonify({'found': False, 'message': 'Item not found'})
                
        except Exception as e:
            logging.error(f"Barcode scan error: {str(e)}")
            return jsonify({'error': 'Scan failed'}), 500

# Add inventory export functionality
@inventory_bp.route("/export", methods=["GET"])
@login_required
def export_inventory():
    """Export inventory to CSV"""
    try:
        # Get all inventory items
        inventory = get_all_consumables()
        
        # Create CSV content
        output = io.StringIO()
        writer = csv.writer(output)
        
        # Headers
        headers = [
            'Internal SKU', 'Name', 'Category', 'Base Name', 'Quantity', 'Unit',
            'Location', 'Bin Location', 'Supplier', 'Supplier SKU', 'Manufacturer',
            'Part Number', 'Cost Per Unit', 'Reorder Threshold', 'Min Stock Level',
            'Max Stock Level', 'Stock Status', 'Barcode', 'Hazmat', 'Requires Certification'
        ]
        writer.writerow(headers)
        
        # Data rows
        for item in inventory:
            row = [
                item.get('internal_sku', ''),
                item.get('name', ''),
                item.get('category', ''),
                item.get('base_name', ''),
                item.get('quantity', 0),
                item.get('unit', ''),
                item.get('location', ''),
                item.get('bin_location', ''),
                item.get('supplier', ''),
                item.get('supplier_sku', ''),
                item.get('manufacturer', ''),
                item.get('part_number', ''),
                item.get('cost_per_unit', ''),
                item.get('reorder_threshold', ''),
                item.get('minimum_stock_level', ''),
                item.get('maximum_stock_level', ''),
                item.get('stock_status', ''),
                item.get('barcode', ''),
                'Yes' if item.get('hazmat') else 'No',
                'Yes' if item.get('requires_certification') else 'No'
            ]
            writer.writerow(row)
        
        # Create response
        output.seek(0)
        
        return Response(
            output.getvalue(),
            mimetype='text/csv',
            headers={
                'Content-Disposition': f'attachment; filename=inventory_export_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
            }
        )
        
    except Exception as e:
        logging.error(f"Export error: {str(e)}")
        flash(f"Export failed: {str(e)}", 'error')
        return redirect(url_for('inventory.inventory_list'))

# Add automated reordering functionality
def check_and_process_automatic_reorders():
    """Check for items that need automatic reordering"""
    try:
        conn = get_connection()
        c = conn.cursor()
        
        # Find items below reorder threshold that haven't been reordered recently
        c.execute("""
            SELECT c.id, c.name, c.quantity, c.reorder_threshold, c.minimum_stock_level,
                   s.supplier_name, s.supplier_sku, s.cost_per_unit, s.minimum_order_quantity
            FROM consumables c
            LEFT JOIN supplier_items s ON c.id = s.consumable_id AND s.current_supplier = TRUE
            WHERE c.active = TRUE
            AND c.quantity <= COALESCE(c.minimum_stock_level, c.reorder_threshold, 0)
            AND c.reorder_threshold IS NOT NULL
            AND NOT EXISTS (
                SELECT 1 FROM inventory_alerts ia
                WHERE ia.consumable_id = c.id
                AND ia.alert_type = 'auto_reorder'
                AND ia.created_at > CURRENT_DATE - INTERVAL '7 days'
            )
            ORDER BY (c.quantity::float / NULLIF(c.reorder_threshold, 0)) ASC
        """)
        
        items_to_reorder = c.fetchall()
        reorder_count = 0
        
        for item in items_to_reorder:
            item_id, name, current_qty, threshold, min_level, supplier, sku, cost, min_order = item
            
            # Calculate suggested order quantity
            target_level = min_level or (threshold * 2) if threshold else 100
            order_qty = max(min_order or 1, target_level - current_qty)
            
            # Create reorder alert
            message = f"Automatic reorder suggestion: {name} - Order {order_qty} units from {supplier or 'TBD'}"
            create_inventory_alert(item_id, 'auto_reorder', 'info', message)
            
            # Log the suggestion
            logging.info(f"Auto-reorder suggested for {name}: {order_qty} units")
            reorder_count += 1
        
        release_connection(conn)
        return reorder_count
        
    except Exception as e:
        logging.error(f"Auto-reorder check failed: {str(e)}")
        return 0

@inventory_bp.route("/inventory")
@login_required
def inventory():
    """Redirect to main inventory list"""
    return redirect(url_for("inventory.inventory_list"))

@inventory_bp.route("/list", methods=["GET", "POST"])
@login_required
def inventory_list():
    """Enhanced inventory list with advanced filtering and views"""
    # Handle bulk quantity update submissions
    if request.method == 'POST' and 'bulk_update' in request.form:
        conn = None
        try:
            conn = get_connection()
            c = conn.cursor()
            updated = 0
            for key, value in request.form.items():
                if key.startswith('qty_'):
                    try:
                        item_id = int(key.split('_', 1)[1])
                        new_qty = int(value)
                    except (ValueError, IndexError):
                        continue
                    c.execute(
                        "UPDATE consumables SET quantity = %s, updated_at = CURRENT_TIMESTAMP WHERE id = %s",
                        (new_qty, item_id)
                    )
                    updated += c.rowcount
            conn.commit()
            flash(f"Updated quantities for {updated} items.", 'success')
        except Exception as e:
            logging.error(f"Bulk update failed: {str(e)}")
            if conn:
                try:
                    conn.rollback()
                except Exception:
                    pass
            flash('Bulk update failed.', 'error')
        finally:
            if conn:
                try:
                    release_connection(conn)
                except Exception:
                    pass
        return redirect(url_for('inventory.inventory_list'))
    # Get all parameters
    search_query = request.args.get('search', '').strip()
    category = request.args.get('category', '').strip()
    sort_by = request.args.get('sort', 'name')
    sort_order = request.args.get('order', 'asc')
    filter_status = request.args.get('filter', None)
    view_mode = request.args.get('view', 'individual')
    page = int(request.args.get('page', 1))
    per_page = int(request.args.get('per_page', 50))
    
    # Validate parameters
    valid_sort_fields = ['name', 'category', 'supplier', 'quantity', 'internal_sku', 'cost', 'location']
    if sort_by not in valid_sort_fields:
        sort_by = 'name'
    
    if sort_order not in ['asc', 'desc']:
        sort_order = 'asc'
    
    valid_filters = ['low_stock', 'out_of_stock', 'in_stock', 'overstocked']
    if filter_status not in valid_filters:
        filter_status = None
    
    valid_views = ['individual', 'grouped', 'grid']
    if view_mode not in valid_views:
        view_mode = 'individual'
    
    # Get inventory data
    inventory = get_all_consumables(
        search_query=search_query,
        category=category,
        sort_by=sort_by,
        sort_order=sort_order,
        filter_status=filter_status,
        view_mode=view_mode
    )
    
    # Pagination
    total_items = len(inventory)
    start_idx = (page - 1) * per_page
    end_idx = start_idx + per_page
    paginated_inventory = inventory[start_idx:end_idx]
    total_pages = (total_items + per_page - 1) // per_page
    
    # Get statistics
    stats = get_inventory_stats()
    
    # Get categories for filter dropdown
    categories_data = get_categories_and_base_names()
    
    # Get recent alerts
    recent_alerts = get_recent_inventory_alerts()
    
    # Determine template based on view mode
    if view_mode == 'grouped':
        template = "inventory_grouped.html"
    elif view_mode == 'grid':
        template = "inventory_grid.html"
    else:
        template = "inventory.html"
    
    return render_template(template,
                         inventory=paginated_inventory,
                         search_query=search_query,
                         current_category=category,
                         current_sort=sort_by,
                         current_order=sort_order,
                         current_filter=filter_status,
                         current_view=view_mode,
                         current_page=page,
                         total_pages=total_pages,
                         total_items=total_items,
                         per_page=per_page,
                         stats=stats,
                         categories=categories_data['categories'],
                         recent_alerts=recent_alerts,
                         can_edit=current_user.role in ['manager', 'admin'])

@inventory_bp.route('/inventory/<int:item_id>')
@login_required
def inventory_detail(item_id):
    """Enhanced inventory detail view with analytics"""
    item = get_enhanced_inventory_item(item_id)
    if not item:
        flash('Item not found.', 'error')
        return redirect(url_for('inventory.inventory_list'))
    
    # Calculate usage statistics
    usage_stats = calculate_usage_statistics(item_id)
    
    # Get similar items for comparison
    similar_items = find_similar_items(
        item['category'],
        item['base_name'],
        item['specifications'],
        exclude_supplier=item['supplier']
    )
    
    # Calculate additional analytics
    avg_monthly_usage = usage_stats['avg_monthly_usage']
    days_until_reorder = usage_stats['days_until_reorder']
    total_used = usage_stats['total_used']
    
    return render_template('inventory_detail.html',
                         item=item,
                         similar_items=similar_items,
                         avg_monthly_usage=avg_monthly_usage,
                         days_until_reorder=days_until_reorder,
                         total_used=total_used,
                         can_edit=current_user.role in ['manager', 'admin'])

@inventory_bp.route("/add", methods=["GET", "POST"])
@login_required
def add_inventory():
    """Add new inventory item with enhanced features"""
    if current_user.role not in ['manager', 'admin']:
        flash("Permission denied.", 'error')
        return redirect(url_for('inventory.inventory_list'))
    
    if request.method == "GET":
        # Get templates and categories for form
        templates = get_item_templates()
        categories_data = get_categories_and_base_names()
        
        # Get recent items for quick reference
        recent_items = get_recent_items()
        
        return render_template("add_inventory.html", 
                             templates=templates,
                             categories=categories_data['categories'],
                             base_names_by_category=categories_data['base_names_by_category'],
                             recent_items=recent_items)
    
    elif request.method == "POST":
        try:
            # Process form data
            result = add_consumable_with_grouping(request.form)
            
            if result['action'] == 'confirm_grouping':
                # Show similar items and ask user what to do
                templates = get_item_templates()
                categories_data = get_categories_and_base_names()
                recent_items = get_recent_items()
                
                return render_template("add_inventory.html",
                                     templates=templates,
                                     categories=categories_data['categories'],
                                     base_names_by_category=categories_data['base_names_by_category'],
                                     recent_items=recent_items,
                                     similar_items=result['similar_items'],
                                     form_data=result['form_data'],
                                     show_grouping_options=True)
            
            elif result['action'] == 'created':
                item_id = result['item_id']
                
                # Handle image upload if present
                if 'item_image' in request.files:
                    file = request.files['item_image']
                    if file and file.filename and allowed_file(file.filename, {'png', 'jpg', 'jpeg', 'gif'}):
                        try:
                            upload_item_image(item_id, file)
                        except Exception as e:
                            logging.error(f"Error uploading image: {str(e)}")
                            flash(f"Item created but image upload failed: {str(e)}", 'warning')
                
                flash("Inventory item added successfully.", 'success')
                
                # Check if user wants to add another
                if request.form.get('action') == 'add_another':
                    return redirect(url_for("inventory.add_inventory"))
                else:
                    return redirect(url_for("inventory.inventory_detail", item_id=item_id))
                
        except ValueError as e:
            flash(str(e), 'error')
        except Exception as e:
            logging.error(f"Error adding inventory: {str(e)}")
            flash(f"An error occurred while adding item: {str(e)}", 'error')
        
        # On error, return form with data
        templates = get_item_templates()
        categories_data = get_categories_and_base_names()
        recent_items = get_recent_items()
        return render_template("add_inventory.html", 
                             templates=templates,
                             categories=categories_data['categories'],
                             base_names_by_category=categories_data['base_names_by_category'],
                             recent_items=recent_items,
                             form_data=request.form)

@inventory_bp.route("/duplicate/<int:item_id>")
@login_required
def duplicate_item(item_id):
    """Duplicate an existing inventory item"""
    if current_user.role not in ['manager', 'admin']:
        flash("Permission denied.", 'error')
        return redirect(url_for('inventory.inventory_list'))
    
    item = get_enhanced_inventory_item(item_id)
    if not item:
        flash("Item not found.", 'error')
        return redirect(url_for("inventory.inventory_list"))
    
    # Prepare form data for new item (copy most fields but reset quantity)
    form_data = {
        'category': item.get('category', ''),
        'base_name': item.get('base_name', ''),
        'specifications': json.dumps(item.get('specifications', {})),
        'supplier': (item.get('supplier', '') + ' (Copy)').strip(),
        'manufacturer': item.get('manufacturer', ''),
        'part_number': item.get('part_number', ''),
        'unit': item.get('unit', 'each'),
        'reorder_threshold': str(item['reorder_threshold']) if item.get('reorder_threshold') else '',
        'min_stock_level': str(item['minimum_stock_level']) if item.get('minimum_stock_level') else '',
        'max_stock_level': str(item['maximum_stock_level']) if item.get('maximum_stock_level') else '',
        'cost_per_unit': str(item['cost_per_unit']) if item.get('cost_per_unit') else '',
        'location': item.get('location', ''),
        'bin_location': item.get('bin_location', ''),
        'hazmat': item.get('hazmat', False),
        'requires_certification': item.get('requires_certification', False),
        'notes': item.get('notes', ''),
        'quantity': '0'  # Start with 0 quantity for duplicate
    }
    
    templates = get_item_templates()
    categories_data = get_categories_and_base_names()
    recent_items = get_recent_items()
    
    return render_template("add_inventory.html",
                         templates=templates,
                         categories=categories_data['categories'],
                         base_names_by_category=categories_data['base_names_by_category'],
                         recent_items=recent_items,
                         form_data=form_data)

@inventory_bp.route("/confirm_grouping", methods=["POST"])
@login_required
def confirm_grouping():
    """Handle user decision on grouping similar items"""
    if current_user.role not in ['manager', 'admin']:
        flash("Permission denied.", 'error')
        return redirect(url_for('inventory.inventory_list'))
    
    action = request.form.get('grouping_action')
    
    try:
        if action == 'merge':
            # User chose to merge with existing item
            existing_item_id = request.form.get('merge_with_id')
            item_id = merge_with_existing_item(existing_item_id, request.form)
            flash("Item merged with existing inventory successfully.", 'success')
            
        elif action == 'create_new':
            # User chose to create new item despite similarities
            item_id = create_enhanced_consumable(request.form)
            flash("New inventory item created successfully.", 'success')
            
        else:
            flash("Invalid action selected.", 'error')
            return redirect(url_for('inventory.add_inventory'))
        
        return redirect(url_for("inventory.inventory_detail", item_id=item_id))
        
    except Exception as e:
        flash(f"An error occurred: {str(e)}", 'error')
        return redirect(url_for('inventory.add_inventory'))

@inventory_bp.route("/edit/<int:item_id>", methods=["GET", "POST"])
@login_required
def edit_inventory(item_id):
    """Edit existing inventory item"""
    if current_user.role not in ['manager', 'admin']:
        flash("Permission denied.", 'error')
        return redirect(url_for('inventory.inventory_list'))
    
    item = get_enhanced_inventory_item(item_id)
    if not item:
        flash("Item not found.", 'error')
        return redirect(url_for("inventory.inventory_list"))
    
    if request.method == "GET":
        templates = get_item_templates()
        categories_data = get_categories_and_base_names()
        
        # Get similar items for reference
        similar_items = find_similar_items(
            item['category'],
            item['base_name'],
            item['specifications'],
            exclude_supplier=item['supplier']
        )[:3]  # Limit to 3 for sidebar
        
        return render_template("edit_inventory.html", 
                             item=item,
                             templates=templates,
                             categories=categories_data['categories'],
                             base_names_by_category=categories_data['base_names_by_category'],
                             similar_items=similar_items)
    
    elif request.method == "POST":
        try:
            # Handle image upload if present
            if 'item_image' in request.files:
                file = request.files['item_image']
                if file and allowed_file(file.filename, ALLOWED_IMAGE_EXTENSIONS):
                    upload_item_image(item_id, file)
            
            # Update the item
            update_consumable(item_id, request.form)
            flash("Inventory item updated successfully.", 'success')
            
            # Check if continue editing
            if request.form.get('continue_editing') == 'true':
                return redirect(url_for("inventory.edit_inventory", item_id=item_id))
            else:
                return redirect(url_for("inventory.inventory_detail", item_id=item_id))
                
        except ValueError as e:
            flash(str(e), 'error')
            templates = get_item_templates()
            categories_data = get_categories_and_base_names()
            # Preserve form data
            form_item = dict(request.form)
            form_item['id'] = item_id
            return render_template("edit_inventory.html", 
                                 item=form_item,
                                 templates=templates,
                                 categories=categories_data['categories'],
                                 base_names_by_category=categories_data['base_names_by_category'])
        except Exception as e:
            flash(f"An error occurred while updating item: {str(e)}", 'error')
            return redirect(url_for("inventory.inventory_detail", item_id=item_id))

@inventory_bp.route("/delete/<int:item_id>", methods=["POST"])
@login_required
def delete_inventory(item_id):
    """Soft delete inventory item"""
    if current_user.role not in ['manager', 'admin']:
        flash("Permission denied.", 'error')
        return redirect(url_for('inventory.inventory_list'))
    
    try:
        delete_consumable(item_id)
        flash("Inventory item deleted successfully.", 'success')
    except Exception as e:
        flash(f"Error deleting item: {str(e)}", 'error')
    
    return redirect(url_for("inventory.inventory_list"))

@inventory_bp.route("/adjust/<int:item_id>", methods=["POST"])
@login_required
def adjust_inventory(item_id):
    """Quick quantity adjustment from inventory list"""
    if current_user.role not in ['manager', 'admin']:
        flash("Permission denied.", 'error')
        return redirect(url_for('inventory.inventory_list'))
    
    item = get_enhanced_inventory_item(item_id)
    if not item:
        flash("Item not found.", 'error')
        return redirect(url_for('inventory.inventory_list'))
    
    try:
        adjustment_type = request.form.get("adjustment_type")
        adjustment_amount = int(request.form.get("adjustment_amount", 0))
        
        if adjustment_amount <= 0:
            flash("Adjustment amount must be greater than 0.", 'error')
            return redirect(url_for('inventory.inventory_list'))
        
        if adjustment_type == "add":
            new_quantity = item["quantity"] + adjustment_amount
            action_text = f"Added {adjustment_amount} to"
        elif adjustment_type == "subtract":
            new_quantity = max(0, item["quantity"] - adjustment_amount)
            action_text = f"Removed {adjustment_amount} from"
        else:
            flash("Invalid adjustment type.", 'error')
            return redirect(url_for('inventory.inventory_list'))
        
        # Create form data for update
        form_data = {
            'name': item['name'],
            'category': item['category'],
            'base_name': item['base_name'],
            'specifications': json.dumps(item['specifications']),
            'location': item['location'],
            'bin_location': item['bin_location'],
            'quantity': str(new_quantity),
            'supplier': item['supplier'],
            'manufacturer': item['manufacturer'],
            'part_number': item['part_number'],
            'supplier_sku': item['supplier_sku'],
            'serial_numbers': '\n'.join(item['serial_numbers']),
            'unit': item['unit'],
            'reorder_threshold': str(item['reorder_threshold']) if item['reorder_threshold'] else '',
            'min_stock_level': str(item['minimum_stock_level']) if item['minimum_stock_level'] else '',
            'max_stock_level': str(item['maximum_stock_level']) if item['maximum_stock_level'] else '',
            'cost_per_unit': str(item['cost_per_unit']) if item['cost_per_unit'] else ''
        }
        
        update_consumable(item_id, form_data)
        flash(f"{action_text} {item['name']}. New quantity: {new_quantity}", 'success')
        
    except ValueError as e:
        flash(f"Error adjusting quantity: {str(e)}", 'error')
    except Exception as e:
        flash(f"An error occurred: {str(e)}", 'error')
    
    return redirect(url_for('inventory.inventory_list'))

# Supplier (Merchant) management pages
@inventory_bp.route("/suppliers", methods=["GET"])
@login_required
def suppliers_page():
    """List suppliers (merchants) with basic info"""
    conn = get_connection()
    c = conn.cursor()
    try:
        c.execute(
            """
            SELECT id, name, contact_person, email, phone, preferred, active
            FROM suppliers
            ORDER BY preferred DESC, name ASC
            """
        )
        suppliers = [
            {
                'id': r[0], 'name': r[1], 'contact_person': r[2], 'email': r[3],
                'phone': r[4], 'preferred': bool(r[5]), 'active': bool(r[6])
            }
            for r in c.fetchall()
        ]
    finally:
        release_connection(conn)
    return render_template("suppliers.html", suppliers=suppliers)

@inventory_bp.route('/suppliers/add', methods=['GET', 'POST'])
@login_required
def add_supplier():
    if current_user.role not in ['manager', 'admin']:
        flash("Permission denied.", 'error')
        return redirect(url_for('inventory.suppliers_page'))
    
    if request.method == 'GET':
        return render_template('add_supplier.html')
    
    # Collect form data
    name = request.form.get('name')
    contact_person = request.form.get('contact_person')
    email = request.form.get('email')
    phone = request.form.get('phone')
    address = request.form.get('address')
    city = request.form.get('city')
    state = request.form.get('state')
    zip_code = request.form.get('zip_code')
    country = request.form.get('country', 'USA')
    website = request.form.get('website')
    account_number = request.form.get('account_number')
    payment_terms = request.form.get('payment_terms')
    shipping_terms = request.form.get('shipping_terms')
    preferred = request.form.get('preferred') == 'on'
    active = request.form.get('active') == 'on'
    notes = request.form.get('notes')
    api_endpoint = request.form.get('api_endpoint')
    api_key = request.form.get('api_key')
    api_username = request.form.get('api_username')
    api_password = request.form.get('api_password')
    api_auth_type = request.form.get('api_auth_type')
    
    if not name:
        flash("Supplier name is required.", 'error')
        return redirect(url_for('inventory.add_supplier'))
    
    try:
        conn = get_connection()
        c = conn.cursor()
        c.execute('''INSERT INTO suppliers 
                     (name, contact_person, email, phone, address, city, state, zip_code, country, 
                      website, account_number, payment_terms, shipping_terms, preferred, active, notes,
                      api_endpoint, api_key, api_username, api_password, api_auth_type)
                     VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)''',
                  (name, contact_person, email, phone, address, city, state, zip_code, country,
                   website, account_number, payment_terms, shipping_terms, preferred, active, notes,
                   api_endpoint, api_key, api_username, api_password, api_auth_type))
        conn.commit()
        release_connection(conn)
        flash("Supplier added successfully.", 'success')
        return redirect(url_for('inventory.suppliers_page'))
    except Exception as e:
        flash(f"Error adding supplier: {str(e)}", 'error')
        return redirect(url_for('inventory.add_supplier'))

# Reorder queue page
@inventory_bp.route("/reorders", methods=["GET"])
@login_required
def reorders_page():
    """Display current reorder requests queue with approve actions"""
    conn = get_connection()
    c = conn.cursor()
    try:
        # Ensure reorder tables exist (safety net)
        c.execute("""
            SELECT EXISTS (
              SELECT FROM information_schema.tables 
              WHERE table_schema = 'public' AND table_name = 'reorder_requests'
            )
        """)
        if not c.fetchone()[0]:
            try:
                from services.init_db import create_reorder_tables
                create_reorder_tables()
            except Exception:
                pass

        c.execute(
            """
            SELECT rr.id, c.name, rr.suggested_quantity, COALESCE(rr.unit, c.unit) as unit,
                   rr.status, rr.estimated_cost, rr.lead_time_days, c.internal_sku
            FROM reorder_requests rr
            JOIN consumables c ON c.id = rr.consumable_id
            ORDER BY rr.created_at DESC
            """
        )
        queue = [
            {
                'request_id': r[0], 'item_name': r[1], 'suggested_quantity': r[2], 'unit': r[3],
                'status': r[4], 'estimated_cost': float(r[5]) if r[5] is not None else None,
                'lead_time_days': r[6], 'internal_sku': r[7]
            }
            for r in c.fetchall()
        ]
    finally:
        release_connection(conn)
    return render_template("reorder_queue.html", queue=queue)

# Helper functions
def get_recent_items(limit=5):
    """Get recently added inventory items"""
    conn = get_connection()
    c = conn.cursor()
    
    try:
        c.execute("""
            SELECT id, name, quantity, unit, created_at
            FROM consumables
            WHERE active = TRUE
            ORDER BY created_at DESC
            LIMIT %s
        """, (limit,))
        
        items = []
        for row in c.fetchall():
            items.append({
                'id': row[0],
                'name': row[1],
                'quantity': row[2],
                'unit': row[3],
                'created_at': row[4]
            })
        
        return items
    except Exception as e:
        logging.error(f"Error getting recent items: {str(e)}")
        return []
    finally:
        release_connection(conn)

# API endpoints for dynamic forms
@inventory_bp.route("/api/templates/<category>", methods=["GET"])
@login_required
def get_category_templates(category):
    """Get templates for a specific category"""
    templates = get_item_templates()
    category_templates = [t for t in templates if t['category'] == category]
    return jsonify(category_templates)

@inventory_bp.route("/api/reorder", methods=["POST"])
@login_required
def api_request_reorder():
    """API endpoint to request reorder for an item"""
    if current_user.role not in ['manager', 'admin']:
        return jsonify({'error': 'Permission denied'}), 403
    
    data = request.get_json()
    item_id = data.get('item_id')
    
    if not item_id:
        return jsonify({'error': 'Item ID required'}), 400
    
    try:
        # Get item details
        item = get_enhanced_inventory_item(item_id)
        if not item:
            return jsonify({'error': 'Item not found'}), 404
        
        # Create reorder alert
        create_inventory_alert(
            item_id, 
            'reorder_requested', 
            'info', 
            f'Reorder requested for {item["name"]} by {current_user.username}'
        )
        
        # Log the action
        log_audit(current_user.id, "request_reorder", {
            "item_id": item_id,
            "item_name": item["name"],
            "current_quantity": item["quantity"]
        })
        
        return jsonify({'message': 'Reorder request submitted successfully'})
        
    except Exception as e:
        logging.error(f"Error requesting reorder: {str(e)}")
        return jsonify({'error': 'Failed to submit reorder request'}), 500


@inventory_bp.route("/api/similar_items", methods=["POST"])
@login_required
def check_similar_items():
    """Check for similar items based on specifications"""
    data = request.get_json()
    category = data.get('category')
    base_name = data.get('base_name')
    specifications = data.get('specifications', {})
    exclude_supplier = data.get('exclude_supplier')
    
    similar_items = find_similar_items(category, base_name, specifications, exclude_supplier)
    return jsonify(similar_items)

@inventory_bp.route("/bulk_import", methods=["POST"])
@login_required
def bulk_import():
    """Handle bulk import of inventory items"""
    if current_user.role not in ['manager', 'admin']:
        flash("Permission denied.", 'error')
        return redirect(url_for('inventory.inventory_list'))
    
    if 'file' not in request.files:
        return jsonify({'error': 'No file uploaded'}), 400
    
    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No file selected'}), 400
    
    if not allowed_file(file.filename, {'csv', 'xlsx', 'xls'}):
        return jsonify({'error': 'Invalid file type. Please upload CSV or Excel files.'}), 400
    
    try:
        # Save uploaded file temporarily
        filename = secure_filename(file.filename)
        temp_path = os.path.join(UPLOAD_FOLDER, 'temp_' + filename)
        file.save(temp_path)
        
        # Determine file type
        file_type = 'csv' if filename.endswith('.csv') else 'xlsx'
        
        # Process the file
        result = bulk_import_inventory(temp_path, file_type)
        
        # Clean up temporary file
        os.remove(temp_path)
        
        return jsonify(result)
        
    except Exception as e:
        # Clean up on error
        if os.path.exists(temp_path):
            os.remove(temp_path)
        
        logging.error(f"Bulk import error: {str(e)}")
        return jsonify({'error': f'Import failed: {str(e)}'}), 500

@inventory_bp.route("/supply_list/<int:job_id>", methods=["GET", "POST"])
@login_required
def job_supply_list(job_id):
    """Generate and display supply list for a job"""
    if request.method == "GET":
        # Show form to specify requirements
        return render_template("job_supply_list.html", job_id=job_id)
    
    elif request.method == "POST":
        # Process requirements and generate supply list
        requirements = request.get_json()
        
        try:
            result = generate_supply_list_for_job(job_id, requirements)
            return jsonify(result)
        except Exception as e:
            return jsonify({'error': str(e)}), 400

@inventory_bp.route("/reports")
@login_required
def inventory_reports():
    """Show comprehensive inventory reports and analytics"""
    try:
        stats = get_inventory_stats()

        # Get stock status data
        low_stock_items = get_all_consumables(filter_status='low_stock')
        out_of_stock_items = get_all_consumables(filter_status='out_of_stock')
        value_report = get_inventory_value_report()

        # Get spending data
        spending = {
            'ytd_total': 0,
            'month_total': 0,
            'avg_monthly': 0,
            'monthly': [],
            'by_category': [],
            'by_supplier': InventoryMetrics.get_spend_by_supplier(10)
        }

        # Get monthly spending
        monthly_data = InventoryMetrics.get_spend_by_period('monthly', 12)
        if monthly_data:
            spending['monthly'] = [{'month': m.get('period', ''), 'amount': m.get('spend', 0)} for m in monthly_data]
            spending['ytd_total'] = sum(m.get('spend', 0) for m in monthly_data)
            spending['avg_monthly'] = spending['ytd_total'] / len(monthly_data) if monthly_data else 0
            if monthly_data:
                spending['month_total'] = monthly_data[-1].get('spend', 0)

        # Get spending by category
        category_spend = InventoryMetrics.get_spend_by_category()
        spending['by_category'] = [{'category': c.get('category', 'Other'), 'amount': c.get('spend', 0)} for c in category_spend]

        # Get supplier performance
        supplier_performance = []
        with db_connection() as conn:
            c = conn.cursor()
            c.execute("""
                SELECT s.id, s.name, s.preferred, s.active,
                       COUNT(DISTINCT po.id) as order_count,
                       COALESCE(SUM(po.total_amount), 0) as total_spend,
                       COUNT(DISTINCT si.consumable_id) as item_count,
                       AVG(si.lead_time_days) as avg_lead_time
                FROM suppliers s
                LEFT JOIN purchase_orders po ON s.id = po.supplier_id
                LEFT JOIN supplier_items si ON s.id = si.supplier_id
                WHERE s.active = TRUE
                GROUP BY s.id, s.name, s.preferred, s.active
                ORDER BY total_spend DESC
            """)
            for row in c.fetchall():
                supplier_performance.append({
                    'id': row[0],
                    'name': row[1],
                    'preferred': row[2],
                    'active': row[3],
                    'order_count': row[4],
                    'total_spend': float(row[5]) if row[5] else 0,
                    'item_count': row[6],
                    'avg_lead_time': row[7]
                })

        # Get dead stock
        dead_stock = InventoryMetrics.get_dead_stock(90)

        # Get price history and stats
        price_history = PriceHistoryTracker.get_price_history(days=90)
        increases = [h for h in price_history if h.get('change_percentage', 0) > 0]
        decreases = [h for h in price_history if h.get('change_percentage', 0) < 0]
        price_stats = {
            'increases': len(increases),
            'decreases': len(decreases),
            'avg_increase': sum(h.get('change_percentage', 0) for h in increases) / len(increases) if increases else 0,
            'avg_decrease': sum(h.get('change_percentage', 0) for h in decreases) / len(decreases) if decreases else 0,
            'items_affected': len(set(h.get('consumable_id') for h in price_history))
        }

        # Get order stats
        order_stats = {'total': 0, 'pending': 0, 'in_transit': 0, 'delivered': 0}
        recent_orders = []
        with db_connection() as conn:
            c = conn.cursor()
            c.execute("""
                SELECT status, COUNT(*) FROM purchase_orders GROUP BY status
            """)
            for row in c.fetchall():
                if row[0] in ['draft', 'submitted', 'acknowledged']:
                    order_stats['pending'] += row[1]
                elif row[0] == 'shipped':
                    order_stats['in_transit'] += row[1]
                elif row[0] == 'delivered':
                    order_stats['delivered'] += row[1]
                order_stats['total'] += row[1]

            c.execute("""
                SELECT po.id, po.po_number, s.name, po.status, po.order_date, po.total_amount
                FROM purchase_orders po
                JOIN suppliers s ON po.supplier_id = s.id
                ORDER BY po.created_at DESC LIMIT 10
            """)
            for row in c.fetchall():
                recent_orders.append({
                    'id': row[0],
                    'po_number': row[1],
                    'supplier_name': row[2],
                    'status': row[3],
                    'order_date': row[4],
                    'total_amount': float(row[5]) if row[5] else 0
                })

        return render_template("inventory_reports.html",
                             stats=stats,
                             value_report=value_report,
                             low_stock_items=low_stock_items,
                             out_of_stock_items=out_of_stock_items,
                             dead_stock=dead_stock,
                             spending=spending,
                             supplier_performance=supplier_performance,
                             price_history=price_history,
                             price_stats=price_stats,
                             order_stats=order_stats,
                             recent_orders=recent_orders)

    except Exception as e:
        logging.error(f"Error loading inventory reports: {str(e)}")
        flash(f"Error loading reports: {str(e)}", 'error')
        return redirect(url_for('inventory.inventory_list'))


@inventory_bp.route("/reports/export/<format>")
@login_required
def export_report(format):
    """Export inventory report data"""
    if format not in ['csv', 'xlsx']:
        flash("Invalid export format", 'error')
        return redirect(url_for('inventory.inventory_reports'))

    try:
        value_report = get_inventory_value_report()
        low_stock_items = get_all_consumables(filter_status='low_stock')

        if format == 'csv':
            output = io.StringIO()
            writer = csv.writer(output)

            # Summary section
            writer.writerow(['INVENTORY REPORT'])
            writer.writerow(['Generated', datetime.now().strftime('%Y-%m-%d %H:%M')])
            writer.writerow([])
            writer.writerow(['SUMMARY'])
            writer.writerow(['Total Value', f"${value_report.get('total_value', 0):,.2f}"])
            writer.writerow(['Total Items', value_report.get('total_items', 0)])
            writer.writerow(['Total Units', value_report.get('total_units', 0)])
            writer.writerow([])

            # Category breakdown
            writer.writerow(['VALUE BY CATEGORY'])
            writer.writerow(['Category', 'Items', 'Units', 'Value'])
            for cat in value_report.get('category_breakdown', []):
                writer.writerow([
                    cat.get('category', 'Uncategorized'),
                    cat.get('item_count', 0),
                    cat.get('total_quantity', 0),
                    f"${cat.get('value', 0):,.2f}"
                ])
            writer.writerow([])

            # Top value items
            writer.writerow(['TOP VALUE ITEMS'])
            writer.writerow(['Name', 'SKU', 'Quantity', 'Unit Cost', 'Total Value'])
            for item in value_report.get('top_value_items', []):
                writer.writerow([
                    item.get('name', ''),
                    item.get('sku', ''),
                    item.get('quantity', 0),
                    f"${item.get('unit_cost', 0):,.2f}",
                    f"${item.get('total_value', 0):,.2f}"
                ])
            writer.writerow([])

            # Low stock items
            writer.writerow(['LOW STOCK ITEMS'])
            writer.writerow(['Name', 'SKU', 'Quantity', 'Reorder Level'])
            for item in low_stock_items:
                writer.writerow([
                    item.get('name', ''),
                    item.get('internal_sku', ''),
                    item.get('quantity', 0),
                    item.get('reorder_threshold', item.get('minimum_stock_level', '-'))
                ])

            output.seek(0)
            return Response(
                output.getvalue(),
                mimetype='text/csv',
                headers={'Content-Disposition': f'attachment;filename=inventory_report_{datetime.now().strftime("%Y%m%d")}.csv'}
            )

    except Exception as e:
        logging.error(f"Error exporting report: {str(e)}")
        flash(f"Error exporting report: {str(e)}", 'error')
        return redirect(url_for('inventory.inventory_reports'))


## Duplicate route is already defined earlier; remove duplicate definition to avoid conflicts.

@inventory_bp.route("/item_history/<int:item_id>")
@login_required
def item_history(item_id):
    """View complete history of an inventory item"""
    item = get_enhanced_inventory_item(item_id)
    if not item:
        flash("Item not found.", 'error')
        return redirect(url_for("inventory.inventory_list"))
    
    # Get all transactions for this item
    conn = get_connection()
    c = conn.cursor()
    
    # Check if inventory_transactions table exists
    c.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_name = 'inventory_transactions'
        )
    """)
    table_exists = c.fetchone()[0]
    
    transactions = []
    if table_exists:
        c.execute("""
            SELECT it.transaction_type, it.quantity_change, it.quantity_before,
                   it.quantity_after, it.transaction_date, it.notes,
                   u.username
            FROM inventory_transactions it
            LEFT JOIN users u ON it.performed_by = u.id
            WHERE it.consumable_id = %s
            ORDER BY it.transaction_date DESC
        """, (item_id,))
        
        for row in c.fetchall():
            transactions.append({
                'type': row[0],
                'quantity_change': row[1],
                'quantity_before': row[2],
                'quantity_after': row[3],
                'date': row[4],
                'notes': row[5],
                'performed_by': row[6]
            })
    
    release_connection(conn)
    
    return render_template("item_history.html",
                         item=item,
                         transactions=transactions)

# Removed stray bulk-update stub; logic now lives inside inventory_list()


# ============================================================================
# INVENTORY METRICS DASHBOARD
# ============================================================================

@inventory_bp.route("/metrics")
@login_required
def inventory_metrics():
    """Comprehensive inventory metrics dashboard"""
    try:
        # Get all dashboard metrics
        metrics = InventoryMetrics.get_dashboard_metrics()

        # Get spending data
        spend_by_period = InventoryMetrics.get_spend_by_period('monthly', 12)
        spend_by_category = InventoryMetrics.get_spend_by_category()
        spend_by_supplier = InventoryMetrics.get_spend_by_supplier(10)

        # Get item movement data
        fast_moving = InventoryMetrics.get_top_moving_items(10, 'fast')
        slow_moving = InventoryMetrics.get_top_moving_items(10, 'slow')
        dead_stock = InventoryMetrics.get_dead_stock(90)

        return render_template("inventory_metrics.html",
                             metrics=metrics,
                             spend_by_period=spend_by_period,
                             spend_by_category=spend_by_category,
                             spend_by_supplier=spend_by_supplier,
                             fast_moving=fast_moving,
                             slow_moving=slow_moving,
                             dead_stock=dead_stock)

    except Exception as e:
        logging.error(f"Error loading inventory metrics: {str(e)}")
        flash(f"Error loading metrics: {str(e)}", 'error')
        return redirect(url_for('inventory.inventory_list'))


@inventory_bp.route("/api/metrics/spend")
@login_required
def api_metrics_spend():
    """API endpoint for spending data with dynamic period"""
    period = request.args.get('period', 'monthly')
    months = int(request.args.get('months', 12))

    try:
        data = InventoryMetrics.get_spend_by_period(period, months)
        return jsonify(data)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@inventory_bp.route("/api/metrics/category")
@login_required
def api_metrics_category():
    """API endpoint for category breakdown"""
    try:
        data = InventoryMetrics.get_spend_by_category()
        return jsonify(data)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ============================================================================
# ENHANCED SUPPLIER MANAGEMENT
# ============================================================================

@inventory_bp.route("/suppliers/<int:supplier_id>")
@login_required
def supplier_detail(supplier_id):
    """Detailed supplier view with all info and metrics"""
    try:
        supplier = EnhancedSupplierManagement.get_supplier_details(supplier_id)
        if not supplier:
            flash("Supplier not found.", 'error')
            return redirect(url_for('inventory.suppliers_page'))

        # Get price history for this supplier
        price_history = PriceHistoryTracker.get_price_history(supplier_id=supplier_id, days=365)

        # Get sync history if API configured
        sync_history = []
        if supplier.get('has_api'):
            sync_history = SupplierAPIIntegration.get_sync_history(supplier_id, 10)

        return render_template("supplier_detail.html",
                             supplier=supplier,
                             price_history=price_history,
                             sync_history=sync_history)

    except Exception as e:
        logging.error(f"Error loading supplier details: {str(e)}")
        flash(f"Error: {str(e)}", 'error')
        return redirect(url_for('inventory.suppliers_page'))


@inventory_bp.route("/suppliers/<int:supplier_id>/edit", methods=["GET", "POST"])
@login_required
def edit_supplier(supplier_id):
    """Edit supplier information"""
    if current_user.role not in ['manager', 'admin']:
        flash("Permission denied.", 'error')
        return redirect(url_for('inventory.suppliers_page'))

    if request.method == "GET":
        supplier = EnhancedSupplierManagement.get_supplier_details(supplier_id)
        if not supplier:
            flash("Supplier not found.", 'error')
            return redirect(url_for('inventory.suppliers_page'))
        return render_template("edit_supplier.html", supplier=supplier)

    elif request.method == "POST":
        try:
            data = {
                'name': request.form.get('name'),
                'contact_person': request.form.get('contact_person'),
                'email': request.form.get('email'),
                'phone': request.form.get('phone'),
                'address': request.form.get('address'),
                'city': request.form.get('city'),
                'state': request.form.get('state'),
                'zip_code': request.form.get('zip_code'),
                'country': request.form.get('country', 'USA'),
                'website': request.form.get('website'),
                'account_number': request.form.get('account_number'),
                'payment_terms': request.form.get('payment_terms'),
                'shipping_terms': request.form.get('shipping_terms'),
                'preferred': request.form.get('preferred') == 'on',
                'active': request.form.get('active') == 'on',
                'notes': request.form.get('notes'),
                'api_endpoint': request.form.get('api_endpoint'),
                'api_key': request.form.get('api_key'),
                'api_username': request.form.get('api_username'),
                'api_password': request.form.get('api_password'),
                'api_auth_type': request.form.get('api_auth_type')
            }

            EnhancedSupplierManagement.update_supplier(supplier_id, data)
            flash("Supplier updated successfully.", 'success')
            return redirect(url_for('inventory.supplier_detail', supplier_id=supplier_id))

        except Exception as e:
            flash(f"Error updating supplier: {str(e)}", 'error')
            return redirect(url_for('inventory.edit_supplier', supplier_id=supplier_id))


@inventory_bp.route("/suppliers/<int:supplier_id>/contacts/add", methods=["POST"])
@login_required
def add_supplier_contact(supplier_id):
    """Add a contact to a supplier"""
    if current_user.role not in ['manager', 'admin']:
        return jsonify({'error': 'Permission denied'}), 403

    try:
        contact_data = {
            'contact_type': request.form.get('contact_type', 'primary'),
            'name': request.form.get('name'),
            'title': request.form.get('title'),
            'email': request.form.get('email'),
            'phone': request.form.get('phone'),
            'mobile': request.form.get('mobile'),
            'preferred_contact_method': request.form.get('preferred_contact_method', 'email'),
            'notes': request.form.get('notes')
        }

        contact_id = EnhancedSupplierManagement.add_supplier_contact(supplier_id, contact_data)
        flash("Contact added successfully.", 'success')
        return redirect(url_for('inventory.supplier_detail', supplier_id=supplier_id))

    except Exception as e:
        flash(f"Error adding contact: {str(e)}", 'error')
        return redirect(url_for('inventory.supplier_detail', supplier_id=supplier_id))


@inventory_bp.route("/suppliers/<int:supplier_id>/api/test", methods=["POST"])
@login_required
def test_supplier_api(supplier_id):
    """Test supplier API connection"""
    if current_user.role not in ['manager', 'admin']:
        return jsonify({'error': 'Permission denied'}), 403

    try:
        result = SupplierAPIIntegration.test_connection(supplier_id)
        return jsonify(result)
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@inventory_bp.route("/suppliers/<int:supplier_id>/api/sync", methods=["POST"])
@login_required
def sync_supplier_catalog(supplier_id):
    """Trigger catalog sync from supplier API"""
    if current_user.role not in ['manager', 'admin']:
        return jsonify({'error': 'Permission denied'}), 403

    try:
        result = SupplierAPIIntegration.sync_catalog(supplier_id, current_user.id)
        return jsonify(result)
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@inventory_bp.route("/suppliers/<int:supplier_id>/delete", methods=["POST"])
@login_required
def delete_supplier(supplier_id):
    """Delete a supplier (soft delete - marks as inactive)"""
    if current_user.role not in ['admin']:
        return jsonify({'error': 'Permission denied - admin only'}), 403

    try:
        with db_connection() as conn:
            c = conn.cursor()
            # Check for linked purchase orders
            c.execute("""
                SELECT COUNT(*) FROM purchase_orders
                WHERE supplier_id = %s AND status NOT IN ('delivered', 'cancelled')
            """, (supplier_id,))
            active_orders = c.fetchone()[0]

            if active_orders > 0:
                return jsonify({
                    'error': f'Cannot delete supplier with {active_orders} active orders'
                }), 400

            # Soft delete - mark as inactive
            c.execute("""
                UPDATE suppliers SET active = FALSE, updated_at = CURRENT_TIMESTAMP
                WHERE id = %s
            """, (supplier_id,))
            conn.commit()

            log_audit(
                current_user.id,
                'supplier_deleted',
                'supplier',
                supplier_id,
                {'action': 'soft_delete'}
            )

            return jsonify({'success': True})

    except Exception as e:
        logging.error(f"Error deleting supplier: {str(e)}")
        return jsonify({'error': str(e)}), 500


# ============================================================================
# PRICE HISTORY TRACKING
# ============================================================================

@inventory_bp.route("/price-history")
@login_required
def price_history_list():
    """View all price changes"""
    days = int(request.args.get('days', 90))
    supplier_id = request.args.get('supplier_id', type=int)
    category = request.args.get('category')
    change_type = request.args.get('change_type')

    try:
        # Get price history
        all_history = PriceHistoryTracker.get_price_history(
            supplier_id=supplier_id,
            days=days
        )

        # Filter by change type if specified
        if change_type == 'increase':
            price_changes = [h for h in all_history if h.get('change_percentage', 0) > 0]
        elif change_type == 'decrease':
            price_changes = [h for h in all_history if h.get('change_percentage', 0) < 0]
        else:
            price_changes = all_history

        # Calculate stats
        increases = [h for h in all_history if h.get('change_percentage', 0) > 0]
        decreases = [h for h in all_history if h.get('change_percentage', 0) < 0]
        unique_items = set(h.get('consumable_id') for h in all_history if h.get('consumable_id'))

        stats = {
            'total_changes': len(all_history),
            'increases': len(increases),
            'decreases': len(decreases),
            'avg_increase': sum(h.get('change_percentage', 0) for h in increases) / len(increases) if increases else 0,
            'avg_decrease': sum(h.get('change_percentage', 0) for h in decreases) / len(decreases) if decreases else 0,
            'items_affected': len(unique_items)
        }

        suppliers = EnhancedSupplierManagement.get_all_suppliers()

        # Get categories for filter
        with db_connection() as conn:
            c = conn.cursor()
            c.execute("SELECT DISTINCT category FROM consumables WHERE category IS NOT NULL ORDER BY category")
            categories = [row[0] for row in c.fetchall()]

        return render_template("price_history.html",
                             price_changes=price_changes,
                             stats=stats,
                             suppliers=suppliers,
                             categories=categories,
                             days=days,
                             selected_supplier=supplier_id,
                             selected_category=category,
                             change_type=change_type)

    except Exception as e:
        logging.error(f"Error loading price history: {str(e)}")
        flash(f"Error: {str(e)}", 'error')
        return redirect(url_for('inventory.inventory_list'))


@inventory_bp.route("/inventory/<int:item_id>/price-trends")
@login_required
def item_price_trends(item_id):
    """View price trends for specific item"""
    months = int(request.args.get('months', 12))

    try:
        item = get_enhanced_inventory_item(item_id)
        if not item:
            flash("Item not found.", 'error')
            return redirect(url_for('inventory.inventory_list'))

        trends = PriceHistoryTracker.get_price_trends(item_id, months)
        price_history = PriceHistoryTracker.get_price_history(consumable_id=item_id, days=months * 30)

        # Get supplier prices for comparison
        with db_connection() as conn:
            c = conn.cursor()
            c.execute("""
                SELECT si.id, si.supplier_id, s.name as supplier_name,
                       si.unit_price, si.last_updated
                FROM supplier_items si
                JOIN suppliers s ON si.supplier_id = s.id
                WHERE si.consumable_id = %s AND s.active = TRUE
                ORDER BY si.unit_price ASC
            """, (item_id,))
            supplier_prices = []
            for row in c.fetchall():
                supplier_prices.append({
                    'id': row[0],
                    'supplier_id': row[1],
                    'supplier_name': row[2],
                    'unit_price': row[3],
                    'last_updated': row[4]
                })

        return render_template("item_price_trends.html",
                             item=item,
                             trends=trends,
                             price_history=price_history,
                             supplier_prices=supplier_prices,
                             months=months)

    except Exception as e:
        logging.error(f"Error loading price trends: {str(e)}")
        flash(f"Error: {str(e)}", 'error')
        return redirect(url_for('inventory.inventory_detail', item_id=item_id))


@inventory_bp.route("/api/price-history/record", methods=["POST"])
@login_required
def api_record_price_change():
    """API to record a price change"""
    if current_user.role not in ['manager', 'admin']:
        return jsonify({'error': 'Permission denied'}), 403

    data = request.get_json()
    try:
        PriceHistoryTracker.record_price_change(
            supplier_item_id=data['supplier_item_id'],
            old_price=float(data['old_price']),
            new_price=float(data['new_price']),
            source=data.get('source', 'manual'),
            user_id=current_user.id,
            notes=data.get('notes')
        )
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ============================================================================
# PURCHASE ORDER MANAGEMENT
# ============================================================================

@inventory_bp.route("/orders")
@login_required
def purchase_orders_list():
    """List all purchase orders"""
    status = request.args.get('status')
    supplier_id = request.args.get('supplier_id', type=int)

    try:
        orders = PurchaseOrderManager.get_all_purchase_orders(
            status=status,
            supplier_id=supplier_id
        )

        suppliers = EnhancedSupplierManagement.get_all_suppliers()

        return render_template("purchase_orders.html",
                             orders=orders,
                             suppliers=suppliers,
                             selected_status=status,
                             selected_supplier=supplier_id)

    except Exception as e:
        logging.error(f"Error loading purchase orders: {str(e)}")
        flash(f"Error: {str(e)}", 'error')
        return redirect(url_for('inventory.inventory_list'))


@inventory_bp.route("/orders/<int:po_id>")
@login_required
def purchase_order_detail(po_id):
    """View purchase order details"""
    try:
        po = PurchaseOrderManager.get_purchase_order(po_id)
        if not po:
            flash("Purchase order not found.", 'error')
            return redirect(url_for('inventory.purchase_orders_list'))

        return render_template("purchase_order_detail.html", po=po)

    except Exception as e:
        logging.error(f"Error loading PO details: {str(e)}")
        flash(f"Error: {str(e)}", 'error')
        return redirect(url_for('inventory.purchase_orders_list'))


@inventory_bp.route("/orders/create", methods=["GET", "POST"])
@login_required
def create_purchase_order():
    """Create a new purchase order"""
    if current_user.role not in ['manager', 'admin']:
        flash("Permission denied.", 'error')
        return redirect(url_for('inventory.purchase_orders_list'))

    if request.method == "GET":
        suppliers = EnhancedSupplierManagement.get_all_suppliers()
        low_stock = get_all_consumables(filter_status='low_stock')
        return render_template("create_purchase_order.html",
                             suppliers=suppliers,
                             low_stock_items=low_stock)

    elif request.method == "POST":
        try:
            supplier_id = int(request.form.get('supplier_id'))
            notes = request.form.get('notes')

            # Parse items from form
            items = []
            item_ids = request.form.getlist('item_ids[]')
            quantities = request.form.getlist('quantities[]')
            prices = request.form.getlist('prices[]')

            for i, item_id in enumerate(item_ids):
                if item_id and quantities[i]:
                    items.append({
                        'consumable_id': int(item_id),
                        'quantity': int(quantities[i]),
                        'unit_price': float(prices[i]) if prices[i] else 0
                    })

            if not items:
                flash("No items selected for order.", 'error')
                return redirect(url_for('inventory.create_purchase_order'))

            po_id = PurchaseOrderManager.create_purchase_order(
                supplier_id=supplier_id,
                items=items,
                user_id=current_user.id,
                notes=notes
            )

            flash(f"Purchase order created successfully.", 'success')
            return redirect(url_for('inventory.purchase_order_detail', po_id=po_id))

        except Exception as e:
            flash(f"Error creating order: {str(e)}", 'error')
            return redirect(url_for('inventory.create_purchase_order'))


@inventory_bp.route("/orders/<int:po_id>/status", methods=["POST"])
@login_required
def update_order_status(po_id):
    """Update purchase order status"""
    if current_user.role not in ['manager', 'admin']:
        return jsonify({'error': 'Permission denied'}), 403

    new_status = request.form.get('status') or request.json.get('status')

    try:
        success = PurchaseOrderManager.update_order_status(po_id, new_status, current_user.id)
        if success:
            flash(f"Order status updated to {new_status}.", 'success')
        else:
            flash("Failed to update order status.", 'error')
    except Exception as e:
        flash(f"Error: {str(e)}", 'error')

    return redirect(url_for('inventory.purchase_order_detail', po_id=po_id))


@inventory_bp.route("/orders/<int:po_id>/receive", methods=["GET", "POST"])
@login_required
def receive_order(po_id):
    """Receive items from a purchase order"""
    if current_user.role not in ['manager', 'admin']:
        flash("Permission denied.", 'error')
        return redirect(url_for('inventory.purchase_orders_list'))

    po = PurchaseOrderManager.get_purchase_order(po_id)
    if not po:
        flash("Purchase order not found.", 'error')
        return redirect(url_for('inventory.purchase_orders_list'))

    if request.method == "GET":
        return render_template("receive_order.html", po=po)

    elif request.method == "POST":
        try:
            received_items = []
            for item in po['items']:
                qty = request.form.get(f'qty_{item["id"]}')
                if qty:
                    received_items.append({
                        'po_item_id': item['id'],
                        'quantity_received': int(qty),
                        'condition_notes': request.form.get(f'notes_{item["id"]}', '')
                    })

            if received_items:
                PurchaseOrderManager.receive_items(po_id, received_items, current_user.id)
                flash("Items received successfully.", 'success')
            else:
                flash("No items to receive.", 'warning')

        except Exception as e:
            flash(f"Error receiving items: {str(e)}", 'error')

        return redirect(url_for('inventory.purchase_order_detail', po_id=po_id))


# ============================================================================
# INVENTORY TRANSFERS
# ============================================================================

@inventory_bp.route("/transfers")
@login_required
def transfers_list():
    """List inventory transfers"""
    status = request.args.get('status')

    try:
        transfers = InventoryTransferManager.get_transfers(status=status)

        # Get locations for filtering
        conn = get_connection()
        c = conn.cursor()
        c.execute("SELECT id, name, location_type FROM inventory_locations WHERE active = TRUE ORDER BY name")
        locations = [{'id': r[0], 'name': r[1], 'type': r[2]} for r in c.fetchall()]
        release_connection(conn)

        return render_template("inventory_transfers.html",
                             transfers=transfers,
                             locations=locations,
                             selected_status=status)

    except Exception as e:
        logging.error(f"Error loading transfers: {str(e)}")
        flash(f"Error: {str(e)}", 'error')
        return redirect(url_for('inventory.inventory_list'))


@inventory_bp.route("/transfers/create", methods=["GET", "POST"])
@login_required
def create_transfer():
    """Create a new transfer request"""
    if current_user.role not in ['manager', 'admin']:
        flash("Permission denied.", 'error')
        return redirect(url_for('inventory.transfers_list'))

    conn = get_connection()
    c = conn.cursor()

    if request.method == "GET":
        c.execute("SELECT id, name, location_type FROM inventory_locations WHERE active = TRUE ORDER BY name")
        locations = [{'id': r[0], 'name': r[1], 'type': r[2]} for r in c.fetchall()]
        release_connection(conn)

        items = get_all_consumables()

        return render_template("create_transfer.html",
                             locations=locations,
                             items=items)

    elif request.method == "POST":
        release_connection(conn)
        try:
            transfer_id = InventoryTransferManager.create_transfer_request(
                from_location_id=int(request.form.get('from_location')),
                to_location_id=int(request.form.get('to_location')),
                consumable_id=int(request.form.get('item_id')),
                quantity=int(request.form.get('quantity')),
                user_id=current_user.id,
                notes=request.form.get('notes')
            )

            flash("Transfer request created successfully.", 'success')
            return redirect(url_for('inventory.transfers_list'))

        except Exception as e:
            flash(f"Error creating transfer: {str(e)}", 'error')
            return redirect(url_for('inventory.create_transfer'))


@inventory_bp.route("/transfers/<int:transfer_id>/approve", methods=["POST"])
@login_required
def approve_transfer(transfer_id):
    """Approve a transfer request"""
    if current_user.role not in ['manager', 'admin']:
        return jsonify({'error': 'Permission denied'}), 403

    try:
        success = InventoryTransferManager.approve_transfer(transfer_id, current_user.id)
        if success:
            flash("Transfer approved.", 'success')
        else:
            flash("Could not approve transfer.", 'error')
    except Exception as e:
        flash(f"Error: {str(e)}", 'error')

    return redirect(url_for('inventory.transfers_list'))


@inventory_bp.route("/transfers/<int:transfer_id>/ship", methods=["POST"])
@login_required
def ship_transfer(transfer_id):
    """Mark transfer as shipped"""
    if current_user.role not in ['manager', 'admin']:
        return jsonify({'error': 'Permission denied'}), 403

    tracking = request.form.get('tracking_info')

    try:
        success = InventoryTransferManager.ship_transfer(transfer_id, tracking)
        if success:
            flash("Transfer marked as shipped.", 'success')
        else:
            flash("Could not ship transfer.", 'error')
    except Exception as e:
        flash(f"Error: {str(e)}", 'error')

    return redirect(url_for('inventory.transfers_list'))


@inventory_bp.route("/transfers/<int:transfer_id>/receive", methods=["POST"])
@login_required
def receive_transfer(transfer_id):
    """Mark transfer as received"""
    try:
        success = InventoryTransferManager.receive_transfer(transfer_id)
        if success:
            flash("Transfer received successfully.", 'success')
        else:
            flash("Could not receive transfer.", 'error')
    except Exception as e:
        flash(f"Error: {str(e)}", 'error')

    return redirect(url_for('inventory.transfers_list'))


# ============================================================================
# INVENTORY LOCATIONS
# ============================================================================

@inventory_bp.route("/locations")
@login_required
def locations_list():
    """List inventory locations"""
    conn = get_connection()
    c = conn.cursor()

    try:
        c.execute("""
            SELECT il.id, il.name, il.location_type, il.address, il.city, il.state,
                   il.is_primary, il.active, u.username as manager,
                   COUNT(DISTINCT it.id) as transfer_count
            FROM inventory_locations il
            LEFT JOIN users u ON il.manager_id = u.id
            LEFT JOIN inventory_transfers it ON (il.id = it.from_location_id OR il.id = it.to_location_id)
            GROUP BY il.id, u.username
            ORDER BY il.is_primary DESC, il.name
        """)

        locations = []
        for row in c.fetchall():
            locations.append({
                'id': row[0],
                'name': row[1],
                'location_type': row[2],
                'address': row[3],
                'city': row[4],
                'state': row[5],
                'is_primary': row[6],
                'active': row[7],
                'manager': row[8],
                'transfer_count': row[9]
            })

        return render_template("inventory_locations.html", locations=locations)

    except Exception as e:
        logging.error(f"Error loading locations: {str(e)}")
        flash(f"Error: {str(e)}", 'error')
        return redirect(url_for('inventory.inventory_list'))
    finally:
        release_connection(conn)


@inventory_bp.route("/locations/add", methods=["GET", "POST"])
@login_required
def add_location():
    """Add a new inventory location"""
    if current_user.role not in ['manager', 'admin']:
        flash("Permission denied.", 'error')
        return redirect(url_for('inventory.locations_list'))

    if request.method == "GET":
        return render_template("add_location.html")

    elif request.method == "POST":
        conn = get_connection()
        c = conn.cursor()

        try:
            c.execute("""
                INSERT INTO inventory_locations
                (name, location_type, address, city, state, zip_code, is_primary, notes)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                request.form.get('name'),
                request.form.get('location_type', 'warehouse'),
                request.form.get('address'),
                request.form.get('city'),
                request.form.get('state'),
                request.form.get('zip_code'),
                request.form.get('is_primary') == 'on',
                request.form.get('notes')
            ))

            conn.commit()
            flash("Location added successfully.", 'success')
            return redirect(url_for('inventory.locations_list'))

        except Exception as e:
            conn.rollback()
            flash(f"Error adding location: {str(e)}", 'error')
            return redirect(url_for('inventory.add_location'))
        finally:
            release_connection(conn)