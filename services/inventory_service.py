# services/inventory_service.py
import os
import json
import logging
import hashlib
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
from config import NOTIFICATION_RECIPIENTS, UPLOAD_FOLDER
from services.db import db_connection, db_transaction, get_connection, release_connection
from services.logging_service import log_audit
from services.email_service import send_notification
from flask_login import current_user
from flask import flash, redirect, url_for
from werkzeug.utils import secure_filename
from services.init_db import fix_database_schema_inconsistencies, update_item_templates_for_separation
import pandas as pd
from decimal import Decimal
from PIL import Image
import psycopg2  # Keep for error handling only  

logging.basicConfig(filename='inventory.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

class EnhancedInventoryNameGenerator:
    """Advanced name generation for construction inventory items"""
    
    @staticmethod
    def generate_standardized_name(category: str, base_name: str, specifications: dict) -> str:
        """Generate a standardized name based on category and specifications"""
        
        # Construction-specific naming patterns
        if category == 'fasteners':
            if base_name == 'bolt':
                parts = []
                if specifications.get('diameter'):
                    parts.append(specifications['diameter'])
                if specifications.get('length'):
                    parts.append(f"x {specifications['length']}")
                if specifications.get('material'):
                    parts.append(specifications['material'].replace('_', ' '))
                if specifications.get('grade'):
                    parts.append(f"Grade {specifications['grade']}")
                if specifications.get('thread_type'):
                    parts.append(specifications['thread_type'].replace('_', ' '))
                if specifications.get('head_type'):
                    parts.append(specifications['head_type'].replace('_', ' '))
                parts.append('Bolt')
                return ' '.join(parts)
            
            elif base_name == 'screw':
                parts = []
                if specifications.get('diameter'):
                    parts.append(specifications['diameter'])
                if specifications.get('length'):
                    parts.append(f"x {specifications['length']}")
                if specifications.get('head_type'):
                    parts.append(specifications['head_type'].replace('_', ' '))
                if specifications.get('drive_type'):
                    parts.append(specifications['drive_type'].replace('_', ' '))
                if specifications.get('material'):
                    parts.append(specifications['material'].replace('_', ' '))
                parts.append('Screw')
                return ' '.join(parts)
        
        elif category == 'electrical':
            if base_name == 'cable':
                parts = []
                if specifications.get('gauge'):
                    parts.append(f"{specifications['gauge']} AWG")
                if specifications.get('conductor_count'):
                    parts.append(f"{specifications['conductor_count']}-Conductor")
                if specifications.get('conductor_material'):
                    parts.append(specifications['conductor_material'].replace('_', ' '))
                if specifications.get('insulation'):
                    parts.append(specifications['insulation'])
                if specifications.get('voltage_rating'):
                    parts.append(specifications['voltage_rating'])
                parts.append('Cable')
                return ' '.join(parts)
        
        elif category == 'safety':
            if base_name == 'gloves':
                parts = []
                if specifications.get('material'):
                    parts.append(specifications['material'].replace('_', ' '))
                if specifications.get('protection_class'):
                    parts.append(specifications['protection_class'].replace('_', ' '))
                if specifications.get('size'):
                    parts.append(f"Size {specifications['size'].upper()}")
                parts.append('Safety Gloves')
                return ' '.join(parts)
        
        elif category == 'plumbing':
            if base_name == 'pipe':
                parts = []
                if specifications.get('diameter'):
                    parts.append(specifications['diameter'])
                if specifications.get('material'):
                    parts.append(specifications['material'])
                if specifications.get('schedule'):
                    parts.append(f"Schedule {specifications['schedule']}")
                if specifications.get('length'):
                    parts.append(f"{specifications['length']}")
                parts.append('Pipe')
                return ' '.join(parts)
        
        # Default pattern for other items
        spec_parts = []
        priority_specs = ['diameter', 'size', 'gauge', 'length', 'width', 'material', 'type', 'grade']
        
        for spec in priority_specs:
            if spec in specifications and specifications[spec]:
                spec_parts.append(str(specifications[spec]).replace('_', ' '))
        
        # Add remaining specs not in priority list
        for key, value in specifications.items():
            if key not in priority_specs and value and key not in ['dimensions', 'weight_per_unit']:
                spec_parts.append(str(value).replace('_', ' '))
        
        base_display = base_name.replace('_', ' ').title()
        if spec_parts:
            return f"{' '.join(spec_parts)} {base_display}"
        return base_display
    
    @staticmethod
    def generate_separation_key(category: str, base_name: str, specifications: dict, supplier: str) -> str:
        """Generate a unique key for item separation by specifications and supplier"""
        
        # Create a deterministic key from specifications
        key_parts = [category, base_name]
        
        # Add specifications in sorted order for consistency
        spec_keys = sorted(specifications.keys())
        for key in spec_keys:
            if key not in ['notes', 'serial_numbers', 'image_path']:
                value = str(specifications[key]).lower().replace(' ', '_')
                key_parts.append(f"{key}:{value}")
        
        # Add supplier for granular separation
        key_parts.append(f"supplier:{supplier.lower().replace(' ', '_')}")
        
        return '_'.join(key_parts)
    
    @staticmethod
    def generate_internal_sku(category: str, base_name: str, specifications: dict, sequence: int) -> str:
        """Generate internal SKU for tracking"""
        
        # Category codes
        category_codes = {
            'fasteners': 'FST',
            'electrical': 'ELC',
            'safety': 'SAF',
            'hardware': 'HDW',
            'plumbing': 'PLB',
            'concrete': 'CON',
            'lumber': 'LMB',
            'paint': 'PNT'
        }
        
        cat_code = category_codes.get(category, 'GEN')
        
        # Create spec hash for uniqueness
        spec_str = json.dumps(specifications, sort_keys=True)
        spec_hash = hashlib.md5(spec_str.encode()).hexdigest()[:4].upper()
        
        # Format: CAT-HASH-SEQUENCE
        return f"{cat_code}-{spec_hash}-{sequence:05d}"

class InventoryAnalytics:
    """Analytics and prediction support for inventory management"""

    @staticmethod
    def analyze_job_consumption(job_id: int) -> Dict[str, Any]:
        """Analyze consumable usage patterns for a job"""
        with db_connection() as conn:
            c = conn.cursor()

            # Get job details
            c.execute("""
                SELECT job_type, square_footage, crew_size, duration_days
                FROM jobs WHERE id = %s
            """, (job_id,))
            job_data = c.fetchone()

            if not job_data:
                return {}

            # Get consumables used
            c.execute("""
                SELECT c.category, c.base_name, c.specifications, jc.quantity_used, c.unit
                FROM job_consumables jc
                JOIN consumables c ON jc.consumable_id = c.id
                WHERE jc.job_id = %s
            """, (job_id,))

            consumables = []
            for row in c.fetchall():
                consumables.append({
                    'category': row[0],
                    'base_name': row[1],
                    'specifications': json.loads(row[2]) if row[2] else {},
                    'quantity_used': row[3],
                    'unit': row[4]
                })

            return {
                'job_type': job_data[0],
                'square_footage': job_data[1],
                'crew_size': job_data[2],
                'duration_days': job_data[3],
                'consumables_used': consumables
            }
    
    @staticmethod
    def predict_job_requirements(job_type: str, square_footage: int, crew_size: int) -> List[Dict[str, Any]]:
        """Predict consumable requirements based on historical data"""
        with db_connection() as conn:
            c = conn.cursor()

            # Find similar completed jobs
            c.execute("""
                SELECT j.id, j.square_footage, j.crew_size
                FROM jobs j
                WHERE j.job_type = %s
                AND j.status = 'completed'
                AND j.square_footage BETWEEN %s AND %s
                ORDER BY ABS(j.square_footage - %s)
                LIMIT 10
            """, (job_type, square_footage * 0.8, square_footage * 1.2, square_footage))

            similar_jobs = [row[0] for row in c.fetchall()]

            if not similar_jobs:
                return []

            # Aggregate consumable usage from similar jobs
            c.execute("""
                SELECT
                    c.category,
                    c.base_name,
                    c.specifications,
                    AVG(jc.quantity_used * %s::float / j.square_footage) as avg_per_sqft,
                    c.unit,
                    COUNT(DISTINCT jc.job_id) as job_count
                FROM job_consumables jc
                JOIN consumables c ON jc.consumable_id = c.id
                JOIN jobs j ON jc.job_id = j.id
                WHERE jc.job_id = ANY(%s)
                GROUP BY c.category, c.base_name, c.specifications, c.unit
                HAVING COUNT(DISTINCT jc.job_id) > 2
                ORDER BY job_count DESC, avg_per_sqft DESC
            """, (square_footage, similar_jobs))

            predictions = []
            for row in c.fetchall():
                predicted_qty = row[3] * square_footage
                predictions.append({
                    'category': row[0],
                    'base_name': row[1],
                    'specifications': json.loads(row[2]) if row[2] else {},
                    'predicted_quantity': round(predicted_qty, 2),
                    'unit': row[4],
                    'confidence': min(row[5] / len(similar_jobs), 1.0),
                    'based_on_jobs': row[5]
                })

            return predictions

class SupplierManagement:
    """Enhanced supplier tracking and management aligned with database schema"""

    @staticmethod
    def add_supplier_item(consumable_id: int, supplier_data: dict) -> int:
        """Add or update supplier information for an item"""
        with db_transaction() as conn:
            c = conn.cursor()

            # First check if supplier exists in suppliers table
            supplier_name = supplier_data.get('supplier_name')
            c.execute("SELECT id FROM suppliers WHERE name = %s", (supplier_name,))
            supplier_result = c.fetchone()

            if supplier_result:
                supplier_id = supplier_result[0]
            else:
                # Create new supplier
                c.execute("""
                    INSERT INTO suppliers (name, active)
                    VALUES (%s, TRUE)
                    RETURNING id
                """, (supplier_name,))
                supplier_id = c.fetchone()[0]

            # Insert or update supplier_items
            c.execute("""
                INSERT INTO supplier_items
                (consumable_id, supplier_id, supplier_name, supplier_sku, supplier_part_number,
                 cost_per_unit, lead_time_days, minimum_order_quantity,
                 preferred, current_supplier, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                ON CONFLICT (consumable_id, supplier_name, supplier_sku) DO UPDATE SET
                    supplier_part_number = EXCLUDED.supplier_part_number,
                    cost_per_unit = EXCLUDED.cost_per_unit,
                    lead_time_days = EXCLUDED.lead_time_days,
                    minimum_order_quantity = EXCLUDED.minimum_order_quantity,
                    preferred = EXCLUDED.preferred,
                    current_supplier = EXCLUDED.current_supplier,
                    updated_at = CURRENT_TIMESTAMP
                RETURNING id
            """, (
                consumable_id,
                supplier_id,
                supplier_name,
                supplier_data.get('supplier_sku'),
                supplier_data.get('supplier_part_number'),
                supplier_data.get('cost_per_unit'),
                supplier_data.get('lead_time_days'),
                supplier_data.get('minimum_order_quantity', 1),
                supplier_data.get('preferred', False),
                supplier_data.get('current_supplier', False)
            ))

            supplier_item_id = c.fetchone()[0]
            return supplier_item_id

    @staticmethod
    def get_best_supplier(consumable_id: int, criteria: str = 'cost') -> Optional[Dict]:
        """Get the best supplier for an item based on criteria"""
        with db_connection() as conn:
            c = conn.cursor()

            order_by = {
                'cost': 'cost_per_unit ASC',
                'lead_time': 'lead_time_days ASC',
                'preferred': 'preferred DESC, cost_per_unit ASC'
            }.get(criteria, 'cost_per_unit ASC')

            c.execute(f"""
                SELECT supplier_name, supplier_sku, cost_per_unit,
                       lead_time_days, minimum_order_quantity, preferred
                FROM supplier_items
                WHERE consumable_id = %s AND cost_per_unit IS NOT NULL
                ORDER BY {order_by}
                LIMIT 1
            """, (consumable_id,))

            result = c.fetchone()
            if result:
                return {
                    'supplier_name': result[0],
                    'supplier_sku': result[1],
                    'cost_per_unit': float(result[2]) if result[2] else None,
                    'lead_time_days': result[3],
                    'minimum_order_quantity': result[4],
                    'preferred': result[5]
                }

            return None

def validate_form_data(form_data):
    # Validation logic
    pass

def build_specifications(form_data):
    # Build specs
    return specifications

def create_enhanced_consumable(form_data):
    validate_form_data(form_data)
    specifications = build_specifications(form_data)
    # ... rest

def get_enhanced_inventory_item(item_id: int) -> Optional[Dict]:
    """Get comprehensive inventory item details"""
    conn = get_connection()
    c = conn.cursor()

    try:
        c.execute("""
            SELECT 
                c.id, c.name, c.category, c.base_name, c.specifications,
                c.location, c.bin_location, c.quantity, c.supplier, c.manufacturer,
                c.part_number, c.supplier_sku, c.internal_sku, c.serial_numbers,
                c.unit, c.reorder_threshold, c.minimum_stock_level, c.maximum_stock_level,
                c.cost_per_unit, c.weight_per_unit, c.hazmat, c.requires_certification,
                c.product_url, c.notes, c.image_path, c.normalized_name,
                c.created_at, c.updated_at,
                -- Calculate stock status
                CASE 
                    WHEN c.quantity = 0 THEN 'out_of_stock'
                    WHEN c.quantity <= COALESCE(c.minimum_stock_level, c.reorder_threshold, 0) THEN 'low_stock'
                    WHEN c.maximum_stock_level IS NOT NULL AND c.quantity > c.maximum_stock_level THEN 'overstocked'
                    ELSE 'in_stock'
                END as stock_status,
                -- Get last transaction
                (SELECT transaction_date FROM inventory_transactions 
                 WHERE consumable_id = c.id 
                 ORDER BY transaction_date DESC LIMIT 1) as last_transaction_date,
                -- Get supplier count
                (SELECT COUNT(*) FROM supplier_items WHERE consumable_id = c.id) as supplier_count
            FROM consumables c
            WHERE c.id = %s AND c.active = TRUE
        """, (item_id,))
        
        result = c.fetchone()
        if not result:
            return None
        
        # Get all suppliers for this item
        c.execute("""
            SELECT supplier_name, supplier_sku, cost_per_unit, lead_time_days,
                   minimum_order_quantity, preferred, current_supplier
            FROM supplier_items
            WHERE consumable_id = %s
            ORDER BY preferred DESC, current_supplier DESC, cost_per_unit ASC
        """, (item_id,))
        
        suppliers = []
        for row in c.fetchall():
            suppliers.append({
                'name': row[0],
                'sku': row[1],
                'cost': float(row[2]) if row[2] else None,
                'lead_time': row[3],
                'min_order': row[4],
                'preferred': row[5],
                'current': row[6]
            })
        
        # Get recent transactions
        c.execute("""
            SELECT transaction_type, quantity_change, transaction_date, performed_by, notes
            FROM inventory_transactions
            WHERE consumable_id = %s
            ORDER BY transaction_date DESC
            LIMIT 10
        """, (item_id,))
        
        transactions = []
        for row in c.fetchall():
            transactions.append({
                'type': row[0],
                'quantity_change': row[1],
                'date': row[2],
                'performed_by': row[3],
                'notes': row[4]
            })
        
        # Get job usage history
        c.execute("""
            SELECT j.id, j.name, j.job_type, jc.quantity_used, jc.date_used
            FROM job_consumables jc
            JOIN jobs j ON jc.job_id = j.id
            WHERE jc.consumable_id = %s
            ORDER BY jc.date_used DESC
            LIMIT 5
        """, (item_id,))
        
        job_usage = []
        for row in c.fetchall():
            job_usage.append({
                'job_id': row[0],
                'job_name': row[1],
                'job_type': row[2],
                'quantity_used': row[3],
                'date_used': row[4]
            })
        
        # Build comprehensive item dictionary
        item = {
            'id': result[0],
            'name': result[1],
            'category': result[2],
            'base_name': result[3],
            'specifications': json.loads(result[4]) if result[4] else {},
            'location': result[5],
            'bin_location': result[6],
            'quantity': result[7],
            'supplier': result[8],
            'manufacturer': result[9],
            'part_number': result[10],
            'supplier_sku': result[11],
            'internal_sku': result[12],
            'serial_numbers': json.loads(result[13]) if result[13] else [],
            'unit': result[14],
            'reorder_threshold': result[15],
            'minimum_stock_level': result[16],
            'maximum_stock_level': result[17],
            'cost_per_unit': float(result[18]) if result[18] else None,
            'weight_per_unit': float(result[19]) if result[19] else None,
            'hazmat': result[20],
            'requires_certification': result[21],
            'product_url': result[22],
            'notes': result[23],
            'image_path': result[24],
            'normalized_name': result[25],
            'created_at': result[26],
            'updated_at': result[27],
            'stock_status': result[28],
            'last_transaction_date': result[29],
            'supplier_count': result[30],
            'suppliers': suppliers,
            'recent_transactions': transactions,
            'job_usage': job_usage,
            'stock_value': (result[7] * float(result[18])) if result[18] else None,
            'needs_reorder': result[7] <= (result[16] or result[15] or 0) if (result[16] or result[15]) else False
        }
        
        return item
        
    finally:
        release_connection(conn)

def bulk_import_inventory(file_path: str, file_type: str = 'csv') -> Dict[str, Any]:
    """Bulk import inventory from CSV or Excel file"""
    
    try:
        # Read the file
        if file_type == 'csv':
            df = pd.read_csv(file_path)
        elif file_type in ['xlsx', 'xls']:
            df = pd.read_excel(file_path)
        else:
            raise ValueError(f"Unsupported file type: {file_type}")
        
        # Expected columns mapping
        column_mapping = {
            'Category': 'category',
            'Item Type': 'base_name',
            'Supplier': 'supplier',
            'Supplier SKU': 'supplier_sku',
            'Manufacturer': 'manufacturer',
            'Part Number': 'part_number',
            'Quantity': 'quantity',
            'Unit': 'unit',
            'Location': 'location',
            'Bin Location': 'bin_location',
            'Cost Per Unit': 'cost_per_unit',
            'Reorder Point': 'reorder_threshold',
            'Min Stock': 'min_stock_level',
            'Max Stock': 'max_stock_level'
        }
        
        # Rename columns that exist
        existing_columns = {col: column_mapping[col] for col in df.columns if col in column_mapping}
        df = df.rename(columns=existing_columns)
        
        # Process specification columns (any column not in standard fields)
        standard_fields = list(column_mapping.values())
        spec_columns = [col for col in df.columns if col not in standard_fields]
        
        success_count = 0
        error_count = 0
        errors = []
        
        for index, row in df.iterrows():
            try:
                # Build form data
                form_data = {}
                
                # Add standard fields
                for field in standard_fields:
                    if field in row and pd.notna(row[field]):
                        form_data[field] = str(row[field])
                
                # Build specifications from remaining columns
                specifications = {}
                for col in spec_columns:
                    if pd.notna(row[col]):
                        # Clean column name for specification key
                        spec_key = col.lower().replace(' ', '_').replace('-', '_')
                        specifications[spec_key] = str(row[col])
                
                if specifications:
                    form_data['specifications'] = json.dumps(specifications)
                
                # Validate required fields
                if not form_data.get('category'):
                    form_data['category'] = 'general'
                if not form_data.get('base_name'):
                    form_data['base_name'] = 'item'
                if not form_data.get('unit'):
                    form_data['unit'] = 'each'
                if not form_data.get('quantity'):
                    form_data['quantity'] = '0'
                
                # Create the item
                item_id = create_enhanced_consumable(form_data)
                success_count += 1
                
            except Exception as e:
                error_count += 1
                errors.append(f"Row {index + 2}: {str(e)}")
                if len(errors) > 10:
                    errors.append("... and more errors")
                    break
        
        return {
            'success': True,
            'total_rows': len(df),
            'success_count': success_count,
            'error_count': error_count,
            'errors': errors
        }
        
    except Exception as e:
        return {
            'success': False,
            'error': f"Failed to process file: {str(e)}"
        }

def send_low_stock_alert(item_id: int, item_name: str, current_qty: float, threshold: float):
    """Send low stock alert notification"""
    try:
        # Create inventory alert
        create_inventory_alert(
            item_id, 
            'low_stock', 
            'warning' if current_qty > 0 else 'critical',
            f'Low stock alert: {item_name} has {current_qty} units remaining (threshold: {threshold})'
        )
        
        # Send email notification if enabled
        subject = f"Low Stock Alert: {item_name}"
        body = f"""
        Inventory Alert - Immediate Attention Required
        
        Item: {item_name}
        Current Quantity: {current_qty}
        Reorder Threshold: {threshold}
        
        This item has fallen below the reorder threshold and needs to be reordered.
        
        Please log into the inventory system to take action.
        """
        
        if NOTIFICATION_RECIPIENTS:
            send_notification(subject, body, NOTIFICATION_RECIPIENTS)
        
        logging.info(f"Low stock alert sent for item {item_id}: {item_name}")
        
    except Exception as e:
        logging.error(f"Failed to send low stock alert: {str(e)}")

# Add this function to handle template loading issues
def get_item_templates():
    """Get construction-specific item templates with error handling"""
    try:
        with db_connection() as conn:
            c = conn.cursor()

            c.execute("""
                SELECT category, base_name, required_specs, optional_specs,
                       default_unit, naming_pattern, description
                FROM item_templates
                ORDER BY category, base_name
            """)

            templates = []
            for row in c.fetchall():
                templates.append({
                    'category': row[0],
                    'base_name': row[1],
                    'required_specs': json.loads(row[2]) if row[2] else {},
                    'optional_specs': json.loads(row[3]) if row[3] else {},
                    'default_unit': row[4],
                    'naming_pattern': row[5],
                    'description': row[6]
                })

            return templates

    except Exception as e:
        logging.error(f"Error getting templates: {str(e)}")
        # Return fallback templates
        return get_fallback_templates()

def get_fallback_templates():
    """Fallback templates when database is unavailable"""
    return [
        {
            'category': 'fasteners',
            'base_name': 'bolt',
            'required_specs': {
                'diameter': {'type': 'select', 'values': ['1/4"', '3/8"', '1/2"', '5/8"', '3/4"', '1"'], 'label': 'Diameter'},
                'length': {'type': 'text', 'label': 'Length'},
                'material': {'type': 'select', 'values': ['steel', 'stainless_steel', 'galvanized'], 'label': 'Material'},
                'grade': {'type': 'select', 'values': ['2', '5', '8'], 'label': 'Grade'}
            },
            'optional_specs': {
                'head_type': {'type': 'select', 'values': ['hex', 'carriage', 'socket'], 'label': 'Head Type'}
            },
            'default_unit': 'each',
            'naming_pattern': '{diameter} x {length} {material} Grade {grade} Bolt',
            'description': 'Standard construction bolts'
        },
        {
            'category': 'electrical',
            'base_name': 'cable',
            'required_specs': {
                'gauge': {'type': 'select', 'values': ['12', '14', '16', '18'], 'label': 'Wire Gauge'},
                'conductor_material': {'type': 'select', 'values': ['copper', 'aluminum'], 'label': 'Conductor'},
                'insulation': {'type': 'select', 'values': ['THHN', 'THWN'], 'label': 'Insulation'}
            },
            'optional_specs': {},
            'default_unit': 'feet',
            'naming_pattern': '{gauge} AWG {conductor_material} {insulation} Cable',
            'description': 'Electrical cables'
        }
    ]

def get_categories_and_base_names():
    """Get available categories and item types with error handling"""
    try:
        with db_connection() as conn:
            c = conn.cursor()

            # Get categories from templates
            c.execute("SELECT DISTINCT category FROM item_templates ORDER BY category")
            categories = [row[0] for row in c.fetchall()]

            # Get base names by category
            c.execute("""
                SELECT category, base_name
                FROM item_templates
                ORDER BY category, base_name
            """)

            base_names_by_category = {}
            for row in c.fetchall():
                category, base_name = row
                if category not in base_names_by_category:
                    base_names_by_category[category] = []
                base_names_by_category[category].append(base_name)

            # If no templates, use fallback
            if not categories:
                return get_fallback_categories()

            return {
            'categories': categories,
            'base_names_by_category': base_names_by_category
        }
        
    except Exception as e:
        logging.error(f"Error getting categories: {str(e)}")
        return get_fallback_categories()

def get_fallback_categories():
    """Fallback categories when database is unavailable"""
    return {
        'categories': ['fasteners', 'electrical', 'safety', 'hardware', 'plumbing', 'concrete', 'lumber', 'paint'],
        'base_names_by_category': {
            'fasteners': ['bolt', 'screw', 'nut', 'washer', 'anchor', 'nail'],
            'electrical': ['cable', 'wire', 'connector', 'conduit', 'box', 'breaker'],
            'safety': ['gloves', 'helmet', 'goggles', 'vest', 'harness', 'boots'],
            'hardware': ['bracket', 'hinge', 'lock', 'handle', 'chain', 'rope'],
            'plumbing': ['pipe', 'fitting', 'valve', 'coupling', 'elbow', 'tee'],
            'concrete': ['cement', 'rebar', 'mesh', 'form', 'additive', 'sealer'],
            'lumber': ['board', 'plywood', 'beam', 'post', 'stud', 'panel'],
            'paint': ['paint', 'primer', 'stain', 'sealer', 'thinner', 'brush']
        }
    }

def update_consumable(item_id: int, form_data: dict):
    """Update existing consumable with enhanced fields and proper error handling"""
    conn = get_connection()
    c = conn.cursor()

    try:
        # Get current item data first
        c.execute("SELECT quantity, name FROM consumables WHERE id = %s", (item_id,))
        current_data = c.fetchone()
        if not current_data:
            raise ValueError("Item not found")
        
        current_qty, current_name = current_data
        
        # Build specifications from form
        specifications = {}
        for key, value in form_data.items():
            if key.startswith('spec_') and value:
                spec_name = key.replace('spec_', '')
                specifications[spec_name] = value
        
        # Merge with any JSON specifications
        if form_data.get('specifications'):
            try:
                json_specs = json.loads(form_data.get('specifications'))
                specifications.update(json_specs)
            except json.JSONDecodeError:
                pass
        
        # Extract form data with defaults
        name = form_data.get('name', '').strip() or current_name
        category = form_data.get('category', '').strip()
        base_name = form_data.get('base_name', '').strip()
        location = form_data.get('location', '').strip()
        bin_location = form_data.get('bin_location', '').strip()
        quantity = int(form_data.get('quantity', current_qty))
        supplier = form_data.get('supplier', '').strip()
        manufacturer = form_data.get('manufacturer', '').strip()
        part_number = form_data.get('part_number', '').strip()
        supplier_sku = form_data.get('supplier_sku', '').strip()
        unit = form_data.get('unit', 'each').strip()
        
        # Handle optional numeric fields
        def safe_int(value):
            try:
                return int(value) if value else None
            except (ValueError, TypeError):
                return None
        
        def safe_float(value):
            try:
                return float(value) if value else None
            except (ValueError, TypeError):
                return None
        
        reorder_threshold = safe_int(form_data.get('reorder_threshold'))
        min_stock_level = safe_int(form_data.get('min_stock_level'))
        max_stock_level = safe_int(form_data.get('max_stock_level'))
        cost_per_unit = safe_float(form_data.get('cost_per_unit'))
        weight_per_unit = safe_float(form_data.get('weight_per_unit'))
        
        # Handle serial numbers
        serial_numbers_input = form_data.get('serial_numbers', '').strip()
        if serial_numbers_input:
            try:
                # Try JSON first
                serial_numbers = json.loads(serial_numbers_input)
                if not isinstance(serial_numbers, list):
                    serial_numbers = [str(serial_numbers)]
            except json.JSONDecodeError:
                # Fall back to line-separated
                serial_numbers = [s.strip() for s in serial_numbers_input.split('\n') if s.strip()]
        else:
            serial_numbers = []
        
        # Handle boolean fields
        hazmat = form_data.get('hazmat') in ('on', 'true', '1', True)
        requires_certification = form_data.get('requires_certification') in ('on', 'true', '1', True)
        
        # Other text fields
        product_url = form_data.get('product_url', '').strip()
        notes = form_data.get('notes', '').strip()
        barcode = form_data.get('barcode', '').strip()
        
        # Generate normalized name for grouping
        try:
            normalized_name = EnhancedInventoryNameGenerator.generate_separation_key(
                category, base_name, specifications, supplier
            )
        except:
            normalized_name = f"{category}_{base_name}_{supplier}".lower().replace(' ', '_')
        
        # Update the item
        update_sql = """
            UPDATE consumables SET 
                name = %s, category = %s, base_name = %s, specifications = %s,
                location = %s, bin_location = %s, quantity = %s, supplier = %s,
                manufacturer = %s, part_number = %s, supplier_sku = %s,
                serial_numbers = %s, unit = %s, reorder_threshold = %s,
                minimum_stock_level = %s, maximum_stock_level = %s,
                cost_per_unit = %s, weight_per_unit = %s, hazmat = %s,
                requires_certification = %s, product_url = %s, notes = %s,
                barcode = %s, normalized_name = %s, updated_at = CURRENT_TIMESTAMP
            WHERE id = %s
        """
        
        c.execute(update_sql, (
            name, category, base_name, json.dumps(specifications),
            location, bin_location, quantity, supplier, manufacturer,
            part_number, supplier_sku, json.dumps(serial_numbers), unit,
            reorder_threshold, min_stock_level, max_stock_level,
            cost_per_unit, weight_per_unit, hazmat, requires_certification,
            product_url, notes, barcode, normalized_name, item_id
        ))
        
        # If quantity changed, create transaction record
        if current_qty != quantity:
            qty_change = quantity - current_qty
            transaction_type = 'adjustment'
            
            try:
                c.execute("""
                    INSERT INTO inventory_transactions 
                    (consumable_id, transaction_type, quantity_change, quantity_before,
                     quantity_after, performed_by, notes, transaction_date)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                """, (item_id, transaction_type, qty_change, current_qty, quantity,
                      current_user.id if current_user.is_authenticated else None,
                      'Updated via edit form'))
            except Exception as e:
                # If transaction table doesn't exist, log but continue
                logging.warning(f"Could not create transaction record: {str(e)}")
        
        conn.commit()
        
        # Log audit if available
        try:
            if current_user.is_authenticated:
                log_audit(current_user.id, "update_consumable", {
                    "item_id": item_id,
                    "name": name,
                    "quantity_change": quantity - current_qty if current_qty != quantity else 0
                })
        except Exception as e:
            logging.warning(f"Could not log audit: {str(e)}")
        
        # Check if reorder alert needed
        if reorder_threshold and quantity <= reorder_threshold:
            try:
                create_inventory_alert(item_id, 'low_stock', 'warning', 
                                      f'Item {name} updated and is at or below reorder threshold')
            except Exception as e:
                logging.warning(f"Could not create alert: {str(e)}")
        
        logging.info(f"Successfully updated consumable {item_id}: {name}")
        
    except Exception as e:
        conn.rollback()
        logging.error(f"Error updating consumable {item_id}: {str(e)}")
        raise ValueError(f"Database error: {str(e)}")
    finally:
        release_connection(conn)

def check_service_dependencies():
    """Check if all required services and dependencies are available"""
    issues = []

    # Check database connection
    try:
        with db_connection() as conn:
            pass  # Connection test
    except Exception as e:
        issues.append(f"Database connection failed: {str(e)}")
    
    # Check upload folder
    try:
        os.makedirs(UPLOAD_FOLDER, exist_ok=True)
        os.makedirs(os.path.join(UPLOAD_FOLDER, 'inventory_images'), exist_ok=True)
    except Exception as e:
        issues.append(f"Cannot create upload folders: {str(e)}")
    
    # Check required Python packages
    required_packages = ['pandas', 'Pillow', 'psycopg2']
    for package in required_packages:
        try:
            __import__(package)
        except ImportError:
            issues.append(f"Required package '{package}' not found")
    
    return issues

def initialize_inventory_system():
    """Initialize the inventory system and check for issues"""
    logging.info("Initializing inventory system...")
    
    # Check dependencies
    issues = check_service_dependencies()
    if issues:
        logging.error("Inventory system initialization issues:")
        for issue in issues:
            logging.error(f"  - {issue}")
        return False
    
    # Fix database schema
    try:
        fix_database_schema_inconsistencies()
    except Exception as e:
        logging.error(f"Failed to fix database schema: {str(e)}")
        return False
    
    # Initialize templates if none exist
    try:
        with db_connection() as conn:
            c = conn.cursor()
            c.execute("SELECT COUNT(*) FROM item_templates")
            template_count = c.fetchone()[0]

            if template_count == 0:
                logging.info("No templates found, creating default templates...")
                update_item_templates_for_separation()
    except Exception as e:
        logging.warning(f"Could not check templates: {str(e)}")
    
    logging.info("Inventory system initialization completed")
    return True

# Add error handling middleware for inventory routes
def handle_inventory_errors(f):
    """Decorator to handle common inventory errors"""
    def wrapper(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except ValueError as e:
            flash(str(e), 'error')
            return redirect(url_for('inventory.inventory_list'))
        except psycopg2.Error as e:
            logging.error(f"Database error in {f.__name__}: {str(e)}")
            flash("A database error occurred. Please try again.", 'error')
            return redirect(url_for('inventory.inventory_list'))
        except Exception as e:
            logging.error(f"Unexpected error in {f.__name__}: {str(e)}")
            flash("An unexpected error occurred. Please try again.", 'error')
            return redirect(url_for('inventory.inventory_list'))
    
    wrapper.__name__ = f.__name__
    return wrapper

def generate_reorder_report() -> List[Dict]:
    """Generate comprehensive reorder report"""
    conn = get_connection()
    c = conn.cursor()

    try:
        c.execute("""
            SELECT 
                c.id, c.name, c.internal_sku, c.category, c.supplier,
                c.quantity, c.unit, c.reorder_threshold, c.minimum_stock_level,
                si.supplier_name, si.supplier_sku, si.cost_per_unit, 
                si.lead_time_days, si.minimum_order_quantity,
                -- Calculate suggested order quantity
                GREATEST(
                    COALESCE(si.minimum_order_quantity, 1),
                    COALESCE(c.maximum_stock_level, c.reorder_threshold * 2, 100) - c.quantity
                ) as suggested_order_qty,
                -- Calculate estimated cost
                si.cost_per_unit * GREATEST(
                    COALESCE(si.minimum_order_quantity, 1),
                    COALESCE(c.maximum_stock_level, c.reorder_threshold * 2, 100) - c.quantity
                ) as estimated_cost
            FROM consumables c
            LEFT JOIN supplier_items si ON c.id = si.consumable_id AND si.preferred = TRUE
            WHERE c.active = TRUE
            AND c.quantity <= COALESCE(c.minimum_stock_level, c.reorder_threshold, 0)
            ORDER BY 
                CASE 
                    WHEN c.quantity = 0 THEN 1
                    WHEN c.quantity <= c.reorder_threshold * 0.5 THEN 2
                    ELSE 3
                END,
                c.category, c.name
        """)
        
        reorder_items = []
        for row in c.fetchall():
            reorder_items.append({
                'id': row[0],
                'name': row[1],
                'internal_sku': row[2],
                'category': row[3],
                'current_supplier': row[4],
                'current_quantity': row[5],
                'unit': row[6],
                'reorder_threshold': row[7],
                'minimum_stock_level': row[8],
                'preferred_supplier': row[9],
                'supplier_sku': row[10],
                'cost_per_unit': float(row[11]) if row[11] else None,
                'lead_time_days': row[12],
                'minimum_order_quantity': row[13],
                'suggested_order_qty': row[14],
                'estimated_cost': float(row[15]) if row[15] else None,
                'urgency': 'critical' if row[5] == 0 else 'high' if row[5] <= row[7] * 0.5 else 'normal'
            })
        
        return reorder_items
        
    finally:
        release_connection(conn)

def upsert_reorder_suggestions(created_by: int = None) -> int:
    """Create or refresh reorder_requests from current low-stock items. Returns count created."""
    conn = get_connection()
    c = conn.cursor()
    try:
        items = generate_reorder_report()
        created = 0
        for item in items:
            # compute idempotency key per day per item
            c.execute("""
                INSERT INTO reorder_requests (consumable_id, suggested_quantity, unit, reason, preferred_supplier_id, estimated_cost, lead_time_days, status, created_by)
                SELECT %s, %s, %s, %s, si.id, %s, %s, 'suggested', %s
                FROM supplier_items si
                WHERE si.consumable_id = %s AND si.preferred = TRUE
                ON CONFLICT DO NOTHING
            """,
            (
                item['id'],
                int(item['suggested_order_qty'] or 0),
                item['unit'],
                'low_stock',
                item['estimated_cost'],
                item['lead_time_days'],
                created_by,
                item['id'],
            ))
            created += c.rowcount
        conn.commit()
        return created
    finally:
        release_connection(conn)

def track_job_consumable_usage(job_id: int, consumable_id: int, quantity_used: float, notes: str = None):
    """Track consumable usage for a specific job"""
    conn = get_connection()
    c = conn.cursor()

    try:
        # Check current stock
        c.execute("SELECT quantity, name FROM consumables WHERE id = %s AND active = TRUE", (consumable_id,))
        result = c.fetchone()
        
        if not result:
            raise ValueError("Consumable not found")
        
        current_qty, item_name = result
        
        if current_qty < quantity_used:
            raise ValueError(f"Insufficient stock. Available: {current_qty}, Requested: {quantity_used}")
        
        # Deduct from inventory
        new_qty = current_qty - quantity_used
        c.execute("UPDATE consumables SET quantity = %s WHERE id = %s", (new_qty, consumable_id))
        
        # Record job usage
        c.execute("""
            INSERT INTO job_consumables (job_id, consumable_id, quantity_used, date_used, notes)
            VALUES (%s, %s, %s, CURRENT_TIMESTAMP, %s)
        """, (job_id, consumable_id, quantity_used, notes))
        
        # Create transaction record
        c.execute("""
            INSERT INTO inventory_transactions 
            (consumable_id, transaction_type, quantity_change, quantity_before, 
             quantity_after, job_id, performed_by, notes)
            VALUES (%s, 'job_usage', %s, %s, %s, %s, %s, %s)
        """, (
            consumable_id, -quantity_used, current_qty, new_qty, job_id,
            current_user.id if current_user.is_authenticated else None,
            f"Used on job #{job_id}. {notes if notes else ''}"
        ))
        
        conn.commit()
        
        # Check if reorder needed
        c.execute("""
            SELECT reorder_threshold, minimum_stock_level 
            FROM consumables WHERE id = %s
        """, (consumable_id,))
        thresholds = c.fetchone()
        
        if thresholds[0] and new_qty <= thresholds[0]:
            send_low_stock_alert(consumable_id, item_name, new_qty, thresholds[0])
        
        # Log audit
        if current_user.is_authenticated:
            log_audit(current_user.id, "job_consumable_usage", {
                "job_id": job_id,
                "consumable_id": consumable_id,
                "quantity_used": quantity_used,
                "remaining": new_qty
            })
        
    except Exception as e:
        conn.rollback()
        raise
    finally:
        release_connection(conn)


def get_inventory_value_report() -> Dict[str, Any]:
    """Generate inventory valuation report"""
    conn = get_connection()
    c = conn.cursor()
    
    try:
        # Total inventory value
        c.execute("""
            SELECT 
                SUM(quantity * cost_per_unit) as total_value,
                COUNT(*) as total_items,
                SUM(quantity) as total_units
            FROM consumables
            WHERE active = TRUE AND cost_per_unit IS NOT NULL
        """)
        
        totals = c.fetchone()
        
        # Value by category
        c.execute("""
            SELECT 
                category,
                COUNT(*) as item_count,
                SUM(quantity) as total_quantity,
                SUM(quantity * cost_per_unit) as category_value
            FROM consumables
            WHERE active = TRUE AND cost_per_unit IS NOT NULL
            GROUP BY category
            ORDER BY category_value DESC
        """)
        
        category_breakdown = []
        for row in c.fetchall():
            category_breakdown.append({
                'category': row[0],
                'item_count': row[1],
                'total_quantity': row[2],
                'value': float(row[3]) if row[3] else 0
            })
        
        # Top value items
        c.execute("""
            SELECT 
                name, internal_sku, quantity, cost_per_unit,
                (quantity * cost_per_unit) as total_value
            FROM consumables
            WHERE active = TRUE AND cost_per_unit IS NOT NULL
            ORDER BY total_value DESC
            LIMIT 20
        """)
        
        top_value_items = []
        for row in c.fetchall():
            top_value_items.append({
                'name': row[0],
                'sku': row[1],
                'quantity': row[2],
                'unit_cost': float(row[3]),
                'total_value': float(row[4])
            })
        
        return {
            'total_value': float(totals[0]) if totals[0] else 0,
            'total_items': totals[1],
            'total_units': totals[2],
            'category_breakdown': category_breakdown,
            'top_value_items': top_value_items,
            'report_date': datetime.now().isoformat()
        }
        
    finally:
        release_connection(conn)

# Maintain backward compatibility with existing functions
def get_inventory_item(item_id):
    """Backward compatible function"""
    return get_enhanced_inventory_item(item_id)

def add_consumable(form_data):
    """Backward compatible function"""
    return create_enhanced_consumable(form_data)

def add_consumable_with_grouping(form_data):
    """Enhanced grouping check before adding item"""
    category = form_data.get('category', '').strip()
    base_name = form_data.get('base_name', '').strip()
    supplier = form_data.get('supplier', '').strip()
    
    # Build specifications
    specifications = {}
    for key, value in form_data.items():
        if key.startswith('spec_') and value:
            spec_name = key.replace('spec_', '')
            specifications[spec_name] = value
    
    # Check for similar items
    similar_items = find_similar_items(category, base_name, specifications, exclude_supplier=supplier)
    
    if similar_items:
        return {
            'action': 'confirm_grouping',
            'similar_items': similar_items,
            'form_data': form_data
        }
    else:
        item_id = create_enhanced_consumable(form_data)
        return {
            'action': 'created',
            'item_id': item_id
        }

def find_similar_items(category, base_name, specifications, exclude_supplier=None):
    """Find items with same specifications but different suppliers"""
    conn = get_connection()
    c = conn.cursor()
    
    # Create a normalized comparison key
    spec_key = json.dumps(specifications, sort_keys=True)
    
    query = """
        SELECT id, name, supplier, supplier_sku, quantity, unit, location, 
               cost_per_unit, image_path, specifications
        FROM consumables 
        WHERE category = %s AND base_name = %s AND active = TRUE
    """
    params = [category, base_name]
    
    if exclude_supplier:
        query += " AND supplier != %s"
        params.append(exclude_supplier)
    
    c.execute(query, params)
    results = c.fetchall()
    release_connection(conn)
    
    similar = []
    for row in results:
        item_specs = json.loads(row[9]) if row[9] else {}
        
        # Check if specifications match (ignoring supplier-specific fields)
        specs_match = True
        for key in ['diameter', 'length', 'material', 'grade', 'gauge', 'conductor_material']:
            if key in specifications or key in item_specs:
                if specifications.get(key) != item_specs.get(key):
                    specs_match = False
                    break
        
        if specs_match:
            similar.append({
                'id': row[0],
                'name': row[1],
                'supplier': row[2],
                'supplier_sku': row[3],
                'quantity': row[4],
                'unit': row[5],
                'location': row[6],
                'cost_per_unit': float(row[7]) if row[7] else None,
                'image_path': row[8],
                'specifications': item_specs
            })
    
    return similar

def get_all_consumables(search_query=None, sort_by='name', sort_order='asc', 
                        filter_status=None, view_mode='individual', category=None):
    """Get inventory with various filters and views"""
    conn = get_connection()
    c = conn.cursor()
    
    base_query = """
        SELECT 
            c.id, c.name, c.category, c.base_name, c.location, c.bin_location,
            c.quantity, c.supplier, c.manufacturer, c.supplier_sku, c.internal_sku,
            c.unit, c.reorder_threshold, c.minimum_stock_level, c.maximum_stock_level,
            c.cost_per_unit, c.serial_numbers, c.specifications, c.image_path,
            c.hazmat, c.requires_certification,
            CASE 
                WHEN c.quantity = 0 THEN 'out_of_stock'
                WHEN c.quantity <= COALESCE(c.minimum_stock_level, c.reorder_threshold, 0) THEN 'low_stock'
                WHEN c.maximum_stock_level IS NOT NULL AND c.quantity > c.maximum_stock_level THEN 'overstocked'
                ELSE 'in_stock'
            END as stock_status
        FROM consumables c
        WHERE c.active = TRUE
    """
    
    params = []
    where_conditions = []
    
    # Add search conditions
    if search_query:
        search_conditions = [
            "c.name ILIKE %s",
            "c.category ILIKE %s",
            "c.supplier ILIKE %s",
            "c.manufacturer ILIKE %s",
            "c.supplier_sku ILIKE %s",
            "c.internal_sku ILIKE %s",
            "c.location ILIKE %s"
        ]
        where_conditions.append(f"({' OR '.join(search_conditions)})")
        search_param = f"%{search_query}%"
        params.extend([search_param] * len(search_conditions))
    
    # Category filter
    if category:
        where_conditions.append("c.category = %s")
        params.append(category)
    
    # Status filter
    if filter_status == 'low_stock':
        where_conditions.append("c.quantity <= COALESCE(c.minimum_stock_level, c.reorder_threshold, 0)")
    elif filter_status == 'out_of_stock':
        where_conditions.append("c.quantity = 0")
    elif filter_status == 'in_stock':
        where_conditions.append("c.quantity > 0")
    elif filter_status == 'overstocked':
        where_conditions.append("c.maximum_stock_level IS NOT NULL AND c.quantity > c.maximum_stock_level")
    
    # Add WHERE conditions
    if where_conditions:
        base_query += " AND " + " AND ".join(where_conditions)
    
    # Sorting
    sort_mapping = {
        'name': 'c.name',
        'category': 'c.category',
        'supplier': 'c.supplier',
        'quantity': 'c.quantity',
        'internal_sku': 'c.internal_sku',
        'cost': 'c.cost_per_unit',
        'location': 'c.location'
    }
    
    if sort_by in sort_mapping:
        sort_column = sort_mapping[sort_by]
        sort_direction = 'DESC' if sort_order == 'desc' else 'ASC'
        base_query += f" ORDER BY {sort_column} {sort_direction}"
    else:
        base_query += " ORDER BY c.category, c.name"
    
    c.execute(base_query, params)
    rows = c.fetchall()
    release_connection(conn)
    
    items = []
    for row in rows:
        item = {
            'id': row[0],
            'name': row[1],
            'category': row[2],
            'base_name': row[3],
            'location': row[4] or '',
            'bin_location': row[5] or '',
            'quantity': row[6],
            'supplier': row[7] or '',
            'manufacturer': row[8] or '',
            'supplier_sku': row[9] or '',
            'internal_sku': row[10] or '',
            'unit': row[11] or 'each',
            'reorder_threshold': row[12],
            'minimum_stock_level': row[13],
            'maximum_stock_level': row[14],
            'cost_per_unit': float(row[15]) if row[15] else None,
            'serial_numbers': json.loads(row[16]) if row[16] else [],
            'specifications': json.loads(row[17]) if row[17] else {},
            'image_path': row[18],
            'hazmat': row[19],
            'requires_certification': row[20],
            'stock_status': row[21],
            'needs_reorder': row[6] <= (row[13] or row[12] or 0) if (row[13] or row[12]) else False,
            'is_overstocked': row[14] and row[6] > row[14] if row[14] else False,
            'stock_value': (row[6] * float(row[15])) if row[15] else None
        }
        items.append(item)
    
    return items

def get_inventory_stats():
    """Get comprehensive inventory statistics"""
    conn = get_connection()
    c = conn.cursor()
    
    stats = {}
    
    # Basic counts
    c.execute("SELECT COUNT(*) FROM consumables WHERE active = TRUE")
    stats['total_items'] = c.fetchone()[0]
    
    c.execute("SELECT COUNT(DISTINCT normalized_name) FROM consumables WHERE active = TRUE")
    stats['unique_item_types'] = c.fetchone()[0]
    
    c.execute("SELECT COUNT(*) FROM consumables WHERE quantity = 0 AND active = TRUE")
    stats['out_of_stock'] = c.fetchone()[0]
    
    c.execute("""
        SELECT COUNT(*) FROM consumables 
        WHERE quantity <= COALESCE(minimum_stock_level, reorder_threshold, 0) 
        AND quantity > 0 AND active = TRUE
    """)
    stats['low_stock'] = c.fetchone()[0]
    
    stats['in_stock'] = stats['total_items'] - stats['out_of_stock']
    
    # Value statistics
    c.execute("""
        SELECT SUM(quantity * cost_per_unit) 
        FROM consumables 
        WHERE active = TRUE AND cost_per_unit IS NOT NULL
    """)
    total_value = c.fetchone()[0]
    stats['total_value'] = float(total_value) if total_value else 0
    
    # Category breakdown
    c.execute("""
        SELECT category, COUNT(*), SUM(quantity) 
        FROM consumables 
        WHERE active = TRUE 
        GROUP BY category 
        ORDER BY COUNT(*) DESC
    """)
    stats['categories'] = []
    for row in c.fetchall():
        stats['categories'].append({
            'name': row[0],
            'count': row[1],
            'total_quantity': row[2]
        })
    
    release_connection(conn)
    return stats

def upload_item_image(item_id: int, file) -> str:
    """Upload and process item image"""
    if not file or not allowed_file(file.filename, {'png', 'jpg', 'jpeg', 'gif'}):
        raise ValueError("Invalid file type")
    
    # Create upload directory if it doesn't exist
    upload_dir = os.path.join(UPLOAD_FOLDER, 'inventory_images')
    os.makedirs(upload_dir, exist_ok=True)
    
    # Generate secure filename
    filename = secure_filename(file.filename)
    name, ext = os.path.splitext(filename)
    filename = f"item_{item_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}{ext}"
    
    filepath = os.path.join(upload_dir, filename)
    
    # Save and resize image
    try:
        image = Image.open(file)
        # Resize to maximum 800x600 while maintaining aspect ratio
        image.thumbnail((800, 600), Image.Resampling.LANCZOS)
        image.save(filepath, optimize=True, quality=85)
        
        # Update database with image path
        relative_path = os.path.join('inventory_images', filename).replace('\\', '/')
        
        conn = get_connection()
        c = conn.cursor()
        c.execute("UPDATE consumables SET image_path = %s, image_filename = %s WHERE id = %s", 
                 (relative_path, filename, item_id))
        conn.commit()
        release_connection(conn)
        
        return relative_path
        
    except Exception as e:
        if os.path.exists(filepath):
            os.remove(filepath)
        raise ValueError(f"Error processing image: {str(e)}")

def allowed_file(filename: str, allowed_extensions: set) -> bool:
    """Check if file extension is allowed"""
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in allowed_extensions

def update_consumable(item_id, form_data):
    """Update existing consumable with enhanced fields"""
    conn = get_connection()
    c = conn.cursor()
    
    try:
        # Build specifications from form
        specifications = {}
        for key, value in form_data.items():
            if key.startswith('spec_') and value:
                spec_name = key.replace('spec_', '')
                specifications[spec_name] = value
        
        # Merge with any JSON specifications
        if form_data.get('specifications'):
            try:
                json_specs = json.loads(form_data.get('specifications'))
                specifications.update(json_specs)
            except json.JSONDecodeError:
                pass
        
        # Extract form data
        name = form_data.get('name', '').strip()
        category = form_data.get('category', '').strip()
        base_name = form_data.get('base_name', '').strip()
        location = form_data.get('location', '').strip()
        bin_location = form_data.get('bin_location', '').strip()
        quantity = int(form_data.get('quantity', 0))
        supplier = form_data.get('supplier', '').strip()
        manufacturer = form_data.get('manufacturer', '').strip()
        part_number = form_data.get('part_number', '').strip()
        supplier_sku = form_data.get('supplier_sku', '').strip()
        unit = form_data.get('unit', 'each').strip()
        reorder_threshold = int(form_data.get('reorder_threshold', 0)) if form_data.get('reorder_threshold') else None
        min_stock_level = int(form_data.get('min_stock_level', 0)) if form_data.get('min_stock_level') else None
        max_stock_level = int(form_data.get('max_stock_level', 0)) if form_data.get('max_stock_level') else None
        cost_per_unit = float(form_data.get('cost_per_unit', 0)) if form_data.get('cost_per_unit') else None
        
        # Handle serial numbers
        serial_numbers_input = form_data.get('serial_numbers', '').strip()
        if serial_numbers_input:
            try:
                serial_numbers = json.loads(serial_numbers_input)
            except json.JSONDecodeError:
                serial_numbers = [s.strip() for s in serial_numbers_input.split('\n') if s.strip()]
        else:
            serial_numbers = []
        
        # Generate normalized name
        normalized_name = EnhancedInventoryNameGenerator.generate_separation_key(
            category, base_name, specifications, supplier
        )
        
        # Get current quantity for transaction tracking
        c.execute("SELECT quantity FROM consumables WHERE id = %s", (item_id,))
        current_qty = c.fetchone()[0]
        
        # Update consumable
        c.execute("""
            UPDATE consumables SET 
                name = %s, category = %s, base_name = %s, specifications = %s,
                location = %s, bin_location = %s, quantity = %s, supplier = %s,
                manufacturer = %s, part_number = %s, supplier_sku = %s,
                serial_numbers = %s, unit = %s, reorder_threshold = %s,
                minimum_stock_level = %s, maximum_stock_level = %s,
                cost_per_unit = %s, normalized_name = %s,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = %s
        """, (name, category, base_name, json.dumps(specifications),
              location, bin_location, quantity, supplier, manufacturer,
              part_number, supplier_sku, json.dumps(serial_numbers), unit,
              reorder_threshold, min_stock_level, max_stock_level,
              cost_per_unit, normalized_name, item_id))
        
        # If quantity changed, create transaction record
        if current_qty != quantity:
            qty_change = quantity - current_qty
            c.execute("""
                INSERT INTO inventory_transactions 
                (consumable_id, transaction_type, quantity_change, quantity_before,
                 quantity_after, performed_by, notes, transaction_date)
                VALUES (%s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
            """, (item_id, 'adjustment', qty_change, current_qty, quantity,
                  current_user.id if current_user.is_authenticated else None,
                  'Manual adjustment via edit'))
        
        # Update supplier items
        if supplier:
            supplier_data = {
                'supplier_name': supplier,
                'supplier_sku': supplier_sku,
                'supplier_part_number': part_number,
                'cost_per_unit': cost_per_unit
            }
            SupplierManagement.add_supplier_item(item_id, supplier_data)
        
        conn.commit()
        
        if current_user.is_authenticated:
            log_audit(current_user.id, "update_consumable", {
                "item_id": item_id,
                "name": name,
                "quantity_change": quantity - current_qty
            })
        
        # Check if reorder alert needed
        if reorder_threshold and quantity <= reorder_threshold:
            send_low_stock_alert(item_id, name, quantity, reorder_threshold)
        
    except Exception as e:
        conn.rollback()
        raise ValueError(f"Database error: {str(e)}")
    finally:
        release_connection(conn)

def delete_consumable(item_id):
    """Soft delete a consumable"""
    conn = get_connection()
    c = conn.cursor()
    
    try:
        c.execute("UPDATE consumables SET active = FALSE WHERE id = %s", (item_id,))
        conn.commit()
        
        if current_user.is_authenticated:
            log_audit(current_user.id, "delete_consumable", {"item_id": item_id})
        
    finally:
        release_connection(conn)

def merge_with_existing_item(existing_item_id, form_data):
    """Merge new quantity with existing item"""
    conn = get_connection()
    c = conn.cursor()
    
    try:
        additional_quantity = int(form_data.get('quantity', 0))
        
        # Get current quantity
        c.execute("SELECT quantity, name FROM consumables WHERE id = %s", (existing_item_id,))
        current_qty, name = c.fetchone()
        new_qty = current_qty + additional_quantity
        
        # Update quantity
        c.execute("UPDATE consumables SET quantity = %s WHERE id = %s", (new_qty, existing_item_id))
        
        # Add supplier if different
        supplier = form_data.get('supplier', '').strip()
        if supplier:
            supplier_data = {
                'supplier_name': supplier,
                'supplier_sku': form_data.get('supplier_sku', ''),
                'cost_per_unit': float(form_data.get('cost_per_unit', 0)) if form_data.get('cost_per_unit') else None
            }
            SupplierManagement.add_supplier_item(existing_item_id, supplier_data)
        
        # Create transaction record
        c.execute("""
            INSERT INTO inventory_transactions 
            (consumable_id, transaction_type, quantity_change, quantity_before, quantity_after, performed_by)
            VALUES (%s, 'merge', %s, %s, %s, %s)
        """, (existing_item_id, additional_quantity, current_qty, new_qty, 
              current_user.id if current_user.is_authenticated else None))
        
        conn.commit()
        
        if current_user.is_authenticated:
            log_audit(current_user.id, "merge_consumable", {
                "item_id": existing_item_id,
                "added_quantity": additional_quantity
            })
        
        return existing_item_id
        
    finally:
        release_connection(conn)
        
def create_inventory_alert(consumable_id: int, alert_type: str, alert_level: str, message: str):
    """Create an inventory alert"""
    conn = get_connection()
    c = conn.cursor()
    
    try:
        c.execute("""
            INSERT INTO inventory_alerts (consumable_id, alert_type, alert_level, message)
            VALUES (%s, %s, %s, %s)
        """, (consumable_id, alert_type, alert_level, message))
        conn.commit()
    except Exception as e:
        logging.error(f"Error creating inventory alert: {str(e)}")
    finally:
        release_connection(conn)        

def create_consumable(form_data):
    """Wrapper for backward compatibility"""
    return create_enhanced_consumable(form_data)

## Removed duplicate static get_item_templates() (DB-backed + fallback version exists above)

## Removed duplicate static get_categories_and_base_names() (DB-backed + fallback version exists above)

def generate_supply_list_for_job(job_id, requirements):
    """Generate supply list based on job requirements"""
    # Implementation for job supply prediction
    pass

# New helper functions referenced by other modules
def get_inventory_summary() -> list:
    """Return a simple textual summary for tools route."""
    conn = get_connection()
    c = conn.cursor()
    try:
        c.execute("SELECT COUNT(*), COALESCE(SUM(quantity), 0) FROM consumables WHERE active = TRUE")
        total_items, total_units = c.fetchone()
        c.execute("SELECT COUNT(*) FROM consumables WHERE active = TRUE AND quantity = 0")
        out_of_stock = c.fetchone()[0]
        c.execute("SELECT COUNT(*) FROM consumables WHERE active = TRUE AND quantity <= COALESCE(minimum_stock_level, reorder_threshold, 0)")
        low_stock = c.fetchone()[0]
        return [
            f"Items: {total_items}",
            f"Units: {total_units}",
            f"Out of Stock: {out_of_stock}",
            f"Low Stock: {low_stock}"
        ]
    finally:
        release_connection(conn)

def deduct_quantity_on_assignment(item_id: int, quantity: int, user_id: Any = None) -> None:
    """Deduct quantity for an inventory item and record transaction."""
    conn = get_connection()
    c = conn.cursor()
    try:
        c.execute("SELECT quantity, name FROM consumables WHERE id = %s AND active = TRUE", (item_id,))
        row = c.fetchone()
        if not row:
            raise ValueError("Item not found")
        current_qty, name = row
        if quantity <= 0:
            raise ValueError("Quantity must be positive")
        if current_qty < quantity:
            raise ValueError("Insufficient stock")
        new_qty = current_qty - quantity
        c.execute("UPDATE consumables SET quantity = %s WHERE id = %s", (new_qty, item_id))
        c.execute(
            """
            INSERT INTO inventory_transactions
            (consumable_id, transaction_type, quantity_change, quantity_before, quantity_after, performed_by, notes)
            VALUES (%s, 'assignment', %s, %s, %s, %s, %s)
            """,
            (item_id, -quantity, current_qty, new_qty, user_id if user_id else None, 'Deducted via assignment')
        )
        conn.commit()
    finally:
        release_connection(conn)


# ============================================================================
# ENHANCED INVENTORY METRICS & ANALYTICS
# ============================================================================

class InventoryMetrics:
    """Comprehensive inventory metrics and KPI calculations"""

    @staticmethod
    def get_dashboard_metrics() -> Dict[str, Any]:
        """Get all key metrics for the inventory dashboard"""
        conn = get_connection()
        c = conn.cursor()

        try:
            metrics = {}

            # Basic counts
            c.execute("""
                SELECT
                    COUNT(*) as total_items,
                    COUNT(*) FILTER (WHERE quantity > 0) as in_stock,
                    COUNT(*) FILTER (WHERE quantity = 0) as out_of_stock,
                    COUNT(*) FILTER (WHERE quantity <= COALESCE(minimum_stock_level, reorder_threshold, 0) AND quantity > 0) as low_stock,
                    COUNT(*) FILTER (WHERE maximum_stock_level IS NOT NULL AND quantity > maximum_stock_level) as overstocked,
                    COUNT(DISTINCT category) as categories,
                    COUNT(DISTINCT supplier) as suppliers
                FROM consumables WHERE active = TRUE
            """)
            row = c.fetchone()
            metrics['total_items'] = row[0]
            metrics['in_stock'] = row[1]
            metrics['out_of_stock'] = row[2]
            metrics['low_stock'] = row[3]
            metrics['overstocked'] = row[4]
            metrics['total_categories'] = row[5]
            metrics['total_suppliers'] = row[6]

            # Value metrics
            c.execute("""
                SELECT
                    COALESCE(SUM(quantity * cost_per_unit), 0) as total_value,
                    COALESCE(AVG(cost_per_unit), 0) as avg_unit_cost,
                    COALESCE(MAX(quantity * cost_per_unit), 0) as max_item_value
                FROM consumables
                WHERE active = TRUE AND cost_per_unit IS NOT NULL
            """)
            row = c.fetchone()
            metrics['total_inventory_value'] = float(row[0])
            metrics['avg_unit_cost'] = float(row[1])
            metrics['max_item_value'] = float(row[2])

            # Spending metrics (from transactions)
            c.execute("""
                SELECT
                    COALESCE(SUM(CASE WHEN transaction_type = 'purchase' THEN ABS(total_cost) ELSE 0 END), 0) as total_spend,
                    COALESCE(SUM(CASE WHEN transaction_type = 'purchase'
                        AND transaction_date >= DATE_TRUNC('month', CURRENT_DATE)
                        THEN ABS(total_cost) ELSE 0 END), 0) as spend_this_month,
                    COALESCE(SUM(CASE WHEN transaction_type = 'purchase'
                        AND transaction_date >= DATE_TRUNC('quarter', CURRENT_DATE)
                        THEN ABS(total_cost) ELSE 0 END), 0) as spend_this_quarter,
                    COALESCE(SUM(CASE WHEN transaction_type = 'purchase'
                        AND transaction_date >= DATE_TRUNC('year', CURRENT_DATE)
                        THEN ABS(total_cost) ELSE 0 END), 0) as spend_this_year
                FROM inventory_transactions
            """)
            row = c.fetchone()
            metrics['total_spend'] = float(row[0])
            metrics['spend_this_month'] = float(row[1])
            metrics['spend_this_quarter'] = float(row[2])
            metrics['spend_this_year'] = float(row[3])

            # Order metrics
            c.execute("""
                SELECT
                    COUNT(*) FILTER (WHERE status = 'shipped' OR status = 'in_transit') as orders_in_transit,
                    COUNT(*) FILTER (WHERE expected_delivery_date = CURRENT_DATE AND status != 'delivered') as arriving_today,
                    COUNT(*) FILTER (WHERE expected_delivery_date = CURRENT_DATE + 1 AND status != 'delivered') as arriving_tomorrow,
                    COUNT(*) FILTER (WHERE expected_delivery_date BETWEEN CURRENT_DATE AND CURRENT_DATE + 7 AND status != 'delivered') as arriving_this_week,
                    COUNT(*) FILTER (WHERE status = 'delivered' AND actual_delivery_date >= DATE_TRUNC('month', CURRENT_DATE)) as delivered_this_month
                FROM purchase_orders
            """)
            row = c.fetchone()
            metrics['orders_in_transit'] = row[0] or 0
            metrics['arriving_today'] = row[1] or 0
            metrics['arriving_tomorrow'] = row[2] or 0
            metrics['arriving_this_week'] = row[3] or 0
            metrics['delivered_this_month'] = row[4] or 0

            # Calculate KPIs
            if metrics['total_items'] > 0:
                metrics['fill_rate'] = round((metrics['in_stock'] / metrics['total_items']) * 100, 1)
                metrics['stockout_rate'] = round((metrics['out_of_stock'] / metrics['total_items']) * 100, 1)
            else:
                metrics['fill_rate'] = 0
                metrics['stockout_rate'] = 0

            # Inventory turnover (last 12 months)
            c.execute("""
                SELECT
                    COALESCE(SUM(ABS(quantity_change)), 0) as total_usage
                FROM inventory_transactions
                WHERE transaction_type IN ('usage', 'job_usage')
                AND transaction_date >= CURRENT_DATE - INTERVAL '12 months'
            """)
            total_usage = c.fetchone()[0] or 0

            c.execute("""
                SELECT COALESCE(AVG(quantity), 0)
                FROM consumables WHERE active = TRUE
            """)
            avg_inventory = c.fetchone()[0] or 1

            metrics['turnover_ratio'] = round(float(total_usage) / float(avg_inventory), 2) if avg_inventory > 0 else 0

            # Days of inventory on hand
            if total_usage > 0:
                daily_usage = total_usage / 365
                c.execute("SELECT COALESCE(SUM(quantity), 0) FROM consumables WHERE active = TRUE")
                current_inventory = c.fetchone()[0] or 0
                metrics['days_on_hand'] = round(current_inventory / daily_usage, 0) if daily_usage > 0 else 999
            else:
                metrics['days_on_hand'] = 999

            return metrics

        finally:
            release_connection(conn)

    @staticmethod
    def get_spend_by_period(period: str = 'monthly', months: int = 12) -> List[Dict]:
        """Get spending breakdown by time period"""
        conn = get_connection()
        c = conn.cursor()

        try:
            if period == 'monthly':
                c.execute("""
                    SELECT
                        DATE_TRUNC('month', transaction_date) as period,
                        COALESCE(SUM(ABS(total_cost)), 0) as spend,
                        COUNT(*) as transaction_count
                    FROM inventory_transactions
                    WHERE transaction_type = 'purchase'
                    AND transaction_date >= CURRENT_DATE - INTERVAL '%s months'
                    GROUP BY DATE_TRUNC('month', transaction_date)
                    ORDER BY period DESC
                """ % months)
            elif period == 'weekly':
                c.execute("""
                    SELECT
                        DATE_TRUNC('week', transaction_date) as period,
                        COALESCE(SUM(ABS(total_cost)), 0) as spend,
                        COUNT(*) as transaction_count
                    FROM inventory_transactions
                    WHERE transaction_type = 'purchase'
                    AND transaction_date >= CURRENT_DATE - INTERVAL '%s weeks'
                    GROUP BY DATE_TRUNC('week', transaction_date)
                    ORDER BY period DESC
                """ % (months * 4))
            else:  # daily
                c.execute("""
                    SELECT
                        DATE_TRUNC('day', transaction_date) as period,
                        COALESCE(SUM(ABS(total_cost)), 0) as spend,
                        COUNT(*) as transaction_count
                    FROM inventory_transactions
                    WHERE transaction_type = 'purchase'
                    AND transaction_date >= CURRENT_DATE - INTERVAL '%s days'
                    GROUP BY DATE_TRUNC('day', transaction_date)
                    ORDER BY period DESC
                """ % (months * 30))

            results = []
            for row in c.fetchall():
                results.append({
                    'period': row[0].isoformat() if row[0] else None,
                    'spend': float(row[1]),
                    'transaction_count': row[2]
                })
            return results

        finally:
            release_connection(conn)

    @staticmethod
    def get_spend_by_category() -> List[Dict]:
        """Get spending breakdown by category"""
        conn = get_connection()
        c = conn.cursor()

        try:
            c.execute("""
                SELECT
                    c.category,
                    COALESCE(SUM(c.quantity * c.cost_per_unit), 0) as current_value,
                    COALESCE(SUM(ABS(it.total_cost)) FILTER (WHERE it.transaction_type = 'purchase'), 0) as total_spend,
                    COUNT(DISTINCT c.id) as item_count
                FROM consumables c
                LEFT JOIN inventory_transactions it ON c.id = it.consumable_id
                WHERE c.active = TRUE
                GROUP BY c.category
                ORDER BY current_value DESC
            """)

            results = []
            for row in c.fetchall():
                results.append({
                    'category': row[0] or 'Uncategorized',
                    'current_value': float(row[1]),
                    'total_spend': float(row[2]),
                    'item_count': row[3]
                })
            return results

        finally:
            release_connection(conn)

    @staticmethod
    def get_spend_by_supplier(limit: int = 10) -> List[Dict]:
        """Get spending breakdown by supplier"""
        conn = get_connection()
        c = conn.cursor()

        try:
            c.execute("""
                SELECT
                    s.name,
                    s.id,
                    COALESCE(SUM(po.total_amount), 0) as total_orders,
                    COUNT(DISTINCT po.id) as order_count,
                    AVG(CASE WHEN po.actual_delivery_date IS NOT NULL AND po.order_date IS NOT NULL
                        THEN EXTRACT(DAY FROM (po.actual_delivery_date::timestamp - po.order_date::timestamp))
                        ELSE NULL END) as avg_lead_time
                FROM suppliers s
                LEFT JOIN purchase_orders po ON s.id = po.supplier_id
                WHERE s.active = TRUE
                GROUP BY s.id, s.name
                ORDER BY total_orders DESC
                LIMIT %s
            """, (limit,))

            results = []
            for row in c.fetchall():
                results.append({
                    'supplier_name': row[0],
                    'supplier_id': row[1],
                    'total_orders': float(row[2]),
                    'order_count': row[3],
                    'avg_lead_time': round(float(row[4]), 1) if row[4] else None
                })
            return results

        finally:
            release_connection(conn)

    @staticmethod
    def get_top_moving_items(limit: int = 10, direction: str = 'fast') -> List[Dict]:
        """Get fastest or slowest moving inventory items"""
        conn = get_connection()
        c = conn.cursor()

        try:
            order = 'DESC' if direction == 'fast' else 'ASC'
            c.execute(f"""
                SELECT
                    c.id, c.name, c.category, c.quantity, c.unit,
                    COALESCE(SUM(ABS(it.quantity_change)) FILTER (
                        WHERE it.transaction_type IN ('usage', 'job_usage')
                        AND it.transaction_date >= CURRENT_DATE - INTERVAL '90 days'
                    ), 0) as usage_90_days,
                    COALESCE(c.cost_per_unit, 0) as unit_cost
                FROM consumables c
                LEFT JOIN inventory_transactions it ON c.id = it.consumable_id
                WHERE c.active = TRUE
                GROUP BY c.id
                ORDER BY usage_90_days {order}
                LIMIT %s
            """, (limit,))

            results = []
            for row in c.fetchall():
                results.append({
                    'id': row[0],
                    'name': row[1],
                    'category': row[2],
                    'quantity': row[3],
                    'unit': row[4],
                    'usage_90_days': float(row[5]),
                    'unit_cost': float(row[6])
                })
            return results

        finally:
            release_connection(conn)

    @staticmethod
    def get_dead_stock(days: int = 90) -> List[Dict]:
        """Get items with no movement in specified days"""
        conn = get_connection()
        c = conn.cursor()

        try:
            c.execute("""
                SELECT
                    c.id, c.name, c.category, c.quantity, c.unit,
                    c.cost_per_unit,
                    c.quantity * COALESCE(c.cost_per_unit, 0) as dead_value,
                    MAX(it.transaction_date) as last_movement
                FROM consumables c
                LEFT JOIN inventory_transactions it ON c.id = it.consumable_id
                WHERE c.active = TRUE AND c.quantity > 0
                GROUP BY c.id
                HAVING MAX(it.transaction_date) IS NULL
                    OR MAX(it.transaction_date) < CURRENT_DATE - INTERVAL '%s days'
                ORDER BY dead_value DESC
            """ % days)

            results = []
            for row in c.fetchall():
                results.append({
                    'id': row[0],
                    'name': row[1],
                    'category': row[2],
                    'quantity': row[3],
                    'unit': row[4],
                    'unit_cost': float(row[5]) if row[5] else 0,
                    'dead_value': float(row[6]) if row[6] else 0,
                    'last_movement': row[7]
                })
            return results

        finally:
            release_connection(conn)


# ============================================================================
# ENHANCED SUPPLIER MANAGEMENT
# ============================================================================

class EnhancedSupplierManagement:
    """Enhanced supplier management with full details and tracking"""

    @staticmethod
    def get_supplier_details(supplier_id: int) -> Optional[Dict]:
        """Get comprehensive supplier details"""
        conn = get_connection()
        c = conn.cursor()

        try:
            c.execute("""
                SELECT
                    s.id, s.name, s.contact_person, s.email, s.phone,
                    s.address, s.city, s.state, s.zip_code, s.country,
                    s.website, s.account_number, s.payment_terms, s.shipping_terms,
                    s.preferred, s.active, s.notes,
                    s.api_endpoint, s.api_key, s.api_auth_type,
                    s.created_at, s.updated_at
                FROM suppliers s
                WHERE s.id = %s
            """, (supplier_id,))

            row = c.fetchone()
            if not row:
                return None

            supplier = {
                'id': row[0],
                'name': row[1],
                'contact_person': row[2],
                'email': row[3],
                'phone': row[4],
                'address': row[5],
                'city': row[6],
                'state': row[7],
                'zip_code': row[8],
                'country': row[9],
                'website': row[10],
                'account_number': row[11],
                'payment_terms': row[12],
                'shipping_terms': row[13],
                'preferred': row[14],
                'active': row[15],
                'notes': row[16],
                'api_endpoint': row[17],
                'has_api': bool(row[17]),
                'api_auth_type': row[19],
                'created_at': row[20],
                'updated_at': row[21]
            }

            # Get contacts
            c.execute("""
                SELECT id, contact_type, name, title, email, phone, mobile,
                       preferred_contact_method, notes, active
                FROM supplier_contacts
                WHERE supplier_id = %s
                ORDER BY contact_type
            """, (supplier_id,))

            supplier['contacts'] = []
            for row in c.fetchall():
                supplier['contacts'].append({
                    'id': row[0],
                    'contact_type': row[1],
                    'name': row[2],
                    'title': row[3],
                    'email': row[4],
                    'phone': row[5],
                    'mobile': row[6],
                    'preferred_contact_method': row[7],
                    'notes': row[8],
                    'active': row[9]
                })

            # Get performance metrics
            c.execute("""
                SELECT
                    COUNT(*) as total_orders,
                    COUNT(*) FILTER (WHERE status = 'delivered') as completed_orders,
                    AVG(CASE WHEN actual_delivery_date IS NOT NULL AND order_date IS NOT NULL
                        THEN EXTRACT(DAY FROM (actual_delivery_date::timestamp - order_date::timestamp))
                        ELSE NULL END) as avg_lead_time,
                    AVG(CASE WHEN actual_delivery_date IS NOT NULL AND expected_delivery_date IS NOT NULL
                        THEN EXTRACT(DAY FROM (actual_delivery_date::timestamp - expected_delivery_date::timestamp))
                        ELSE NULL END) as avg_delay,
                    SUM(total_amount) as total_spend
                FROM purchase_orders
                WHERE supplier_id = %s
            """, (supplier_id,))

            row = c.fetchone()
            supplier['metrics'] = {
                'total_orders': row[0] or 0,
                'completed_orders': row[1] or 0,
                'avg_lead_time': round(float(row[2]), 1) if row[2] else None,
                'avg_delay': round(float(row[3]), 1) if row[3] else 0,
                'total_spend': float(row[4]) if row[4] else 0,
                'on_time_rate': round((row[1] / row[0] * 100), 1) if row[0] and row[0] > 0 else 0
            }

            # Get items supplied
            c.execute("""
                SELECT
                    si.id, si.consumable_id, c.name, si.supplier_sku,
                    si.cost_per_unit, si.lead_time_days, si.preferred
                FROM supplier_items si
                JOIN consumables c ON si.consumable_id = c.id
                WHERE si.supplier_id = %s
                ORDER BY si.preferred DESC, c.name
            """, (supplier_id,))

            supplier['items'] = []
            for row in c.fetchall():
                supplier['items'].append({
                    'supplier_item_id': row[0],
                    'consumable_id': row[1],
                    'item_name': row[2],
                    'supplier_sku': row[3],
                    'cost_per_unit': float(row[4]) if row[4] else None,
                    'lead_time_days': row[5],
                    'preferred': row[6]
                })

            return supplier

        finally:
            release_connection(conn)

    @staticmethod
    def get_all_suppliers(include_inactive: bool = False) -> List[Dict]:
        """Get all suppliers with summary metrics"""
        conn = get_connection()
        c = conn.cursor()

        try:
            where_clause = "" if include_inactive else "WHERE s.active = TRUE"

            c.execute(f"""
                SELECT
                    s.id, s.name, s.contact_person, s.email, s.phone,
                    s.city, s.state, s.website, s.preferred, s.active,
                    s.api_endpoint IS NOT NULL as has_api,
                    COUNT(DISTINCT si.consumable_id) as item_count,
                    COALESCE(SUM(po.total_amount) FILTER (
                        WHERE po.order_date >= CURRENT_DATE - INTERVAL '12 months'
                    ), 0) as spend_12_months,
                    AVG(si.lead_time_days) as avg_lead_time
                FROM suppliers s
                LEFT JOIN supplier_items si ON s.id = si.supplier_id
                LEFT JOIN purchase_orders po ON s.id = po.supplier_id
                {where_clause}
                GROUP BY s.id
                ORDER BY s.preferred DESC, s.name
            """)

            suppliers = []
            for row in c.fetchall():
                suppliers.append({
                    'id': row[0],
                    'name': row[1],
                    'contact_person': row[2],
                    'email': row[3],
                    'phone': row[4],
                    'city': row[5],
                    'state': row[6],
                    'website': row[7],
                    'preferred': row[8],
                    'active': row[9],
                    'has_api': row[10],
                    'item_count': row[11],
                    'spend_12_months': float(row[12]),
                    'avg_lead_time': round(float(row[13]), 1) if row[13] else None
                })

            return suppliers

        finally:
            release_connection(conn)

    @staticmethod
    def add_supplier_contact(supplier_id: int, contact_data: Dict) -> int:
        """Add a contact to a supplier"""
        conn = get_connection()
        c = conn.cursor()

        try:
            c.execute("""
                INSERT INTO supplier_contacts
                (supplier_id, contact_type, name, title, email, phone, mobile,
                 preferred_contact_method, notes)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (
                supplier_id,
                contact_data.get('contact_type', 'primary'),
                contact_data.get('name'),
                contact_data.get('title'),
                contact_data.get('email'),
                contact_data.get('phone'),
                contact_data.get('mobile'),
                contact_data.get('preferred_contact_method', 'email'),
                contact_data.get('notes')
            ))

            contact_id = c.fetchone()[0]
            conn.commit()
            return contact_id

        finally:
            release_connection(conn)

    @staticmethod
    def update_supplier(supplier_id: int, data: Dict) -> bool:
        """Update supplier information"""
        conn = get_connection()
        c = conn.cursor()

        try:
            c.execute("""
                UPDATE suppliers SET
                    name = COALESCE(%s, name),
                    contact_person = %s,
                    email = %s,
                    phone = %s,
                    address = %s,
                    city = %s,
                    state = %s,
                    zip_code = %s,
                    country = %s,
                    website = %s,
                    account_number = %s,
                    payment_terms = %s,
                    shipping_terms = %s,
                    preferred = %s,
                    active = %s,
                    notes = %s,
                    api_endpoint = %s,
                    api_key = %s,
                    api_username = %s,
                    api_password = %s,
                    api_auth_type = %s,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = %s
            """, (
                data.get('name'),
                data.get('contact_person'),
                data.get('email'),
                data.get('phone'),
                data.get('address'),
                data.get('city'),
                data.get('state'),
                data.get('zip_code'),
                data.get('country'),
                data.get('website'),
                data.get('account_number'),
                data.get('payment_terms'),
                data.get('shipping_terms'),
                data.get('preferred', False),
                data.get('active', True),
                data.get('notes'),
                data.get('api_endpoint'),
                data.get('api_key'),
                data.get('api_username'),
                data.get('api_password'),
                data.get('api_auth_type'),
                supplier_id
            ))

            conn.commit()
            return True

        except Exception as e:
            conn.rollback()
            logging.error(f"Error updating supplier {supplier_id}: {str(e)}")
            return False
        finally:
            release_connection(conn)


# ============================================================================
# PRICE HISTORY TRACKING
# ============================================================================

class PriceHistoryTracker:
    """Track and analyze price changes over time"""

    @staticmethod
    def record_price_change(supplier_item_id: int, old_price: float, new_price: float,
                           source: str = 'manual', user_id: int = None, notes: str = None):
        """Record a price change for a supplier item"""
        conn = get_connection()
        c = conn.cursor()

        try:
            # Get consumable_id and supplier_id
            c.execute("""
                SELECT consumable_id, supplier_id FROM supplier_items WHERE id = %s
            """, (supplier_item_id,))
            row = c.fetchone()
            if not row:
                raise ValueError("Supplier item not found")

            consumable_id, supplier_id = row

            # Calculate change percentage
            change_pct = ((new_price - old_price) / old_price * 100) if old_price > 0 else 0

            c.execute("""
                INSERT INTO supplier_price_history
                (supplier_item_id, consumable_id, supplier_id, old_price, new_price,
                 change_percentage, source, recorded_by, notes)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                supplier_item_id, consumable_id, supplier_id,
                old_price, new_price, change_pct, source, user_id, notes
            ))

            # Update the supplier_items cost
            c.execute("""
                UPDATE supplier_items SET cost_per_unit = %s, updated_at = CURRENT_TIMESTAMP
                WHERE id = %s
            """, (new_price, supplier_item_id))

            conn.commit()

        finally:
            release_connection(conn)

    @staticmethod
    def get_price_history(consumable_id: int = None, supplier_id: int = None,
                         days: int = 365) -> List[Dict]:
        """Get price history for an item or supplier"""
        conn = get_connection()
        c = conn.cursor()

        try:
            where_conditions = ["recorded_at >= CURRENT_DATE - INTERVAL '%s days'" % days]
            params = []

            if consumable_id:
                where_conditions.append("ph.consumable_id = %s")
                params.append(consumable_id)

            if supplier_id:
                where_conditions.append("ph.supplier_id = %s")
                params.append(supplier_id)

            c.execute(f"""
                SELECT
                    ph.id, ph.consumable_id, c.name as item_name,
                    ph.supplier_id, s.name as supplier_name,
                    ph.old_price, ph.new_price, ph.change_percentage,
                    ph.effective_date, ph.recorded_at, ph.source, ph.notes,
                    c.internal_sku
                FROM supplier_price_history ph
                JOIN consumables c ON ph.consumable_id = c.id
                LEFT JOIN suppliers s ON ph.supplier_id = s.id
                WHERE {' AND '.join(where_conditions)}
                ORDER BY ph.recorded_at DESC
            """, params)

            results = []
            for row in c.fetchall():
                results.append({
                    'id': row[0],
                    'consumable_id': row[1],
                    'item_name': row[2],
                    'supplier_id': row[3],
                    'supplier_name': row[4],
                    'old_price': float(row[5]) if row[5] else None,
                    'new_price': float(row[6]),
                    'change_percentage': float(row[7]) if row[7] else 0,
                    'effective_date': row[8],
                    'recorded_at': row[9],
                    'source': row[10],
                    'notes': row[11],
                    'internal_sku': row[12]
                })

            return results

        finally:
            release_connection(conn)

    @staticmethod
    def get_price_trends(consumable_id: int, months: int = 12) -> Dict:
        """Get price trend analysis for an item"""
        conn = get_connection()
        c = conn.cursor()

        try:
            c.execute("""
                SELECT
                    DATE_TRUNC('month', recorded_at) as month,
                    AVG(new_price) as avg_price,
                    MIN(new_price) as min_price,
                    MAX(new_price) as max_price,
                    COUNT(*) as changes
                FROM supplier_price_history
                WHERE consumable_id = %s
                AND recorded_at >= CURRENT_DATE - INTERVAL '%s months'
                GROUP BY DATE_TRUNC('month', recorded_at)
                ORDER BY month
            """, (consumable_id, months))

            trends = []
            for row in c.fetchall():
                trends.append({
                    'month': row[0].isoformat() if row[0] else None,
                    'avg_price': float(row[1]),
                    'min_price': float(row[2]),
                    'max_price': float(row[3]),
                    'change_count': row[4]
                })

            # Calculate overall trend
            if len(trends) >= 2:
                first_price = trends[0]['avg_price']
                last_price = trends[-1]['avg_price']
                overall_change = ((last_price - first_price) / first_price * 100) if first_price > 0 else 0
            else:
                overall_change = 0

            return {
                'monthly_data': trends,
                'overall_change_percent': round(overall_change, 2),
                'trend_direction': 'up' if overall_change > 0 else 'down' if overall_change < 0 else 'stable'
            }

        finally:
            release_connection(conn)


# ============================================================================
# PURCHASE ORDER MANAGEMENT
# ============================================================================

class PurchaseOrderManager:
    """Manage purchase orders end-to-end"""

    @staticmethod
    def create_purchase_order(supplier_id: int, items: List[Dict],
                             user_id: int, notes: str = None) -> int:
        """Create a new purchase order"""
        conn = get_connection()
        c = conn.cursor()

        try:
            # Generate PO number
            c.execute("SELECT COALESCE(MAX(id), 0) + 1 FROM purchase_orders")
            next_id = c.fetchone()[0]
            po_number = f"PO-{datetime.now().strftime('%Y%m')}-{next_id:05d}"

            # Calculate totals
            subtotal = sum(item.get('quantity', 0) * item.get('unit_price', 0) for item in items)
            tax = subtotal * 0.0  # Can be customized
            total = subtotal + tax

            # Get expected delivery based on supplier lead time
            c.execute("SELECT AVG(lead_time_days) FROM supplier_items WHERE supplier_id = %s", (supplier_id,))
            avg_lead = c.fetchone()[0] or 7
            expected_delivery = datetime.now() + timedelta(days=int(avg_lead))

            # Create the PO
            c.execute("""
                INSERT INTO purchase_orders
                (po_number, supplier_id, status, order_date, expected_delivery_date,
                 subtotal, tax_amount, total_amount, notes, created_by)
                VALUES (%s, %s, 'draft', CURRENT_DATE, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (po_number, supplier_id, expected_delivery, subtotal, tax, total, notes, user_id))

            po_id = c.fetchone()[0]

            # Add line items
            for item in items:
                total_cost = item.get('quantity', 0) * item.get('unit_price', 0)
                c.execute("""
                    INSERT INTO purchase_order_items
                    (po_id, consumable_id, quantity, unit_cost, total_cost)
                    VALUES (%s, %s, %s, %s, %s)
                """, (po_id, item['consumable_id'], item['quantity'],
                      item.get('unit_price', 0), total_cost))

            conn.commit()
            return po_id

        except Exception as e:
            conn.rollback()
            logging.error(f"Error creating purchase order: {str(e)}")
            raise
        finally:
            release_connection(conn)

    @staticmethod
    def get_purchase_order(po_id: int) -> Optional[Dict]:
        """Get purchase order with full details"""
        conn = get_connection()
        c = conn.cursor()

        try:
            c.execute("""
                SELECT
                    po.id, po.po_number, po.supplier_id, s.name as supplier_name,
                    po.status, po.order_date, po.expected_delivery_date, po.actual_delivery_date,
                    po.subtotal, po.tax_amount, po.shipping_cost, po.total_amount,
                    po.notes, po.created_by, u.username as created_by_name,
                    po.created_at, po.updated_at
                FROM purchase_orders po
                JOIN suppliers s ON po.supplier_id = s.id
                LEFT JOIN users u ON po.created_by = u.id
                WHERE po.id = %s
            """, (po_id,))

            row = c.fetchone()
            if not row:
                return None

            po = {
                'id': row[0],
                'po_number': row[1],
                'supplier_id': row[2],
                'supplier_name': row[3],
                'status': row[4],
                'order_date': row[5],
                'expected_delivery_date': row[6],
                'actual_delivery_date': row[7],
                'subtotal': float(row[8]) if row[8] else 0,
                'tax_amount': float(row[9]) if row[9] else 0,
                'shipping_cost': float(row[10]) if row[10] else 0,
                'total_amount': float(row[11]) if row[11] else 0,
                'notes': row[12],
                'created_by': row[13],
                'created_by_name': row[14],
                'created_at': row[15],
                'updated_at': row[16]
            }

            # Get line items
            c.execute("""
                SELECT
                    poi.id, poi.consumable_id, c.name, c.internal_sku,
                    poi.quantity, poi.received_quantity,
                    poi.unit_cost, poi.total_cost, poi.notes
                FROM purchase_order_items poi
                JOIN consumables c ON poi.consumable_id = c.id
                WHERE poi.po_id = %s
            """, (po_id,))

            po['items'] = []
            for row in c.fetchall():
                po['items'].append({
                    'id': row[0],
                    'consumable_id': row[1],
                    'item_name': row[2],
                    'internal_sku': row[3],
                    'quantity_ordered': row[4],
                    'quantity_received': row[5] or 0,
                    'unit_price': float(row[6]) if row[6] else 0,
                    'line_total': float(row[7]) if row[7] else 0,
                    'received_date': None,
                    'condition_notes': row[8]
                })

            # Get shipment tracking
            c.execute("""
                SELECT
                    id, carrier, tracking_number, status,
                    estimated_arrival, actual_arrival, current_location
                FROM shipment_tracking
                WHERE po_id = %s
                ORDER BY created_at DESC
            """, (po_id,))

            po['shipments'] = []
            for row in c.fetchall():
                po['shipments'].append({
                    'id': row[0],
                    'carrier': row[1],
                    'tracking_number': row[2],
                    'status': row[3],
                    'estimated_arrival': row[4],
                    'actual_arrival': row[5],
                    'current_location': row[6]
                })

            return po

        finally:
            release_connection(conn)

    @staticmethod
    def get_all_purchase_orders(status: str = None, supplier_id: int = None,
                               limit: int = 50) -> List[Dict]:
        """Get all purchase orders with optional filtering"""
        conn = get_connection()
        c = conn.cursor()

        try:
            where_conditions = []
            params = []

            if status:
                where_conditions.append("po.status = %s")
                params.append(status)

            if supplier_id:
                where_conditions.append("po.supplier_id = %s")
                params.append(supplier_id)

            where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""

            c.execute(f"""
                SELECT
                    po.id, po.po_number, s.name as supplier_name,
                    po.status, po.order_date, po.expected_delivery_date,
                    po.total_amount, COUNT(poi.id) as item_count
                FROM purchase_orders po
                JOIN suppliers s ON po.supplier_id = s.id
                LEFT JOIN purchase_order_items poi ON po.id = poi.po_id
                {where_clause}
                GROUP BY po.id, s.name
                ORDER BY po.order_date DESC
                LIMIT %s
            """, params + [limit])

            orders = []
            for row in c.fetchall():
                orders.append({
                    'id': row[0],
                    'po_number': row[1],
                    'supplier_name': row[2],
                    'status': row[3],
                    'order_date': row[4],
                    'expected_delivery_date': row[5],
                    'total_amount': float(row[6]) if row[6] else 0,
                    'item_count': row[7]
                })

            return orders

        finally:
            release_connection(conn)

    @staticmethod
    def update_order_status(po_id: int, new_status: str, user_id: int = None) -> bool:
        """Update purchase order status"""
        conn = get_connection()
        c = conn.cursor()

        try:
            valid_statuses = ['draft', 'submitted', 'acknowledged', 'shipped',
                             'partially_received', 'delivered', 'cancelled']
            if new_status not in valid_statuses:
                raise ValueError(f"Invalid status: {new_status}")

            updates = ["status = %s", "updated_at = CURRENT_TIMESTAMP"]
            params = [new_status]

            if new_status == 'delivered':
                updates.append("actual_delivery_date = CURRENT_DATE")

            c.execute(f"""
                UPDATE purchase_orders SET {', '.join(updates)} WHERE id = %s
            """, params + [po_id])

            conn.commit()
            return True

        except Exception as e:
            conn.rollback()
            logging.error(f"Error updating PO status: {str(e)}")
            return False
        finally:
            release_connection(conn)

    @staticmethod
    def receive_items(po_id: int, received_items: List[Dict], user_id: int) -> bool:
        """Process receiving of purchase order items"""
        conn = get_connection()
        c = conn.cursor()

        try:
            for item in received_items:
                poi_id = item['po_item_id']
                qty_received = item['quantity_received']
                condition_notes = item.get('condition_notes', '')

                # Update PO item
                c.execute("""
                    UPDATE purchase_order_items
                    SET received_quantity = COALESCE(received_quantity, 0) + %s,
                        notes = %s
                    WHERE id = %s
                    RETURNING consumable_id, quantity
                """, (qty_received, condition_notes, poi_id))

                result = c.fetchone()
                if not result:
                    continue

                consumable_id = result[0]

                # Update inventory
                c.execute("""
                    UPDATE consumables SET quantity = quantity + %s WHERE id = %s
                    RETURNING quantity
                """, (qty_received, consumable_id))

                new_qty = c.fetchone()[0]

                # Record transaction
                c.execute("""
                    INSERT INTO inventory_transactions
                    (consumable_id, transaction_type, quantity_change, quantity_after,
                     reference_id, reference_type, performed_by, notes)
                    VALUES (%s, 'purchase', %s, %s, %s, 'purchase_order', %s, %s)
                """, (consumable_id, qty_received, new_qty, po_id, user_id,
                      f"Received from PO #{po_id}"))

            # Check if all items received
            c.execute("""
                SELECT
                    SUM(quantity) as total_ordered,
                    SUM(COALESCE(received_quantity, 0)) as total_received
                FROM purchase_order_items
                WHERE po_id = %s
            """, (po_id,))

            row = c.fetchone()
            if row[1] >= row[0]:
                new_status = 'delivered'
            else:
                new_status = 'partially_received'

            c.execute("""
                UPDATE purchase_orders SET status = %s, updated_at = CURRENT_TIMESTAMP
                WHERE id = %s
            """, (new_status, po_id))

            if new_status == 'delivered':
                c.execute("""
                    UPDATE purchase_orders SET actual_delivery_date = CURRENT_DATE
                    WHERE id = %s
                """, (po_id,))

            conn.commit()
            return True

        except Exception as e:
            conn.rollback()
            logging.error(f"Error receiving items: {str(e)}")
            return False
        finally:
            release_connection(conn)


# ============================================================================
# INVENTORY TRANSFERS
# ============================================================================

class InventoryTransferManager:
    """Manage inventory transfers between locations"""

    @staticmethod
    def create_transfer_request(from_location_id: int, to_location_id: int,
                                consumable_id: int, quantity: int,
                                user_id: int, notes: str = None) -> int:
        """Create a new transfer request"""
        conn = get_connection()
        c = conn.cursor()

        try:
            c.execute("""
                INSERT INTO inventory_transfers
                (from_location_id, to_location_id, consumable_id, quantity,
                 status, requested_by, notes)
                VALUES (%s, %s, %s, %s, 'requested', %s, %s)
                RETURNING id
            """, (from_location_id, to_location_id, consumable_id, quantity, user_id, notes))

            transfer_id = c.fetchone()[0]
            conn.commit()
            return transfer_id

        finally:
            release_connection(conn)

    @staticmethod
    def approve_transfer(transfer_id: int, user_id: int) -> bool:
        """Approve a transfer request"""
        conn = get_connection()
        c = conn.cursor()

        try:
            c.execute("""
                UPDATE inventory_transfers
                SET status = 'approved', approved_by = %s, approval_date = CURRENT_TIMESTAMP
                WHERE id = %s AND status = 'requested'
            """, (user_id, transfer_id))

            conn.commit()
            return c.rowcount > 0

        finally:
            release_connection(conn)

    @staticmethod
    def ship_transfer(transfer_id: int, tracking_info: str = None) -> bool:
        """Mark transfer as shipped"""
        conn = get_connection()
        c = conn.cursor()

        try:
            # Get transfer details
            c.execute("""
                SELECT consumable_id, quantity, from_location_id
                FROM inventory_transfers
                WHERE id = %s AND status = 'approved'
            """, (transfer_id,))

            row = c.fetchone()
            if not row:
                return False

            consumable_id, quantity, from_location = row

            # Deduct from source location (for now just from main inventory)
            c.execute("""
                UPDATE consumables SET quantity = quantity - %s WHERE id = %s
            """, (quantity, consumable_id))

            # Update transfer status
            c.execute("""
                UPDATE inventory_transfers
                SET status = 'in_transit', ship_date = CURRENT_TIMESTAMP, tracking_info = %s
                WHERE id = %s
            """, (tracking_info, transfer_id))

            conn.commit()
            return True

        except Exception as e:
            conn.rollback()
            logging.error(f"Error shipping transfer: {str(e)}")
            return False
        finally:
            release_connection(conn)

    @staticmethod
    def receive_transfer(transfer_id: int) -> bool:
        """Mark transfer as received"""
        conn = get_connection()
        c = conn.cursor()

        try:
            # Get transfer details
            c.execute("""
                SELECT consumable_id, quantity, to_location_id
                FROM inventory_transfers
                WHERE id = %s AND status = 'in_transit'
            """, (transfer_id,))

            row = c.fetchone()
            if not row:
                return False

            consumable_id, quantity, to_location = row

            # Add to destination (for now just add back - future: per-location inventory)
            # In full implementation, would track inventory per location

            # Update transfer status
            c.execute("""
                UPDATE inventory_transfers
                SET status = 'received', received_date = CURRENT_TIMESTAMP
                WHERE id = %s
            """, (transfer_id,))

            conn.commit()
            return True

        finally:
            release_connection(conn)

    @staticmethod
    def get_transfers(status: str = None, location_id: int = None) -> List[Dict]:
        """Get transfer requests with optional filtering"""
        conn = get_connection()
        c = conn.cursor()

        try:
            where_conditions = []
            params = []

            if status:
                where_conditions.append("it.status = %s")
                params.append(status)

            if location_id:
                where_conditions.append("(it.from_location_id = %s OR it.to_location_id = %s)")
                params.extend([location_id, location_id])

            where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""

            c.execute(f"""
                SELECT
                    it.id, it.status,
                    fl.name as from_location, tl.name as to_location,
                    c.name as item_name, it.quantity,
                    it.request_date, it.ship_date, it.received_date,
                    u.username as requested_by
                FROM inventory_transfers it
                JOIN inventory_locations fl ON it.from_location_id = fl.id
                JOIN inventory_locations tl ON it.to_location_id = tl.id
                JOIN consumables c ON it.consumable_id = c.id
                LEFT JOIN users u ON it.requested_by = u.id
                {where_clause}
                ORDER BY it.request_date DESC
            """, params)

            transfers = []
            for row in c.fetchall():
                transfers.append({
                    'id': row[0],
                    'status': row[1],
                    'from_location': row[2],
                    'to_location': row[3],
                    'item_name': row[4],
                    'quantity': row[5],
                    'request_date': row[6],
                    'ship_date': row[7],
                    'received_date': row[8],
                    'requested_by': row[9]
                })

            return transfers

        finally:
            release_connection(conn)


# ============================================================================
# SUPPLIER API INTEGRATION
# ============================================================================

class SupplierAPIIntegration:
    """Handle supplier API integrations for catalog sync"""

    @staticmethod
    def test_connection(supplier_id: int) -> Dict:
        """Test API connection to a supplier"""
        conn = get_connection()
        c = conn.cursor()

        try:
            c.execute("""
                SELECT api_endpoint, api_key, api_username, api_password, api_auth_type
                FROM suppliers WHERE id = %s
            """, (supplier_id,))

            row = c.fetchone()
            if not row or not row[0]:
                return {'success': False, 'error': 'No API configuration found'}

            endpoint, api_key, username, password, auth_type = row

            # Attempt connection based on auth type
            import requests

            headers = {'Content-Type': 'application/json'}

            if auth_type == 'api_key':
                headers['Authorization'] = f'Bearer {api_key}'
            elif auth_type == 'basic':
                import base64
                credentials = base64.b64encode(f"{username}:{password}".encode()).decode()
                headers['Authorization'] = f'Basic {credentials}'

            try:
                response = requests.get(endpoint, headers=headers, timeout=10)

                if response.status_code == 200:
                    return {
                        'success': True,
                        'message': 'Connection successful',
                        'status_code': response.status_code
                    }
                else:
                    return {
                        'success': False,
                        'error': f'API returned status {response.status_code}',
                        'status_code': response.status_code
                    }
            except requests.RequestException as e:
                return {'success': False, 'error': str(e)}

        finally:
            release_connection(conn)

    @staticmethod
    def sync_catalog(supplier_id: int, user_id: int = None) -> Dict:
        """Sync product catalog from supplier API"""
        conn = get_connection()
        c = conn.cursor()

        try:
            # Log sync start
            c.execute("""
                INSERT INTO supplier_api_sync_log
                (supplier_id, sync_type, status, started_at)
                VALUES (%s, 'catalog', 'running', CURRENT_TIMESTAMP)
                RETURNING id
            """, (supplier_id,))
            sync_id = c.fetchone()[0]
            conn.commit()

            # Get API config
            c.execute("""
                SELECT api_endpoint, api_key, api_username, api_password, api_auth_type
                FROM suppliers WHERE id = %s
            """, (supplier_id,))

            row = c.fetchone()
            if not row or not row[0]:
                c.execute("""
                    UPDATE supplier_api_sync_log
                    SET status = 'failed', completed_at = CURRENT_TIMESTAMP,
                        error_message = 'No API configuration'
                    WHERE id = %s
                """, (sync_id,))
                conn.commit()
                return {'success': False, 'error': 'No API configuration'}

            # In real implementation, would call supplier API and process response
            # For now, return success placeholder

            c.execute("""
                UPDATE supplier_api_sync_log
                SET status = 'completed', completed_at = CURRENT_TIMESTAMP,
                    items_synced = 0
                WHERE id = %s
            """, (sync_id,))
            conn.commit()

            return {
                'success': True,
                'sync_id': sync_id,
                'message': 'Catalog sync initiated. Implementation pending API specs.'
            }

        except Exception as e:
            c.execute("""
                UPDATE supplier_api_sync_log
                SET status = 'failed', completed_at = CURRENT_TIMESTAMP,
                    error_message = %s
                WHERE id = %s
            """, (str(e), sync_id))
            conn.commit()
            return {'success': False, 'error': str(e)}
        finally:
            release_connection(conn)

    @staticmethod
    def get_sync_history(supplier_id: int, limit: int = 20) -> List[Dict]:
        """Get API sync history for a supplier"""
        conn = get_connection()
        c = conn.cursor()

        try:
            c.execute("""
                SELECT
                    id, sync_type, status, started_at, completed_at,
                    items_synced, items_failed, error_message
                FROM supplier_api_sync_log
                WHERE supplier_id = %s
                ORDER BY started_at DESC
                LIMIT %s
            """, (supplier_id, limit))

            history = []
            for row in c.fetchall():
                history.append({
                    'id': row[0],
                    'sync_type': row[1],
                    'status': row[2],
                    'started_at': row[3],
                    'completed_at': row[4],
                    'items_synced': row[5],
                    'items_failed': row[6],
                    'error_message': row[7]
                })

            return history

        finally:
            release_connection(conn)

def get_inventory_quantity(item_name: str) -> Optional[int]:
    """Return total quantity for items whose name matches item_name (case-insensitive)."""
    if not item_name:
        return None
    conn = get_connection()
    c = conn.cursor()
    try:
        c.execute(
            """
            SELECT COALESCE(SUM(quantity), 0)
            FROM consumables
            WHERE active = TRUE AND name ILIKE %s
            """,
            (f"%{item_name}%",)
        )
        total = c.fetchone()[0]
        return int(total) if total is not None else 0
    finally:
        release_connection(conn)


# ============================================================================
# ENHANCED INVENTORY FEATURES - Parts Kits, Cross-Reference, Min/Max Levels
# ============================================================================

class PartsKitManager:
    """Manage parts kits - bundled groups of items for common maintenance tasks"""

    @staticmethod
    def create_parts_kit(name: str, description: str = None, kit_type: str = 'maintenance',
                        equipment_type: str = None) -> int:
        """Create a new parts kit"""
        conn = get_connection()
        c = conn.cursor()
        try:
            c.execute("""
                INSERT INTO parts_kits (name, description, kit_type, equipment_type, is_active, created_at)
                VALUES (%s, %s, %s, %s, TRUE, CURRENT_TIMESTAMP)
                RETURNING id
            """, (name, description, kit_type, equipment_type))
            kit_id = c.fetchone()[0]
            conn.commit()
            logging.info(f"Created parts kit: {name} (ID: {kit_id})")
            return kit_id
        except Exception as e:
            conn.rollback()
            logging.error(f"Error creating parts kit: {e}")
            raise
        finally:
            release_connection(conn)

    @staticmethod
    def add_kit_item(kit_id: int, consumable_id: int, quantity: int, is_optional: bool = False,
                     notes: str = None) -> int:
        """Add an item to a parts kit"""
        conn = get_connection()
        c = conn.cursor()
        try:
            c.execute("""
                INSERT INTO parts_kit_items (kit_id, consumable_id, quantity, is_optional, notes)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (kit_id, consumable_id) DO UPDATE SET
                    quantity = EXCLUDED.quantity,
                    is_optional = EXCLUDED.is_optional,
                    notes = EXCLUDED.notes
                RETURNING id
            """, (kit_id, consumable_id, quantity, is_optional, notes))
            item_id = c.fetchone()[0]
            conn.commit()
            return item_id
        except Exception as e:
            conn.rollback()
            logging.error(f"Error adding item to kit: {e}")
            raise
        finally:
            release_connection(conn)

    @staticmethod
    def remove_kit_item(kit_id: int, consumable_id: int) -> bool:
        """Remove an item from a parts kit"""
        conn = get_connection()
        c = conn.cursor()
        try:
            c.execute("""
                DELETE FROM parts_kit_items
                WHERE kit_id = %s AND consumable_id = %s
            """, (kit_id, consumable_id))
            deleted = c.rowcount > 0
            conn.commit()
            return deleted
        finally:
            release_connection(conn)

    @staticmethod
    def get_parts_kit(kit_id: int) -> Optional[Dict]:
        """Get a parts kit with all its items"""
        conn = get_connection()
        c = conn.cursor()
        try:
            c.execute("""
                SELECT id, name, description, kit_type, equipment_type, is_active, created_at
                FROM parts_kits WHERE id = %s
            """, (kit_id,))
            row = c.fetchone()
            if not row:
                return None

            kit = {
                'id': row[0],
                'name': row[1],
                'description': row[2],
                'kit_type': row[3],
                'equipment_type': row[4],
                'is_active': row[5],
                'created_at': row[6],
                'items': [],
                'total_cost': 0,
                'availability_status': 'available'
            }

            # Get kit items with inventory details
            c.execute("""
                SELECT pki.id, pki.consumable_id, pki.quantity, pki.is_optional, pki.notes,
                       c.name, c.quantity as stock_qty, c.cost_per_unit, c.unit, c.location
                FROM parts_kit_items pki
                JOIN consumables c ON pki.consumable_id = c.id
                WHERE pki.kit_id = %s
                ORDER BY pki.is_optional, c.name
            """, (kit_id,))

            unavailable_items = 0
            for item_row in c.fetchall():
                item_qty = item_row[2]
                stock_qty = item_row[6] or 0
                cost = float(item_row[7]) if item_row[7] else 0

                item = {
                    'kit_item_id': item_row[0],
                    'consumable_id': item_row[1],
                    'required_qty': item_qty,
                    'is_optional': item_row[3],
                    'notes': item_row[4],
                    'name': item_row[5],
                    'stock_qty': stock_qty,
                    'cost_per_unit': cost,
                    'unit': item_row[8],
                    'location': item_row[9],
                    'line_cost': cost * item_qty,
                    'is_available': stock_qty >= item_qty
                }
                kit['items'].append(item)
                kit['total_cost'] += item['line_cost']

                if not item['is_optional'] and not item['is_available']:
                    unavailable_items += 1

            if unavailable_items > 0:
                kit['availability_status'] = 'partial'
            if unavailable_items == len([i for i in kit['items'] if not i['is_optional']]):
                kit['availability_status'] = 'unavailable'

            return kit
        finally:
            release_connection(conn)

    @staticmethod
    def list_parts_kits(kit_type: str = None, equipment_type: str = None,
                        active_only: bool = True) -> List[Dict]:
        """List all parts kits with optional filtering"""
        conn = get_connection()
        c = conn.cursor()
        try:
            query = """
                SELECT pk.id, pk.name, pk.description, pk.kit_type, pk.equipment_type,
                       pk.is_active, pk.created_at,
                       COUNT(pki.id) as item_count,
                       COALESCE(SUM(pki.quantity * c.cost_per_unit), 0) as total_cost
                FROM parts_kits pk
                LEFT JOIN parts_kit_items pki ON pk.id = pki.kit_id
                LEFT JOIN consumables c ON pki.consumable_id = c.id
                WHERE 1=1
            """
            params = []

            if active_only:
                query += " AND pk.is_active = TRUE"
            if kit_type:
                query += " AND pk.kit_type = %s"
                params.append(kit_type)
            if equipment_type:
                query += " AND pk.equipment_type = %s"
                params.append(equipment_type)

            query += " GROUP BY pk.id ORDER BY pk.name"

            c.execute(query, params)

            kits = []
            for row in c.fetchall():
                kits.append({
                    'id': row[0],
                    'name': row[1],
                    'description': row[2],
                    'kit_type': row[3],
                    'equipment_type': row[4],
                    'is_active': row[5],
                    'created_at': row[6],
                    'item_count': row[7],
                    'total_cost': float(row[8]) if row[8] else 0
                })
            return kits
        finally:
            release_connection(conn)

    @staticmethod
    def reserve_kit_for_job(kit_id: int, job_id: int, quantity: int = 1) -> Dict:
        """Reserve all items in a kit for a maintenance job"""
        conn = get_connection()
        c = conn.cursor()
        try:
            # Get kit items
            c.execute("""
                SELECT pki.consumable_id, pki.quantity, pki.is_optional, c.name, c.quantity as stock
                FROM parts_kit_items pki
                JOIN consumables c ON pki.consumable_id = c.id
                WHERE pki.kit_id = %s
            """, (kit_id,))

            items = c.fetchall()
            reserved = []
            failed = []

            for item in items:
                consumable_id, req_qty, is_optional, name, stock = item
                total_needed = req_qty * quantity

                if stock >= total_needed:
                    # Reserve the item
                    c.execute("""
                        INSERT INTO job_parts_reserved (job_id, consumable_id, quantity_reserved,
                                                        reserved_at, status)
                        VALUES (%s, %s, %s, CURRENT_TIMESTAMP, 'reserved')
                        ON CONFLICT (job_id, consumable_id) DO UPDATE SET
                            quantity_reserved = job_parts_reserved.quantity_reserved + EXCLUDED.quantity_reserved
                    """, (job_id, consumable_id, total_needed))

                    # Reduce available quantity
                    c.execute("""
                        UPDATE consumables SET quantity = quantity - %s WHERE id = %s
                    """, (total_needed, consumable_id))

                    reserved.append({'name': name, 'quantity': total_needed})
                else:
                    if not is_optional:
                        failed.append({'name': name, 'needed': total_needed, 'available': stock})

            if failed:
                conn.rollback()
                return {
                    'success': False,
                    'message': 'Insufficient stock for required items',
                    'failed_items': failed
                }

            conn.commit()
            return {
                'success': True,
                'reserved_items': reserved,
                'job_id': job_id
            }
        except Exception as e:
            conn.rollback()
            return {'success': False, 'error': str(e)}
        finally:
            release_connection(conn)


class PartsCrossReference:
    """Manage cross-references between equivalent parts from different manufacturers/suppliers"""

    @staticmethod
    def add_cross_reference(primary_id: int, equivalent_id: int, match_type: str = 'equivalent',
                           notes: str = None) -> int:
        """Add a cross-reference between two parts"""
        conn = get_connection()
        c = conn.cursor()
        try:
            # Check that both items exist
            c.execute("SELECT id FROM consumables WHERE id IN (%s, %s)", (primary_id, equivalent_id))
            if len(c.fetchall()) != 2:
                raise ValueError("One or both items not found")

            c.execute("""
                INSERT INTO parts_cross_reference
                (primary_consumable_id, equivalent_consumable_id, match_type, notes, verified, created_at)
                VALUES (%s, %s, %s, %s, FALSE, CURRENT_TIMESTAMP)
                ON CONFLICT (primary_consumable_id, equivalent_consumable_id) DO UPDATE SET
                    match_type = EXCLUDED.match_type,
                    notes = EXCLUDED.notes
                RETURNING id
            """, (primary_id, equivalent_id, match_type, notes))
            ref_id = c.fetchone()[0]

            # Create reverse reference for bidirectional lookup
            c.execute("""
                INSERT INTO parts_cross_reference
                (primary_consumable_id, equivalent_consumable_id, match_type, notes, verified, created_at)
                VALUES (%s, %s, %s, %s, FALSE, CURRENT_TIMESTAMP)
                ON CONFLICT (primary_consumable_id, equivalent_consumable_id) DO NOTHING
            """, (equivalent_id, primary_id, match_type, notes))

            conn.commit()
            return ref_id
        except Exception as e:
            conn.rollback()
            logging.error(f"Error adding cross-reference: {e}")
            raise
        finally:
            release_connection(conn)

    @staticmethod
    def get_equivalents(consumable_id: int, include_out_of_stock: bool = False) -> List[Dict]:
        """Get all equivalent parts for a given item"""
        conn = get_connection()
        c = conn.cursor()
        try:
            query = """
                SELECT pcr.id, pcr.equivalent_consumable_id, pcr.match_type, pcr.notes, pcr.verified,
                       c.name, c.part_number, c.supplier, c.manufacturer, c.quantity,
                       c.cost_per_unit, c.location
                FROM parts_cross_reference pcr
                JOIN consumables c ON pcr.equivalent_consumable_id = c.id
                WHERE pcr.primary_consumable_id = %s AND c.active = TRUE
            """
            params = [consumable_id]

            if not include_out_of_stock:
                query += " AND c.quantity > 0"

            query += " ORDER BY pcr.verified DESC, c.quantity DESC"

            c.execute(query, params)

            equivalents = []
            for row in c.fetchall():
                equivalents.append({
                    'ref_id': row[0],
                    'consumable_id': row[1],
                    'match_type': row[2],
                    'notes': row[3],
                    'verified': row[4],
                    'name': row[5],
                    'part_number': row[6],
                    'supplier': row[7],
                    'manufacturer': row[8],
                    'quantity': row[9],
                    'cost_per_unit': float(row[10]) if row[10] else None,
                    'location': row[11]
                })
            return equivalents
        finally:
            release_connection(conn)

    @staticmethod
    def find_best_available(consumable_id: int, quantity_needed: int) -> Optional[Dict]:
        """Find the best available equivalent part when primary is out of stock"""
        conn = get_connection()
        c = conn.cursor()
        try:
            # First check the primary item
            c.execute("""
                SELECT id, name, quantity, cost_per_unit FROM consumables
                WHERE id = %s AND active = TRUE
            """, (consumable_id,))
            primary = c.fetchone()

            if primary and primary[2] >= quantity_needed:
                return {
                    'consumable_id': primary[0],
                    'name': primary[1],
                    'quantity_available': primary[2],
                    'cost_per_unit': float(primary[3]) if primary[3] else None,
                    'is_primary': True,
                    'match_type': 'primary'
                }

            # Check equivalents
            c.execute("""
                SELECT c.id, c.name, c.quantity, c.cost_per_unit, pcr.match_type, pcr.verified
                FROM parts_cross_reference pcr
                JOIN consumables c ON pcr.equivalent_consumable_id = c.id
                WHERE pcr.primary_consumable_id = %s
                  AND c.active = TRUE
                  AND c.quantity >= %s
                ORDER BY pcr.verified DESC,
                         CASE pcr.match_type WHEN 'exact' THEN 1 WHEN 'equivalent' THEN 2 ELSE 3 END,
                         c.cost_per_unit ASC
                LIMIT 1
            """, (consumable_id, quantity_needed))

            equiv = c.fetchone()
            if equiv:
                return {
                    'consumable_id': equiv[0],
                    'name': equiv[1],
                    'quantity_available': equiv[2],
                    'cost_per_unit': float(equiv[3]) if equiv[3] else None,
                    'is_primary': False,
                    'match_type': equiv[4],
                    'verified': equiv[5]
                }

            return None
        finally:
            release_connection(conn)

    @staticmethod
    def verify_cross_reference(ref_id: int, verified_by: int = None) -> bool:
        """Mark a cross-reference as verified"""
        conn = get_connection()
        c = conn.cursor()
        try:
            c.execute("""
                UPDATE parts_cross_reference
                SET verified = TRUE, verified_by = %s, verified_at = CURRENT_TIMESTAMP
                WHERE id = %s
            """, (verified_by, ref_id))
            conn.commit()
            return c.rowcount > 0
        finally:
            release_connection(conn)


class StockLevelManager:
    """Manage min/max stock levels, auto-reorder, and stock optimization"""

    @staticmethod
    def set_stock_levels(consumable_id: int, min_level: int = None, max_level: int = None,
                        reorder_point: int = None, reorder_quantity: int = None,
                        safety_stock: int = None) -> bool:
        """Set stock level parameters for an item"""
        conn = get_connection()
        c = conn.cursor()
        try:
            c.execute("""
                UPDATE consumables SET
                    minimum_stock_level = COALESCE(%s, minimum_stock_level),
                    maximum_stock_level = COALESCE(%s, maximum_stock_level),
                    reorder_threshold = COALESCE(%s, reorder_threshold),
                    reorder_quantity = COALESCE(%s, reorder_quantity),
                    safety_stock = COALESCE(%s, safety_stock),
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = %s
            """, (min_level, max_level, reorder_point, reorder_quantity, safety_stock, consumable_id))
            conn.commit()
            return c.rowcount > 0
        finally:
            release_connection(conn)

    @staticmethod
    def get_items_below_minimum() -> List[Dict]:
        """Get all items that are below their minimum stock level"""
        conn = get_connection()
        c = conn.cursor()
        try:
            c.execute("""
                SELECT c.id, c.name, c.part_number, c.quantity, c.minimum_stock_level,
                       c.reorder_threshold, c.reorder_quantity, c.supplier, c.category,
                       c.cost_per_unit,
                       COALESCE(c.minimum_stock_level, c.reorder_threshold, 0) as effective_min
                FROM consumables c
                WHERE c.active = TRUE
                  AND c.quantity <= COALESCE(c.minimum_stock_level, c.reorder_threshold, 0)
                  AND COALESCE(c.minimum_stock_level, c.reorder_threshold, 0) > 0
                ORDER BY (c.quantity::float / NULLIF(COALESCE(c.minimum_stock_level, c.reorder_threshold, 1), 0)) ASC
            """)

            items = []
            for row in c.fetchall():
                deficit = (row[10] or 0) - row[3]
                items.append({
                    'id': row[0],
                    'name': row[1],
                    'part_number': row[2],
                    'current_qty': row[3],
                    'min_level': row[4],
                    'reorder_point': row[5],
                    'reorder_qty': row[6],
                    'supplier': row[7],
                    'category': row[8],
                    'cost_per_unit': float(row[9]) if row[9] else None,
                    'effective_min': row[10],
                    'deficit': max(0, deficit),
                    'stock_percentage': round((row[3] / row[10] * 100) if row[10] else 0, 1)
                })
            return items
        finally:
            release_connection(conn)

    @staticmethod
    def get_items_above_maximum() -> List[Dict]:
        """Get all items that are above their maximum stock level (overstocked)"""
        conn = get_connection()
        c = conn.cursor()
        try:
            c.execute("""
                SELECT c.id, c.name, c.part_number, c.quantity, c.maximum_stock_level,
                       c.supplier, c.category, c.cost_per_unit
                FROM consumables c
                WHERE c.active = TRUE
                  AND c.maximum_stock_level IS NOT NULL
                  AND c.quantity > c.maximum_stock_level
                ORDER BY (c.quantity - c.maximum_stock_level) DESC
            """)

            items = []
            for row in c.fetchall():
                excess = row[3] - row[4]
                items.append({
                    'id': row[0],
                    'name': row[1],
                    'part_number': row[2],
                    'current_qty': row[3],
                    'max_level': row[4],
                    'supplier': row[5],
                    'category': row[6],
                    'cost_per_unit': float(row[7]) if row[7] else None,
                    'excess_qty': excess,
                    'excess_value': excess * float(row[7]) if row[7] else None
                })
            return items
        finally:
            release_connection(conn)

    @staticmethod
    def generate_reorder_suggestions() -> List[Dict]:
        """Generate automatic reorder suggestions based on stock levels and usage"""
        conn = get_connection()
        c = conn.cursor()
        try:
            c.execute("""
                WITH usage_stats AS (
                    SELECT consumable_id,
                           AVG(quantity_change) as avg_daily_usage,
                           COUNT(*) as transaction_count
                    FROM inventory_transactions
                    WHERE transaction_type = 'usage'
                      AND transaction_date > CURRENT_DATE - INTERVAL '90 days'
                    GROUP BY consumable_id
                )
                SELECT c.id, c.name, c.part_number, c.quantity,
                       c.minimum_stock_level, c.reorder_threshold,
                       c.reorder_quantity, c.supplier, c.cost_per_unit,
                       COALESCE(us.avg_daily_usage, 0) as avg_daily_usage,
                       COALESCE(c.reorder_quantity,
                                GREATEST(c.maximum_stock_level - c.quantity,
                                        c.minimum_stock_level * 2)) as suggested_qty
                FROM consumables c
                LEFT JOIN usage_stats us ON c.id = us.consumable_id
                WHERE c.active = TRUE
                  AND c.quantity <= COALESCE(c.reorder_threshold, c.minimum_stock_level, 0)
                  AND COALESCE(c.reorder_threshold, c.minimum_stock_level, 0) > 0
                ORDER BY
                    CASE WHEN c.quantity = 0 THEN 0 ELSE 1 END,
                    us.avg_daily_usage DESC NULLS LAST
            """)

            suggestions = []
            for row in c.fetchall():
                current = row[3]
                min_level = row[4] or row[5] or 0
                suggested_qty = row[10] or min_level * 2

                # Calculate days until stockout
                avg_usage = abs(row[9]) if row[9] else 0
                days_remaining = int(current / avg_usage) if avg_usage > 0 else None

                suggestions.append({
                    'id': row[0],
                    'name': row[1],
                    'part_number': row[2],
                    'current_qty': current,
                    'min_level': min_level,
                    'reorder_point': row[5],
                    'supplier': row[7],
                    'cost_per_unit': float(row[8]) if row[8] else None,
                    'avg_daily_usage': abs(avg_usage),
                    'suggested_order_qty': int(suggested_qty),
                    'estimated_cost': int(suggested_qty) * float(row[8]) if row[8] else None,
                    'days_until_stockout': days_remaining,
                    'urgency': 'critical' if current == 0 else ('high' if days_remaining and days_remaining < 7 else 'normal')
                })
            return suggestions
        finally:
            release_connection(conn)

    @staticmethod
    def calculate_optimal_stock_levels(consumable_id: int, service_level: float = 0.95) -> Dict:
        """Calculate optimal min/max stock levels based on historical usage"""
        conn = get_connection()
        c = conn.cursor()
        try:
            # Get usage history
            c.execute("""
                SELECT ABS(quantity_change) as usage, transaction_date
                FROM inventory_transactions
                WHERE consumable_id = %s
                  AND transaction_type = 'usage'
                  AND transaction_date > CURRENT_DATE - INTERVAL '180 days'
                ORDER BY transaction_date
            """, (consumable_id,))

            usage_data = c.fetchall()

            if len(usage_data) < 10:
                return {'error': 'Insufficient usage history for optimization'}

            # Calculate statistics
            usages = [row[0] for row in usage_data]
            avg_usage = sum(usages) / len(usages)
            variance = sum((u - avg_usage) ** 2 for u in usages) / len(usages)
            std_dev = variance ** 0.5

            # Get lead time
            c.execute("""
                SELECT AVG(lead_time_days) FROM supplier_items WHERE consumable_id = %s
            """, (consumable_id,))
            lead_time = c.fetchone()[0] or 7  # Default 7 days

            # Calculate safety stock using statistical method
            # Z-score for service level (0.95 = 1.65)
            z_score = 1.65 if service_level == 0.95 else 2.33 if service_level == 0.99 else 1.28
            safety_stock = int(z_score * std_dev * (lead_time ** 0.5))

            # Calculate reorder point
            reorder_point = int(avg_usage * lead_time + safety_stock)

            # Calculate max level (EOQ-based)
            # Simplified: max = reorder point + typical order quantity
            c.execute("SELECT reorder_quantity FROM consumables WHERE id = %s", (consumable_id,))
            typical_order = c.fetchone()[0] or int(avg_usage * 30)  # Default 30 days supply
            max_level = reorder_point + typical_order

            return {
                'consumable_id': consumable_id,
                'avg_daily_usage': round(avg_usage, 2),
                'usage_std_dev': round(std_dev, 2),
                'lead_time_days': lead_time,
                'service_level': service_level,
                'recommended_safety_stock': safety_stock,
                'recommended_reorder_point': reorder_point,
                'recommended_min_level': reorder_point,
                'recommended_max_level': max_level,
                'recommended_order_qty': typical_order,
                'data_points': len(usage_data)
            }
        finally:
            release_connection(conn)


def get_inventory_dashboard_stats() -> Dict:
    """Get comprehensive inventory dashboard statistics"""
    conn = get_connection()
    c = conn.cursor()
    try:
        stats = {}

        # Total items and value
        c.execute("""
            SELECT COUNT(*),
                   COALESCE(SUM(quantity), 0),
                   COALESCE(SUM(quantity * cost_per_unit), 0)
            FROM consumables WHERE active = TRUE
        """)
        row = c.fetchone()
        stats['total_items'] = row[0]
        stats['total_units'] = row[1]
        stats['total_value'] = float(row[2])

        # Stock status counts
        c.execute("""
            SELECT
                COUNT(*) FILTER (WHERE quantity = 0) as out_of_stock,
                COUNT(*) FILTER (WHERE quantity > 0 AND quantity <= COALESCE(minimum_stock_level, reorder_threshold, 0)) as low_stock,
                COUNT(*) FILTER (WHERE maximum_stock_level IS NOT NULL AND quantity > maximum_stock_level) as overstocked,
                COUNT(*) FILTER (WHERE quantity > COALESCE(minimum_stock_level, reorder_threshold, 0)
                                  AND (maximum_stock_level IS NULL OR quantity <= maximum_stock_level)) as healthy
            FROM consumables WHERE active = TRUE
        """)
        row = c.fetchone()
        stats['out_of_stock'] = row[0]
        stats['low_stock'] = row[1]
        stats['overstocked'] = row[2]
        stats['healthy_stock'] = row[3]

        # Recent activity
        c.execute("""
            SELECT COUNT(*) FROM inventory_transactions
            WHERE transaction_date > CURRENT_DATE - INTERVAL '7 days'
        """)
        stats['transactions_this_week'] = c.fetchone()[0]

        # Parts kits count
        c.execute("SELECT COUNT(*) FROM parts_kits WHERE is_active = TRUE")
        stats['active_kits'] = c.fetchone()[0]

        # Pending reorders
        c.execute("""
            SELECT COUNT(*) FROM consumables
            WHERE active = TRUE
              AND quantity <= COALESCE(reorder_threshold, minimum_stock_level, 0)
              AND COALESCE(reorder_threshold, minimum_stock_level, 0) > 0
        """)
        stats['items_need_reorder'] = c.fetchone()[0]

        # Value by category
        c.execute("""
            SELECT category, SUM(quantity * cost_per_unit) as value
            FROM consumables
            WHERE active = TRUE AND cost_per_unit IS NOT NULL
            GROUP BY category
            ORDER BY value DESC
            LIMIT 10
        """)
        stats['value_by_category'] = {row[0]: float(row[1]) for row in c.fetchall()}

        return stats
    finally:
        release_connection(conn)