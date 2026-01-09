"""
Monitoring and maintenance functions for inventory system
"""

import logging
import json
from datetime import datetime, timedelta
from typing import Dict, List, Any
from services.db import db_connection, db_transaction, get_connection, release_connection
from services.email_service import send_notification
import schedule
import time
import threading

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class InventoryMonitoring:
    """Monitoring service for inventory system health and alerts"""
    
    def __init__(self):
        self.monitoring_active = False
        self.monitoring_thread = None
    
    def start_monitoring(self):
        """Start the monitoring service"""
        if self.monitoring_active:
            logger.warning("Monitoring already active")
            return
        
        self.monitoring_active = True
        
        # Schedule monitoring tasks
        schedule.every(1).hours.do(self.check_stock_levels)
        schedule.every(4).hours.do(self.check_system_health)
        schedule.every().day.at("08:00").do(self.generate_daily_report)
        schedule.every().monday.at("09:00").do(self.generate_weekly_report)
        schedule.every().day.at("02:00").do(self.cleanup_old_data)
        
        # Start monitoring thread
        self.monitoring_thread = threading.Thread(target=self._run_scheduler, daemon=True)
        self.monitoring_thread.start()
        
        logger.info("Inventory monitoring service started")
    
    def stop_monitoring(self):
        """Stop the monitoring service"""
        self.monitoring_active = False
        schedule.clear()
        logger.info("Inventory monitoring service stopped")
    
    def _run_scheduler(self):
        """Run the scheduler in a separate thread"""
        while self.monitoring_active:
            schedule.run_pending()
            time.sleep(60)  # Check every minute
    
    def check_stock_levels(self):
        """Check for stock level issues and create alerts"""
        try:
            conn = get_connection()
            c = conn.cursor()
            
            # Check for out of stock items
            c.execute("""
                SELECT id, name, quantity, unit, reorder_threshold, minimum_stock_level
                FROM consumables 
                WHERE active = TRUE 
                AND quantity = 0
                AND NOT EXISTS (
                    SELECT 1 FROM inventory_alerts 
                    WHERE consumable_id = consumables.id 
                    AND alert_type = 'out_of_stock'
                    AND created_at > CURRENT_DATE - INTERVAL '1 day'
                )
            """)
            
            out_of_stock = c.fetchall()
            for item in out_of_stock:
                self._create_stock_alert(item[0], item[1], 'out_of_stock', 'critical',
                                       f"CRITICAL: {item[1]} is completely out of stock")
            
            # Check for low stock items
            c.execute("""
                SELECT id, name, quantity, unit, reorder_threshold, minimum_stock_level
                FROM consumables 
                WHERE active = TRUE 
                AND quantity > 0
                AND quantity <= COALESCE(minimum_stock_level, reorder_threshold, 0)
                AND COALESCE(minimum_stock_level, reorder_threshold, 0) > 0
                AND NOT EXISTS (
                    SELECT 1 FROM inventory_alerts 
                    WHERE consumable_id = consumables.id 
                    AND alert_type = 'low_stock'
                    AND created_at > CURRENT_DATE - INTERVAL '1 day'
                )
            """)
            
            low_stock = c.fetchall()
            for item in low_stock:
                threshold = item[5] or item[4]  # min_stock_level or reorder_threshold
                self._create_stock_alert(item[0], item[1], 'low_stock', 'warning',
                                       f"LOW STOCK: {item[1]} has {item[2]} {item[3]} remaining (threshold: {threshold})")
            
            # Check for overstocked items
            c.execute("""
                SELECT id, name, quantity, unit, maximum_stock_level
                FROM consumables 
                WHERE active = TRUE 
                AND maximum_stock_level IS NOT NULL
                AND quantity > maximum_stock_level
                AND NOT EXISTS (
                    SELECT 1 FROM inventory_alerts 
                    WHERE consumable_id = consumables.id 
                    AND alert_type = 'overstocked'
                    AND created_at > CURRENT_DATE - INTERVAL '7 days'
                )
            """)
            
            overstocked = c.fetchall()
            for item in overstocked:
                self._create_stock_alert(item[0], item[1], 'overstocked', 'info',
                                       f"OVERSTOCKED: {item[1]} has {item[2]} {item[3]} (max: {item[4]})")
            
            release_connection(conn)
            
            total_alerts = len(out_of_stock) + len(low_stock) + len(overstocked)
            if total_alerts > 0:
                logger.info(f"Created {total_alerts} stock level alerts")
            
        except Exception as e:
            logger.error(f"Error checking stock levels: {str(e)}")
    
    def check_system_health(self):
        """Check overall system health"""
        health_report = {
            'timestamp': datetime.now(),
            'database_status': 'unknown',
            'total_items': 0,
            'active_items': 0,
            'recent_transactions': 0,
            'pending_alerts': 0,
            'issues': []
        }
        
        try:
            conn = get_connection()
            c = conn.cursor()
            health_report['database_status'] = 'connected'
            
            # Get basic counts
            c.execute("SELECT COUNT(*) FROM consumables")
            health_report['total_items'] = c.fetchone()[0]
            
            c.execute("SELECT COUNT(*) FROM consumables WHERE active = TRUE")
            health_report['active_items'] = c.fetchone()[0]
            
            # Check recent transactions
            c.execute("""
                SELECT COUNT(*) FROM inventory_transactions 
                WHERE transaction_date > CURRENT_DATE - INTERVAL '24 hours'
            """)
            health_report['recent_transactions'] = c.fetchone()[0]
            
            # Check pending alerts
            c.execute("""
                SELECT COUNT(*) FROM inventory_alerts 
                WHERE acknowledged = FALSE
            """)
            health_report['pending_alerts'] = c.fetchone()[0]
            
            # Check for data integrity issues
            c.execute("""
                SELECT COUNT(*) FROM consumables 
                WHERE active = TRUE AND (name IS NULL OR name = '')
            """)
            unnamed_items = c.fetchone()[0]
            if unnamed_items > 0:
                health_report['issues'].append(f"{unnamed_items} items missing names")
            
            c.execute("""
                SELECT COUNT(*) FROM consumables 
                WHERE active = TRUE AND quantity < 0
            """)
            negative_quantities = c.fetchone()[0]
            if negative_quantities > 0:
                health_report['issues'].append(f"{negative_quantities} items with negative quantities")
            
            # Check for orphaned data
            c.execute("""
                SELECT COUNT(*) FROM supplier_items si
                WHERE NOT EXISTS (SELECT 1 FROM consumables c WHERE c.id = si.consumable_id)
            """)
            orphaned_suppliers = c.fetchone()[0]
            if orphaned_suppliers > 0:
                health_report['issues'].append(f"{orphaned_suppliers} orphaned supplier records")
            
            release_connection(conn)
            
            # Log health status
            if health_report['issues']:
                logger.warning(f"System health issues detected: {'; '.join(health_report['issues'])}")
            else:
                logger.info("System health check passed")
            
            return health_report
            
        except Exception as e:
            health_report['database_status'] = f'error: {str(e)}'
            health_report['issues'].append(f"Database connection failed: {str(e)}")
            logger.error(f"System health check failed: {str(e)}")
            return health_report
    
    def generate_daily_report(self):
        """Generate daily inventory summary report"""
        try:
            conn = get_connection()
            c = conn.cursor()
            
            # Get daily statistics
            c.execute("""
                SELECT 
                    COUNT(*) as total_items,
                    COUNT(CASE WHEN quantity = 0 THEN 1 END) as out_of_stock,
                    COUNT(CASE WHEN quantity <= COALESCE(minimum_stock_level, reorder_threshold, 0) 
                               AND quantity > 0 THEN 1 END) as low_stock,
                    SUM(CASE WHEN cost_per_unit IS NOT NULL THEN quantity * cost_per_unit ELSE 0 END) as total_value
                FROM consumables 
                WHERE active = TRUE
            """)
            
            stats = c.fetchone()
            
            # Get recent transactions
            c.execute("""
                SELECT transaction_type, COUNT(*), SUM(ABS(quantity_change))
                FROM inventory_transactions 
                WHERE transaction_date >= CURRENT_DATE - INTERVAL '1 day'
                GROUP BY transaction_type
                ORDER BY COUNT(*) DESC
            """)
            
            transactions = c.fetchall()
            
            # Get top consumed items
            c.execute("""
                SELECT c.name, SUM(ABS(it.quantity_change)) as total_consumed
                FROM inventory_transactions it
                JOIN consumables c ON it.consumable_id = c.id
                WHERE it.transaction_date >= CURRENT_DATE - INTERVAL '1 day'
                AND it.quantity_change < 0
                GROUP BY c.name
                ORDER BY total_consumed DESC
                LIMIT 10
            """)
            
            top_consumed = c.fetchall()
            
            release_connection(conn)
            
            # Format report
            report = f"""
Daily Inventory Report - {datetime.now().strftime('%Y-%m-%d')}

SUMMARY:
- Total Active Items: {stats[0]}
- Out of Stock: {stats[1]}
- Low Stock: {stats[2]}
- Total Inventory Value: ${stats[3]:,.2f}

RECENT ACTIVITY (Last 24 hours):
"""
            
            for trans_type, count, total_qty in transactions:
                report += f"- {trans_type.title()}: {count} transactions, {total_qty} total quantity\n"
            
            if top_consumed:
                report += "\nTOP CONSUMED ITEMS:\n"
                for name, consumed in top_consumed:
                    report += f"- {name}: {consumed} units\n"
            
            # Send report if there are significant changes
            if stats[1] > 0 or stats[2] > 5 or sum(t[1] for t in transactions) > 10:
                try:
                    from config import NOTIFICATION_RECIPIENTS
                    if NOTIFICATION_RECIPIENTS:
                        send_notification(
                            "Daily Inventory Report", 
                            report, 
                            NOTIFICATION_RECIPIENTS
                        )
                except Exception as e:
                    logger.error(f"Failed to send daily report: {str(e)}")
            
            logger.info("Daily inventory report generated")
            
        except Exception as e:
            logger.error(f"Error generating daily report: {str(e)}")
    
    def generate_weekly_report(self):
        """Generate comprehensive weekly inventory report"""
        try:
            conn = get_connection()
            c = conn.cursor()
            
            # Weekly trend analysis
            c.execute("""
                SELECT 
                    DATE(transaction_date) as day,
                    transaction_type,
                    COUNT(*) as count,
                    SUM(ABS(quantity_change)) as total_quantity
                FROM inventory_transactions 
                WHERE transaction_date >= CURRENT_DATE - INTERVAL '7 days'
                GROUP BY DATE(transaction_date), transaction_type
                ORDER BY day DESC, count DESC
            """)
            
            weekly_trends = c.fetchall()
            
            # Items needing attention
            c.execute("""
                SELECT name, quantity, unit, reorder_threshold, minimum_stock_level,
                       CASE 
                           WHEN quantity = 0 THEN 'OUT OF STOCK'
                           WHEN quantity <= COALESCE(minimum_stock_level, reorder_threshold, 0) THEN 'LOW STOCK'
                           ELSE 'OK'
                       END as status
                FROM consumables 
                WHERE active = TRUE
                AND (quantity = 0 OR quantity <= COALESCE(minimum_stock_level, reorder_threshold, 0))
                ORDER BY 
                    CASE 
                        WHEN quantity = 0 THEN 1
                        ELSE 2
                    END,
                    quantity ASC
                LIMIT 20
            """)
            
            attention_items = c.fetchall()
            
            release_connection(conn)
            
            # Create comprehensive report
            report = f"""
Weekly Inventory Report - {datetime.now().strftime('%Y-%m-%d')}

WEEKLY ACTIVITY TRENDS:
"""
            
            for day, trans_type, count, total_qty in weekly_trends[:10]:
                report += f"- {day}: {trans_type} - {count} transactions, {total_qty} units\n"
            
            if attention_items:
                report += f"\nITEMS REQUIRING ATTENTION ({len(attention_items)}):\n"
                for name, qty, unit, reorder_thresh, min_level, status in attention_items:
                    threshold = reorder_thresh or min_level or 0
                    report += f"- {status}: {name} - {qty} {unit} (threshold: {threshold})\n"
            
            report += "\n" + "="*50 + "\nGenerated by Potelco Inventory System\n"
            
            # Always send weekly report
            try:
                from config import NOTIFICATION_RECIPIENTS
                if NOTIFICATION_RECIPIENTS:
                    send_notification(
                        "Weekly Inventory Report", 
                        report, 
                        NOTIFICATION_RECIPIENTS
                    )
            except Exception as e:
                logger.error(f"Failed to send weekly report: {str(e)}")
            
            logger.info("Weekly inventory report generated")
            
        except Exception as e:
            logger.error(f"Error generating weekly report: {str(e)}")
    
    def cleanup_old_data(self):
        """Clean up old data to maintain performance"""
        try:
            conn = get_connection()
            c = conn.cursor()
            
            # Archive old acknowledged alerts
            c.execute("""
                DELETE FROM inventory_alerts 
                WHERE acknowledged = TRUE 
                AND acknowledged_at < CURRENT_DATE - INTERVAL '90 days'
            """)
            deleted_alerts = c.rowcount
            
            # Archive very old transactions (keep 2 years)
            c.execute("""
                DELETE FROM inventory_transactions 
                WHERE transaction_date < CURRENT_DATE - INTERVAL '2 years'
            """)
            deleted_transactions = c.rowcount
            
            # Clean up orphaned supplier items
            c.execute("""
                DELETE FROM supplier_items 
                WHERE consumable_id NOT IN (SELECT id FROM consumables WHERE active = TRUE)
            """)
            deleted_suppliers = c.rowcount
            
            conn.commit()
            release_connection(conn)
            
            if deleted_alerts > 0 or deleted_transactions > 0 or deleted_suppliers > 0:
                logger.info(f"Cleanup completed: {deleted_alerts} alerts, {deleted_transactions} transactions, {deleted_suppliers} supplier records")
            
        except Exception as e:
            logger.error(f"Error during cleanup: {str(e)}")
    
    def _create_stock_alert(self, item_id: int, item_name: str, alert_type: str, 
                           alert_level: str, message: str):
        """Create a stock-related alert"""
        try:
            from services.inventory_service import create_inventory_alert
            create_inventory_alert(item_id, alert_type, alert_level, message)
            if alert_level == 'critical':
                from config import NOTIFICATION_RECIPIENTS
                if NOTIFICATION_RECIPIENTS:
                    send_notification("Critical Inventory Alert", message, NOTIFICATION_RECIPIENTS)
        except Exception as e:
            logger.error(f"Failed to create alert for item {item_id}: {str(e)}")

# Data backup and restore functionality
class InventoryBackup:
    """Backup and restore functionality for inventory data"""
    
    @staticmethod
    def create_backup() -> Dict[str, Any]:
        """Create a comprehensive backup of inventory data"""
        try:
            conn = get_connection()
            c = conn.cursor()
            
            backup_data = {
                'created_at': datetime.now().isoformat(),
                'version': '1.0',
                'tables': {}
            }
            
            # Tables to backup
            tables = [
                'consumables', 'supplier_items', 'item_templates', 
                'inventory_transactions', 'inventory_alerts'
            ]
            
            for table in tables:
                try:
                    c.execute(f"SELECT * FROM {table}")
                    columns = [desc[0] for desc in c.description]
                    rows = c.fetchall()
                    
                    backup_data['tables'][table] = {
                        'columns': columns,
                        'rows': [list(row) for row in rows]
                    }
                    
                except Exception as e:
                    logger.warning(f"Could not backup table {table}: {str(e)}")
            
            release_connection(conn)
            
            return backup_data
            
        except Exception as e:
            logger.error(f"Backup creation failed: {str(e)}")
            return {'error': str(e)}
    
    @staticmethod
    def restore_from_backup(backup_data: Dict[str, Any], restore_mode: str = 'merge') -> bool:
        """Restore inventory data from backup
        
        Args:
            backup_data: Backup data dictionary
            restore_mode: 'replace' (delete existing) or 'merge' (keep existing)
        """
        try:
            conn = get_connection()
            c = conn.cursor()
            
            if restore_mode == 'replace':
                # Clear existing data (in reverse dependency order)
                clear_tables = [
                    'inventory_alerts', 'inventory_transactions', 
                    'supplier_items', 'consumables'
                ]
                
                for table in clear_tables:
                    try:
                        c.execute(f"DELETE FROM {table}")
                    except Exception as e:
                        logger.warning(f"Could not clear table {table}: {str(e)}")
            
            # Restore data
            for table_name, table_data in backup_data.get('tables', {}).items():
                columns = table_data['columns']
                rows = table_data['rows']
                
                if not rows:
                    continue
                
                # Create placeholders for SQL
                placeholders = ', '.join(['%s'] * len(columns))
                column_names = ', '.join(columns)
                
                if restore_mode == 'merge':
                    # Use ON CONFLICT for merge mode
                    if table_name == 'consumables':
                        conflict_resolution = "ON CONFLICT (internal_sku) DO NOTHING"
                    else:
                        conflict_resolution = "ON CONFLICT DO NOTHING"
                else:
                    conflict_resolution = ""
                
                insert_sql = f"""
                    INSERT INTO {table_name} ({column_names}) 
                    VALUES ({placeholders})
                    {conflict_resolution}
                """
                
                try:
                    c.executemany(insert_sql, rows)
                except Exception as e:
                    logger.error(f"Failed to restore table {table_name}: {str(e)}")
                    continue
            
            conn.commit()
            release_connection(conn)
            
            logger.info(f"Backup restore completed in {restore_mode} mode")
            return True
            
        except Exception as e:
            logger.error(f"Backup restore failed: {str(e)}")
            return False

# Performance monitoring
class InventoryPerformanceMonitor:
    """Monitor inventory system performance and optimization"""
    
    @staticmethod
    def analyze_database_performance():
        """Analyze database query performance and suggest optimizations"""
        try:
            conn = get_connection()
            c = conn.cursor()
            
            performance_report = {
                'timestamp': datetime.now(),
                'table_sizes': {},
                'index_usage': {},
                'slow_queries': [],
                'recommendations': []
            }
            
            # Get table sizes
            c.execute("""
                SELECT 
                    schemaname,
                    tablename,
                    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size,
                    pg_total_relation_size(schemaname||'.'||tablename) as bytes
                FROM pg_tables 
                WHERE schemaname = 'public'
                AND tablename IN ('consumables', 'inventory_transactions', 'supplier_items')
                ORDER BY bytes DESC
            """)
            
            for schema, table, size, bytes_size in c.fetchall():
                performance_report['table_sizes'][table] = {
                    'formatted_size': size,
                    'bytes': bytes_size
                }
            
            # Check for missing indexes
            c.execute("""
                SELECT tablename, attname, n_distinct, correlation
                FROM pg_stats 
                WHERE schemaname = 'public' 
                AND tablename IN ('consumables', 'inventory_transactions')
                AND n_distinct > 100
                ORDER BY n_distinct DESC
            """)
            
            high_cardinality_columns = c.fetchall()
            
            # Analyze query patterns (simplified)
            c.execute("""
                SELECT COUNT(*) as consumables_count FROM consumables;
            """)
            consumables_count = c.fetchone()[0]
            
            c.execute("""
                SELECT COUNT(*) as transactions_count FROM inventory_transactions;
            """)
            transactions_count = c.fetchone()[0]
            
            # Generate recommendations
            if consumables_count > 10000:
                performance_report['recommendations'].append(
                    "Consider partitioning consumables table by category"
                )
            
            if transactions_count > 100000:
                performance_report['recommendations'].append(
                    "Consider archiving old transaction records"
                )
            
            release_connection(conn)
            return performance_report
            
        except Exception as e:
            logger.error(f"Performance analysis failed: {str(e)}")
            return {'error': str(e)}

# Initialize monitoring service
monitoring_service = InventoryMonitoring()

# Export functions for use in routes
def start_monitoring():
    """Start inventory monitoring"""
    monitoring_service.start_monitoring()

def stop_monitoring():
    """Stop inventory monitoring"""
    monitoring_service.stop_monitoring()

def get_system_health():
    """Get current system health status"""
    return monitoring_service.check_system_health()

def create_manual_backup():
    """Create manual backup"""
    return InventoryBackup.create_backup()

def restore_manual_backup(backup_data, mode='merge'):
    """Restore from manual backup"""
    return InventoryBackup.restore_from_backup(backup_data, mode)

def analyze_performance():
    """Analyze system performance"""
    return InventoryPerformanceMonitor.analyze_database_performance()