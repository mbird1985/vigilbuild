# routes/integrations.py
from flask import Blueprint, redirect, request, render_template, session, url_for, flash, jsonify
from flask_login import login_required, current_user
from config import OUTLOOK_CLIENT_ID, OUTLOOK_CLIENT_SECRET, OUTLOOK_AUTHORITY, OUTLOOK_REDIRECT_URI, OUTLOOK_SCOPES
from services.logging_service import log_audit
from services.elasticsearch_client import es
from services.db import get_connection, release_connection
import msal
import json
import requests
from datetime import datetime

integration_bp = Blueprint("integration", __name__, url_prefix="/integrations")

def get_integration_status():
    """Check which integrations are currently connected"""
    conn = get_connection()
    c = conn.cursor()
    
    # Check if integrations table exists, create if not
    c.execute("""
        CREATE TABLE IF NOT EXISTS integrations (
            id SERIAL PRIMARY KEY,
            user_id INTEGER,
            system_type TEXT NOT NULL,
            system_name TEXT,
            config JSONB,
            enabled BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users(id)
        )
    """)
    
    c.execute("""
        SELECT system_type, enabled FROM integrations 
        WHERE user_id = %s AND enabled = TRUE
    """, (current_user.id,))
    
    active_integrations = {row[0]: row[1] for row in c.fetchall()}
    release_connection(conn)
    
    return {
        'outlook_connected': 'outlook' in active_integrations,
        'powerbi_connected': 'powerbi' in active_integrations,
        'salesforce_connected': 'salesforce' in active_integrations
    }

@integration_bp.route("/")
@login_required
def integrate():
    """Main integrations page showing available systems"""
    integration_status = get_integration_status()
    
    # Get custom integrations
    conn = get_connection()
    c = conn.cursor()
    c.execute("""
        SELECT system_type, system_name, config FROM integrations 
        WHERE user_id = %s AND system_type NOT IN ('outlook', 'powerbi', 'salesforce')
        AND enabled = TRUE
        ORDER BY system_name
    """, (current_user.id,))
    
    custom_integrations = []
    for row in c.fetchall():
        system_type, system_name, config = row
        config_dict = json.loads(config) if isinstance(config, str) else config
        
        # Generate a simple icon based on system name
        icon_svg = f"""
        <div class="w-10 h-10 bg-gradient-to-br from-blue-400 to-purple-500 rounded-lg flex items-center justify-center">
            <span class="text-white font-bold text-sm">{system_name[:2].upper()}</span>
        </div>
        """
        
        custom_integrations.append({
            'system_type': system_type,
            'system_name': system_name,
            'description': config_dict.get('description', 'Custom API integration'),
            'icon_svg': icon_svg
        })
    
    # Fetch all integrations for management table
    c.execute(
        """
        SELECT id, system_type, system_name, enabled, created_at
        FROM integrations
        WHERE user_id = %s
        ORDER BY created_at DESC
        """,
        (current_user.id,)
    )
    all_integrations = [
        {
            'id': r[0],
            'system_type': r[1],
            'system_name': r[2],
            'enabled': bool(r[3]),
            'created_at': r[4]
        }
        for r in c.fetchall()
    ]

    release_connection(conn)

    return render_template(
        "integrate.html",
        custom_integrations=custom_integrations,
        all_integrations=all_integrations,
        **integration_status,
    )

@integration_bp.route("/configure/<system>")
@login_required
def configure_integration(system):
    """Configuration page for specific integration system"""
    valid_systems = ['outlook', 'powerbi', 'salesforce', 'custom', 'gps']
    if system not in valid_systems:
        flash("Invalid integration system.")
        return redirect(url_for('integration.integrate'))
    
    # Get existing configuration if any
    conn = get_connection()
    c = conn.cursor()
    name = request.args.get('name') if system == 'custom' else None
    if system == 'custom' and name:
        c.execute(
            """
            SELECT config FROM integrations
            WHERE user_id = %s AND system_type = %s AND system_name = %s
            """,
            (current_user.id, system, name),
        )
    else:
        c.execute(
            """
            SELECT config FROM integrations
            WHERE user_id = %s AND system_type = %s
            """,
            (current_user.id, system),
        )
    
    existing_config = None
    row = c.fetchone()
    if row:
        existing_config = row[0]
    
    release_connection(conn)
    
    system_names = {
        'outlook': 'Microsoft Outlook',
        'powerbi': 'Power BI',
        'salesforce': 'Salesforce',
        'custom': 'Custom System',
        'gps': 'GPS/Telematics Provider'
    }

    return render_template(
        "configure_integration.html",
        system=system,
        system_name=system_names[system],
        existing_config=existing_config,
        existing_name=name,
    )

@integration_bp.route("/configure/<system>", methods=["POST"])
@login_required
def save_integration_config(system):
    """Save integration configuration"""
    action = request.form.get('action', 'create')
    
    try:
        config = {}
        
        # Build configuration based on system type
        if system == 'outlook':
            config = {
                'client_id': request.form.get('client_id'),
                'client_secret': request.form.get('client_secret'),
                'tenant_id': request.form.get('tenant_id'),
                'scopes': ['https://graph.microsoft.com/Mail.Read', 'https://graph.microsoft.com/Calendars.Read']
            }
        elif system == 'powerbi':
            config = {
                'client_id': request.form.get('client_id'),
                'client_secret': request.form.get('client_secret'),
                'workspace_id': request.form.get('workspace_id')
            }
        elif system == 'salesforce':
            config = {
                'instance_url': request.form.get('instance_url'),
                'client_id': request.form.get('client_id'),
                'client_secret': request.form.get('client_secret'),
                'username': request.form.get('username'),
                'password': request.form.get('password')
            }
        else:  # custom
            connection_type = request.form.get('connection_type', 'api')
            config = {
                'system_name': request.form.get('system_name'),
                'connection_type': connection_type,
            }
            if connection_type == 'api':
                config.update({
                    'api_url': request.form.get('api_url'),
                    'auth_type': request.form.get('auth_type'),
                    'api_key': request.form.get('api_key'),
                    'api_key_header': request.form.get('api_key_header', 'X-API-Key'),
                    'extra_headers': request.form.get('extra_headers') or '{}',
                    'extra_params': request.form.get('extra_params') or '{}',
                })
            elif connection_type == 'oauth2':
                config.update({
                    'api_url': request.form.get('api_url'),
                    'token_url': request.form.get('token_url'),
                    'client_id': request.form.get('client_id'),
                    'client_secret': request.form.get('client_secret'),
                    'scope': request.form.get('scope'),
                    'grant_type': request.form.get('grant_type', 'client_credentials'),
                    'auth_url': request.form.get('auth_url'),
                    'redirect_uri': request.form.get('redirect_uri'),
                })
            elif connection_type == 'webhook':
                config.update({
                    'webhook_url': request.form.get('webhook_url'),
                    'webhook_secret': request.form.get('webhook_secret'),
                    'signature_header': request.form.get('signature_header', 'X-Signature'),
                    'verify_token': request.form.get('verify_token'),
                    'events': request.form.get('events'),
                    'method': request.form.get('method', 'POST'),
                })
        
        config['description'] = request.form.get('description', '')
        enabled = 'enabled' in request.form
        
        conn = get_connection()
        c = conn.cursor()
        
        if action == 'test':
            # Test the connection
            success, message = test_integration_connection(system, config)
            flash(f"Connection test: {message}")
            if system == 'custom' and config.get('system_name'):
                return redirect(url_for('integration.configure_integration', system=system, name=config['system_name']))
            return redirect(url_for('integration.configure_integration', system=system))
        
        elif action == 'delete':
            if system == 'custom':
                target_name = request.form.get('existing_name') or request.form.get('system_name')
                c.execute(
                    """
                    DELETE FROM integrations
                    WHERE user_id = %s AND system_type = %s AND system_name = %s
                    """,
                    (current_user.id, system, target_name),
                )
            else:
                c.execute(
                    """
                    DELETE FROM integrations
                    WHERE user_id = %s AND system_type = %s
                    """,
                    (current_user.id, system),
                )
            flash(f"{system.title()} integration deleted successfully!")
            
        else:  # create or update
            # Check if integration already exists
            if system == 'custom':
                system_name = request.form.get('system_name')
                existing_name = request.form.get('existing_name') or system_name
                c.execute(
                    """
                    SELECT id FROM integrations
                    WHERE user_id = %s AND system_type = %s AND system_name = %s
                    """,
                    (current_user.id, system, existing_name),
                )
                row = c.fetchone()
            else:
                c.execute(
                    """
                    SELECT id FROM integrations
                    WHERE user_id = %s AND system_type = %s
                    """,
                    (current_user.id, system),
                )
                row = c.fetchone()

            if row:
                # Update existing
                if system == 'custom':
                    c.execute(
                        """
                        UPDATE integrations
                        SET system_name = %s, config = %s, enabled = %s, updated_at = CURRENT_TIMESTAMP
                        WHERE user_id = %s AND system_type = %s AND system_name = %s
                        """,
                        (
                            system_name,
                            json.dumps(config),
                            enabled,
                            current_user.id,
                            system,
                            existing_name,
                        ),
                    )
                else:
                    c.execute(
                        """
                        UPDATE integrations
                        SET config = %s, enabled = %s, updated_at = CURRENT_TIMESTAMP
                        WHERE user_id = %s AND system_type = %s
                        """,
                        (json.dumps(config), enabled, current_user.id, system),
                    )
                flash(f"{system.title()} integration updated successfully!")
            else:
                # Create new
                c.execute(
                    """
                    INSERT INTO integrations (user_id, system_type, system_name, config, enabled)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (
                        current_user.id,
                        system,
                        config.get('system_name', system.title()),
                        json.dumps(config),
                        enabled,
                    ),
                )
                flash(f"{system.title()} integration created successfully!")
        
        conn.commit()
        release_connection(conn)
        
        log_audit(es, f"integration_{action}", current_user.id, None, {
            "system": system, "action": action
        })
        
        return redirect(url_for('integration.integrate'))
        
    except Exception as e:
        flash(f"Error saving integration: {str(e)}")
        return redirect(url_for('integration.configure_integration', system=system))

def test_integration_connection(system, config):
    """Test connection to integration system"""
    try:
        if system == 'outlook':
            # Test Microsoft Graph connection
            if not all([config.get('client_id'), config.get('client_secret')]):
                return False, "Missing required credentials"
            
            auth_app = msal.ConfidentialClientApplication(
                config['client_id'],
                authority=OUTLOOK_AUTHORITY,
                client_credential=config['client_secret']
            )
            
            # Try to get app-only token for testing
            result = auth_app.acquire_token_for_client(scopes=['https://graph.microsoft.com/.default'])
            if 'access_token' in result:
                return True, "Successfully connected to Microsoft Graph"
            else:
                return False, f"Failed to authenticate: {result.get('error_description', 'Unknown error')}"
                
        elif system == 'powerbi':
            # Test Power BI connection
            if not all([config.get('client_id'), config.get('client_secret'), config.get('workspace_id')]):
                return False, "Missing required credentials"
            return True, "Power BI connection configured (full test requires user authentication)"
            
        elif system == 'salesforce':
            # Test Salesforce connection
            if not all([config.get('instance_url'), config.get('client_id'), 
                       config.get('client_secret'), config.get('username'), config.get('password')]):
                return False, "Missing required credentials"
            
            # Try to authenticate with Salesforce
            auth_url = f"{config['instance_url']}/services/oauth2/token"
            auth_data = {
                'grant_type': 'password',
                'client_id': config['client_id'],
                'client_secret': config['client_secret'],
                'username': config['username'],
                'password': config['password']
            }
            
            response = requests.post(auth_url, data=auth_data)
            if response.status_code == 200:
                return True, "Successfully connected to Salesforce"
            else:
                return False, f"Salesforce authentication failed: {response.text}"
                
        else:  # custom
            # Test custom API connection
            if not all([config.get('api_url'), config.get('api_key')]):
                return False, "Missing required API URL or key"
            
            headers = {}
            auth_type = config.get('auth_type', 'api_key')
            api_key_header = config.get('api_key_header', 'X-API-Key')
            
            if auth_type == 'api_key':
                headers[api_key_header] = config['api_key']
            elif auth_type == 'bearer_token':
                headers['Authorization'] = f"Bearer {config['api_key']}"
            elif auth_type == 'basic_auth':
                import base64
                credentials = base64.b64encode(config['api_key'].encode()).decode()
                headers['Authorization'] = f"Basic {credentials}"
            
            response = requests.get(config['api_url'], headers=headers, timeout=10)
            if response.status_code < 400:
                return True, f"Successfully connected to API (Status: {response.status_code})"
            else:
                return False, f"API connection failed (Status: {response.status_code})"
                
    except Exception as e:
        return False, f"Connection test failed: {str(e)}"

@integration_bp.route("/outlook_auth")
@login_required
def outlook_auth():
    """Start OAuth flow for Outlook (legacy route for compatibility)"""
    session["state"] = "secure_state"
    auth_app = msal.ConfidentialClientApplication(
        OUTLOOK_CLIENT_ID,
        authority=OUTLOOK_AUTHORITY,
        client_credential=OUTLOOK_CLIENT_SECRET
    )
    auth_url = auth_app.get_authorization_request_url(
        scopes=OUTLOOK_SCOPES,
        state=session["state"],
        redirect_uri=OUTLOOK_REDIRECT_URI
    )
    log_audit(es, "outlook_auth_start", current_user.id, None, {"scopes": OUTLOOK_SCOPES})
    return redirect(auth_url)

@integration_bp.route("/outlook_callback")
@login_required
def outlook_callback():
    """Handle OAuth callback for Outlook (legacy route for compatibility)"""
    if request.args.get("state") != session.get("state"):
        flash("Invalid state parameter.")
        log_audit(es, "outlook_auth_error", current_user.id, None, {"error": "Invalid state"})
        return redirect(url_for("integration.integrate"))

    code = request.args.get("code")
    if not code:
        flash("Authorization failed: No code provided.")
        log_audit(es, "outlook_auth_error", current_user.id, None, {"error": "No code provided"})
        return redirect(url_for("integration.integrate"))

    auth_app = msal.ConfidentialClientApplication(
        OUTLOOK_CLIENT_ID,
        authority=OUTLOOK_AUTHORITY,
        client_credential=OUTLOOK_CLIENT_SECRET
    )
    token = auth_app.acquire_token_by_authorization_code(
        code,
        scopes=OUTLOOK_SCOPES,
        redirect_uri=OUTLOOK_REDIRECT_URI
    )

    if "access_token" in token:
        session["access_token"] = token["access_token"]
        flash("Outlook integration successful!")
        log_audit(es, "outlook_auth_success", current_user.id, None, {"scopes": OUTLOOK_SCOPES})
    else:
        flash("Failed to authenticate with Microsoft.")
        log_audit(es, "outlook_auth_error", current_user.id, None, {"error": token.get("error_description", "Unknown error")})
    return redirect(url_for("integration.integrate"))

@integration_bp.route("/api/integrations")
@login_required
def api_get_integrations():
    """API endpoint to get user's integrations"""
    conn = get_connection()
    c = conn.cursor()
    c.execute("""
        SELECT system_type, system_name, enabled, created_at 
        FROM integrations 
        WHERE user_id = %s
        ORDER BY created_at DESC
    """, (current_user.id,))
    
    integrations = []
    for row in c.fetchall():
        integrations.append({
            'system_type': row[0],
            'system_name': row[1],
            'enabled': row[2],
            'created_at': row[3].isoformat() if row[3] else None
        })
    
    release_connection(conn)
    return jsonify(integrations)

@integration_bp.route("/webhook/<system_type>", methods=["POST"])
def handle_webhook(system_type):
    # Verify signature, process event
    pass