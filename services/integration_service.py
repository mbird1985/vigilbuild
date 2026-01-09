# services/integration_service.py
import requests
import json
from msal import ConfidentialClientApplication
from config import (
    OUTLOOK_CLIENT_ID, OUTLOOK_CLIENT_SECRET, OUTLOOK_AUTHORITY, 
    OUTLOOK_SCOPES, OUTLOOK_REDIRECT_URI
)
from flask import session
from services.db import get_connection, release_connection
from datetime import datetime, timedelta
import base64

# Redis client for token caching (optional - fallback to session if not available)
try:
    import redis
    redis_client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
except ImportError:
    redis_client = None  # Fallback to session storage if Redis not available

class IntegrationManager:
    def __init__(self, user_id):
        self.user_id = user_id
        self.integrations = self._load_user_integrations()
    
    def _load_user_integrations(self):
        """Load all integrations for the current user"""
        conn = get_connection()
        c = conn.cursor()
        c.execute("""
            SELECT system_type, config, enabled 
            FROM integrations 
            WHERE user_id = %s AND enabled = TRUE
        """, (self.user_id,))
        
        integrations = {}
        for row in c.fetchall():
            system_type, config, enabled = row
            if enabled:
                integrations[system_type] = json.loads(config) if isinstance(config, str) else config
        
        release_connection(conn)
        return integrations
    
    def get_integration(self, system_type):
        """Get specific integration configuration"""
        return self.integrations.get(system_type)
    
    def is_connected(self, system_type):
        """Check if a specific integration is connected"""
        return system_type in self.integrations
    
    def get_access_token(self, system_type):
        """Get cached access token for a system"""
        if redis_client:
            try:
                token = redis_client.get(f"token_{self.user_id}_{system_type}")
                if token and self.is_token_expired(token):
                    self.refresh_token(system_type)
                    token = redis_client.get(f"token_{self.user_id}_{system_type}") # Reload after refresh
                return token
            except:
                pass
        return session.get(f"{system_type}_token")
    
    def set_access_token(self, system_type, token, expires_in=3600):
        """Cache access token for a system"""
        if redis_client:
            try:
                redis_client.setex(f"token_{self.user_id}_{system_type}", expires_in, token)
                return
            except:
                pass
        session[f"{system_type}_token"] = token

    def is_token_expired(self, token):
        """Implement expiration check"""
        # This is a placeholder. In a real application, you'd parse the token
        # and check its expiration claim.
        return False

    def refresh_token(self, system_type):
        """Implement refresh logic for OAuth"""
        # This is a placeholder. In a real application, you'd implement
        # the OAuth refresh flow here.
        print(f"Placeholder refresh_token for {system_type}")

# Outlook Integration Functions
def build_outlook_msal_app(config=None):
    """Build MSAL app for Outlook integration"""
    if config:
        client_id = config.get('client_id', OUTLOOK_CLIENT_ID)
        client_secret = config.get('client_secret', OUTLOOK_CLIENT_SECRET)
        tenant_id = config.get('tenant_id', 'common')
        authority = f"https://login.microsoftonline.com/{tenant_id}"
    else:
        client_id = OUTLOOK_CLIENT_ID
        client_secret = OUTLOOK_CLIENT_SECRET
        authority = OUTLOOK_AUTHORITY
    
    return ConfidentialClientApplication(
        client_id,
        authority=authority,
        client_credential=client_secret
    )

def get_outlook_auth_url(user_id, config=None):
    """Get OAuth URL for Outlook authentication"""
    session["state"] = f"secure_state_{user_id}"
    app = build_outlook_msal_app(config)
    scopes = config.get('scopes', OUTLOOK_SCOPES) if config else OUTLOOK_SCOPES
    
    return app.get_authorization_request_url(
        scopes=scopes,
        state=session["state"],
        redirect_uri=OUTLOOK_REDIRECT_URI
    )

def get_outlook_token_from_code(code, config=None):
    """Exchange OAuth code for access token"""
    app = build_outlook_msal_app(config)
    scopes = config.get('scopes', OUTLOOK_SCOPES) if config else OUTLOOK_SCOPES
    
    try:
        result = app.acquire_token_by_authorization_code(
            code,
            scopes=scopes,
            redirect_uri=OUTLOOK_REDIRECT_URI
        )
        return result
    except Exception as e:
        print(f"MSAL token error: {e}")
        return None

def fetch_outlook_events(user_id, limit=10):
    """Fetch calendar events from Outlook"""
    integration_mgr = IntegrationManager(user_id)
    if not integration_mgr.is_connected('outlook'):
        return []
    
    token = integration_mgr.get_access_token('outlook')
    if not token:
        return []
    
    headers = {"Authorization": f"Bearer {token}"}
    graph_url = f"https://graph.microsoft.com/v1.0/me/events?$top={limit}"
    
    try:
        response = requests.get(graph_url, headers=headers)
        response.raise_for_status()
        return response.json().get("value", [])
    except requests.RequestException as e:
        print(f"Failed to fetch Outlook events: {e}")
        return []

def send_outlook_email(user_id, to_email, subject, body, is_html=False):
    """Send email via Outlook Graph API"""
    integration_mgr = IntegrationManager(user_id)
    if not integration_mgr.is_connected('outlook'):
        return False, "Outlook not connected"
    
    token = integration_mgr.get_access_token('outlook')
    if not token:
        return False, "No valid token"
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    email_data = {
        "message": {
            "subject": subject,
            "body": {
                "contentType": "HTML" if is_html else "Text",
                "content": body
            },
            "toRecipients": [
                {
                    "emailAddress": {
                        "address": to_email
                    }
                }
            ]
        }
    }
    
    try:
        response = requests.post(
            "https://graph.microsoft.com/v1.0/me/sendMail",
            headers=headers,
            json=email_data
        )
        response.raise_for_status()
        return True, "Email sent successfully"
    except requests.RequestException as e:
        return False, f"Failed to send email: {e}"

# Power BI Integration Functions
def get_powerbi_token(user_id):
    """Get Power BI access token"""
    integration_mgr = IntegrationManager(user_id)
    config = integration_mgr.get_integration('powerbi')
    if not config:
        return None
    
    # Check cached token first
    cached_token = integration_mgr.get_access_token('powerbi')
    if cached_token:
        return cached_token
    
    # Get new token
    auth_url = "https://login.microsoftonline.com/common/oauth2/v2.0/token"
    auth_data = {
        'grant_type': 'client_credentials',
        'client_id': config['client_id'],
        'client_secret': config['client_secret'],
        'scope': 'https://analysis.windows.net/powerbi/api/.default'
    }
    
    try:
        response = requests.post(auth_url, data=auth_data)
        response.raise_for_status()
        token_data = response.json()
        
        access_token = token_data['access_token']
        expires_in = token_data.get('expires_in', 3600)
        
        # Cache the token
        integration_mgr.set_access_token('powerbi', access_token, expires_in)
        return access_token
    except requests.RequestException as e:
        print(f"Failed to get Power BI token: {e}")
        return None

def get_powerbi_datasets(user_id):
    """Get Power BI datasets"""
    integration_mgr = IntegrationManager(user_id)
    config = integration_mgr.get_integration('powerbi')
    if not config:
        return []
    
    token = get_powerbi_token(user_id)
    if not token:
        return []
    
    headers = {"Authorization": f"Bearer {token}"}
    workspace_id = config['workspace_id']
    url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets"
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json().get("value", [])
    except requests.RequestException as e:
        print(f"Failed to fetch Power BI datasets: {e}")
        return []

# Salesforce Integration Functions
def get_salesforce_token(user_id):
    """Get Salesforce access token"""
    integration_mgr = IntegrationManager(user_id)
    config = integration_mgr.get_integration('salesforce')
    if not config:
        return None
    
    # Check cached token first
    cached_token = integration_mgr.get_access_token('salesforce')
    if cached_token:
        return cached_token
    
    # Get new token using username/password flow
    auth_url = f"{config['instance_url']}/services/oauth2/token"
    auth_data = {
        'grant_type': 'password',
        'client_id': config['client_id'],
        'client_secret': config['client_secret'],
        'username': config['username'],
        'password': config['password']  # Should include security token
    }
    
    try:
        response = requests.post(auth_url, data=auth_data)
        response.raise_for_status()
        token_data = response.json()
        
        access_token = token_data['access_token']
        # Salesforce tokens typically last 2 hours
        integration_mgr.set_access_token('salesforce', access_token, 7200)
        return access_token
    except requests.RequestException as e:
        print(f"Failed to get Salesforce token: {e}")
        return None

def query_salesforce(user_id, soql_query):
    """Execute SOQL query in Salesforce"""
    integration_mgr = IntegrationManager(user_id)
    config = integration_mgr.get_integration('salesforce')
    if not config:
        return None
    
    token = get_salesforce_token(user_id)
    if not token:
        return None
    
    headers = {"Authorization": f"Bearer {token}"}
    url = f"{config['instance_url']}/services/data/v52.0/query/"
    params = {"q": soql_query}
    
    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f"Failed to query Salesforce: {e}")
        return None

def get_salesforce_accounts(user_id, limit=10):
    """Get Salesforce accounts"""
    query = f"SELECT Id, Name, Type, Industry FROM Account LIMIT {limit}"
    result = query_salesforce(user_id, query)
    return result.get('records', []) if result else []

# Custom API Integration Functions
def call_custom_api(user_id, system_type, endpoint, method='GET', data=None):
    """Make API call to custom integration"""
    integration_mgr = IntegrationManager(user_id)
    config = integration_mgr.get_integration(system_type)
    if not config:
        return None
    
    # Build URL
    base_url = config['api_url'].rstrip('/')
    url = f"{base_url}/{endpoint.lstrip('/')}"
    
    # Build headers
    headers = {}
    auth_type = config.get('auth_type', 'api_key')
    api_key = config['api_key']
    api_key_header = config.get('api_key_header', 'X-API-Key')
    
    if auth_type == 'api_key':
        headers[api_key_header] = api_key
    elif auth_type == 'bearer_token':
        headers['Authorization'] = f"Bearer {api_key}"
    elif auth_type == 'basic_auth':
        credentials = base64.b64encode(api_key.encode()).decode()
        headers['Authorization'] = f"Basic {credentials}"
    elif auth_type == 'jwt':
        # For JWT, the token is typically passed in the Authorization header
        # as a Bearer token. The 'api_key' field in config might be the JWT itself.
        headers['Authorization'] = f"Bearer {api_key}"
    
    if data:
        headers['Content-Type'] = 'application/json'
    
    try:
        if method.upper() == 'GET':
            response = requests.get(url, headers=headers)
        elif method.upper() == 'POST':
            response = requests.post(url, headers=headers, json=data)
        elif method.upper() == 'PUT':
            response = requests.put(url, headers=headers, json=data)
        elif method.upper() == 'DELETE':
            response = requests.delete(url, headers=headers)
        else:
            return None
        
        response.raise_for_status()
        return response.json() if response.content else {"status": "success"}
    except requests.RequestException as e:
        print(f"Failed to call custom API: {e}")
        return None

# Utility Functions
def refresh_all_tokens(user_id):
    """Refresh all expired tokens for a user"""
    integration_mgr = IntegrationManager(user_id)
    
    for system_type in integration_mgr.integrations.keys():
        if system_type == 'outlook':
            # Outlook tokens need to be refreshed via OAuth flow
            pass
        elif system_type == 'powerbi':
            get_powerbi_token(user_id)  # This will refresh if expired
        elif system_type == 'salesforce':
            get_salesforce_token(user_id)  # This will refresh if expired

def get_integration_status(user_id):
    """Get status of all integrations for a user"""
    integration_mgr = IntegrationManager(user_id)
    status = {}
    
    for system_type in ['outlook', 'powerbi', 'salesforce']:
        status[f"{system_type}_connected"] = integration_mgr.is_connected(system_type)
        if integration_mgr.is_connected(system_type):
            token = integration_mgr.get_access_token(system_type)
            status[f"{system_type}_token_valid"] = bool(token)
    
    return status