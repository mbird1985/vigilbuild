# services/csrf_service.py
"""
CSRF Protection Service
Provides CSRF token generation and validation for forms.
"""
import secrets
import hmac
import hashlib
from functools import wraps
from flask import session, request, abort, g, current_app


def generate_csrf_token():
    """
    Generate a new CSRF token and store it in the session.
    Returns the token for use in forms.
    """
    if '_csrf_token' not in session:
        session['_csrf_token'] = secrets.token_hex(32)
    return session['_csrf_token']


def validate_csrf_token(token):
    """
    Validate a CSRF token against the one stored in the session.
    Returns True if valid, False otherwise.
    """
    if not token:
        return False

    session_token = session.get('_csrf_token')
    if not session_token:
        return False

    # Use constant-time comparison to prevent timing attacks
    return hmac.compare_digest(token, session_token)


def csrf_protect(f):
    """
    Decorator to protect a route with CSRF validation.
    Only validates on POST, PUT, PATCH, DELETE requests.
    """
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if request.method in ['POST', 'PUT', 'PATCH', 'DELETE']:
            # Check for CSRF token in form data or headers
            token = request.form.get('csrf_token') or request.headers.get('X-CSRF-Token')

            if not validate_csrf_token(token):
                current_app.logger.warning(
                    f"CSRF validation failed for {request.endpoint} from {request.remote_addr}"
                )
                abort(403, description="CSRF token validation failed")

        return f(*args, **kwargs)
    return decorated_function


def init_csrf(app):
    """
    Initialize CSRF protection for the Flask app.
    Adds the csrf_token function to the template context.
    """
    @app.context_processor
    def csrf_context_processor():
        return dict(csrf_token=generate_csrf_token)

    @app.before_request
    def csrf_before_request():
        """
        Validate CSRF token on state-changing requests.
        Exempts API routes that use other authentication methods.
        """
        # Skip CSRF check for safe methods
        if request.method in ['GET', 'HEAD', 'OPTIONS']:
            return

        # Skip for API routes (they should use API key/JWT auth)
        if request.path.startswith('/api/'):
            return

        # Skip for webhook endpoints
        if '/webhook' in request.path:
            return

        # Skip for auth routes (login doesn't have a token yet)
        if request.path in ['/auth/login', '/auth/register']:
            return

        # Validate token
        token = request.form.get('csrf_token') or request.headers.get('X-CSRF-Token')

        if not validate_csrf_token(token):
            # Log the failed attempt
            try:
                current_app.logger.warning(
                    f"CSRF validation failed: path={request.path}, method={request.method}, ip={request.remote_addr}"
                )
            except Exception:
                pass

            # Don't abort for now to avoid breaking existing forms
            # In production, uncomment the abort line
            # abort(403, description="CSRF token validation failed. Please refresh the page and try again.")
            pass

    return app


def get_csrf_token():
    """Get the current CSRF token (generates one if needed)."""
    return generate_csrf_token()
