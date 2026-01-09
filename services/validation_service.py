# services/validation_service.py
"""
Input validation service for sanitizing and validating user inputs.
Provides reusable validation functions to prevent injection attacks and ensure data integrity.
"""
import re
import html
from typing import Optional, List, Tuple, Any
from functools import wraps
from flask import request, flash, redirect, url_for


class ValidationError(Exception):
    """Custom exception for validation errors"""
    def __init__(self, message: str, field: str = None):
        self.message = message
        self.field = field
        super().__init__(self.message)


# Email validation regex (RFC 5322 simplified)
EMAIL_REGEX = re.compile(
    r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
)

# Phone validation regex (allows common formats)
PHONE_REGEX = re.compile(
    r'^[\d\s\-\+\(\)\.]{7,20}$'
)

# Username validation (alphanumeric, underscores, dots)
USERNAME_REGEX = re.compile(
    r'^[a-zA-Z][a-zA-Z0-9._]{2,49}$'
)

# Safe filename characters
SAFE_FILENAME_REGEX = re.compile(
    r'^[a-zA-Z0-9][a-zA-Z0-9._\-\s]{0,254}$'
)

# SQL injection patterns to detect
SQL_INJECTION_PATTERNS = [
    r"(\-\-)",
    r"(;)",
    r"(\/\*)",
    r"(\*\/)",
    r"(@@)",
    r"(@)",
    r"(char\s*\()",
    r"(nchar\s*\()",
    r"(varchar\s*\()",
    r"(nvarchar\s*\()",
    r"(alter\s+)",
    r"(begin\s+)",
    r"(cast\s*\()",
    r"(create\s+)",
    r"(cursor\s+)",
    r"(declare\s+)",
    r"(delete\s+)",
    r"(drop\s+)",
    r"(end\s+)",
    r"(exec\s+)",
    r"(execute\s+)",
    r"(fetch\s+)",
    r"(insert\s+)",
    r"(kill\s+)",
    r"(open\s+)",
    r"(select\s+)",
    r"(sys\.)",
    r"(sysobjects)",
    r"(syscolumns)",
    r"(table\s+)",
    r"(update\s+)",
    r"(union\s+)",
    r"(xp_)",
]

# XSS patterns to detect
XSS_PATTERNS = [
    r"<script",
    r"javascript:",
    r"vbscript:",
    r"onload\s*=",
    r"onerror\s*=",
    r"onclick\s*=",
    r"onmouseover\s*=",
    r"onfocus\s*=",
    r"onblur\s*=",
    r"expression\s*\(",
    r"eval\s*\(",
]


def sanitize_string(value: str, max_length: int = 500, allow_html: bool = False) -> str:
    """
    Sanitize a string input by:
    - Stripping whitespace
    - Limiting length
    - Escaping HTML (unless explicitly allowed)
    - Removing null bytes
    """
    if value is None:
        return ''

    value = str(value).strip()

    # Remove null bytes
    value = value.replace('\x00', '')

    # Limit length
    if len(value) > max_length:
        value = value[:max_length]

    # Escape HTML unless explicitly allowed
    if not allow_html:
        value = html.escape(value)

    return value


def sanitize_int(value: Any, min_val: int = None, max_val: int = None, default: int = 0) -> int:
    """
    Safely convert value to integer with optional range validation.
    """
    try:
        result = int(value)
        if min_val is not None and result < min_val:
            return min_val
        if max_val is not None and result > max_val:
            return max_val
        return result
    except (ValueError, TypeError):
        return default


def sanitize_float(value: Any, min_val: float = None, max_val: float = None, default: float = 0.0) -> float:
    """
    Safely convert value to float with optional range validation.
    """
    try:
        result = float(value)
        if min_val is not None and result < min_val:
            return min_val
        if max_val is not None and result > max_val:
            return max_val
        return result
    except (ValueError, TypeError):
        return default


def validate_email(email: str) -> Tuple[bool, str]:
    """
    Validate email format.
    Returns (is_valid, sanitized_email or error_message)
    """
    if not email:
        return True, ''  # Empty is valid (optional field)

    email = sanitize_string(email, max_length=254).lower()

    if EMAIL_REGEX.match(email):
        return True, email
    else:
        return False, 'Invalid email format'


def validate_phone(phone: str) -> Tuple[bool, str]:
    """
    Validate phone number format.
    Returns (is_valid, sanitized_phone or error_message)
    """
    if not phone:
        return True, ''  # Empty is valid (optional field)

    phone = sanitize_string(phone, max_length=20)

    # Remove common separators for validation
    cleaned = re.sub(r'[\s\-\(\)\.]', '', phone)

    if PHONE_REGEX.match(phone) and len(cleaned) >= 7:
        return True, phone
    else:
        return False, 'Invalid phone number format'


def validate_username(username: str) -> Tuple[bool, str]:
    """
    Validate username format.
    Returns (is_valid, sanitized_username or error_message)
    """
    if not username:
        return False, 'Username is required'

    username = sanitize_string(username, max_length=50)

    if USERNAME_REGEX.match(username):
        return True, username
    else:
        return False, 'Username must be 3-50 characters, start with a letter, and contain only letters, numbers, dots, or underscores'


def validate_password(password: str, min_length: int = 8) -> Tuple[bool, str]:
    """
    Validate password strength.
    Returns (is_valid, error_message or empty string)
    """
    if not password:
        return False, 'Password is required'

    if len(password) < min_length:
        return False, f'Password must be at least {min_length} characters'

    # Check for at least one uppercase, lowercase, and digit
    if not re.search(r'[A-Z]', password):
        return False, 'Password must contain at least one uppercase letter'

    if not re.search(r'[a-z]', password):
        return False, 'Password must contain at least one lowercase letter'

    if not re.search(r'\d', password):
        return False, 'Password must contain at least one digit'

    return True, ''


def validate_required(value: str, field_name: str) -> Tuple[bool, str]:
    """
    Check if a required field has a value.
    """
    if not value or not str(value).strip():
        return False, f'{field_name} is required'
    return True, sanitize_string(value)


def check_sql_injection(value: str) -> bool:
    """
    Check if value contains potential SQL injection patterns.
    Returns True if suspicious patterns are found.
    """
    if not value:
        return False

    value_lower = value.lower()
    for pattern in SQL_INJECTION_PATTERNS:
        if re.search(pattern, value_lower, re.IGNORECASE):
            return True
    return False


def check_xss(value: str) -> bool:
    """
    Check if value contains potential XSS patterns.
    Returns True if suspicious patterns are found.
    """
    if not value:
        return False

    value_lower = value.lower()
    for pattern in XSS_PATTERNS:
        if re.search(pattern, value_lower, re.IGNORECASE):
            return True
    return False


def validate_date(date_str: str, format: str = '%Y-%m-%d') -> Tuple[bool, str]:
    """
    Validate date string format.
    Returns (is_valid, sanitized_date or error_message)
    """
    if not date_str:
        return True, ''  # Empty is valid (optional field)

    from datetime import datetime
    try:
        parsed = datetime.strptime(date_str.strip(), format)
        return True, parsed.strftime(format)
    except ValueError:
        return False, f'Invalid date format. Expected {format}'


def validate_url(url: str) -> Tuple[bool, str]:
    """
    Validate URL format.
    Returns (is_valid, sanitized_url or error_message)
    """
    if not url:
        return True, ''  # Empty is valid (optional field)

    url = sanitize_string(url, max_length=2048)

    # Simple URL validation
    url_pattern = re.compile(
        r'^https?://'  # http:// or https://
        r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+[A-Z]{2,6}\.?|'  # domain
        r'localhost|'  # localhost
        r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # or IP
        r'(?::\d+)?'  # optional port
        r'(?:/?|[/?]\S+)$', re.IGNORECASE
    )

    if url_pattern.match(url):
        return True, url
    else:
        return False, 'Invalid URL format'


def validate_filename(filename: str) -> Tuple[bool, str]:
    """
    Validate and sanitize filename.
    Returns (is_valid, sanitized_filename or error_message)
    """
    if not filename:
        return False, 'Filename is required'

    # Remove path components
    filename = filename.replace('\\', '/').split('/')[-1]

    # Remove dangerous characters
    filename = re.sub(r'[<>:"/\\|?*\x00-\x1f]', '', filename)

    # Limit length
    if len(filename) > 255:
        name, ext = filename.rsplit('.', 1) if '.' in filename else (filename, '')
        filename = name[:255 - len(ext) - 1] + '.' + ext if ext else name[:255]

    if not filename or filename in ['.', '..']:
        return False, 'Invalid filename'

    return True, filename


def validate_role(role: str, allowed_roles: List[str]) -> Tuple[bool, str]:
    """
    Validate that role is in allowed list.
    """
    if not role:
        return False, 'Role is required'

    role = sanitize_string(role, max_length=50).lower()

    if role in [r.lower() for r in allowed_roles]:
        return True, role
    else:
        return False, f'Invalid role. Allowed roles: {", ".join(allowed_roles)}'


def validate_form_data(form_data: dict, validations: dict) -> Tuple[bool, dict, List[str]]:
    """
    Validate multiple form fields based on validation rules.

    Args:
        form_data: Dictionary of form field values
        validations: Dictionary mapping field names to validation functions/rules

    Returns:
        (all_valid, sanitized_data, error_messages)
    """
    errors = []
    sanitized = {}

    for field, rules in validations.items():
        value = form_data.get(field, '')

        if isinstance(rules, dict):
            # Complex validation rules
            if rules.get('required') and not value:
                errors.append(f'{rules.get("label", field)} is required')
                continue

            if rules.get('type') == 'email':
                valid, result = validate_email(value)
            elif rules.get('type') == 'phone':
                valid, result = validate_phone(value)
            elif rules.get('type') == 'int':
                result = sanitize_int(
                    value,
                    rules.get('min'),
                    rules.get('max'),
                    rules.get('default', 0)
                )
                valid = True
            elif rules.get('type') == 'float':
                result = sanitize_float(
                    value,
                    rules.get('min'),
                    rules.get('max'),
                    rules.get('default', 0.0)
                )
                valid = True
            elif rules.get('type') == 'date':
                valid, result = validate_date(value, rules.get('format', '%Y-%m-%d'))
            elif rules.get('type') == 'url':
                valid, result = validate_url(value)
            else:
                result = sanitize_string(value, rules.get('max_length', 500))
                valid = True

            if not valid:
                errors.append(f'{rules.get("label", field)}: {result}')
            else:
                sanitized[field] = result

        elif callable(rules):
            # Custom validation function
            valid, result = rules(value)
            if not valid:
                errors.append(result)
            else:
                sanitized[field] = result

        else:
            # Simple sanitization
            sanitized[field] = sanitize_string(value)

    return len(errors) == 0, sanitized, errors


def require_valid_int_param(param_name: str, min_val: int = 0, max_val: int = None):
    """
    Decorator to validate integer route parameters.
    """
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            if param_name in kwargs:
                try:
                    value = int(kwargs[param_name])
                    if min_val is not None and value < min_val:
                        flash(f'Invalid {param_name}', 'error')
                        return redirect(url_for('system.dashboard'))
                    if max_val is not None and value > max_val:
                        flash(f'Invalid {param_name}', 'error')
                        return redirect(url_for('system.dashboard'))
                except (ValueError, TypeError):
                    flash(f'Invalid {param_name}', 'error')
                    return redirect(url_for('system.dashboard'))
            return f(*args, **kwargs)
        return decorated_function
    return decorator
