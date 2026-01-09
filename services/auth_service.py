# services/auth_service.py
from flask_login import UserMixin
from services.db import get_connection, release_connection
from werkzeug.security import check_password_hash

class User(UserMixin):
    def __init__(self, id_or_username, by_username=False):
        conn = get_connection()
        c = conn.cursor()
        if by_username:
            c.execute("SELECT id, username, email, role, password FROM users WHERE username = %s", (id_or_username,))
        else:
            c.execute("SELECT id, username, email, role, password FROM users WHERE id = %s", (id_or_username,))
        result = c.fetchone()
        release_connection(conn)
        if result:
            self.id = str(result[0])  # Flask-Login requires id to be a string
            self.username = result[1]
            self.email = result[2]
            self.role = result[3]
            self.password_hash = result[4]
        else:
            raise ValueError(f"User with {'username' if by_username else 'id'} {id_or_username} not found")

    def is_admin(self):
        return self.role == "admin"

    def verify_password(self, password_plain):
        """
        Compare a plain password to the stored hash.
        """
        return check_password_hash(self.password_hash, password_plain) if self.password_hash else False
    
    def has_role(self, *roles):
        return self.role in roles

