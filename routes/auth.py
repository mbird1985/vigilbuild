# routes/auth.py
from flask import Blueprint, request, redirect, url_for, flash, render_template
from flask_login import login_user, logout_user, login_required
from services.auth_service import User
from services.db import get_connection

auth_bp = Blueprint("auth", __name__, url_prefix="/auth")

@auth_bp.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')
        try:
            user = User(username, by_username=True)
            if user.verify_password(password):
                login_user(user)
                flash('Logged in successfully')
                return redirect(url_for('system.dashboard'))
            else:
                flash('Invalid username or password')
        except ValueError:
            flash('Invalid username or password')
    return render_template('login.html')

@auth_bp.route('/logout')
@login_required
def logout():
    logout_user()
    flash('You have been logged out.')
    return redirect(url_for('auth.login'))