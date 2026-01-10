# routes/marketing.py
"""
Marketing website routes for Vigil Build
These pages are publicly accessible (no authentication required)
"""

from flask import Blueprint, render_template, request, jsonify, flash, redirect, url_for
from datetime import datetime
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import os

marketing_bp = Blueprint('marketing', __name__)

def get_email_config():
    """Get email configuration from environment variables at runtime"""
    # Use EMAIL_PASSWORD instead of SMTP_PASS (DigitalOcean was filtering SMTP_PASS)
    smtp_user = os.getenv('SMTP_USER', '')
    email_password = os.getenv('EMAIL_PASSWORD', '')

    # Debug logging
    print(f"DEBUG - SMTP_USER: '{smtp_user}', EMAIL_PASSWORD length: {len(email_password)}, first 4 chars: '{email_password[:4] if len(email_password) >= 4 else email_password}'")

    return {
        'smtp_server': os.getenv('SMTP_SERVER', 'smtp.gmail.com'),
        'smtp_port': int(os.getenv('SMTP_PORT', '587')),
        'smtp_user': smtp_user,
        'smtp_pass': email_password,
        'marketing_email': os.getenv('MARKETING_EMAIL', 'info@vigilbuild.com'),
        'sales_email': os.getenv('SALES_EMAIL', 'info@vigilbuild.com'),
    }


def send_email(subject, body, recipients, is_html=True):
    """Send email using SMTP"""
    config = get_email_config()

    if not config['smtp_user'] or not config['smtp_pass']:
        print(f"SMTP not configured - Email not sent: {subject}")
        print(f"SMTP_USER set: {bool(config['smtp_user'])}, SMTP_PASS set: {bool(config['smtp_pass'])}")
        return False

    try:
        msg = MIMEMultipart('alternative')
        msg['Subject'] = subject
        msg['From'] = config['smtp_user']
        msg['To'] = ', '.join(recipients)

        content_type = 'html' if is_html else 'plain'
        msg.attach(MIMEText(body, content_type))

        with smtplib.SMTP(config['smtp_server'], config['smtp_port']) as server:
            server.starttls()
            server.login(config['smtp_user'], config['smtp_pass'])
            server.sendmail(config['smtp_user'], recipients, msg.as_string())

        print(f"Email sent: {subject}")
        return True
    except Exception as e:
        print(f"Failed to send email: {e}")
        return False


@marketing_bp.route('/')
def home():
    """Marketing home/landing page"""
    return render_template('marketing_home.html', current_year=datetime.now().year)


@marketing_bp.route('/features')
def features():
    """Features page showcasing all modules"""
    return render_template('marketing_features.html', current_year=datetime.now().year)


@marketing_bp.route('/about')
def about():
    """About/Why Vigil Build page"""
    return render_template('marketing_about.html', current_year=datetime.now().year)


@marketing_bp.route('/contact')
def contact():
    """Contact/Demo request page"""
    return render_template('marketing_contact.html', current_year=datetime.now().year)


@marketing_bp.route('/demo-request', methods=['POST'])
def demo_request():
    """Handle demo request form submission"""
    try:
        # Get form data
        data = {
            'first_name': request.form.get('first_name'),
            'last_name': request.form.get('last_name'),
            'email': request.form.get('email'),
            'phone': request.form.get('phone'),
            'company': request.form.get('company'),
            'job_title': request.form.get('job_title'),
            'industry': request.form.get('industry'),
            'company_size': request.form.get('company_size'),
            'interests': request.form.getlist('interests'),
            'message': request.form.get('message'),
            'submitted_at': datetime.now().isoformat()
        }

        # Validate required fields
        required_fields = ['first_name', 'last_name', 'email', 'company', 'industry']
        missing_fields = [field for field in required_fields if not data.get(field)]

        if missing_fields:
            return jsonify({
                'success': False,
                'message': f'Missing required fields: {", ".join(missing_fields)}'
            }), 400

        # Format interests list
        interests_text = ', '.join(data['interests']) if data['interests'] else 'Not specified'

        # Industry display names
        industry_names = {
            'power_distribution': 'Power Distribution / Electric Utility',
            'municipality': 'Municipality / Public Works',
            'contractor': 'General Contractor',
            'electrical_contractor': 'Electrical Contractor',
            'telecom': 'Telecommunications',
            'oil_gas': 'Oil & Gas',
            'other': 'Other'
        }
        industry_display = industry_names.get(data['industry'], data['industry'])

        # 1. Send notification email to sales team
        sales_subject = f"New Demo Request: {data['company']} - {data['first_name']} {data['last_name']}"
        sales_body = f"""
        <html>
        <body style="font-family: Arial, sans-serif; line-height: 1.6; color: #333;">
            <div style="max-width: 600px; margin: 0 auto; padding: 20px;">
                <div style="background: linear-gradient(135deg, #1e3a5f 0%, #152a45 100%); padding: 20px; border-radius: 8px 8px 0 0;">
                    <h1 style="color: white; margin: 0; font-size: 24px;">New Demo Request</h1>
                </div>
                <div style="background: #f9fafb; padding: 20px; border: 1px solid #e5e7eb; border-top: none; border-radius: 0 0 8px 8px;">
                    <h2 style="color: #1e3a5f; margin-top: 0;">Contact Information</h2>
                    <table style="width: 100%; border-collapse: collapse;">
                        <tr>
                            <td style="padding: 8px 0; border-bottom: 1px solid #e5e7eb; font-weight: bold; width: 140px;">Name:</td>
                            <td style="padding: 8px 0; border-bottom: 1px solid #e5e7eb;">{data['first_name']} {data['last_name']}</td>
                        </tr>
                        <tr>
                            <td style="padding: 8px 0; border-bottom: 1px solid #e5e7eb; font-weight: bold;">Email:</td>
                            <td style="padding: 8px 0; border-bottom: 1px solid #e5e7eb;"><a href="mailto:{data['email']}">{data['email']}</a></td>
                        </tr>
                        <tr>
                            <td style="padding: 8px 0; border-bottom: 1px solid #e5e7eb; font-weight: bold;">Phone:</td>
                            <td style="padding: 8px 0; border-bottom: 1px solid #e5e7eb;">{data['phone'] or 'Not provided'}</td>
                        </tr>
                        <tr>
                            <td style="padding: 8px 0; border-bottom: 1px solid #e5e7eb; font-weight: bold;">Company:</td>
                            <td style="padding: 8px 0; border-bottom: 1px solid #e5e7eb;">{data['company']}</td>
                        </tr>
                        <tr>
                            <td style="padding: 8px 0; border-bottom: 1px solid #e5e7eb; font-weight: bold;">Job Title:</td>
                            <td style="padding: 8px 0; border-bottom: 1px solid #e5e7eb;">{data['job_title'] or 'Not provided'}</td>
                        </tr>
                        <tr>
                            <td style="padding: 8px 0; border-bottom: 1px solid #e5e7eb; font-weight: bold;">Industry:</td>
                            <td style="padding: 8px 0; border-bottom: 1px solid #e5e7eb;">{industry_display}</td>
                        </tr>
                        <tr>
                            <td style="padding: 8px 0; border-bottom: 1px solid #e5e7eb; font-weight: bold;">Company Size:</td>
                            <td style="padding: 8px 0; border-bottom: 1px solid #e5e7eb;">{data['company_size'] or 'Not provided'}</td>
                        </tr>
                        <tr>
                            <td style="padding: 8px 0; border-bottom: 1px solid #e5e7eb; font-weight: bold;">Interests:</td>
                            <td style="padding: 8px 0; border-bottom: 1px solid #e5e7eb;">{interests_text}</td>
                        </tr>
                    </table>

                    <h3 style="color: #1e3a5f; margin-top: 20px;">Message / Challenges:</h3>
                    <div style="background: white; padding: 15px; border-radius: 4px; border: 1px solid #e5e7eb;">
                        {data['message'] or 'No message provided'}
                    </div>

                    <p style="margin-top: 20px; padding: 15px; background: #dbeafe; border-radius: 4px; color: #1e40af;">
                        <strong>Action Required:</strong> Please respond to this lead within 24 hours.
                    </p>

                    <p style="color: #6b7280; font-size: 12px; margin-top: 20px;">
                        Submitted: {data['submitted_at']}
                    </p>
                </div>
            </div>
        </body>
        </html>
        """

        # 2. Send confirmation email to prospect
        prospect_subject = "Thank you for your interest in Vigil Build"
        prospect_body = f"""
        <html>
        <body style="font-family: Arial, sans-serif; line-height: 1.6; color: #333;">
            <div style="max-width: 600px; margin: 0 auto; padding: 20px;">
                <div style="background: linear-gradient(135deg, #1e3a5f 0%, #152a45 100%); padding: 30px; border-radius: 8px 8px 0 0; text-align: center;">
                    <h1 style="color: white; margin: 0; font-size: 28px;">Vigil Build</h1>
                    <p style="color: #93c5fd; margin: 10px 0 0 0;">Construction Management Software</p>
                </div>
                <div style="background: #ffffff; padding: 30px; border: 1px solid #e5e7eb; border-top: none; border-radius: 0 0 8px 8px;">
                    <h2 style="color: #1e3a5f; margin-top: 0;">Hi {data['first_name']},</h2>

                    <p>Thank you for your interest in Vigil Build! We've received your demo request and are excited to show you how our platform can help {data['company']} manage projects, equipment, and teams more effectively.</p>

                    <p><strong>What happens next?</strong></p>
                    <ul style="color: #4b5563;">
                        <li>A member of our team will reach out within 24 hours</li>
                        <li>We'll schedule a personalized demo at your convenience</li>
                        <li>We'll focus on the features most relevant to your operations</li>
                    </ul>

                    <p>In the meantime, feel free to explore our website to learn more about our features:</p>

                    <div style="text-align: center; margin: 30px 0;">
                        <a href="https://vigilbuild.com/marketing/features" style="display: inline-block; background: #2563eb; color: white; padding: 12px 30px; border-radius: 6px; text-decoration: none; font-weight: bold;">Explore Features</a>
                    </div>

                    <p>If you have any immediate questions, don't hesitate to reach out to us at <a href="mailto:info@vigilbuild.com">info@vigilbuild.com</a>.</p>

                    <p>We look forward to speaking with you soon!</p>

                    <p style="margin-top: 30px;">
                        Best regards,<br>
                        <strong>The Vigil Build Team</strong>
                    </p>
                </div>
                <div style="text-align: center; padding: 20px; color: #6b7280; font-size: 12px;">
                    <p>&copy; {datetime.now().year} Vigil Build. All rights reserved.</p>
                    <p>
                        <a href="https://vigilbuild.com" style="color: #2563eb; text-decoration: none;">vigilbuild.com</a>
                    </p>
                </div>
            </div>
        </body>
        </html>
        """

        # Send emails
        try:
            config = get_email_config()
            send_email(sales_subject, sales_body, [config['sales_email']])
            send_email(prospect_subject, prospect_body, [data['email']])
            print(f"Demo request processed for {data['first_name']} {data['last_name']} at {data['company']}")
        except Exception as email_error:
            print(f"Email sending failed: {str(email_error)}")
            # Continue - don't fail the form submission just because email failed

        # Return success response - always return JSON for fetch requests
        # Check if request wants JSON (from fetch/AJAX)
        wants_json = request.is_json or request.headers.get('Accept', '').startswith('*/*') or 'fetch' in request.headers.get('Sec-Fetch-Mode', '')

        if wants_json:
            return jsonify({
                'success': True,
                'message': 'Thank you for your interest! Our team will contact you within 24 hours.'
            })
        else:
            flash('Thank you for your interest! Our team will contact you within 24 hours.', 'success')
            return redirect(url_for('marketing.contact'))

    except Exception as e:
        print(f"Error processing demo request: {str(e)}")
        wants_json = request.is_json or request.headers.get('Accept', '').startswith('*/*') or 'fetch' in request.headers.get('Sec-Fetch-Mode', '')
        if wants_json:
            return jsonify({
                'success': False,
                'message': 'An error occurred. Please try again.'
            }), 500
        else:
            flash('An error occurred. Please try again.', 'error')
            return redirect(url_for('marketing.contact'))


@marketing_bp.route('/newsletter', methods=['POST'])
def newsletter_signup():
    """Handle newsletter signup"""
    try:
        email = request.form.get('email') or (request.json.get('email') if request.is_json else None)

        if not email:
            return jsonify({'success': False, 'message': 'Email is required'}), 400

        # Log the signup (in production, integrate with email service like Mailchimp)
        print(f"Newsletter signup: {email}")

        return jsonify({
            'success': True,
            'message': 'Thank you for subscribing!'
        })

    except Exception as e:
        print(f"Error processing newsletter signup: {str(e)}")
        return jsonify({
            'success': False,
            'message': 'An error occurred. Please try again.'
        }), 500
