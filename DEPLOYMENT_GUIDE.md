# Vigil Build Deployment Guide

## Hosting Options for vigilbuild.com

### Recommended Option: Railway, Render, or DigitalOcean App Platform

These are the easiest options for deploying a Flask application with PostgreSQL and Elasticsearch.

---

## Option 1: Railway (Recommended for Quick Start)

### Why Railway?
- Easy deployment from GitHub
- Automatic SSL certificates
- Built-in PostgreSQL and Redis
- Pay-as-you-go pricing

### Steps:
1. Push your code to GitHub
2. Go to [railway.app](https://railway.app)
3. Connect your GitHub repository
4. Add services: PostgreSQL, Elasticsearch
5. Set environment variables (see below)
6. Deploy

### Custom Domain Setup:
1. In Railway dashboard, go to Settings > Domains
2. Add `vigilbuild.com` and `www.vigilbuild.com`
3. Update DNS at your domain registrar:
   - Add CNAME record: `www` -> `your-app.railway.app`
   - Add A record for root domain (Railway provides IP)

---

## Option 2: DigitalOcean App Platform

### Steps:
1. Create a DigitalOcean account
2. Go to App Platform > Create App
3. Connect GitHub repository
4. Configure:
   - Runtime: Python 3.10
   - Build Command: `pip install -r requirements.txt`
   - Run Command: `gunicorn app:app`
5. Add managed PostgreSQL database
6. Add environment variables

### Custom Domain:
1. In App settings > Domains
2. Add `vigilbuild.com`
3. Update DNS records as instructed

---

## Option 3: AWS (More Complex, More Control)

For production-grade deployment:
- EC2 or ECS for the Flask app
- RDS for PostgreSQL
- OpenSearch Service for Elasticsearch
- CloudFront for CDN
- Route 53 for DNS
- ACM for SSL certificates

---

## Required Environment Variables

Create a `.env` file (never commit this!) or set these in your hosting platform:

```bash
# Required for Production
FLASK_ENV=production
SECRET_KEY=your-very-secure-random-secret-key-here
DATABASE_URL=postgresql://user:password@host:5432/vigilbuild

# Elasticsearch
ES_HOST=https://your-elasticsearch-host:9200
ES_USER=elastic
ES_PASS=your-es-password

# Email Configuration (for demo requests)
SMTP_SERVER=smtp.gmail.com  # or your email provider
SMTP_PORT=587
SMTP_USER=info@vigilbuild.com
SMTP_PASS=your-app-password

# Marketing Email Recipients
MARKETING_EMAIL=info@vigilbuild.com
SALES_EMAIL=info@vigilbuild.com

# Optional
WEATHER_API_KEY=your-openweathermap-key
OLLAMA_HOST=http://your-ollama-host:11434
```

---

## Setting Up Email (info@vigilbuild.com)

### Option A: Google Workspace (Recommended)
1. Purchase Google Workspace ($6/user/month)
2. Verify domain ownership
3. Create info@vigilbuild.com mailbox
4. For SMTP, create an App Password:
   - Go to Google Account > Security > 2-Step Verification > App passwords
   - Generate password for "Mail"
   - Use this as SMTP_PASS

### Option B: Microsoft 365
1. Purchase Microsoft 365 Business Basic
2. Add domain and verify
3. Create info@vigilbuild.com mailbox
4. Use Outlook SMTP settings or Microsoft Graph API

### Option C: Zoho Mail (Free for 5 users)
1. Sign up at zoho.com/mail
2. Add and verify vigilbuild.com
3. Create info@vigilbuild.com
4. SMTP settings:
   - Server: smtp.zoho.com
   - Port: 587
   - TLS: Yes

---

## DNS Configuration for vigilbuild.com

At your domain registrar (where you purchased vigilbuild.com), add these records:

### For Website:
```
Type    Host    Value                   TTL
A       @       [IP from hosting]       300
CNAME   www     [your-app-url]          300
```

### For Email (Google Workspace example):
```
Type    Host    Value                           Priority    TTL
MX      @       aspmx.l.google.com             1           300
MX      @       alt1.aspmx.l.google.com        5           300
MX      @       alt2.aspmx.l.google.com        5           300
TXT     @       v=spf1 include:_spf.google.com ~all        300
```

---

## Production Checklist

Before going live:

- [ ] Set `FLASK_ENV=production`
- [ ] Generate a strong `SECRET_KEY` (use `python -c "import secrets; print(secrets.token_hex(32))"`)
- [ ] Configure SSL certificate (usually automatic with hosting platforms)
- [ ] Set up email (SMTP credentials)
- [ ] Test demo request form sends emails
- [ ] Set up monitoring (consider Sentry for error tracking)
- [ ] Configure database backups
- [ ] Test all pages load correctly
- [ ] Verify social media links work (LinkedIn, X)

---

## Quick Start Commands

### Generate a secure SECRET_KEY:
```bash
python -c "import secrets; print(secrets.token_hex(32))"
```

### Create requirements.txt for production:
```bash
pip freeze > requirements.txt
```

### Test locally with production settings:
```bash
FLASK_ENV=production python app.py
```

---

## File Structure for Deployment

Make sure these files are in your repository:
```
/
├── app.py                 # Main application
├── config.py              # Configuration
├── requirements.txt       # Dependencies
├── Procfile              # For Heroku/Railway (optional)
├── runtime.txt           # Python version (optional)
├── routes/
│   ├── marketing.py      # Marketing routes
│   └── ...
├── templates/
│   ├── marketing_base.html
│   ├── marketing_home.html
│   ├── marketing_features.html
│   ├── marketing_about.html
│   ├── marketing_contact.html
│   └── ...
├── static/
│   ├── vigil_build_logo_transparent.png
│   ├── favicon.ico
│   └── ...
└── services/
    ├── email_service.py
    └── ...
```

### Procfile (for Railway/Heroku):
```
web: gunicorn app:app --bind 0.0.0.0:$PORT
```

### runtime.txt:
```
python-3.10.12
```

---

## Support

If you need help with deployment, the key areas to focus on are:
1. Domain DNS configuration
2. Email setup with your domain
3. Environment variables for production
4. SSL certificate (usually automatic)

Most modern hosting platforms handle SSL and scaling automatically.
