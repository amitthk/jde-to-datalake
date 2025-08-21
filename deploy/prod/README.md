# Production Deployment Guide

## Prerequisites
Before deploying, ensure the following are installed on the server:
- Node.js (v16 or higher)
- npm
- Python 3.8+
- Virtual environment for Python

## Directory Structure
```
/var/www/stical-data-mig/
├── backend/
│   ├── venv/                 # Python virtual environment
│   ├── main.py
│   ├── .env                  # Backend configuration
│   └── ...
├── ui/
│   ├── src/
│   ├── public/
│   ├── package.json
│   └── ...
└── deploy/
    └── prod/
        ├── setup-production.sh
        ├── build-prod.sh
        ├── api.js
        ├── package.json
        └── prepare-prod.js
```

## Deployment Steps

### 1. Prepare the Server
```bash
# Copy application to server
scp -r /path/to/stical-data-mig user@server:/var/www/

# Set proper ownership
sudo chown -R www-data:www-data /var/www/stical-data-mig
```

### 2. Install Backend Dependencies
```bash
# Navigate to backend directory
cd /var/www/stical-data-mig/backend

# Create virtual environment if it doesn't exist
python3 -m venv venv

# Activate virtual environment
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
pip install gunicorn uvicorn[standard]
```

### 3. Install Frontend Dependencies
```bash
# Navigate to UI directory
cd /var/www/stical-data-mig/ui

# Install dependencies
npm install

# Install serve locally
npm install serve
```

### 4. Configure Environment
```bash
# Copy and edit backend environment file
cp /var/www/stical-data-mig/backend/.env.example /var/www/stical-data-mig/backend/.env
nano /var/www/stical-data-mig/backend/.env
```

### 5. Run Setup Script
```bash
# Run the production setup script
sudo bash /var/www/stical-data-mig/deploy/prod/setup-production.sh
```

### 6. Build and Deploy
```bash
# Build frontend for production
STICAL-ctl build

# Start services
STICAL-ctl start

# Check status
STICAL-ctl status
```

## Service Management

### Commands
- `STICAL-ctl build` - Build frontend for production
- `STICAL-ctl start` - Start both services
- `STICAL-ctl stop` - Stop both services
- `STICAL-ctl restart` - Restart both services
- `STICAL-ctl status` - Check service status
- `STICAL-ctl logs` - View logs for both services
- `STICAL-ctl logs backend` - View backend logs only
- `STICAL-ctl logs frontend` - View frontend logs only

### Manual Service Control
```bash
# Backend service
sudo systemctl start stical-data-mig-backend
sudo systemctl stop stical-data-mig-backend
sudo systemctl status stical-data-mig-backend

# Frontend service
sudo systemctl start stical-data-mig-frontend
sudo systemctl stop stical-data-mig-frontend
sudo systemctl status stical-data-mig-frontend
```

## Network Access

The application will be accessible on:
- **Frontend**: `http://server-ip:9999`
- **Backend API**: `http://server-ip:9998`

## Production Configuration

### API Configuration
The production build automatically configures the frontend to connect to:
- Backend API: `http://{server-hostname}:9998`

### Environment Variables
Ensure the following are configured in `/var/www/stical-data-mig/backend/.env`:
```env
# Database
PG_DATABASE_URL=postgresql://user:pass@localhost:5432/db
DB_NAME=ingredients_db

# JDE Configuration
JDE_BUSINESS_UNIT=1110
JDE_CARDEX_URL=https://jde-server/jderest/v3/orchestrator/STICAL_PO_SUMMARY
JDE_CARDEX_USERNAME=username
JDE_CARDEX_PASSWORD=password

# Bakery System Configuration
OUTLET_ID=your_outlet_id
BAKERY_SYSTEM_BASE_URL=https://bakery_system_server
BAKERY_SYSTEM_API_TOKEN=your_token

# Authentication
SECRET_KEY=your-production-secret-key
AUTH_LDAP_SERVER=ldap-server:389
AUTH_LDAP_BIND_USER=bind_user
AUTH_LDAP_BIND_PASSWORD=bind_password
```

## Troubleshooting

### Check Logs
```bash
# All logs
STICAL-ctl logs

# Backend only
STICAL-ctl logs backend

# Frontend only
STICAL-ctl logs frontend
```

### Check Service Status
```bash
STICAL-ctl status
```

### Rebuild Frontend
```bash
STICAL-ctl build
STICAL-ctl restart
```

### Common Issues
1. **Permission errors**: Ensure proper ownership with `sudo chown -R www-data:www-data /var/www/stical-data-mig`
2. **Port conflicts**: Check if ports 9998 and 9999 are available
3. **Environment variables**: Verify all required variables are set in `.env`
4. **API connection**: Frontend automatically connects to backend on port 9998
