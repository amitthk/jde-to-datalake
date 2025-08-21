#!/bin/bash

# Production STICAL Data - Systemd Services Setup with Gunicorn
# This script creates production-ready systemd services

set -e

# Configuration
APP_DIR="/var/www/stical-data-mig"
VENV_DIR="/var/www/stical-data-mig/backend/venv"
BACKEND_DIR="$APP_DIR/backend"
FRONTEND_DIR="$APP_DIR/ui"
SERVICE_USER="www-data"
SERVICE_GROUP="www-data"
WORKERS=2
BACKEND_PORT=9998
FRONTEND_PORT=9999

echo "========================================="
echo "  STICAL Data - Production Setup "
echo "========================================="

# Root check
if [ "$EUID" -ne 0 ]; then
    echo "Please run as root"
    exit 1
fi

# Install dependencies
echo "Installing dependencies..."
apt-get update -qq
apt-get install -y python3-pip nodejs npm

# Install gunicorn in venv
echo "Installing gunicorn..."
$VENV_DIR/bin/pip install gunicorn uvicorn[standard]

# Install serve globally
npm install -g serve

# Create gunicorn config
echo "Creating gunicorn configuration..."
cat > $BACKEND_DIR/gunicorn.conf.py << EOF
# Gunicorn configuration for STICAL Data Backend

bind = "0.0.0.0:$BACKEND_PORT"
workers = $WORKERS
worker_class = "uvicorn.workers.UvicornWorker"
worker_connections = 1000
max_requests = 1000
max_requests_jitter = 50
timeout = 30
keepalive = 2
preload_app = True
access_log = "/var/log/stical-data-mig/access.log"
error_log = "/var/log/stical-data-mig/error.log"
log_level = "info"
EOF

# Create log directory
mkdir -p /var/log/stical-data-mig
chown -R $SERVICE_USER:$SERVICE_GROUP /var/log/stical-data-mig

# Backend service with gunicorn
echo "Creating backend service..."
cat > /etc/systemd/system/stical-data-mig-backend.service << EOF
[Unit]
Description=STICAL Data Backend (Gunicorn)
After=network.target
Requires=network.target

[Service]
Type=exec
User=$SERVICE_USER
Group=$SERVICE_GROUP
WorkingDirectory=$BACKEND_DIR
Environment=PATH=$VENV_DIR/bin
ExecStart=$VENV_DIR/bin/gunicorn main:app -c gunicorn.conf.py
ExecReload=/bin/kill -s HUP \$MAINPID
Restart=always
RestartSec=3
TimeoutStopSec=30
KillMode=mixed

[Install]
WantedBy=multi-user.target
EOF

# Frontend service
echo "Creating frontend service..."
cat > /etc/systemd/system/stical-data-mig-frontend.service << EOF
[Unit]
Description=STICAL Data Frontend (React)
After=network.target stical-data-mig-backend.service
Wants=stical-data-mig-backend.service

[Service]
Type=exec
User=$SERVICE_USER
Group=$SERVICE_GROUP
WorkingDirectory=$FRONTEND_DIR
ExecStartPre=/bin/bash -c 'if [ ! -d "build" ]; then npm run build; fi'
ExecStart=/usr/bin/npx serve -s build -l $FRONTEND_PORT -H 0.0.0.0
Restart=always
RestartSec=5
TimeoutStopSec=15

[Install]
WantedBy=multi-user.target
EOF

# Set permissions
chown -R $SERVICE_USER:$SERVICE_GROUP $APP_DIR
chmod +x $BACKEND_DIR/gunicorn.conf.py

# Reload and enable
systemctl daemon-reload
systemctl enable stical-data-mig-backend.service
systemctl enable stical-data-mig-frontend.service

# Create management script
cat > /usr/local/bin/stical-ctl << 'EOF'
#!/bin/bash
case "$1" in
    start)
        systemctl start stical-data-mig-backend stical-data-mig-frontend
        ;;
    stop)
        systemctl stop stical-data-mig-frontend stical-data-mig-backend
        ;;
    restart)
        systemctl restart stical-data-mig-backend stical-data-mig-frontend
        ;;
    status)
        systemctl status stical-data-mig-backend stical-data-mig-frontend
        ;;
    logs)
        if [ "$2" = "backend" ]; then
            journalctl -u stical-data-mig-backend -f
        elif [ "$2" = "frontend" ]; then
            journalctl -u stical-data-mig-frontend -f
        else
            journalctl -u stical-data-mig-backend -u stical-data-mig-frontend -f
        fi
        ;;
    *)
        echo "Usage: $0 {start|stop|restart|status|logs [backend|frontend]}"
        ;;
esac
EOF

chmod +x /usr/local/bin/stical-ctl

echo "✓ Production setup complete!"
echo ""
echo "Services created:"
echo "  - stical-data-mig-backend (port $BACKEND_PORT)"
echo "  - stical-data-mig-frontend (port $FRONTEND_PORT)"
echo ""
echo "Management:"
echo "  stical-ctl start    - Start services"
echo "  stical-ctl stop     - Stop services"
echo "  stical-ctl restart  - Restart services"
echo "  stical-ctl status   - Check status"
echo "  stical-ctl logs     - View logs"
echo ""
echo "Next steps:"
echo "1. Configure $BACKEND_DIR/.env file"
echo "2. Run: stical-ctl start"
