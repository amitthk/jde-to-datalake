#!/bin/bash

# Simple STICAL Data - Systemd Services Setup Script
# Backend on port 9998, Frontend on port 9999, accessible from network

set -e  # Exit on any error

# Configuration variables
APP_DIR="/var/www/stical-data-mig"
VENV_DIR="/var/www/stical-data-mig/backend/venv"
BACKEND_DIR="$APP_DIR/backend"
FRONTEND_DIR="$APP_DIR/ui"
SERVICE_USER="www-data"
SERVICE_GROUP="www-data"
BACKEND_PORT="9998"
FRONTEND_PORT="9999"

echo "========================================="
echo "  STICAL Data - Simple Setup     "
echo "  Backend Port: $BACKEND_PORT          "
echo "  Frontend Port: $FRONTEND_PORT        "
echo "========================================="

# Check if running as root
if [ "$EUID" -ne 0 ]; then
    echo "Please run this script as root (use sudo)"
    exit 1
fi

# Verify directories exist
echo "Checking directories..."
for dir in "$APP_DIR" "$VENV_DIR" "$BACKEND_DIR" "$FRONTEND_DIR"; do
    if [ ! -d "$dir" ]; then
        echo "Error: Directory $dir does not exist"
        exit 1
    fi
done
echo "✓ All directories found"

# Install dependencies
echo "Installing dependencies..."
apt-get update -qq
apt-get install -y nodejs npm
npm install -g serve

# Install gunicorn in virtual environment
echo "Installing gunicorn..."
$VENV_DIR/bin/pip install gunicorn uvicorn[standard]

# Create backend systemd service
echo "Creating backend systemd service..."
cat > /etc/systemd/system/stical-data-mig-backend.service << EOF
[Unit]
Description=STICAL Data Backend FastAPI
After=network.target
Wants=network-online.target

[Service]
Type=exec
User=$SERVICE_USER
Group=$SERVICE_GROUP
WorkingDirectory=$BACKEND_DIR
Environment=PATH=$VENV_DIR/bin
ExecStart=$VENV_DIR/bin/gunicorn main:app -w 4 -k uvicorn.workers.UvicornWorker --bind 0.0.0.0:$BACKEND_PORT --timeout 120
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal
SyslogIdentifier=stical-backend

# Environment variables
Environment=PYTHONPATH=$BACKEND_DIR
Environment=PYTHONUNBUFFERED=1

[Install]
WantedBy=multi-user.target
EOF

# Create frontend systemd service
echo "Creating frontend systemd service..."
cat > /etc/systemd/system/stical-data-mig-frontend.service << EOF
[Unit]
Description=STICAL Data Frontend React
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
RestartSec=10
StandardOutput=journal
StandardError=journal
SyslogIdentifier=stical-frontend

# Environment variables
Environment=NODE_ENV=production

[Install]
WantedBy=multi-user.target
EOF

# Set ownership
echo "Setting ownership..."
chown -R $SERVICE_USER:$SERVICE_GROUP $APP_DIR

# Reload systemd
echo "Reloading systemd..."
systemctl daemon-reload
systemctl enable stical-data-mig-backend.service
systemctl enable stical-data-mig-frontend.service

# Create management script
echo "Creating management script..."
cat > /usr/local/bin/stical-ctl << 'EOF'
#!/bin/bash

case "$1" in
    start)
        echo "Starting STICAL Data services..."
        systemctl start stical-data-mig-backend stical-data-mig-frontend
        ;;
    stop)
        echo "Stopping STICAL Data services..."
        systemctl stop stical-data-mig-frontend stical-data-mig-backend
        ;;
    restart)
        echo "Restarting STICAL Data services..."
        systemctl restart stical-data-mig-backend stical-data-mig-frontend
        ;;
    status)
        systemctl status stical-data-mig-backend stical-data-mig-frontend --no-pager
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

echo "✓ Setup complete!"
echo ""
echo "🌐 Services will be accessible on:"
echo "  Backend API:  http://0.0.0.0:$BACKEND_PORT"
echo "  Frontend Web: http://0.0.0.0:$FRONTEND_PORT"
echo ""
echo "🎮 Management commands:"
echo "  stical-ctl start     - Start both services"
echo "  stical-ctl stop      - Stop both services"
echo "  stical-ctl restart   - Restart both services"
echo "  stical-ctl status    - Check service status"
echo "  stical-ctl logs      - Follow logs"
echo ""
echo "📝 Next steps:"
echo "1. Configure: $BACKEND_DIR/.env"
echo "2. Start: stical-ctl start"
echo "3. Check: stical-ctl status"
