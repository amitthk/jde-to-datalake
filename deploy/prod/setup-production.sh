#!/bin/bash

# Simple Production Deployment Setup
# Creates systemd services without requiring sudo privileges for package installation

set -e

# Configuration
APP_DIR="/var/www/stical-data-mig"
VENV_DIR="/var/www/stical-data-mig/venv"
BACKEND_PORT="9998"
FRONTEND_PORT="9999"
USER="www-data"
GROUP="www-data"

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
for dir in "$APP_DIR" "$VENV_DIR" "$APP_DIR/backend" "$APP_DIR/ui"; do
    if [ ! -d "$dir" ]; then
        echo "Error: Directory $dir does not exist"
        exit 1
    fi
done
echo "✓ All directories found"

# Create backend systemd service
echo "Creating backend systemd service..."
cat > /etc/systemd/system/stical-data-mig-backend.service << EOF
[Unit]
Description=STICAL Data Backend FastAPI
After=network.target
Wants=network-online.target

[Service]
Type=exec
User=$USER
Group=$GROUP
WorkingDirectory=$APP_DIR/backend
Environment=PATH=$VENV_DIR/bin
Environment=PYTHONPATH=$APP_DIR/backend
Environment=PYTHONUNBUFFERED=1
ExecStart=$VENV_DIR/bin/gunicorn main:app -w 4 -k uvicorn.workers.UvicornWorker --bind 0.0.0.0:$BACKEND_PORT --timeout 120
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal
SyslogIdentifier=stical-backend

# Security settings
NoNewPrivileges=yes
PrivateTmp=yes
ProtectSystem=strict
ProtectHome=yes
ReadWritePaths=$APP_DIR

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
User=$USER
Group=$GROUP
WorkingDirectory=$APP_DIR/ui
Environment=NODE_ENV=production
Environment=NODE_OPTIONS=
ExecStart=$APP_DIR/ui/node_modules/.bin/serve -s build --listen tcp://0.0.0.0:$FRONTEND_PORT
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal
SyslogIdentifier=stical-frontend

# Security settings
NoNewPrivileges=yes
PrivateTmp=yes
ProtectSystem=strict
ProtectHome=yes
ReadWritePaths=$APP_DIR

[Install]
WantedBy=multi-user.target
EOF

# Set ownership
echo "Setting ownership..."
chown -R $USER:$GROUP $APP_DIR

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
    build)
        echo "Building frontend for production..."
        cd /var/www/stical-data-mig && bash deploy/prod/build-prod.sh
        ;;
    *)
        echo "Usage: $0 {start|stop|restart|status|logs [backend|frontend]|build}"
        ;;
esac
EOF

chmod +x /usr/local/bin/stical-ctl

echo ""
echo "✅ Setup complete!"
echo ""
echo "🌐 Services will be accessible on:"
echo "  Backend API:  http://0.0.0.0:$BACKEND_PORT"
echo "  Frontend Web: http://0.0.0.0:$FRONTEND_PORT"
echo ""
echo "🎮 Management commands:"
echo "  stical-ctl build     - Build frontend for production"
echo "  stical-ctl start     - Start both services"
echo "  stical-ctl stop      - Stop both services"
echo "  stical-ctl restart   - Restart both services"
echo "  stical-ctl status    - Check service status"
echo "  stical-ctl logs      - Follow logs"
echo ""
echo "📝 Next steps:"
echo "1. Configure: $APP_DIR/backend/.env"
echo "2. Build: stical-ctl build"
echo "3. Start: stical-ctl start"
