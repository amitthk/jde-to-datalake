#!/bin/bash

# STICAL Data - Systemd Services Setup Script
# This script creates systemd services for the FastAPI backend and React frontend

set -e  # Exit on any error

# Configuration variables
APP_DIR="/var/www/stical-data-mig"
VENV_DIR="/var/www/stical-data-mig/backend/venv"
BACKEND_DIR="$APP_DIR/backend"
FRONTEND_DIR="$APP_DIR/ui"
SERVICE_USER="www-data"
SERVICE_GROUP="www-data"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}  STICAL Data Systemd Setup     ${NC}"
echo -e "${GREEN}========================================${NC}"

# Check if running as root
if [ "$EUID" -ne 0 ]; then
    echo -e "${RED}Please run this script as root (use sudo)${NC}"
    exit 1
fi

# Verify directories exist
echo -e "${YELLOW}Checking directories...${NC}"
if [ ! -d "$APP_DIR" ]; then
    echo -e "${RED}Error: Application directory $APP_DIR does not exist${NC}"
    exit 1
fi

if [ ! -d "$VENV_DIR" ]; then
    echo -e "${RED}Error: Virtual environment directory $VENV_DIR does not exist${NC}"
    exit 1
fi

if [ ! -d "$BACKEND_DIR" ]; then
    echo -e "${RED}Error: Backend directory $BACKEND_DIR does not exist${NC}"
    exit 1
fi

if [ ! -d "$FRONTEND_DIR" ]; then
    echo -e "${RED}Error: Frontend directory $FRONTEND_DIR does not exist${NC}"
    exit 1
fi

echo -e "${GREEN}✓ All directories found${NC}"

# Create backend systemd service
echo -e "${YELLOW}Creating backend systemd service...${NC}"
cat > /etc/systemd/system/stical-data-mig-backend.service << EOF
[Unit]
Description=STICAL Data Backend - FastAPI Application
After=network.target postgresql.service
Wants=postgresql.service

[Service]
Type=exec
User=$SERVICE_USER
Group=$SERVICE_GROUP
WorkingDirectory=$BACKEND_DIR
Environment=PATH=$VENV_DIR/bin
Environment=PYTHONPATH=$BACKEND_DIR
ExecStart=$VENV_DIR/bin/uvicorn main:app --host 0.0.0.0 --port 8000 --workers 1
ExecReload=/bin/kill -HUP \$MAINPID
Restart=always
RestartSec=3
KillMode=mixed
TimeoutStopSec=30

# Security settings
NoNewPrivileges=yes
PrivateTmp=yes
ProtectSystem=strict
ReadWritePaths=$APP_DIR
ProtectHome=yes

# Resource limits
LimitNOFILE=65536
MemoryMax=1G

[Install]
WantedBy=multi-user.target
EOF

echo -e "${GREEN}✓ Backend service created${NC}"

# Create frontend build and serve script
echo -e "${YELLOW}Creating frontend build and serve script...${NC}"
cat > $FRONTEND_DIR/serve-frontend.sh << 'EOF'
#!/bin/bash

# Frontend serving script for STICAL Data
FRONTEND_DIR="/var/www/stical-data-mig/ui"
BUILD_DIR="$FRONTEND_DIR/build"
PORT=3000

cd "$FRONTEND_DIR"

# Check if build directory exists, if not create it
if [ ! -d "$BUILD_DIR" ]; then
    echo "Build directory not found. Running npm run build..."
    npm run build
fi

# Serve the built application
echo "Starting frontend server on port $PORT..."
npx serve -s build -l $PORT
EOF

chmod +x $FRONTEND_DIR/serve-frontend.sh

# Create frontend systemd service
echo -e "${YELLOW}Creating frontend systemd service...${NC}"
cat > /etc/systemd/system/stical-data-mig-frontend.service << EOF
[Unit]
Description=STICAL Data Frontend - React Application
After=network.target stical-data-mig-backend.service
Wants=stical-data-mig-backend.service

[Service]
Type=exec
User=$SERVICE_USER
Group=$SERVICE_GROUP
WorkingDirectory=$FRONTEND_DIR
ExecStartPre=/usr/bin/npm install
ExecStartPre=/usr/bin/npm run build
ExecStart=/usr/bin/npx serve -s build -l 3000
Restart=always
RestartSec=5
KillMode=mixed
TimeoutStopSec=30

# Security settings
NoNewPrivileges=yes
PrivateTmp=yes
ProtectSystem=strict
ReadWritePaths=$FRONTEND_DIR
ProtectHome=yes

# Resource limits
LimitNOFILE=65536
MemoryMax=512M

[Install]
WantedBy=multi-user.target
EOF

echo -e "${GREEN}✓ Frontend service created${NC}"

# Create nginx configuration (optional)
echo -e "${YELLOW}Creating nginx configuration...${NC}"
cat > /etc/nginx/sites-available/stical-data-mig << 'EOF'
server {
    listen 80;
    server_name your-domain.com;  # Replace with your actual domain
    
    # Frontend (React)
    location / {
        proxy_pass http://localhost:3000;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection 'upgrade';
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_cache_bypass $http_upgrade;
    }
    
    # Backend API
    location /api/ {
        proxy_pass http://localhost:8000/;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection 'upgrade';
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_cache_bypass $http_upgrade;
        
        # CORS headers
        add_header 'Access-Control-Allow-Origin' '*' always;
        add_header 'Access-Control-Allow-Methods' 'GET, POST, OPTIONS, PUT, DELETE' always;
        add_header 'Access-Control-Allow-Headers' 'DNT,User-Agent,X-Requested-With,If-Modified-Since,Cache-Control,Content-Type,Range,Authorization' always;
        
        if ($request_method = 'OPTIONS') {
            add_header 'Access-Control-Allow-Origin' '*';
            add_header 'Access-Control-Allow-Methods' 'GET, POST, OPTIONS, PUT, DELETE';
            add_header 'Access-Control-Allow-Headers' 'DNT,User-Agent,X-Requested-With,If-Modified-Since,Cache-Control,Content-Type,Range,Authorization';
            add_header 'Access-Control-Max-Age' 1728000;
            add_header 'Content-Type' 'text/plain; charset=utf-8';
            add_header 'Content-Length' 0;
            return 204;
        }
    }
    
    # Static files for React
    location /static/ {
        alias $FRONTEND_DIR/build/static/;
        expires 1y;
        add_header Cache-Control "public, immutable";
    }
}
EOF

# Set proper ownership
echo -e "${YELLOW}Setting proper ownership...${NC}"
chown -R $SERVICE_USER:$SERVICE_GROUP $APP_DIR
chmod -R 755 $APP_DIR

# Install required system packages
echo -e "${YELLOW}Installing required system packages...${NC}"
apt-get update
apt-get install -y python3-pip nodejs npm nginx

# Install global npm packages
echo -e "${YELLOW}Installing global npm packages...${NC}"
npm install -g serve

# Install Python packages in virtual environment
echo -e "${YELLOW}Installing Python packages...${NC}"
$VENV_DIR/bin/pip install uvicorn gunicorn

# Reload systemd and enable services
echo -e "${YELLOW}Reloading systemd and enabling services...${NC}"
systemctl daemon-reload
systemctl enable stical-data-mig-backend.service
systemctl enable stical-data-mig-frontend.service

# Create management script
echo -e "${YELLOW}Creating management script...${NC}"
cat > /usr/local/bin/stical-data-mig-ctl << 'EOF'
#!/bin/bash

# STICAL Data Control Script

BACKEND_SERVICE="stical-data-mig-backend"
FRONTEND_SERVICE="stical-data-mig-frontend"

case "$1" in
    start)
        echo "Starting STICAL Data services..."
        systemctl start $BACKEND_SERVICE
        systemctl start $FRONTEND_SERVICE
        ;;
    stop)
        echo "Stopping STICAL Data services..."
        systemctl stop $FRONTEND_SERVICE
        systemctl stop $BACKEND_SERVICE
        ;;
    restart)
        echo "Restarting STICAL Data services..."
        systemctl restart $BACKEND_SERVICE
        systemctl restart $FRONTEND_SERVICE
        ;;
    status)
        echo "=== Backend Service Status ==="
        systemctl status $BACKEND_SERVICE --no-pager
        echo ""
        echo "=== Frontend Service Status ==="
        systemctl status $FRONTEND_SERVICE --no-pager
        ;;
    logs)
        if [ "$2" = "backend" ]; then
            journalctl -u $BACKEND_SERVICE -f
        elif [ "$2" = "frontend" ]; then
            journalctl -u $FRONTEND_SERVICE -f
        else
            echo "Usage: $0 logs [backend|frontend]"
        fi
        ;;
    enable)
        systemctl enable $BACKEND_SERVICE
        systemctl enable $FRONTEND_SERVICE
        echo "Services enabled for auto-start"
        ;;
    disable)
        systemctl disable $BACKEND_SERVICE
        systemctl disable $FRONTEND_SERVICE
        echo "Services disabled from auto-start"
        ;;
    *)
        echo "Usage: $0 {start|stop|restart|status|logs|enable|disable}"
        echo "  logs: use 'logs backend' or 'logs frontend' to view specific logs"
        exit 1
        ;;
esac
EOF

chmod +x /usr/local/bin/stical-data-mig-ctl

# Create environment file template
echo -e "${YELLOW}Creating environment file template...${NC}"
cat > $BACKEND_DIR/.env.template << 'EOF'
# STICAL Data Environment Variables
# Copy this file to .env and fill in the actual values

# Database
PG_DATABASE_URL=postgresql://username:password@localhost:5432/database_name
DB_NAME=ingredients_db

# JDE Configuration
JDE_BUSINESS_UNIT=1110
JDE_CARDEX_URL=https://your-jde-server/jderest/v3/orchestrator/STICAL_PO_SUMMARY
JDE_CARDEX_USERNAME=your_username
JDE_CARDEX_PASSWORD=your_password
JDE_ITEM_MASTER_UPDATES_URL=https://your-jde-server/jderest/v3/orchestrator/JDE_ITEM_MASTER_TO_BAKERY-SYSTEM

# Bakery-System Configuration
OUTLET_ID=your_outlet_id
BAKERY_SYSTEM_BASE_URL=https://your-bakery_system_server
BAKERY_SYSTEM_API_TOKEN=your_api_token

# Authentication
SECRET_KEY=your-secret-key-change-this-in-production
ALGORITHM=HS256
AUTH_LDAP_SERVER=your-ldap-server:389
AUTH_LDAP_USE_TLS=False
AUTH_LDAP_BIND_USER=your_bind_user
AUTH_LDAP_BIND_PASSWORD=your_bind_password
AUTH_LDAP_GROUP_FIELD=memberOf
AUTH_USER_SEARCH_FIELD=UserPrincipalName
AUTH_LDAP_USER_SEARCH_BASE=dc=your,dc=domain,dc=com
EOF

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}  Setup Complete!                      ${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""
echo -e "${YELLOW}Next steps:${NC}"
echo "1. Copy and configure the environment file:"
echo "   cp $BACKEND_DIR/.env.template $BACKEND_DIR/.env"
echo "   nano $BACKEND_DIR/.env"
echo ""
echo "2. Update the frontend API URL if needed:"
echo "   nano $FRONTEND_DIR/src/components/LiveDataComparison.js"
echo "   nano $FRONTEND_DIR/src/components/JdeItemMasterReview.js"
echo "   nano $FRONTEND_DIR/src/context/AuthContext.js"
echo ""
echo "3. Configure nginx (optional):"
echo "   ln -s /etc/nginx/sites-available/stical-data-mig /etc/nginx/sites-enabled/"
echo "   nginx -t && systemctl reload nginx"
echo ""
echo "4. Start the services:"
echo "   stical-data-mig-ctl start"
echo ""
echo -e "${YELLOW}Management commands:${NC}"
echo "  stical-data-mig-ctl start     - Start both services"
echo "  stical-data-mig-ctl stop      - Stop both services"
echo "  stical-data-mig-ctl restart   - Restart both services"
echo "  stical-data-mig-ctl status    - Show service status"
echo "  stical-data-mig-ctl logs backend   - View backend logs"
echo "  stical-data-mig-ctl logs frontend  - View frontend logs"
echo ""
echo -e "${YELLOW}Service files created:${NC}"
echo "  /etc/systemd/system/stical-data-mig-backend.service"
echo "  /etc/systemd/system/stical-data-mig-frontend.service"
echo "  /etc/nginx/sites-available/stical-data-mig"
echo "  /usr/local/bin/stical-data-mig-ctl"
echo ""
echo -e "${GREEN}Setup completed successfully!${NC}"
