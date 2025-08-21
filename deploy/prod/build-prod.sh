#!/bin/bash

# Simple Production Build Script
set -e

# Configuration
BACKEND_HOST=${BACKEND_HOST:-localhost}
BACKEND_PORT=${BACKEND_PORT:-9998}
FRONTEND_PORT=${FRONTEND_PORT:-9999}

echo "Building STICAL Data App for production..."
echo "Backend Host: $BACKEND_HOST"
echo "Backend Port: $BACKEND_PORT"
echo "Frontend Port: $FRONTEND_PORT"

# Set environment variables for build
export REACT_APP_BACKEND_HOST=$BACKEND_HOST
export REACT_APP_BACKEND_PORT=$BACKEND_PORT
export REACT_APP_BACKEND_URL="http://$BACKEND_HOST:$BACKEND_PORT"
export REACT_APP_ENVIRONMENT=production

echo "Environment variables:"
echo "  REACT_APP_BACKEND_HOST=$REACT_APP_BACKEND_HOST"
echo "  REACT_APP_BACKEND_PORT=$REACT_APP_BACKEND_PORT"
echo "  REACT_APP_BACKEND_URL=$REACT_APP_BACKEND_URL"
echo "  REACT_APP_ENVIRONMENT=$REACT_APP_ENVIRONMENT"

# Navigate to UI directory
cd ui

# Install dependencies if needed
if [ ! -d "node_modules" ]; then
    echo "Installing dependencies..."
    npm install
fi

# Build the React app
echo "Building React app..."
npm run build

# Copy production API configuration
echo "Copying production API configuration..."
cp ../deploy/prod/api.js build/static/js/

# Create runtime config file
echo "Creating runtime configuration..."
cat > build/config.js << EOF
// Runtime configuration for production
window.STICAL_CONFIG = {
  BACKEND_URL: 'http://$BACKEND_HOST:$BACKEND_PORT',
  BACKEND_HOST: '$BACKEND_HOST',
  BACKEND_PORT: '$BACKEND_PORT',
  FRONTEND_PORT: '$FRONTEND_PORT'
};
console.log('Runtime config loaded:', window.STICAL_CONFIG);
EOF

# Update index.html to include config.js
echo "Updating index.html..."
if ! grep -q "config.js" build/index.html; then
    sed -i 's|<head>|<head>\n    <script src="/config.js"></script>|' build/index.html
fi

echo ""
echo "Production build completed successfully!"
echo "Build directory: $(pwd)/build"
echo ""
echo "To serve the production build:"
echo "  npx serve -s build -l $FRONTEND_PORT"
echo ""
echo "To serve on all interfaces:"
echo "  npx serve -s build --listen tcp://0.0.0.0:$FRONTEND_PORT"
echo ""
echo "Access URLs:"
echo "  Frontend: http://localhost:$FRONTEND_PORT"
echo "  Backend:  http://$BACKEND_HOST:$BACKEND_PORT"
echo ""
echo "To build with custom host:"
echo "  BACKEND_HOST=127.0.0.1 ./deploy/prod/build-prod.sh"
echo "  BACKEND_HOST=192.168.1.100 ./deploy/prod/build-prod.sh"
