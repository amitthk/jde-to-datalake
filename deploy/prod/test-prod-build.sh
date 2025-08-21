#!/bin/bash

# Test script for production build
set -e

echo "Testing production build..."

# Set backend URL explicitly for testing
export REACT_APP_BACKEND_URL="http://localhost:9998"

# Build for production
echo "Building React app..."
cd ui
npm run build

# Copy production config
echo "Copying production API configuration..."
cp ../deploy/prod/api.js build/static/js/api.js

# Create config.js in build directory for runtime config
echo "Creating runtime configuration..."
cat > build/config.js << 'EOF'
// Runtime configuration
window.STICAL_CONFIG = {
  BACKEND_URL: 'http://localhost:9998'
};
EOF

# Update index.html to include config.js
echo "Updating index.html to include runtime config..."
sed -i 's|<head>|<head>\n    <script src="/config.js"></script>|' build/index.html

echo "Production build ready. Contents of build directory:"
ls -la build/

echo ""
echo "To test the production build:"
echo "1. Ensure backend is running on port 9998"
echo "2. Run: npx serve -s build -l 9999"
echo "3. Open browser to http://localhost:9999"

echo ""
echo "To check API configuration in browser console:"
echo "- Open developer tools"
echo "- Look for 'Production API Base URL:' message"
echo "- Check network tab for actual API calls"
