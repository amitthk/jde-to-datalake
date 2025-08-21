const fs = require('fs');
const path = require('path');

console.log('Preparing production build...');

// Source and destination paths
const prodApiFile = path.join(__dirname, '..', '..', 'deploy', 'prod', 'api.js');
const targetApiFile = path.join(__dirname, '..', 'src', 'config', 'api.js');

// Copy production API configuration
try {
  if (fs.existsSync(prodApiFile)) {
    fs.copyFileSync(prodApiFile, targetApiFile);
    console.log('✓ Production API configuration copied');
  } else {
    console.log('! Production API config not found, using default');
  }
} catch (error) {
  console.error('Error copying API config:', error.message);
  process.exit(1);
}

console.log('Production preparation complete!');
