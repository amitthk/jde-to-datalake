# Test script for production build on Windows
param(
    [string]$BackendUrl = "http://localhost:9998"
)

Write-Host "Testing production build..." -ForegroundColor Green

# Set backend URL explicitly for testing
$env:REACT_APP_BACKEND_URL = $BackendUrl

try {
    # Build for production
    Write-Host "Building React app..." -ForegroundColor Yellow
    Set-Location -Path "ui"
    npm run build
    
    if ($LASTEXITCODE -ne 0) {
        throw "Build failed"
    }

    # Copy production config
    Write-Host "Copying production API configuration..." -ForegroundColor Yellow
    Copy-Item -Path "..\deploy\prod\api.js" -Destination "build\static\js\api.js" -Force

    # Create config.js in build directory for runtime config
    Write-Host "Creating runtime configuration..." -ForegroundColor Yellow
    $configContent = @"
// Runtime configuration
window.STICAL_CONFIG = {
  BACKEND_URL: '$BackendUrl'
};
"@
    $configContent | Out-File -FilePath "build\config.js" -Encoding UTF8

    # Update index.html to include config.js
    Write-Host "Updating index.html to include runtime config..." -ForegroundColor Yellow
    $indexPath = "build\index.html"
    $indexContent = Get-Content -Path $indexPath -Raw
    
    if ($indexContent -notmatch '<script src="/config.js"></script>') {
        $indexContent = $indexContent -replace '<head>', "<head>`n    <script src=`"/config.js`"></script>"
        $indexContent | Out-File -FilePath $indexPath -Encoding UTF8
    }

    Write-Host "Production build ready. Contents of build directory:" -ForegroundColor Green
    Get-ChildItem -Path "build" | Select-Object Name, Length, LastWriteTime

    Write-Host ""
    Write-Host "To test the production build:" -ForegroundColor Cyan
    Write-Host "1. Ensure backend is running on port 9998" -ForegroundColor White
    Write-Host "2. Run: npx serve -s build -l 9999" -ForegroundColor White
    Write-Host "3. Open browser to http://localhost:9999" -ForegroundColor White

    Write-Host ""
    Write-Host "To check API configuration in browser console:" -ForegroundColor Cyan
    Write-Host "- Open developer tools" -ForegroundColor White
    Write-Host "- Look for 'Production API Base URL:' message" -ForegroundColor White
    Write-Host "- Check network tab for actual API calls" -ForegroundColor White

} catch {
    Write-Error "Build test failed: $_"
    exit 1
} finally {
    Set-Location -Path ".."
}
