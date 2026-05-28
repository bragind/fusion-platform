# add_metrics_endpoint.ps1
# Добавляет эндпоинт /metrics и нужные импорты в serving/api.py

$apiFile = "serving/api.py"

Write-Host "Updating $apiFile..." -ForegroundColor Cyan

# Читаем текущее содержимое
$content = Get-Content $apiFile -Raw

# 1. Добавляем импорты prometheus_client и Response
$importBlock = @"
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
from starlette.responses import Response
"@

if ($content -notmatch "from prometheus_client") {
    $content = $content -replace "(from fastapi.responses import HTMLResponse)", "`$1`n$importBlock"
    Write-Host "Added prometheus_client imports" -ForegroundColor Green
}

# 2. Добавляем эндпоинт /metrics перед if __name__
$metricsEndpoint = @"

@app.get("/metrics")
async def metrics():
    """Prometheus metrics endpoint"""
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)
"@

if ($content -notmatch "@app.get\(\"/metrics\"\)") {
    $content = $content -replace '(if __name__ == "__main__":)', "$metricsEndpoint`n`n`$1"
    Write-Host "Added /metrics endpoint" -ForegroundColor Green
}

# Сохраняем
Set-Content $apiFile -Value $content -NoNewline
Write-Host "Done! Check $apiFile" -ForegroundColor Green
