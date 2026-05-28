# scripts/start_all.ps1
$ErrorActionPreference = "Stop"

# Check if NATS is available
Write-Host "Checking NATS server..." -ForegroundColor Cyan
try {
    $response = Invoke-WebRequest -Uri "http://192.168.2.123:8222" -TimeoutSec 5
    Write-Host "NATS is running" -ForegroundColor Green
} catch {
    Write-Host "ERROR: NATS is not available! Start NATS container first." -ForegroundColor Red
    exit 1
}

# Define services
$services = @(
    @{Name="IMU Simulator";      Script="sensors/imu_sim.py"},
    @{Name="GPS Simulator";      Script="sensors/gps_sim.py"},
    @{Name="Time Synchronizer";  Script="processing/time_sync/sync_service.py"},
    @{Name="Outlier Filter";     Script="processing/noise_filter/outlier_filter.py"},
    @{Name="Fusion Core (C++)";  Script="processing/kalman/fusion_service.py"},
    @{Name="Fused Subscriber";   Script="tests/manual/fused_subscriber_test.py"}
)

# Start each service in a new PowerShell window
$rootPath = (Get-Item .).Parent.FullName

foreach ($service in $services) {
    Write-Host "Starting: $($service.Name)" -ForegroundColor Yellow
    Start-Process powershell -ArgumentList @(
        "-NoExit",
        "-Command",
        "cd '$rootPath'; .venv\Scripts\Activate.ps1; python $($service.Script)"
    )
}

Write-Host "All services started in separate windows!" -ForegroundColor Green
Write-Host "Close each window or press Ctrl+C to stop." -ForegroundColor Yellow