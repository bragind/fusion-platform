import asyncio
import json
import sys
import os
import time
from typing import Optional

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from config_loader import get_nats_url
from nats.aio.client import Client as NATS
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from prometheus_client import Histogram, Counter, Gauge, generate_latest, CONTENT_TYPE_LATEST
from starlette.responses import Response

# Добавляем путь к C++ модулю
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'processing', 'kalman', 'build'))
import kalman_core

app = FastAPI(title="Sensor Fusion API", version="1.0.0")

# Глобальные переменные
nats_client: Optional[NATS] = None
latest_state: dict = {"state": [0, 0, 0, 0], "covariance": [1000, 1000, 1000, 1000], "timestamp": 0}
websocket_clients: list = []

# Prometheus метрики
FUSION_LATENCY = Histogram('fusion_latency_seconds', 'Time for predict/update cycle')
SENSOR_DROP_TOTAL = Counter('sensor_drop_total', 'Number of dropped sensor messages')
KALMAN_INNOVATION = Gauge('kalman_innovation_magnitude', 'Current innovation magnitude')

# Fusion сервис (запускается в фоне)
async def run_fusion():
    global nats_client, latest_state
    kf = kalman_core.KalmanFilter()
    last_predict_time = time.time()

    async def handler(msg):
        nonlocal last_predict_time
        start = time.time()
        
        try:
            packet = json.loads(msg.data.decode())
            current_time = packet["timestamp"]

            if "imu" in packet:
                imu = packet["imu"]
                ax = imu.get("accel_x", 0.0)
                ay = imu.get("accel_y", 0.0)
                dt = current_time - last_predict_time
                if dt > 0 and dt < 0.5:
                    kf.predict(ax, ay, dt)
            else:
                SENSOR_DROP_TOTAL.inc()

            last_predict_time = current_time

            if "gps" in packet:
                gps = packet["gps"]
                lat = gps.get("lat", 0.0)
                lon = gps.get("lon", 0.0)
                kf.update_gps(lat, lon)
            else:
                SENSOR_DROP_TOTAL.inc()

            state = kf.get_state()
            cov = kf.get_covariance_diag()
            
            innovation = sum(abs(x) for x in cov) / max(len(cov), 1)
            KALMAN_INNOVATION.set(innovation)
            
            latest_state = {
                "timestamp": current_time,
                "state": [float(x) for x in state],
                "covariance": [float(x) for x in cov]
            }
            await nats_client.publish("fused.state", json.dumps(latest_state).encode())
            
            # Рассылаем WebSocket клиентам
            for ws in websocket_clients:
                try:
                    await ws.send_json(latest_state)
                except:
                    websocket_clients.remove(ws)
            
            FUSION_LATENCY.observe(time.time() - start)
            
        except Exception as e:
            print(f"Error in fusion: {e}")

    await nats_client.subscribe("filtered.sync", cb=handler)
    print("Fusion service started inside API")

@app.on_event("startup")
async def startup():
    global nats_client
    nats_client = NATS()
    await nats_client.connect(get_nats_url())
    asyncio.create_task(run_fusion())
    print("Serving API with embedded fusion started")

@app.on_event("shutdown")
async def shutdown():
    if nats_client:
        await nats_client.close()

@app.get("/state")
async def get_state():
    return latest_state

@app.get("/covariance")
async def get_covariance():
    return {"timestamp": latest_state["timestamp"], "covariance": latest_state["covariance"]}

@app.get("/health")
async def health():
    return {"status": "healthy", "latest_timestamp": latest_state["timestamp"]}

@app.get("/metrics")
async def metrics():
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    websocket_clients.append(websocket)
    try:
        await websocket.send_json(latest_state)
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        if websocket in websocket_clients:
            websocket_clients.remove(websocket)

@app.get("/", response_class=HTMLResponse)
async def root():
    return """<!DOCTYPE html>
<html><head><title>Fusion Dashboard</title>
<style>body{font-family:Arial;margin:20px;background:#1e1e1e;color:#fff}#state{background:#2d2d2d;padding:15px;border-radius:8px;font-family:monospace}.label{color:#4ec9b0}.value{color:#ce9178}</style></head>
<body><h1>Sensor Fusion Live</h1><div id="state">Waiting...</div>
<script>const ws=new WebSocket('ws://'+location.host+'/ws');ws.onmessage=(e)=>{const d=JSON.parse(e.data);document.getElementById('state').innerHTML='<span class=label>X:</span> <span class=value>'+d.state[0].toFixed(6)+'</span> | <span class=label>Y:</span> <span class=value>'+d.state[1].toFixed(6)+'</span>'}</script></body></html>"""

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8002)