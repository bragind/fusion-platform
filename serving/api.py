import asyncio
import json
import sys
import os
from typing import Optional

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from config_loader import get_nats_url
from nats.aio.client import Client as NATS
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
from starlette.responses import Response

app = FastAPI(title="Sensor Fusion API", version="1.0.0")

nats_client: Optional[NATS] = None
latest_state: dict = {"state": [0, 0, 0, 0], "covariance": [1000, 1000, 1000, 1000], "timestamp": 0}
websocket_clients: list = []

@app.on_event("startup")
async def startup():
    global nats_client
    nats_client = NATS()
    await nats_client.connect(get_nats_url())
    
    async def state_handler(msg):
        global latest_state
        data = json.loads(msg.data.decode())
        latest_state = data
        
        for ws in websocket_clients:
            try:
                await ws.send_json(data)
            except:
                websocket_clients.remove(ws)
    
    await nats_client.subscribe("fused.state", cb=state_handler)
    print("Serving API started, listening for fused state...")

@app.on_event("shutdown")
async def shutdown():
    if nats_client:
        await nats_client.close()

@app.get("/state")
async def get_state():
    return latest_state

@app.get("/covariance")
async def get_covariance():
    return {
        "timestamp": latest_state["timestamp"],
        "covariance": latest_state["covariance"]
    }

@app.get("/health")
async def health():
    return {
        "status": "healthy",
        "nats_connected": nats_client is not None,
        "latest_timestamp": latest_state["timestamp"]
    }

@app.get("/metrics")
async def metrics():
    """Prometheus metrics endpoint"""
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    websocket_clients.append(websocket)
    try:
        await websocket.send_json(latest_state)
        while True:
            data = await websocket.receive_text()
            if data == "ping":
                await websocket.send_text("pong")
    except WebSocketDisconnect:
        pass
    finally:
        if websocket in websocket_clients:
            websocket_clients.remove(websocket)

@app.get("/", response_class=HTMLResponse)
async def root():
    return """<!DOCTYPE html>
<html>
<head>
    <title>Sensor Fusion Dashboard</title>
    <style>
        body { font-family: Arial; margin: 20px; background: #1e1e1e; color: #fff; }
        #state { background: #2d2d2d; padding: 15px; border-radius: 8px; font-family: monospace; }
        .label { color: #4ec9b0; } .value { color: #ce9178; }
    </style>
</head>
<body>
    <h1>Sensor Fusion Live</h1>
    <div id="state">Waiting for data...</div>
    <script>
        const ws = new WebSocket('ws://' + location.host + '/ws');
        ws.onmessage = (event) => {
            const data = JSON.parse(event.data);
            document.getElementById('state').innerHTML = 
                '<span class="label">Timestamp:</span> ' + data.timestamp.toFixed(3) +
                ' | <span class="label">X:</span> ' + data.state[0].toFixed(6) +
                ' | <span class="label">Y:</span> ' + data.state[1].toFixed(6) +
                ' | <span class="label">Vx:</span> ' + data.state[2].toFixed(4) +
                ' | <span class="label">Vy:</span> ' + data.state[3].toFixed(4);
        };
    </script>
</body>
</html>"""

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8002)
