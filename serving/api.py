"""
Serving API — основной интерфейс платформы.

Предоставляет доступ к fused-состоянию через:
- REST API (GET /state, /covariance, /health)
- WebSocket (ws://host:8002/ws) — стриминг в реальном времени
- Live Dashboard (GET /) — веб-интерфейс
- Prometheus метрики (GET /metrics)

Фильтр Калмана запущен внутри этого процесса для
единого пространства метрик Prometheus.
"""

import asyncio
import json
import sys
import os
import time
from typing import Optional

# Настройка путей импорта
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from config_loader import get_nats_url
from nats.aio.client import Client as NATS

# FastAPI и связанные импорты
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse

# Prometheus метрики
from prometheus_client import (
    Histogram, Counter, Gauge,
    generate_latest, CONTENT_TYPE_LATEST
)
from starlette.responses import Response

# C++ модуль фильтра Калмана
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'processing', 'kalman', 'build'))
import kalman_core


# ==================== Инициализация ====================

app = FastAPI(title="Sensor Fusion API", version="1.0.0")

# Глобальные переменные
nats_client: Optional[NATS] = None

# Последнее fused-состояние (для REST и WebSocket)
latest_state: dict = {
    "state": [0, 0, 0, 0],
    "covariance": [1000, 1000, 1000, 1000],
    "timestamp": 0
}

# Активные WebSocket-клиенты
websocket_clients: list = []

# ==================== Prometheus метрики ====================

FUSION_LATENCY = Histogram(
    'fusion_latency_seconds',
    'Time for predict/update cycle',
    buckets=[0.001, 0.005, 0.01, 0.05, 0.1, 0.5]
)

SENSOR_DROP_TOTAL = Counter(
    'sensor_drop_total',
    'Number of dropped sensor messages'
)

KALMAN_INNOVATION = Gauge(
    'kalman_innovation_magnitude',
    'Current innovation magnitude'
)


# ==================== Fusion Service ====================

async def run_fusion():
    """
    Запускает фильтр Калмана внутри API-процесса.
    
    Подписывается на 'filtered.sync', выполняет predict/update
    и публикует результат в 'fused.state' + рассылает WebSocket.
    """
    global nats_client, latest_state
    
    # Создаём экземпляр C++ фильтра
    kf = kalman_core.KalmanFilter()
    last_predict_time = time.time()

    async def handler(msg):
        nonlocal last_predict_time
        start = time.time()
        
        try:
            packet = json.loads(msg.data.decode())
            current_time = packet["timestamp"]

            # Predict по IMU
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

            # Update по GPS
            if "gps" in packet:
                gps = packet["gps"]
                lat = gps.get("lat", 0.0)
                lon = gps.get("lon", 0.0)
                kf.update_gps(lat, lon)
            else:
                SENSOR_DROP_TOTAL.inc()

            # Получаем состояние и ковариацию
            state = kf.get_state()
            cov = kf.get_covariance_diag()
            
            # Вычисляем инновацию для метрик
            innovation = sum(abs(x) for x in cov) / max(len(cov), 1)
            KALMAN_INNOVATION.set(innovation)
            
            # Формируем результат
            latest_state = {
                "timestamp": current_time,
                "state": [float(x) for x in state],
                "covariance": [float(x) for x in cov]
            }
            
            # Публикуем в NATS для других потребителей
            await nats_client.publish("fused.state", json.dumps(latest_state).encode())
            
            # Рассылаем WebSocket-клиентам
            for ws in websocket_clients:
                try:
                    await ws.send_json(latest_state)
                except Exception:
                    websocket_clients.remove(ws)
            
            # Записываем задержку
            FUSION_LATENCY.observe(time.time() - start)
            
        except Exception as e:
            print(f"Error in fusion: {e}")

    # Подписываемся на отфильтрованные синхропакеты
    await nats_client.subscribe("filtered.sync", cb=handler)
    print("Fusion service started inside API")


# ==================== Lifespan Events ====================

@app.on_event("startup")
async def startup():
    """Запуск при старте приложения."""
    global nats_client
    nats_client = NATS()
    await nats_client.connect(get_nats_url())
    
    # Запускаем fusion в фоне
    asyncio.create_task(run_fusion())
    print("Serving API with embedded fusion started")


@app.on_event("shutdown")
async def shutdown():
    """Остановка приложения."""
    if nats_client:
        await nats_client.close()


# ==================== REST Endpoints ====================

@app.get("/state")
async def get_state():
    """
    Возвращает текущее fused-состояние.
    
    Returns:
        dict: состояние [x, y, vx, vy] и ковариация.
    """
    return latest_state


@app.get("/covariance")
async def get_covariance():
    """
    Возвращает диагональ ковариационной матрицы.
    
    Показывает уверенность фильтра в каждой компоненте состояния.
    """
    return {
        "timestamp": latest_state["timestamp"],
        "covariance": latest_state["covariance"]
    }


@app.get("/health")
async def health():
    """Проверка здоровья сервиса."""
    return {
        "status": "healthy",
        "latest_timestamp": latest_state["timestamp"]
    }


@app.get("/metrics")
async def metrics():
    """
    Endpoint для сбора метрик Prometheus.
    
    Возвращает метрики в формате, понятном Prometheus:
    - fusion_latency_seconds
    - sensor_drop_total
    - kalman_innovation_magnitude
    """
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


# ==================== WebSocket ====================

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """
    WebSocket для стриминга fused-состояния в реальном времени.
    
    При подключении сразу отправляет текущее состояние,
    затем шлёт обновления по мере поступления.
    """
    await websocket.accept()
    websocket_clients.append(websocket)
    
    try:
        # Сразу отправляем текущее состояние
        await websocket.send_json(latest_state)
        
        # Держим соединение открытым
        while True:
            data = await websocket.receive_text()
            if data == "ping":
                await websocket.send_text("pong")
    except WebSocketDisconnect:
        pass
    finally:
        if websocket in websocket_clients:
            websocket_clients.remove(websocket)


# ==================== Dashboard ====================

@app.get("/", response_class=HTMLResponse)
async def root():
    """
    Живой дашборд — HTML-страница с WebSocket-подключением.
    
    Отображает координаты X, Y и скорость в реальном времени.
    """
    return """<!DOCTYPE html>
<html>
<head>
    <title>Sensor Fusion Dashboard</title>
    <style>
        body {
            font-family: 'Segoe UI', Arial, sans-serif;
            margin: 20px;
            background: #1e1e1e;
            color: #ffffff;
        }
        h1 { color: #4ec9b0; }
        #state {
            background: #2d2d2d;
            padding: 20px;
            border-radius: 8px;
            font-family: 'Consolas', monospace;
            font-size: 18px;
            line-height: 1.8;
        }
        .label { color: #4ec9b0; }
        .value { color: #ce9178; }
    </style>
</head>
<body>
    <h1>🛰️ Sensor Fusion Live</h1>
    <div id="state">Waiting for data...</div>

    <script>
        // Подключаемся к WebSocket
        const ws = new WebSocket('ws://' + location.host + '/ws');
        
        ws.onmessage = (event) => {
            const data = JSON.parse(event.data);
            document.getElementById('state').innerHTML =
                '<span class="label">Timestamp:</span> <span class="value">' +
                data.timestamp.toFixed(3) + '</span><br>' +
                '<span class="label">Position X:</span> <span class="value">' +
                data.state[0].toFixed(6) + '</span><br>' +
                '<span class="label">Position Y:</span> <span class="value">' +
                data.state[1].toFixed(6) + '</span><br>' +
                '<span class="label">Velocity X:</span> <span class="value">' +
                data.state[2].toFixed(6) + '</span><br>' +
                '<span class="label">Velocity Y:</span> <span class="value">' +
                data.state[3].toFixed(6) + '</span>';
        };
        
        ws.onerror = () => {
            document.getElementById('state').textContent = 'Connection error';
        };
        
        ws.onclose = () => {
            document.getElementById('state').textContent = 'Connection closed. Refresh page.';
        };
    </script>
</body>
</html>"""


# ==================== Запуск ====================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8002)