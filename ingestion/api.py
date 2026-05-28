import asyncio
import json
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from nats.aio.client import Client as NATS

app = FastAPI(title="Sensor Ingestion API")
nats_client: NATS = None

# Модели данных для валидации
class IMUData(BaseModel):
    timestamp: float
    accel_x: float
    accel_y: float
    accel_z: float
    gyro_x: float
    gyro_y: float
    gyro_z: float

class GPSData(BaseModel):
    timestamp: float
    lat: float
    lon: float
    alt: float
    eph: float

class LiDARData(BaseModel):
    timestamp: float
    points: list  # список [x,y,z]

# Фоновое подключение к NATS при старте
@app.on_event("startup")
async def connect_nats():
    global nats_client
    nats_client = NATS()
    await nats_client.connect("nats://192.168.2.123:4222")

@app.on_event("shutdown")
async def disconnect_nats():
    if nats_client:
        await nats_client.close()

# Эндпоинты для разных типов сенсоров
@app.post("/ingest/imu")
async def ingest_imu(data: IMUData):
    if nats_client is None:
        raise HTTPException(status_code=503, detail="NATS not connected")
    await nats_client.publish("sensor.imu", data.json().encode())
    return {"status": "ok", "sensor": "imu"}

@app.post("/ingest/gps")
async def ingest_gps(data: GPSData):
    if nats_client is None:
        raise HTTPException(status_code=503, detail="NATS not connected")
    await nats_client.publish("sensor.gps", data.json().encode())
    return {"status": "ok", "sensor": "gps"}

@app.post("/ingest/lidar")
async def ingest_lidar(data: LiDARData):
    if nats_client is None:
        raise HTTPException(status_code=503, detail="NATS not connected")
    await nats_client.publish("sensor.lidar", data.json().encode())
    return {"status": "ok", "sensor": "lidar"}

# Проверка здоровья
@app.get("/health")
async def health():
    return {"status": "healthy", "nats": nats_client is not None}
