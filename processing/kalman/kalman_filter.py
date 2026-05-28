import asyncio
import json
import sys
import os
import time

# Добавляем путь к собранному модулю
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'build'))

import kalman_core
from nats.aio.client import Client as NATS

async def main():
    nc = NATS()
    await nc.connect("nats://192.168.2.123:4222")
    kf = kalman_core.KalmanFilter()
    last_predict_time = time.time()

    async def handler(msg):
        nonlocal last_predict_time
        packet = json.loads(msg.data.decode())
        current_time = packet["timestamp"]

        # Predict по данным IMU
        if "imu" in packet:
            imu = packet["imu"]
            ax = imu.get("accel_x", 0.0)
            ay = imu.get("accel_y", 0.0)
            dt = current_time - last_predict_time
            if dt > 0 and dt < 0.5:
                kf.predict(ax, ay, dt)

        last_predict_time = current_time

        # Update по GPS
        if "gps" in packet:
            gps = packet["gps"]
            lat = gps.get("lat", 0.0)
            lon = gps.get("lon", 0.0)
            kf.update_gps(lat, lon)

        # Публикуем fused-состояние
        state = kf.get_state()
        cov = kf.get_covariance_diag()
        fused_msg = {
            "timestamp": current_time,
            "state": [float(x) for x in state],
            "covariance": [float(x) for x in cov]
        }
        await nc.publish("fused.state", json.dumps(fused_msg).encode())
        print(f"Fused: {fused_msg}")

    await nc.subscribe("filtered.sync", cb=handler)
    print("Kalman fusion service (C++ core) started...")
    try:
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        print("Shutting down...")
    finally:
        await nc.close()

if __name__ == "__main__":
    asyncio.run(main())
