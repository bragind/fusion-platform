import asyncio
import json
import sys
import os
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from config_loader import get_nats_url
from nats.aio.client import Client as NATS

# Добавляем путь к C++ модулю
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'build'))
import kalman_core

async def main():
    nc = NATS()
    await nc.connect(get_nats_url())
    kf = kalman_core.KalmanFilter()
    last_predict_time = time.time()

    async def handler(msg):
        nonlocal last_predict_time
        packet = json.loads(msg.data.decode())
        current_time = packet["timestamp"]

        if "imu" in packet:
            imu = packet["imu"]
            ax = imu.get("accel_x", 0.0)
            ay = imu.get("accel_y", 0.0)
            dt = current_time - last_predict_time
            if dt > 0 and dt < 0.5:
                kf.predict(ax, ay, dt)

        last_predict_time = current_time

        if "gps" in packet:
            gps = packet["gps"]
            lat = gps.get("lat", 0.0)
            lon = gps.get("lon", 0.0)
            kf.update_gps(lat, lon)

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
    print(f"Kalman fusion service (C++ core) started on {get_nats_url()}...")
    try:
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        print("Shutting down...")
    finally:
        await nc.close()

if __name__ == "__main__":
    asyncio.run(main())