import asyncio
import json
import sys
import os
import time
from prometheus_client import Histogram, Counter, Gauge

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from config_loader import get_nats_url
from nats.aio.client import Client as NATS

sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'build'))
import kalman_core

# Prometheus метрики
FUSION_LATENCY = Histogram('fusion_latency_seconds', 'Time for predict/update cycle', buckets=[0.001, 0.005, 0.01, 0.05, 0.1, 0.5])
SENSOR_DROP_TOTAL = Counter('sensor_drop_total', 'Number of dropped sensor messages')
KALMAN_INNOVATION = Gauge('kalman_innovation_magnitude', 'Current innovation magnitude')

async def main():
    nc = NATS()
    await nc.connect(get_nats_url())
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
            
            fused_msg = {
                "timestamp": current_time,
                "state": [float(x) for x in state],
                "covariance": [float(x) for x in cov]
            }
            await nc.publish("fused.state", json.dumps(fused_msg).encode())
            
            FUSION_LATENCY.observe(time.time() - start)
            
        except Exception as e:
            print(f"Error in fusion: {e}")

    await nc.subscribe("filtered.sync", cb=handler)
    print(f"Fusion service with metrics started on {get_nats_url()}...")
    
    try:
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        print("Shutting down...")
    finally:
        await nc.close()

if __name__ == "__main__":
    asyncio.run(main())