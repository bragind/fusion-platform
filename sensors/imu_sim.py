import asyncio
import json
import time
import math
import random
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from config_loader import get_nats_url
from nats.aio.client import Client as NATS

async def main():
    nc = NATS()
    await nc.connect(get_nats_url())

    radius = 10.0
    omega = 1.0

    try:
        while True:
            t = time.time()
            true_accel_x = -radius * omega**2 * math.cos(omega * t)
            true_accel_y = -radius * omega**2 * math.sin(omega * t)
            true_accel_z = 9.81

            accel_x = true_accel_x + random.gauss(0, 0.1)
            accel_y = true_accel_y + random.gauss(0, 0.1)
            accel_z = true_accel_z + random.gauss(0, 0.1)
            gyro_z = omega + random.gauss(0, 0.01)

            message = {
                "timestamp": time.time(),
                "accel_x": accel_x,
                "accel_y": accel_y,
                "accel_z": accel_z,
                "gyro_x": 0.0,
                "gyro_y": 0.0,
                "gyro_z": gyro_z
            }

            await nc.publish("sensor.imu", json.dumps(message).encode())
            print(f"IMU published: {message}")
            await asyncio.sleep(0.01)

    except KeyboardInterrupt:
        print("IMU simulator stopped")
    finally:
        await nc.close()

if __name__ == "__main__":
    asyncio.run(main())