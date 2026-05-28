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
    base_lat = 55.7558
    base_lon = 37.6176
    lat_per_m = 1 / 111320.0
    lon_per_m = 1 / (111320.0 * math.cos(math.radians(base_lat)))

    try:
        while True:
            t = time.time()
            dx = radius * math.cos(omega * t)
            dy = radius * math.sin(omega * t)
            lat = base_lat + dy * lat_per_m + random.gauss(0, 3.0) * lat_per_m
            lon = base_lon + dx * lon_per_m + random.gauss(0, 3.0) * lon_per_m
            alt = 150.0 + random.gauss(0, 5.0)
            eph = 3.0

            message = {
                "timestamp": time.time(),
                "lat": lat,
                "lon": lon,
                "alt": alt,
                "eph": eph
            }

            await nc.publish("sensor.gps", json.dumps(message).encode())
            print(f"GPS published: {message}")
            await asyncio.sleep(0.1)

    except KeyboardInterrupt:
        print("GPS simulator stopped")
    finally:
        await nc.close()

if __name__ == "__main__":
    asyncio.run(main())