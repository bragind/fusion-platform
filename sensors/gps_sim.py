import asyncio
import json
import time
import math
import random
from nats.aio.client import Client as NATS

async def main():
    nc = NATS()
    await nc.connect("nats://localhost:4222")

    # Параметры движения: такая же окружность, как у IMU
    radius = 10.0          # метров
    omega = 1.0            # рад/с
    # Начальная точка (широта, долгота) – центр Москвы для примера
    base_lat = 55.7558
    base_lon = 37.6176
    # Перевод метров в градусы (грубо)
    lat_per_m = 1 / 111320.0
    lon_per_m = 1 / (111320.0 * math.cos(math.radians(base_lat)))

    start_time = time.time()

    try:
        while True:
            t = time.time() - start_time
            # Истинное положение на окружности (в метрах от центра)
            dx = radius * math.cos(omega * t)
            dy = radius * math.sin(omega * t)
            # Пересчитываем в широту/долготу
            lat = base_lat + dy * lat_per_m
            lon = base_lon + dx * lon_per_m
            # Добавляем гауссов шум (σ=3 м)
            lat += random.gauss(0, 3.0) * lat_per_m
            lon += random.gauss(0, 3.0) * lon_per_m
            # Высота (пусть постоянная, тоже с шумом)
            alt = 150.0 + random.gauss(0, 5.0)
            # EPH – оценка точности по горизонтали в метрах
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
            await asyncio.sleep(0.1)  # 10 Гц (типичная частота GPS)

    except KeyboardInterrupt:
        print("GPS simulator stopped")
    finally:
        await nc.close()

if __name__ == "__main__":
    asyncio.run(main())