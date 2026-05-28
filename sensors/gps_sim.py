"""
Симулятор GPS-приёмника.

Генерирует синтетические координаты (широта, долгота, высота)
для объекта, движущегося по окружности. Частота публикации: 10 Гц.

Данные публикуются в NATS-топик 'sensor.gps'.
"""

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
    """
    Основная функция GPS-симулятора.
    
    Моделирует движение по окружности с пересчётом
    в географические координаты (WGS84).
    """
    nc = NATS()
    await nc.connect(get_nats_url())

    # Параметры движения
    radius = 10.0          # радиус окружности в метрах
    omega = 1.0            # угловая скорость в рад/с
    
    # Начальная точка — центр Москвы
    base_lat = 55.7558
    base_lon = 37.6176
    
    # Перевод метров в градусы (приближённо)
    lat_per_m = 1 / 111320.0                          # 1° широты ≈ 111.32 км
    lon_per_m = 1 / (111320.0 * math.cos(math.radians(base_lat)))  # зависит от широты

    try:
        while True:
            t = time.time()
            
            # Истинное положение на окружности (в метрах от центра)
            dx = radius * math.cos(omega * t)
            dy = radius * math.sin(omega * t)
            
            # Пересчитываем в градусы
            lat = base_lat + dy * lat_per_m
            lon = base_lon + dx * lon_per_m
            
            # Добавляем шум GPS (σ ≈ 3 метра)
            lat += random.gauss(0, 3.0) * lat_per_m
            lon += random.gauss(0, 3.0) * lon_per_m
            
            # Высота с шумом (σ = 5 м)
            alt = 150.0 + random.gauss(0, 5.0)
            
            # Estimated Position Error — точность по горизонтали
            eph = 3.0

            message = {
                "timestamp": time.time(),
                "lat": lat,      # широта, градусы
                "lon": lon,      # долгота, градусы
                "alt": alt,      # высота, метры
                "eph": eph       # ожидаемая ошибка, метры
            }

            await nc.publish("sensor.gps", json.dumps(message).encode())
            print(f"GPS published: {message}")
            
            # 10 Гц = 100 мс
            await asyncio.sleep(0.1)

    except KeyboardInterrupt:
        print("GPS simulator stopped")
    finally:
        await nc.close()


if __name__ == "__main__":
    asyncio.run(main())