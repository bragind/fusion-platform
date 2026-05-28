"""
Симулятор IMU (Inertial Measurement Unit).

Генерирует синтетические данные акселерометра и гироскопа,
моделируя движение объекта по окружности радиусом 10 метров
с угловой скоростью 1 рад/с. Частота публикации: 100 Гц.

Данные публикуются в NATS-топик 'sensor.imu'.
"""

import asyncio
import json
import time
import math
import random
import sys
import os

# Добавляем корень проекта в путь для импорта config_loader
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from config_loader import get_nats_url
from nats.aio.client import Client as NATS


async def main():
    """
    Основная функция симулятора.
    
    Подключается к NATS и в бесконечном цикле генерирует
    показания IMU с частотой 100 Гц (каждые 10 мс).
    """
    # Подключаемся к NATS
    nc = NATS()
    await nc.connect(get_nats_url())

    # Параметры модели движения
    radius = 10.0     # радиус окружности в метрах
    omega = 1.0       # угловая скорость в рад/с

    try:
        while True:
            # Текущее время симуляции
            t = time.time()
            
            # Истинные ускорения (без шума)
            # Центростремительное ускорение: a = ω²r
            true_accel_x = -radius * omega**2 * math.cos(omega * t)
            true_accel_y = -radius * omega**2 * math.sin(omega * t)
            true_accel_z = 9.81  # сила тяжести

            # Добавляем гауссов шум (σ = 0.1 м/с² для акселерометра)
            accel_x = true_accel_x + random.gauss(0, 0.1)
            accel_y = true_accel_y + random.gauss(0, 0.1)
            accel_z = true_accel_z + random.gauss(0, 0.1)
            
            # Гироскоп: угловая скорость вокруг оси Z + шум
            gyro_z = omega + random.gauss(0, 0.01)

            # Формируем сообщение
            message = {
                "timestamp": time.time(),  # Unix timestamp
                "accel_x": accel_x,        # ускорение по X, м/с²
                "accel_y": accel_y,        # ускорение по Y, м/с²
                "accel_z": accel_z,        # ускорение по Z, м/с²
                "gyro_x": 0.0,             # угловая скорость по X
                "gyro_y": 0.0,             # угловая скорость по Y
                "gyro_z": gyro_z           # угловая скорость по Z
            }

            # Публикуем в NATS
            await nc.publish("sensor.imu", json.dumps(message).encode())
            print(f"IMU published: {message}")
            
            # Ждём 10 мс (100 Гц)
            await asyncio.sleep(0.01)

    except KeyboardInterrupt:
        print("IMU simulator stopped")
    finally:
        await nc.close()


if __name__ == "__main__":
    asyncio.run(main())