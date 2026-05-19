import asyncio
import json
import time
import math
import random
from nats.aio.client import Client as NATS

async def main():
    nc = NATS()
    await nc.connect("nats://localhost:4222")

    # Параметры симуляции: движение по окружности радиусом 10 м, скорость 1 рад/с
    radius = 10.0
    omega = 1.0  # угловая скорость рад/с
    start_time = time.time()

    try:
        while True:
            t = time.time() - start_time
            # Истинные ускорения без шума
            true_accel_x = -radius * omega**2 * math.cos(omega * t)
            true_accel_y = -radius * omega**2 * math.sin(omega * t)
            true_accel_z = 9.81  # сила тяжести

            # Добавляем гауссов шум (стандартное отклонение 0.1 м/с²)
            accel_x = true_accel_x + random.gauss(0, 0.1)
            accel_y = true_accel_y + random.gauss(0, 0.1)
            accel_z = true_accel_z + random.gauss(0, 0.1)

            # Гироскоп: угловая скорость вокруг Z плюс шум
            gyro_z = omega + random.gauss(0, 0.01)

            message = {
                "timestamp": t,
                "accel_x": accel_x,
                "accel_y": accel_y,
                "accel_z": accel_z,
                "gyro_x": 0.0,
                "gyro_y": 0.0,
                "gyro_z": gyro_z
            }

            await nc.publish("sensor.imu", json.dumps(message).encode())
            print(f"Published: {message}")
            await asyncio.sleep(0.01)  # 100 Hz

    except KeyboardInterrupt:
        print("Simulator stopped")
    finally:
        await nc.close()

if __name__ == "__main__":
    asyncio.run(main())