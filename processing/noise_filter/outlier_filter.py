import asyncio
import json
from collections import deque
from typing import Dict, Optional
import numpy as np
from nats.aio.client import Client as NATS

class OutlierFilter:
    def __init__(self, window_size: int = 20, sigma_threshold: float = 3.0):
        self.window_size = window_size
        self.sigma_threshold = sigma_threshold
        # Для каждого поля сенсора храним очередь последних "хороших" значений
        self.history: Dict[str, deque] = {}

    def _is_outlier(self, key: str, value: float) -> bool:
        # Если история пуста или значение не числовое, принимаем как нормальное
        if key not in self.history:
            self.history[key] = deque(maxlen=self.window_size)
        q = self.history[key]
        if len(q) < 3:  # недостаточно данных для статистики
            q.append(value)
            return False
        mean = np.mean(q)
        std = np.std(q)
        if std == 0:
            return False
        deviation = abs(value - mean)
        return deviation > self.sigma_threshold * std

    def _update_history(self, key: str, value: float):
        if key not in self.history:
            self.history[key] = deque(maxlen=self.window_size)
        self.history[key].append(value)

    def process(self, sync_packet: dict) -> dict:
        filtered = {"timestamp": sync_packet["timestamp"]}
        for sensor_type, data in sync_packet.items():
            if sensor_type == "timestamp" or data is None:
                continue
            filtered_data = {}
            drop_sensor = False
            for field, value in data.items():
                if isinstance(value, (int, float)):
                    key = f"{sensor_type}.{field}"
                    if self._is_outlier(key, value):
                        # Выброс! Помечаем, что сенсор нужно исключить целиком
                        drop_sensor = True
                        print(f"Outlier detected: {key}={value}")
                        break
                    else:
                        filtered_data[field] = value
                else:
                    filtered_data[field] = value
            if not drop_sensor:
                filtered[sensor_type] = filtered_data
                # Обновляем историю только для хороших значений
                for field, value in filtered_data.items():
                    if isinstance(value, (int, float)):
                        self._update_history(f"{sensor_type}.{field}", value)
        return filtered

async def main():
    nc = NATS()
    await nc.connect("nats://localhost:4222")
    filter_engine = OutlierFilter()

    async def handler(msg):
        packet = json.loads(msg.data.decode())
        filtered_packet = filter_engine.process(packet)
        await nc.publish("filtered.sync", json.dumps(filtered_packet).encode())
        print(f"Filtered: {filtered_packet}")

    await nc.subscribe("sync.sensors", cb=handler)
    print("Outlier filter listening on sync.sensors...")
    try:
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        print("Filter stopped")
    finally:
        await nc.close()

if __name__ == "__main__":
    asyncio.run(main())