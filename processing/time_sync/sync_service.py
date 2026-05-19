import asyncio
import json
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Dict, List, Optional
from nats.aio.client import Client as NATS

# Хранилище одного измерения с временной меткой
@dataclass
class Measurement:
    timestamp: float
    data: dict

# Буфер для одного типа сенсора (хранит последние N измерений)
class SensorBuffer:
    def __init__(self, max_size: int = 50):
        self.buffer: deque[Measurement] = deque(maxlen=max_size)

    def add(self, ts: float, data: dict):
        self.buffer.append(Measurement(ts, data))

    def get_closest(self, target_ts: float) -> Optional[dict]:
        """Возвращает данные ближайшего измерения к target_ts, если оно не старше 500 мс."""
        if not self.buffer:
            return None
        best = min(self.buffer, key=lambda m: abs(m.timestamp - target_ts))
        # Проверяем, не устарело ли измерение (более 500 мс)
        if abs(best.timestamp - target_ts) > 0.5:
            return None
        return best.data

class TimeSynchronizer:
    def __init__(self):
        self.buffers: Dict[str, SensorBuffer] = {
            "imu": SensorBuffer(),
            "gps": SensorBuffer(),
            "lidar": SensorBuffer(),
            "camera": SensorBuffer(),
            "telemetry": SensorBuffer(),
        }
        self.sync_interval = 0.05  # 50 мс (20 Гц)

    def handle_message(self, subject: str, data: dict):
        # subject вида "sensor.imu" → sensor_type = "imu"
        sensor_type = subject.split(".")[-1]
        if sensor_type in self.buffers:
            self.buffers[sensor_type].add(data["timestamp"], data)

    def get_sync_packet(self, sync_ts: float) -> dict:
        packet = {"timestamp": sync_ts}
        for s_type, buf in self.buffers.items():
            measurement = buf.get_closest(sync_ts)
            if measurement is not None:
                packet[s_type] = measurement
        return packet

async def main():
    nc = NATS()
    await nc.connect("nats://localhost:4222")
    sync = TimeSynchronizer()

    # Подписываемся на все сенсорные топики
    async def handler(msg):
        subject = msg.subject
        data = json.loads(msg.data.decode())
        sync.handle_message(subject, data)

    await nc.subscribe("sensor.*", cb=handler)

    # Периодически публикуем синхронизированный пакет
    async def publish_sync():
        while True:
            sync_ts = time.time()
            packet = sync.get_sync_packet(sync_ts)
            await nc.publish("sync.sensors", json.dumps(packet).encode())
            print(f"Sync packet: {packet}")
            await asyncio.sleep(sync.sync_interval)

    # Запускаем обе задачи параллельно
    await asyncio.gather(
        asyncio.create_task(publish_sync()),
        # Подписка уже работает в фоне через callback
    )

    # Держим соединение открытым
    try:
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        print("Synchronizer stopped")
    finally:
        await nc.close()

if __name__ == "__main__":
    asyncio.run(main())