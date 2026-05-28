import asyncio
import json
import time
import sys
import os
from collections import deque

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from config_loader import get_nats_url
from nats.aio.client import Client as NATS

class SensorBuffer:
    def __init__(self, max_size=50):
        self.buffer = deque(maxlen=max_size)

    def add(self, ts, data):
        self.buffer.append((ts, data))

    def get_closest(self, target_ts):
        if not self.buffer:
            return None
        best = min(self.buffer, key=lambda m: abs(m[0] - target_ts))
        if abs(best[0] - target_ts) > 2.0:
            return None
        return best[1]

class TimeSynchronizer:
    def __init__(self):
        self.buffers = {
            "imu": SensorBuffer(),
            "gps": SensorBuffer(),
            "lidar": SensorBuffer(),
            "camera": SensorBuffer(),
            "telemetry": SensorBuffer(),
        }

    def handle_message(self, subject, data):
        sensor_type = subject.split(".")[-1]
        if sensor_type in self.buffers:
            self.buffers[sensor_type].add(data["timestamp"], data)

    def get_sync_packet(self, sync_ts):
        packet = {"timestamp": sync_ts}
        for s_type, buf in self.buffers.items():
            measurement = buf.get_closest(sync_ts)
            if measurement is not None:
                packet[s_type] = measurement
        return packet

async def main():
    nc = NATS()
    await nc.connect(get_nats_url())
    sync = TimeSynchronizer()

    async def handler(msg):
        data = json.loads(msg.data.decode())
        sync.handle_message(msg.subject, data)

    await nc.subscribe("sensor.*", cb=handler)

    async def publish_sync():
        while True:
            sync_ts = time.time()
            packet = sync.get_sync_packet(sync_ts)
            await nc.publish("sync.sensors", json.dumps(packet).encode())
            print(f"Sync packet: {packet}")
            await asyncio.sleep(0.05)

    await asyncio.create_task(publish_sync())
    try:
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        print("Synchronizer stopped")
    finally:
        await nc.close()

if __name__ == "__main__":
    asyncio.run(main())