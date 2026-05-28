import asyncio
import json
import sys
import os
import time
from datetime import datetime
from collections import defaultdict

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from config_loader import get_nats_url
from nats.aio.client import Client as NATS
import pyarrow as pa
import pyarrow.parquet as pq

class Recorder:
    def __init__(self, base_path="data/recordings"):
        self.base_path = base_path
        os.makedirs(base_path, exist_ok=True)
        self.buffers = defaultdict(list)
        self.flush_interval = 10
        self.last_flush = time.time()

    def add_message(self, subject, data):
        data["_subject"] = subject
        data["_recorded_at"] = time.time()
        self.buffers[subject].append(data)

    def flush(self):
        if not any(self.buffers.values()):
            return

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        for subject, messages in self.buffers.items():
            if not messages:
                continue

            safe_subject = subject.replace(".", "_")
            folder = os.path.join(self.base_path, safe_subject)
            os.makedirs(folder, exist_ok=True)

            filename = os.path.join(folder, f"{timestamp}.parquet")
            
            if messages:
                all_keys = set()
                for msg in messages:
                    all_keys.update(msg.keys())
                
                columns = {}
                for key in all_keys:
                    columns[key] = [msg.get(key) for msg in messages]
                
                table = pa.table(columns)
                pq.write_table(table, filename)
                print(f"Flushed {len(messages)} messages to {filename}")

        self.buffers.clear()
        self.last_flush = time.time()

async def main():
    nc = NATS()
    await nc.connect(get_nats_url())
    recorder = Recorder()

    async def handler(msg):
        try:
            data = json.loads(msg.data.decode())
            recorder.add_message(msg.subject, data)
        except Exception as e:
            print(f"Error: {e}")

    await nc.subscribe("sensor.*", cb=handler)
    await nc.subscribe("sync.sensors", cb=handler)
    await nc.subscribe("filtered.sync", cb=handler)

    print(f"Recorder started. Flushing every {recorder.flush_interval}s")
    
    try:
        while True:
            await asyncio.sleep(1)
            if time.time() - recorder.last_flush >= recorder.flush_interval:
                recorder.flush()
    except KeyboardInterrupt:
        print("Flushing final data...")
        recorder.flush()
        print("Recorder stopped")
    finally:
        await nc.close()

if __name__ == "__main__":
    asyncio.run(main())
