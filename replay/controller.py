import asyncio
import json
import sys
import os
import time
from glob import glob

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from config_loader import get_nats_url
from nats.aio.client import Client as NATS
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import pyarrow.parquet as pq

app = FastAPI(title="Replay Controller API")
nats_client = None

class ReplayRequest(BaseModel):
    start_time: float
    end_time: float
    speed: float = 1.0
    topics: list = ["sensor.imu", "sensor.gps", "sync.sensors"]

class ReplayController:
    def __init__(self, base_path="data/recordings"):
        self.base_path = base_path

    def load_data(self, start_time, end_time, topics):
        all_messages = []
        
        for topic in topics:
            safe_topic = topic.replace(".", "_")
            folder = os.path.join(self.base_path, safe_topic)
            
            if not os.path.exists(folder):
                continue
            
            parquet_files = sorted(glob(os.path.join(folder, "*.parquet")))
            
            for file in parquet_files:
                try:
                    table = pq.read_table(file)
                    df = table.to_pandas()
                    
                    if "timestamp" in df.columns:
                        mask = (df["timestamp"] >= start_time) & (df["timestamp"] <= end_time)
                        df = df[mask]
                    
                    df["_topic"] = topic
                    messages = df.to_dict('records')
                    all_messages.extend(messages)
                except Exception as e:
                    print(f"Error reading {file}: {e}")
        
        all_messages.sort(key=lambda x: x.get("timestamp", 0))
        return all_messages

    async def replay(self, messages, speed):
        nc = NATS()
        await nc.connect(get_nats_url())
        
        if not messages:
            return 0
        
        start_time = messages[0].get("timestamp", 0)
        replay_start = time.time()
        count = 0
        
        for msg in messages:
            msg_time = msg.get("timestamp", 0)
            elapsed = (msg_time - start_time) / speed
            real_elapsed = time.time() - replay_start
            
            if elapsed > real_elapsed:
                await asyncio.sleep(elapsed - real_elapsed)
            
            topic = msg.pop("_topic", "replay.unknown")
            msg.pop("_subject", None)
            msg.pop("_recorded_at", None)
            msg.pop("timestamp", None)
            
            await nc.publish(topic, json.dumps(msg).encode())
            count += 1
        
        await nc.close()
        return count

controller = ReplayController()

@app.on_event("startup")
async def startup():
    global nats_client
    nats_client = NATS()
    await nats_client.connect(get_nats_url())

@app.on_event("shutdown")
async def shutdown():
    if nats_client:
        await nats_client.close()

@app.post("/replay")
async def start_replay(request: ReplayRequest):
    messages = controller.load_data(
        request.start_time,
        request.end_time,
        request.topics
    )
    
    if not messages:
        raise HTTPException(status_code=404, detail="No data found")
    
    asyncio.create_task(controller.replay(messages, request.speed))
    
    return {
        "status": "replay_started",
        "messages_count": len(messages),
        "speed": request.speed
    }

@app.get("/health")
async def health():
    return {"status": "ok", "nats": nats_client is not None}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)
