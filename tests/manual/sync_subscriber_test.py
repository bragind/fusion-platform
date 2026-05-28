import asyncio
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from config_loader import get_nats_url
from nats.aio.client import Client as NATS

async def main():
    nc = NATS()
    await nc.connect(get_nats_url())

    async def handler(msg):
        print(f"Sync received: {msg.data.decode()}")

    await nc.subscribe("sync.sensors", cb=handler)
    print("Listening for sync packets...")
    while True:
        await asyncio.sleep(1)

if __name__ == "__main__":
    asyncio.run(main())
