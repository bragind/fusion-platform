import asyncio
from nats.aio.client import Client as NATS

async def main():
    nc = NATS()
    await nc.connect("nats://localhost:4222")

    async def handler(msg):
        print(f"Sync received: {msg.data.decode()}")

    await nc.subscribe("sync.sensors", cb=handler)
    print("Listening for sync packets...")
    while True:
        await asyncio.sleep(1)

if __name__ == "__main__":
    asyncio.run(main())