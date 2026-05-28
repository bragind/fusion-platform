import asyncio
from nats.aio.client import Client as NATS

async def main():
    nc = NATS()
    await nc.connect("nats://192.168.2.123:4222")

    async def message_handler(msg):
        print(f"Received: {msg.data.decode()}")

    await nc.subscribe("sensor.gps", cb=message_handler)
    print("Listening for IMU data...")
    while True:
        await asyncio.sleep(1)

if __name__ == "__main__":
    asyncio.run(main())
