import asyncio
from nats.aio.client import Client as NATS

async def main():
    nc = NATS()
    try:
        # Явно IPv4
        await nc.connect("nats://192.168.2.123:4222", connect_timeout=5)
        print("✅ Connected to remote NATS!")

        # Тест pub/sub
        async def handler(msg):
            print(f"✅ Received: {msg.data.decode()}")

        await nc.subscribe("test.remote", cb=handler)
        await nc.publish("test.remote", b"Hello from local!")
        await asyncio.sleep(0.5)
        await nc.close()
        print("✅ Remote NATS works!")
    except Exception as e:
        print(f"❌ Error: {e}")

asyncio.run(main())