"""
Сервис временной синхронизации.

Принимает сообщения от разных сенсоров, буферизирует их
и формирует синхронизированные пакеты, где измерения
приведены к единому моменту времени.

Допустимая задержка: 2.0 секунды (настраивается).
Частота синхронизации: 20 Гц (каждые 50 мс).
"""

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
    """
    Кольцевой буфер для измерений одного типа сенсора.
    
    Хранит последние N измерений и умеет находить
    ближайшее к заданному моменту времени.
    """
    
    def __init__(self, max_size=50):
        """
        Args:
            max_size: максимальное количество хранимых измерений.
        """
        self.buffer = deque(maxlen=max_size)

    def add(self, ts, data):
        """
        Добавляет измерение в буфер.
        
        Args:
            ts: временная метка (float, секунды).
            data: словарь с данными сенсора.
        """
        self.buffer.append((ts, data))

    def get_closest(self, target_ts):
        """
        Находит измерение, ближайшее к заданному времени.
        
        Args:
            target_ts: целевая временная метка.
            
        Returns:
            Данные ближайшего измерения или None,
            если буфер пуст или измерение устарело (>2 с).
        """
        if not self.buffer:
            return None
        
        # Ищем измерение с минимальной разницей по времени
        best = min(self.buffer, key=lambda m: abs(m[0] - target_ts))
        
        # Отбрасываем слишком старые измерения
        if abs(best[0] - target_ts) > 2.0:
            return None
        
        return best[1]


class TimeSynchronizer:
    """
    Синхронизатор данных от нескольких сенсоров.
    
    Поддерживает сенсоры: imu, gps, lidar, camera, telemetry.
    """
    
    def __init__(self):
        # Буферы для каждого типа сенсора
        self.buffers = {
            "imu": SensorBuffer(),
            "gps": SensorBuffer(),
            "lidar": SensorBuffer(),
            "camera": SensorBuffer(),
            "telemetry": SensorBuffer(),
        }

    def handle_message(self, subject, data):
        """
        Обрабатывает входящее сообщение от сенсора.
        
        Args:
            subject: NATS-топик (например, 'sensor.imu').
            data: словарь с данными сенсора.
        """
        # Извлекаем тип сенсора из топика
        sensor_type = subject.split(".")[-1]
        if sensor_type in self.buffers:
            self.buffers[sensor_type].add(data["timestamp"], data)

    def get_sync_packet(self, sync_ts):
        """
        Формирует синхронизированный пакет.
        
        Для каждого типа сенсора находит измерение,
        ближайшее к sync_ts.
        
        Args:
            sync_ts: целевая временная метка.
            
        Returns:
            Словарь с синхронизированными данными.
        """
        packet = {"timestamp": sync_ts}
        for s_type, buf in self.buffers.items():
            measurement = buf.get_closest(sync_ts)
            if measurement is not None:
                packet[s_type] = measurement
        return packet


async def main():
    """Основная функция сервиса синхронизации."""
    nc = NATS()
    await nc.connect(get_nats_url())
    sync = TimeSynchronizer()

    # Обработчик входящих сообщений
    async def handler(msg):
        data = json.loads(msg.data.decode())
        sync.handle_message(msg.subject, data)

    # Подписываемся на все сенсорные топики
    await nc.subscribe("sensor.*", cb=handler)

    # Периодическая публикация синхропакетов
    async def publish_sync():
        while True:
            sync_ts = time.time()
            packet = sync.get_sync_packet(sync_ts)
            await nc.publish("sync.sensors", json.dumps(packet).encode())
            print(f"Sync packet: {packet}")
            await asyncio.sleep(0.05)  # 20 Гц

    # Запускаем публикацию в фоне
    asyncio.create_task(publish_sync())
    
    print("Time synchronizer started...")
    try:
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        print("Synchronizer stopped")
    finally:
        await nc.close()


if __name__ == "__main__":
    asyncio.run(main())