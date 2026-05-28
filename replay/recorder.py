"""
Recorder — запись потоков данных в Apache Parquet.

Подписывается на все сенсорные топики NATS и сохраняет
сообщения в Parquet-файлы, секционированные по типу сенсора.
Сброс на диск происходит каждые 10 секунд.

Структура хранения:
    data/recordings/
    ├── sensor_imu/
    │   ├── 20260101_120000.parquet
    │   └── ...
    ├── sensor_gps/
    │   └── ...
    └── sync_sensors/
        └── ...

Parquet — столбцовый формат, оптимизированный для
аналитических запросов и временных рядов.
"""

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
    """
    Записывает сообщения NATS в Parquet-файлы.
    
    Буферизирует сообщения в памяти и сбрасывает их
    на диск с заданным интервалом.
    """
    
    def __init__(self, base_path="data/recordings"):
        """
        Args:
            base_path: корневая папка для хранения записей.
        """
        self.base_path = base_path
        os.makedirs(base_path, exist_ok=True)
        
        # Буферы: subject → список сообщений
        self.buffers = defaultdict(list)
        
        # Интервал сброса в секундах
        self.flush_interval = 10
        self.last_flush = time.time()

    def add_message(self, subject, data):
        """
        Добавляет сообщение в буфер.
        
        Args:
            subject: NATS-топик (например, 'sensor.imu').
            data: словарь с данными сообщения.
        """
        # Добавляем служебные поля
        data["_subject"] = subject
        data["_recorded_at"] = time.time()
        self.buffers[subject].append(data)

    def flush(self):
        """
        Сбрасывает все накопленные буферы в Parquet-файлы.
        
        Для каждого топика создаётся отдельный файл
        с временной меткой в имени.
        """
        if not any(self.buffers.values()):
            return

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        for subject, messages in self.buffers.items():
            if not messages:
                continue

            # Безопасное имя папки (точки заменяем на подчёркивания)
            safe_subject = subject.replace(".", "_")
            folder = os.path.join(self.base_path, safe_subject)
            os.makedirs(folder, exist_ok=True)

            filename = os.path.join(folder, f"{timestamp}.parquet")
            
            # Собираем все возможные поля из всех сообщений
            all_keys = set()
            for msg in messages:
                all_keys.update(msg.keys())
            
            # Создаём колонки для таблицы
            columns = {}
            for key in all_keys:
                columns[key] = [msg.get(key) for msg in messages]
            
            # Пишем в Parquet
            table = pa.table(columns)
            pq.write_table(table, filename)
            print(f"Flushed {len(messages)} messages to {filename}")

        # Очищаем буферы
        self.buffers.clear()
        self.last_flush = time.time()


async def main():
    """Основная функция Recorder."""
    nc = NATS()
    await nc.connect(get_nats_url())
    recorder = Recorder()

    async def handler(msg):
        """Обработчик входящих сообщений."""
        try:
            data = json.loads(msg.data.decode())
            recorder.add_message(msg.subject, data)
        except Exception as e:
            print(f"Error processing message: {e}")

    # Подписываемся на все сенсорные и промежуточные топики
    await nc.subscribe("sensor.*", cb=handler)
    await nc.subscribe("sync.sensors", cb=handler)
    await nc.subscribe("filtered.sync", cb=handler)

    print(f"Recorder started. Flushing every {recorder.flush_interval}s")
    
    try:
        while True:
            await asyncio.sleep(1)
            # Периодический сброс на диск
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