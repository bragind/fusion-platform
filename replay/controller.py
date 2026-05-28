"""
Replay Controller — HTTP API для воспроизведения записанных данных.

Позволяет загрузить исторические данные из Parquet-файлов
и воспроизвести их в NATS с заданной скоростью.
Используется для отладки, ретроспективного анализа
и регрессионного тестирования алгоритмов.

API:
    POST /replay — запустить воспроизведение
    GET  /health — статус сервиса
"""

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

# FastAPI приложение
app = FastAPI(title="Replay Controller API")
nats_client = None


class ReplayRequest(BaseModel):
    """
    Модель запроса на воспроизведение.
    
    Attributes:
        start_time: начало интервала (Unix timestamp, секунды).
        end_time: конец интервала.
        speed: скорость воспроизведения (1.0 — реальное время, 2.0 — вдвое быстрее).
        topics: список топиков для воспроизведения.
    """
    start_time: float
    end_time: float
    speed: float = 1.0
    topics: list = ["sensor.imu", "sensor.gps", "sync.sensors"]


class ReplayController:
    """
    Контроллер воспроизведения данных.
    
    Загружает сообщения из Parquet-файлов и публикует их
    в NATS с соблюдением временных интервалов.
    """
    
    def __init__(self, base_path="data/recordings"):
        """
        Args:
            base_path: корневая папка с записями.
        """
        self.base_path = base_path

    def load_data(self, start_time, end_time, topics):
        """
        Загружает данные из Parquet за указанный период.
        
        Args:
            start_time: начало интервала.
            end_time: конец интервала.
            topics: список топиков.
            
        Returns:
            Список сообщений, отсортированный по времени.
        """
        all_messages = []
        
        for topic in topics:
            safe_topic = topic.replace(".", "_")
            folder = os.path.join(self.base_path, safe_topic)
            
            if not os.path.exists(folder):
                print(f"No recordings for topic {topic}")
                continue
            
            # Ищем все Parquet-файлы в папке
            parquet_files = sorted(glob(os.path.join(folder, "*.parquet")))
            
            for file in parquet_files:
                try:
                    table = pq.read_table(file)
                    df = table.to_pandas()
                    
                    # Фильтруем по времени
                    if "timestamp" in df.columns:
                        mask = (df["timestamp"] >= start_time) & (df["timestamp"] <= end_time)
                        df = df[mask]
                    
                    # Добавляем имя топика
                    df["_topic"] = topic
                    
                    # Конвертируем в список словарей
                    messages = df.to_dict('records')
                    all_messages.extend(messages)
                except Exception as e:
                    print(f"Error reading {file}: {e}")
        
        # Сортируем по времени
        all_messages.sort(key=lambda x: x.get("timestamp", 0))
        return all_messages

    async def replay(self, messages, speed):
        """
        Воспроизводит сообщения в NATS.
        
        Args:
            messages: список сообщений.
            speed: скорость воспроизведения.
            
        Returns:
            Количество воспроизведённых сообщений.
        """
        nc = NATS()
        await nc.connect(get_nats_url())
        
        if not messages:
            return 0
        
        # Время первого сообщения — начало отсчёта
        start_time = messages[0].get("timestamp", 0)
        replay_start = time.time()
        count = 0
        
        for msg in messages:
            msg_time = msg.get("timestamp", 0)
            
            # Вычисляем, сколько должно пройти времени от начала
            elapsed = (msg_time - start_time) / speed
            real_elapsed = time.time() - replay_start
            
            # Ждём, если воспроизведение опережает реальное время
            if elapsed > real_elapsed:
                await asyncio.sleep(elapsed - real_elapsed)
            
            # Извлекаем топик и очищаем служебные поля
            topic = msg.pop("_topic", "replay.unknown")
            msg.pop("_subject", None)
            msg.pop("_recorded_at", None)
            
            # Публикуем в оригинальный топик
            await nc.publish(topic, json.dumps(msg).encode())
            count += 1
        
        await nc.close()
        return count


# Создаём экземпляр контроллера
controller = ReplayController()


@app.on_event("startup")
async def startup():
    """Подключение к NATS при старте."""
    global nats_client
    nats_client = NATS()
    await nats_client.connect(get_nats_url())


@app.on_event("shutdown")
async def shutdown():
    """Отключение от NATS при остановке."""
    if nats_client:
        await nats_client.close()


@app.post("/replay")
async def start_replay(request: ReplayRequest):
    """
    Запускает воспроизведение исторических данных.
    
    Загружает данные из Parquet и запускает фоновую задачу
    для их публикации в NATS.
    """
    messages = controller.load_data(
        request.start_time,
        request.end_time,
        request.topics
    )
    
    if not messages:
        raise HTTPException(status_code=404, detail="No data found for the specified period")
    
    # Запускаем воспроизведение в фоне (не блокируем ответ)
    asyncio.create_task(controller.replay(messages, request.speed))
    
    return {
        "status": "replay_started",
        "messages_count": len(messages),
        "speed": request.speed
    }


@app.get("/health")
async def health():
    """Проверка здоровья сервиса."""
    return {"status": "ok", "nats": nats_client is not None}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)