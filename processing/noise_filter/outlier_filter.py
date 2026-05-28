"""
Фильтр выбросов (Outlier Filter).

Отсеивает аномальные измерения сенсоров перед подачей
в фильтр Калмана. Использует правило трёх сигм (3σ):
если значение отличается от среднего более чем на 3
стандартных отклонения — оно считается выбросом.

Работает на уровне отдельных полей внутри пакета сенсора.
Если хотя бы одно поле сенсора — выброс, весь блок данных
этого сенсора исключается из синхропакета.
"""

import asyncio
import json
import sys
import os
from collections import deque
import numpy as np

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from config_loader import get_nats_url
from nats.aio.client import Client as NATS


class OutlierFilter:
    """
    Фильтр выбросов на основе скользящего окна и правила 3σ.
    
    Для каждого поля (например, 'gps.lat') ведётся история
    последних N измерений. При поступлении нового значения
    вычисляется среднее и стандартное отклонение; если значение
    выходит за границы [μ-3σ, μ+3σ] — это выброс.
    """
    
    def __init__(self, window_size=20, sigma_threshold=3.0):
        """
        Args:
            window_size: размер скользящего окна (количество измерений).
            sigma_threshold: порог в сигмах для определения выброса.
        """
        self.window_size = window_size
        self.sigma_threshold = sigma_threshold
        # История для каждого поля: ключ → deque значений
        self.history = {}

    def _is_outlier(self, key, value):
        """
        Проверяет, является ли значение выбросом.
        
        Args:
            key: имя поля (например, 'gps.lat').
            value: числовое значение.
            
        Returns:
            True, если значение — выброс.
        """
        if key not in self.history:
            self.history[key] = deque(maxlen=self.window_size)
        
        q = self.history[key]
        
        # Пока мало данных — не фильтруем
        if len(q) < 3:
            q.append(value)
            return False
        
        # Вычисляем статистики по окну
        mean = np.mean(q)
        std = np.std(q)
        
        if std == 0:
            return False
        
        deviation = abs(value - mean)
        return deviation > self.sigma_threshold * std

    def _update_history(self, key, value):
        """Добавляет «хорошее» значение в историю."""
        if key not in self.history:
            self.history[key] = deque(maxlen=self.window_size)
        self.history[key].append(value)

    def process(self, sync_packet):
        """
        Обрабатывает синхронизированный пакет.
        
        Для каждого сенсора проверяет все числовые поля.
        Если обнаружен выброс — сенсор исключается из пакета.
        
        Args:
            sync_packet: словарь с синхронизированными данными.
            
        Returns:
            Отфильтрованный пакет (без выбросов).
        """
        filtered = {"timestamp": sync_packet["timestamp"]}
        
        for sensor_type, data in sync_packet.items():
            if sensor_type == "timestamp" or data is None:
                continue
            
            filtered_data = {}
            drop_sensor = False
            
            for field, value in data.items():
                if isinstance(value, (int, float)):
                    key = f"{sensor_type}.{field}"
                    
                    if self._is_outlier(key, value):
                        # Обнаружен выброс — исключаем весь сенсор
                        drop_sensor = True
                        print(f"Outlier detected: {key}={value}")
                        break
                    else:
                        filtered_data[field] = value
                else:
                    filtered_data[field] = value
            
            # Если выбросов не было — сохраняем сенсор в пакет
            if not drop_sensor:
                filtered[sensor_type] = filtered_data
                
                # Обновляем историю только для хороших значений
                for field, value in filtered_data.items():
                    if isinstance(value, (int, float)):
                        self._update_history(f"{sensor_type}.{field}", value)
        
        return filtered


async def main():
    """Основная функция сервиса фильтрации."""
    nc = NATS()
    await nc.connect(get_nats_url())
    filter_engine = OutlierFilter()

    async def handler(msg):
        packet = json.loads(msg.data.decode())
        filtered_packet = filter_engine.process(packet)
        await nc.publish("filtered.sync", json.dumps(filtered_packet).encode())
        print(f"Filtered: {filtered_packet}")

    # Слушаем синхронизированные пакеты
    await nc.subscribe("sync.sensors", cb=handler)
    print("Outlier filter listening on sync.sensors...")
    
    try:
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        print("Filter stopped")
    finally:
        await nc.close()


if __name__ == "__main__":
    asyncio.run(main())