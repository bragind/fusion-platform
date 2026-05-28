"""
Модуль загрузки конфигурации из JSON-файла.

Позволяет централизованно управлять настройками всех сервисов
без необходимости изменять код каждого компонента.
"""

import json
import os

# Абсолютный путь к config.json в корне проекта
CONFIG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config.json')


def load_config():
    """
    Загружает конфигурацию из JSON-файла.
    
    Returns:
        dict: Словарь с настройками проекта.
    """
    with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
        return json.load(f)


def get_nats_url():
    """
    Возвращает URL для подключения к NATS-серверу.
    
    Если параметр nats_url не указан в конфиге,
    используется localhost:4222 по умолчанию.
    
    Returns:
        str: URL NATS-сервера (например, 'nats://192.168.2.123:4222').
    """
    config = load_config()
    return config.get('nats_url', 'nats://localhost:4222')