# 🛰️ Industrial Sensor Fusion Platform

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python](https://img.shields.io/badge/Python-3.11%2B-blue)](https://www.python.org/)
[![C++](https://img.shields.io/badge/C%2B%2B-17-red)](https://isocpp.org/)

**Production‑ready платформа для сенсорного слияния:** объединяет данные IMU, GPS, лидара, камеры и телеметрии через расширенный фильтр Калмана в реальном времени. Подходит для беспилотников, AGV, подводных аппаратов и промышленной диагностики.

---

## 📋 Содержание

- [Возможности](#-возможности)
- [Архитектура](#-архитектура)
- [Технологический стек](#-технологический-стек)
- [Быстрый старт](#-быстрый-старт)
- [Структура проекта](#-структура-проекта)
- [API](#-api)
- [Мониторинг](#-мониторинг)
- [Replay](#-replay)
- [Разработка](#-разработка)
- [Лицензия](#-лицензия)

---

## 🚀 Возможности

- **Multi‑sensor fusion** — IMU, GPS, LiDAR, camera, telemetry через Extended Kalman Filter
- **C++ ядро** — фильтр Калмана на C++/Eigen с Python‑обёрткой (pybind11)
- **Time synchronization** — выравнивание асинхронных потоков с настраиваемой задержкой
- **Outlier rejection** — медианный и 3σ‑фильтр для отсеивания выбросов
- **Hot‑swappable матрицы** — обновление Q/R матриц без перезапуска
- **Replay mode** — запись сырых данных в Apache Parquet и воспроизведение с ускорением
- **REST + WebSocket API** — доступ к fused‑состоянию и стриминг в реальном времени
- **Live dashboard** — веб‑интерфейс для визуализации в браузере
- **Prometheus + Grafana** — полная наблюдаемость с метриками и алертами
- **Production‑grade** — детерминированный пайплайн, Docker, CI/CD ready

---

## 🏗 Архитектура

```
┌─────────────────────────────────────────────────────────────┐
│                     SENSORS (SIM / REAL)                     │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌─────────────┐  │
│  │ IMU 100Hz│  │ GPS 10Hz │  │ LiDAR    │  │ Telemetry   │  │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘  └──────┬──────┘  │
└───────┼──────────────┼─────────────┼───────────────┼─────────┘
        │              │             │               │
        ▼              ▼             ▼               ▼
┌─────────────────────────────────────────────────────────────┐
│                    NATS JETSTREAM (MESSAGE BUS)              │
│              Persistence · Replay · Guaranteed Delivery      │
└─────────────────────────────────────────────────────────────┘
        │              │             │               │
        ▼              ▼             ▼               ▼
┌─────────────────────────────────────────────────────────────┐
│                    PROCESSING PIPELINE                       │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────┐   │
│  │ Time Sync    │→│ Noise Filter │→│ Kalman (C++/Eigen)│   │
│  └──────────────┘  └──────────────┘  └──────────────────┘   │
└─────────────────────────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────────────────────────┐
│                      SERVING LAYER                           │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────┐   │
│  │ REST API     │  │ WebSocket    │  │ Live Dashboard   │   │
│  └──────────────┘  └──────────────┘  └──────────────────┘   │
└─────────────────────────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────────────────────────┐
│                     OBSERVABILITY                            │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────┐   │
│  │ Prometheus   │  │ Grafana      │  │ Alerts           │   │
│  └──────────────┘  └──────────────┘  └──────────────────┘   │
└─────────────────────────────────────────────────────────────┘
```

---

## 🛠 Технологический стек

| Компонент          | Технологии                                    |
|-------------------|-----------------------------------------------|
| **Ядро фильтра**  | C++17, Eigen 3.4, pybind11                    |
| **Сервисы**       | Python 3.11, FastAPI, NumPy, asyncio          |
| **Шина сообщений**| NATS JetStream                                |
| **Replay**        | Apache Parquet, PyArrow                       |
| **Контейнеры**    | Docker, Docker Compose                        |
| **Мониторинг**    | Prometheus, Grafana                           |
| **CI/CD**         | GitHub Actions                                |
| **Тесты**         | pytest, GoogleTest                            |

---

## ⚡ Быстрый старт

### Предварительные требования

- Python 3.11+
- Git
- NATS сервер (локально или удалённо)

### Установка

```bash
# Клонируйте репозиторий
git clone https://github.com/bragind/fusion-platform.git
cd fusion-platform

# Создайте виртуальное окружение
python -m venv .venv
source .venv/bin/activate  # Linux/macOS
# или
.venv\Scripts\Activate.ps1  # Windows

# Установите зависимости
pip install -r requirements.txt
```

### Настройка

Создайте `config.json` в корне проекта:

```json
{
    "nats_url": "nats://localhost:4222"
}
```

Для удалённого сервера укажите его IP: `"nats://192.168.2.123:4222"`.

### Запуск всей платформы

```bash
# Windows (открывает отдельные окна PowerShell)
.\scripts\start_all.ps1

# Linux/macOS (запуск в фоне)
python sensors/imu_sim.py &
python sensors/gps_sim.py &
python processing/time_sync/sync_service.py &
python processing/noise_filter/outlier_filter.py &
python replay/recorder.py &
python replay/controller.py &
python serving/api.py &
```

### Проверка

- **REST API:** http://localhost:8002/state
- **Документация API:** http://localhost:8002/docs
- **Live Dashboard:** http://localhost:8002
- **Метрики:** http://localhost:8002/metrics

---

## 📂 Структура проекта

```
fusion-platform/
├── sensors/                    # Симуляторы сенсоров
│   ├── imu_sim.py              #   IMU (100 Гц, акселерометр + гироскоп)
│   └── gps_sim.py              #   GPS (10 Гц, координаты + точность)
│
├── processing/                 # Обработка данных
│   ├── time_sync/              #   Временная синхронизация
│   │   └── sync_service.py
│   ├── noise_filter/           #   Фильтр выбросов (3σ)
│   │   └── outlier_filter.py
│   └── kalman/                 #   Фильтр Калмана
│       ├── kalman_core.cpp     #     C++ ядро (Eigen)
│       └── fusion_service.py   #     Python обёртка
│
├── replay/                     # Запись и воспроизведение
│   ├── recorder.py             #   Запись в Parquet
│   └── controller.py           #   HTTP API для replay
│
├── serving/                    # API и визуализация
│   └── api.py                  #   REST + WebSocket + Dashboard
│
├── docker/                     # Docker конфигурация
│   ├── docker-compose.dev.yml  #   NATS + Prometheus + Grafana
│   └── prometheus.yml          #   Конфигурация сбора метрик
│
├── scripts/                    # Утилиты
│   └── start_all.ps1           #   Запуск всех сервисов (Windows)
│
├── tests/                      # Тесты
│   └── manual/                 #   Ручные тесты подписчиков
│
├── config.json                 # Конфигурация подключений
├── config_loader.py            # Загрузчик конфигурации
├── requirements.txt            # Python зависимости
└── README.md                   # Документация
```

---

## 📡 API

### REST Endpoints

| Метод | URL | Описание |
|-------|-----|----------|
| `GET` | `/state` | Текущее fused‑состояние `[x, y, vx, vy]` |
| `GET` | `/covariance` | Диагональ матрицы ковариации |
| `GET` | `/health` | Статус сервиса |
| `GET` | `/metrics` | Prometheus метрики |

### Replay API (порт 8001)

| Метод | URL | Описание |
|-------|-----|----------|
| `POST` | `/replay` | Запустить воспроизведение |
| `GET` | `/health` | Статус контроллера |

### WebSocket

```javascript
const ws = new WebSocket('ws://localhost:8002/ws');
ws.onmessage = (event) => {
    const state = JSON.parse(event.data);
    console.log(state.state, state.covariance);
};
```

---

## 📊 Мониторинг

### Prometheus метрики

| Метрика | Тип | Описание |
|---------|-----|----------|
| `fusion_latency_seconds` | Histogram | Задержка цикла predict/update |
| `sensor_drop_total` | Counter | Количество потерянных сообщений |
| `kalman_innovation_magnitude` | Gauge | Величина инновации фильтра |

### Grafana

1. Откройте http://192.168.2.123:3000 (admin/admin)
2. Добавьте Data Source → Prometheus (http://prometheus:9090)
3. Создайте дашборд с графиками:
   - `rate(fusion_latency_seconds_sum[1m]) / rate(fusion_latency_seconds_count[1m])`
   - `rate(sensor_drop_total[1m])`
   - `kalman_innovation_magnitude`

---

## 🔁 Replay

Запись данных происходит автоматически при запущенном Recorder:

```bash
python replay/recorder.py
```

Воспроизведение через API:

```bash
curl -X POST http://localhost:8001/replay \
  -H "Content-Type: application/json" \
  -d '{"start_time": 1700000000.0, "end_time": 1700000060.0, "speed": 2.0}'
```

Данные сохраняются в `data/recordings/` в формате Parquet.

---

## 👨‍💻 Разработка

### Ветвление (Git Flow)

- `main` — стабильные релизы
- `develop` — интеграционная ветка
- `feature/*` — новые возможности
- `release/*` — подготовка к релизу
- `hotfix/*` — срочные исправления

### Сборка C++ модуля

```bash
cd processing/kalman
mkdir build && cd build
cmake .. -DPython_EXECUTABLE=$(which python)
make
```

### Тестирование

```bash
# Python unit-тесты
pytest tests/

# C++ unit-тесты
cd processing/kalman/build && ctest

# Интеграционные тесты (требуют NATS)
pytest tests/integration/
```

---

## 📄 Лицензия

MIT License — см. [LICENSE](LICENSE)

---

## 🌟 Благодарности

- [Eigen](https://eigen.tuxfamily.org/) — линейная алгебра
- [pybind11](https://github.com/pybind/pybind11) — C++/Python мост
- [NATS](https://nats.io/) — сообщения
- [FastAPI](https://fastapi.tiangolo.com/) — API фреймворк
- [Prometheus](https://prometheus.io/) — мониторинг

---

*Создано с ❤️ для production и обучения.*