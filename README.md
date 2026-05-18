# Industrial Sensor Fusion Platform

[![CI](https://github.com/your-org/fusion-platform/actions/workflows/ci.yml/badge.svg)](https://github.com/your-org/fusion-platform/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Docker Pulls](https://img.shields.io/docker/pulls/your-org/fusion-platform)](https://hub.docker.com/r/your-org/fusion-platform)

Real‑time sensor fusion for autonomous systems: IMU, GPS, LiDAR, camera, and telemetry combined by an Extended Kalman Filter (C++/Eigen). Designed for production but documented for learners.

## Features

- **Multi‑sensor fusion** – IMU, GPS, LiDAR/Radar, camera, telemetry via EKF with configurable process/measurement noise.
- **Time synchronization** – aligns data from asynchronous sources with pluggable NTP/simulation time.
- **Outlier rejection** – median and 3‑sigma filters before fusion to survive sensor glitches.
- **Hot‑swappable matrices** – update Q/R online via NATS without restart.
- **Replay mode** – record raw streams to Parquet and replay at any speed for debugging.
- **WebSocket streaming** – real‑time fused state pushed to clients.
- **Full observability** – Prometheus metrics, JSON logs, OpenTelemetry tracing, Grafana dashboards.
- **Deterministic pipeline** – zero randomness in the fusion core; every run is reproducible.
- **Production‑grade deployment** – Docker Compose, CI/CD, multi‑arch (amd64 + arm64), resource limits.

## Architecture
Sensors (sim or real)
│
├── ROS2 nodes / HTTP ingestion
│
└── NATS JetStream message bus
│
├── Time Synchronizer
├── Noise Filter
├── Kalman Filter Core (C++/pybind)
├── Replay Recorder & Controller
│
└── FastAPI Serving (REST + WebSocket)
│
└── Prometheus ← Grafana

For a detailed diagram see [docs/architecture.md](docs/architecture.md).

## Tech Stack

| Layer          | Technology                                      |
|----------------|-------------------------------------------------|
| Fusion core    | C++17, Eigen, pybind11                          |
| Services       | Python 3.11, FastAPI, NumPy, OpenCV             |
| Message bus    | NATS (JetStream)                                |
| Robotics       | ROS2 Humble (optional)                          |
| Replay storage | Apache Parquet / PyArrow                        |
| Containers     | Docker, Docker Compose                          |
| Monitoring     | Prometheus, Grafana, Loki, OpenTelemetry        |
| CI/CD          | GitHub Actions, pytest, GoogleTest              |
| Docs           | MkDocs, ADR, OpenAPI                            |

## Project Structure
.
├── sensors/ # Sensor simulators & ROS2 nodes
├── ingestion/ # ROS2-NATS bridge & FastAPI ingestion
├── bus/ # NATS configuration
├── processing/
│ ├── time_sync/ # Time synchronizer
│ ├── noise_filter/ # Outlier filter
│ └── kalman/ # Kalman core (C++ & Python wrapper)
├── serving/ # FastAPI service (REST + WebSocket)
├── replay/ # Recorder & replay controller
├── observability/ # Prometheus, Grafana dashboards
├── tests/ # Unit, integration, benchmarks
├── docker/ # Dockerfiles & compose files
├── docs/ # MkDocs documentation
├── scripts/ # Utility scripts
├── CMakeLists.txt # C++ build
└── pyproject.toml # Python deps

## Quick Start

1. Clone the repo:
   ```bash
   git clone https://github.com/your-org/fusion-platform.git
   cd fusion-platform
 2. Launch the platform:  
   docker compose -f docker/docker-compose.dev.yml up -d
3. Verify:
API docs: http://localhost:8000/docs

Grafana: http://localhost:3000 (admin/admin)

Current state: http://localhost:8000/state
API Examples
Get fused state

bash
curl -s http://localhost:8000/state | jq
Get covariance diagonal

bash
curl http://localhost:8000/covariance
Start a replay

bash
curl -X POST http://localhost:8000/replay \
  -H "Content-Type: application/json" \
  -d '{"start_time": "2024-01-01T00:00:00Z", "end_time": "2024-01-01T00:01:00Z", "speed": 2.0}'
WebSocket stream (JavaScript)

js
const ws = new WebSocket('ws://localhost:8000/ws');
ws.onmessage = (e) => console.log(JSON.parse(e.data));
Documentation
Full docs are built with MkDocs. To view locally:

bash
pip install mkdocs mkdocs-material
mkdocs serve
Includes: architecture decisions (ADR), Kalman filter tutorial, developer guide, and API reference.

Testing
Run all tests:

bash
docker compose -f docker/docker-compose.test.yml up --abort-on-container-exit
Unit tests: pytest tests/unit (Python) and cd processing/kalman/build && ctest (C++).

Integration tests: pytest tests/integration --docker.

Benchmarks: results published as CI artifacts and visualized in Grafana.

Metrics & Monitoring
Key metrics exported to Prometheus:

fusion_latency_seconds – predict/update cycle time

sensor_drop_rate – fraction of missed messages

kalman_innovation_magnitude – innovation norm (divergence detection)

Grafana dashboard “Fusion Overview” shows real‑time plots and alerts.

Roadmap
Python EKF prototype

C++ core with pybind11

Parquet replay

LiDAR odometry integration

Automatic noise covariance tuning

Jetson (ARM64) deployment

Kubernetes Helm chart

Contributing
Pull requests are welcome! See CONTRIBUTING.md for guidelines. Please ensure all tests pass and add new ones for your changes.

License
This project is licensed under the MIT License – see LICENSE for details.
