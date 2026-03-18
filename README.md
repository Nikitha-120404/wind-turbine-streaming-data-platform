# 🌪️ Wind Turbine Streaming Data Platform

This project simulates wind turbine sensor data and processes it through a real-time data streaming pipeline using **Apache Kafka**, **Python services**, and **TimescaleDB** for time-series storage.  
The system also includes **Prometheus and Grafana** for monitoring and visualization.

---

# 🏗️ Architecture Flow

Wind Turbine Sensor Simulator  
↓  
Kafka Producer  
↓  
Kafka Broker  
↓  
Kafka Consumer  
↓  
TimescaleDB  
↓  
Continuous Aggregates & Analytics  
↓  
Grafana Dashboards  

---

# ⚙️ Technologies Used

- Python  
- Apache Kafka  
- Confluent Kafka Client  
- PostgreSQL / TimescaleDB  
- SQL  
- Docker  
- Prometheus  
- Grafana  
- Pytest  

---

# 📂 Project Structure


wind-turbine-streaming-data-platform
│
├── infra/
│ ├── grafana/
│ ├── prometheus/
│ └── postgres/
│
├── scripts/
│
├── services/
│
├── shared/
│ ├── config.py
│ ├── kafka_admin.py
│ ├── logging_config.py
│ ├── metrics.py
│ └── schema.py
│
├── tests/
│ ├── test_config.py
│ ├── test_schema.py
│ ├── test_simulator.py
│ └── test_anomaly_detector.py
│
├── .env.example
├── .gitignore
├── docker-compose.yml
├── requirements.txt
├── pytest.ini
└── README.md


---

# 🌊 Data Pipeline Flow

Wind Turbine Simulator generates sensor data  
↓  
Kafka Producer publishes events to Kafka topics  
↓  
Kafka Broker manages streaming messages  
↓  
Kafka Consumer services process events  
↓  
TimescaleDB stores turbine metrics as time-series data  
↓  
Continuous aggregates generate analytical insights  
↓  
Prometheus collects service metrics  
↓  
Grafana displays monitoring dashboards  

---

# 📊 Example Sensor Data

```json
{
  "turbine_id": 4,
  "timestamp": "2026-03-16T10:12:22",
  "wind_speed": 14.3,
  "power_output": 520,
  "temperature": 41
}
⚡ Running the Project
Clone Repository
git clone https://github.com/Nikitha-120404/wind-turbine-streaming-data-platform.git
cd wind-turbine-streaming-data-platform
Setup Environment Variables

Create a local environment file:

cp .env.example .env
Start the Platform

Run all services using Docker:

docker-compose up --build
🌐 Service Endpoints

Grafana
http://localhost:3000

Prometheus
http://localhost:9090

TimescaleDB
localhost:5432

🧪 Running Tests
pytest
👩‍💻 Author

Nikitha Mandla
Computer Science Student
University of Missouri – Kansas City

GitHub
https://github.com/Nikitha-120404