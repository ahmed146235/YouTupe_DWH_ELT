# 🎬 YouTube API ELT Pipeline

[![Python](https://img.shields.io/badge/Python-3.10-blue?logo=python&logoColor=white)](https://www.python.org/)
[![Docker](https://img.shields.io/badge/Docker-20.10-blue?logo=docker&logoColor=white)](https://www.docker.com/)
[![Airflow](https://img.shields.io/badge/Airflow-2.6-orange?logo=apache-airflow&logoColor=white)](https://airflow.apache.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-blue?logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![License](https://img.shields.io/badge/License-MIT-green)](LICENSE)

---

## 🏗 Project Overview
This project demonstrates a **complete ELT pipeline** using Python, Docker, and Airflow.  
It extracts data from **YouTube API** and performs **data transformations, quality checks, and CI/CD testing**.  

Data is pulled from the **MrBeast YouTube channel**, but can be easily adapted to any other channel by updating the **Channel ID/Handle**.

---

## 📊 Dataset
- **Source**: YouTube API  
- **Channel**: MrBeast  
- **Stored Data**: JSON files under `/data` folder:
  - `YT_data_2025-11-17.json`
  - `YT_data_2025-11-18.json`

**Extracted Fields**:
- 🎥 Video ID  
- 📝 Video Title  
- 📅 Upload Date  
- ⏱ Duration  
- 👁 Views Count  
- 👍 Likes Count  
- 💬 Comments Count  

> Initial API pull performs a full upload; subsequent pulls **upsert** only updated fields.

---

## 🏗 Architecture & Workflow

### Architecture Diagram
![ELT Pipeline Architecture](images/Wrokflow.png)

### 1️⃣ Containerization
- Uses **Docker & Docker Compose** for Airflow and PostgreSQL containers.
- Custom Airflow Docker image is built via `Dockerfile` and pushed/pulled using **GitHub Actions CI/CD** workflow (`.github/workflows/ci-cd_yt_etl.yaml`).
- Environment variables manage credentials, encrypted with **Fernet keys**.
- Initialization scripts:
  - `/docker/postgres/init-multiple-databases.sh` for creating databases and users.

### 2️⃣ Orchestration (Airflow DAGs)
- **Folder**: `/dags`  
- **Main DAG file**: `/dags/Main.py`
- **Submodules**:
  - `/dags/api/video_stats.py` → Data extraction from YouTube API
  - `/dags/dataquality/soda.py` → Data quality checks using SODA
  - `/dags/datawarehouse/*` → Database ETL scripts:
    - `data_loading.py`, `data_modification.py`, `data_transformation.py`, `data_utils.py`, `dwh.py`

- **Three main DAGs**:
  1. `produce_json` → Extracts raw data into JSON files  
  2. `update_db` → Loads JSON data into staging & core schemas  
  3. `data_quality` → Validates data quality in both schemas  

Access Airflow UI at [http://localhost:8080](http://localhost:8080).

---

## 💾 Data Storage
- PostgreSQL staging & core schemas
- Access via:
  - `psql` inside PostgreSQL container  
  - Database tools like **DBeaver**  

---

## 🧪 Testing
- **Unit Tests**: `/tests/unit_test.py` using `pytest`  
- **Integration Tests**: `/tests/integration_test.py`  
- **Configuration**: `/tests/conftest.py`  
- **Data Quality Tests**: SODA checks defined in `/include/soda/checks.yml` and `/include/soda/configuration.yml`

---

## 🔄 CI/CD
- **Workflow file**: `.github/workflows/ci-cd_yt_etl.yaml`  
- Automates:
  - Docker image build & push  
  - Unit, integration, and end-to-end DAG tests  
  - Triggered on `push` to `main`, `feature/*`, or manually via `workflow_dispatch`  

---

## 🛠 Tools & Technologies
| Category         | Tools & Libraries                  |
|-----------------|-----------------------------------|
| Containerization | Docker, Docker Compose           |
| Orchestration    | Airflow                           |
| Database         | PostgreSQL                        |
| Languages        | Python, SQL                       |
| Testing          | pytest, SODA Core                  |
| CI/CD            | GitHub Actions                    |

---

## 🔧 Project Structure
```text
├─ .github/workflows
│ └─ ci-cd_yt_etl.yaml
├─ dags
│ ├─ api/video_stats.py
│ ├─ dataquality/soda.py
│ ├─ datawarehouse/
│ │ ├─ data_loading.py
│ │ ├─ data_modification.py
│ │ ├─ data_transformation.py
│ │ ├─ data_utils.py
│ │ └─ dwh.py
│ └─ Main.py
├─ data
│ ├─ YT_data_2025-11-17.json
│ └─ YT_data_2025-11-18.json
├─ docker/postgres
│ └─ init-multiple-databases.sh
├─ images
│ └─ WrokFlow.png
├─ include/soda
│ ├─ checks.yml
│ └─ configuration.yml
├─ tests
│ ├─ conftest.py
│ ├─ integration_test.py
│ └─ unit_test.py
├─ Dockerfile
├─ docker-compose.yaml
├─ requirements.txt
└─ README.md
```
---

## 📜 License
This project is licensed under the **MIT License**.  
See the [LICENSE](LICENSE) file for details.

---

## ⚡ Optional Enhancements
- Dynamic DAG discovery instead of hardcoding DAG names.  
- Versioned Docker image tags for better traceability.  
- Add multiple YouTube channels with configurable environment variables.

## 👤 Author
**Ahmed Mohamed**  
[LinkedIn](https://www.linkedin.com/in/ahmed-muhammed-93416b243)

