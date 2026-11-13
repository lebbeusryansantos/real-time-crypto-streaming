# Real-Time Cryptocurrency Streaming & Forecasting Pipeline

An end-to-end real-time data engineering and streaming analytics pipeline that ingests live **Dogecoin (`DOGE`)** and **Solana (`SOL`)** market metrics from the CoinGecko API, processes micro-batches via Apache Kafka and Apache Spark Structured Streaming, persists records in Apache Cassandra, and serves a live monitoring dashboard with Flask.

Originally developed for specializations **Data Science 2** and extended for **Data Science 3 (1-Hour-Ahead Spot Price Forecasting)** at Mapúa University.

---

## Architecture Overview

```text
CoinGecko API (REST / JSON)
       │
       ├──► producer_doge.py   ──► Kafka Topic: crypto-dogecoin ──┐
       └──► producer_solana.py ──► Kafka Topic: crypto-solana   ──┼──► Apache Spark Structured Streaming
                                                                  │          (5-second micro-batches)
                                                                  │                     │
                                                                  │                     ▼
                                                                  │        Apache Cassandra (NoSQL)
                                                                  │      crypto_stream.market_data
                                                                  │                     │
                                                                  │                     ▼
                                                                  └────────► Flask Web Dashboard
                                                                             (http://localhost:5000)
```

---

## Tech Stack

- **Data Source:** Public CoinGecko `/simple/price` API (polled at 10-second intervals)
- **Message Broker:** Apache Kafka 7.5.0 & Apache Zookeeper 7.5.0 (Docker)
- **Stream Processing:** Apache Spark Structured Streaming (PySpark 4.3.0)
- **Database:** Apache Cassandra 4.1 (Docker)
- **Web Dashboard:** Python Flask + HTML/CSS auto-polling frontend
- **Data Quality & Inspection:** Custom CQL audit script (`inspect_100.py`)

---

## Repository Structure

| File / Folder | Description |
|---|---|
| `docker-compose.yml` | Spins up Zookeeper (2181), Kafka (9092), and Cassandra (9042) containers |
| `producer_doge.py` | Polls CoinGecko for Dogecoin market data and publishes JSON payloads to `crypto-dogecoin` |
| `producer_solana.py` | Polls CoinGecko for Solana market data and publishes JSON payloads to `crypto-solana` |
| `spark_streaming.py` | Reads both Kafka topics, parses JSON schemas, and writes micro-batches to Cassandra |
| `app.py` | Flask backend querying Cassandra (`LIMIT 50` per asset) to serve the live dashboard |
| `templates/index.html` | Auto-refreshing web UI displaying live Dogecoin and Solana price tables |
| `inspect_100.py` | Audits 100 sample Cassandra records for null values and CoinGecko HTTP 429 rate-limit gaps |
| `consumer_*.py` | Standalone Kafka consumer scripts for topic-level debugging |
| `requirements.txt` | Pinned Python dependencies compatible with Python 3.12+ / 3.14+ |
| `GRP5_AM1_PROJECT.pdf` | Original Data Science 2 system documentation and architecture report |

---

## Cassandra Data Schema

Records are persisted in keyspace `crypto_stream` under table `market_data`:

| Field | Type | Unit / Format | Description |
|---|---|---|---|
| `crypto_id` | `text` (Partition Key) | String (`dogecoin`, `solana`) | Asset identifier |
| `timestamp` | `timestamp` (Clustering Key) | `YYYY-MM-DD HH:MM:SS` | Extraction timestamp |
| `symbol` | `text` | String (`DOGE`, `SOL`) | Ticker symbol |
| `price_usd` | `double` | US Dollars ($) | Spot price in USD |
| `volume_24h` | `double` | US Dollars ($) | Global 24-hour trading volume |
| `change_24h` | `double` | Percentage (%) | 24-hour price percentage change |

---

## Local Setup & Quickstart

### Prerequisites

- Docker Desktop (with WSL 2 backend enabled on Windows)
- Java Development Kit (JDK) (required by Apache Spark)
- Python 3.12+

### 1. Start Infrastructure Containers

```bash
docker compose up -d
```

### 2. Initialize Cassandra Keyspace & Table (First-Time Setup)

Once Cassandra is healthy (`docker ps`), create the target schema:

```bash
docker exec -it cassandra cqlsh -e "
CREATE KEYSPACE IF NOT EXISTS crypto_stream
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE IF NOT EXISTS crypto_stream.market_data (
    crypto_id text,
    symbol text,
    price_usd double,
    volume_24h double,
    change_24h double,
    timestamp timestamp,
    PRIMARY KEY (crypto_id, timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC);"
```

### 3. Set Up Python Virtual Environment

**Windows (PowerShell):**

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

**WSL 2 / Linux / macOS:**

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

---

## Running the Streaming Pipeline

Open four terminals with `.venv` activated in each, then run:

**Terminal 1 — Dogecoin Producer:**

```bash
python producer_doge.py
```

**Terminal 2 — Solana Producer:**

```bash
python producer_solana.py
```

**Terminal 3 — Spark Structured Streaming Engine:**

```bash
python spark_streaming.py
```

**Terminal 4 — Flask Web Dashboard:**

```bash
python app.py
```

### Monitoring Interfaces

- **Live Crypto Dashboard:** [http://localhost:5000](http://localhost:5000)
- **Apache Spark Web UI:** [http://localhost:4040](http://localhost:4040)

---

## Inspecting Stored Records

Run the audit script in a separate terminal to verify record counts, schema completeness, and API rate-limit gaps:

```bash
python inspect_100.py
```

---

## Shutting Down

Stop each running Python script with `Ctrl + C`, then stop the Docker containers without deleting stored Cassandra data:

```bash
docker compose down
```

---

## Team & Acknowledgments

- **Data Science 3 (Group 8 - AM3):** Lebbeus Ryan V. Santos, Xyrelle John M. Bauza, Carl Rohan M. Castillo
- **Data Science 2 (Group 5 - AM1):** Lebbeus Ryan V. Santos, Ramuell Franz C. Floranda
