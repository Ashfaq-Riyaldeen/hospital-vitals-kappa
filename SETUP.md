# How to Run the Project — Quick Setup Guide

Follow these steps to spin up the Hospital Patient Vital Signs Monitoring platform (Kappa Architecture).

## 1. Prerequisites

Before starting, ensure the following are installed:
- **Docker Desktop** (running, with WSL 2 integration enabled if on Windows)
- **Git**
- **uv** (fast Python package manager — for local host testing and linting)
- Minimum host memory: ~12 GB recommended (the stack allocates ~9-10 GB peak)

## 2. Environment Configuration

Clone the repository and copy the pre-configured environment template:

```bash
git clone https://github.com/munsif-dev/hospital-vitals-kappa.git
cd hospital-vitals-kappa
cp .env.example .env
```

The default values in `.env` are pre-tuned for local execution with 288x time acceleration (1 simulated day = 5 real minutes).

## 3. Start the Stack

Start all containers (Kafka, Schema Registry, Kafka UI, Cassandra, Bedside Monitors, Lab Uploader, Stream Processor, Serving API, Airflow, Prometheus, Alertmanager, and Grafana):

```bash
make up
```

Wait until all services report healthy:

```bash
make wait
```

Check the status of running containers:

```bash
make ps
```

## 4. Platform Ports & Web Interfaces

| Service | URL / Port | Credentials / Purpose |
|---|---|---|
| **Grafana Dashboards** | [http://localhost:3100](http://localhost:3100) | Anonymous Admin (No login required) |
| **FastAPI Swagger Docs** | [http://localhost:8100/docs](http://localhost:8100/docs) | Interactive clinical serving endpoints |
| **Airflow Web UI** | [http://localhost:8182](http://localhost:8182) | `admin` / `admin` |
| **Kafka UI** | [http://localhost:8180](http://localhost:8180) | Topic partitions, messages, consumer lag |
| **Prometheus** | [http://localhost:9190](http://localhost:9190) | Metric queries and active alert rules |
| **Alertmanager** | [http://localhost:9193](http://localhost:9193) | Clinical safety and infrastructure routing |
| **Schema Registry** | [http://localhost:8181/subjects](http://localhost:8181/subjects) | Avro schemas |
| **Cassandra** | `localhost:9142` | CQL interface |
| **Kafka Broker** | `localhost:9192` | Plaintext bootstrap listener |

## 5. Demonstration & Verification Workflow

### Step 1: Inspect Live Clinical Ward Monitoring
- Open Grafana at [http://localhost:3100](http://localhost:3100) and view the **Ward · Clinical Monitor (live)** dashboard.
- Observe the 40-bed monitoring grid, deteriorating patient counts, stale lab indicators, and live clinical alerts.
- Query the serving layer via curl:
  ```bash
  curl -s http://localhost:8100/api/v1/ward/WARD-A/monitor | jq .
  curl -s http://localhost:8100/api/v1/patient/P031 | jq .
  ```

### Step 2: Trigger Historical Stream Replay (Kappa Reprocessing)
Reprocess historical observations under the revised clinical rule (NEWS2 SpO2 Scale 2 for COPD patients):

```bash
make replay VERSION=v2
```

Track the replay consumer group lag catching up in Kafka UI or via:

```bash
make replay-status
```

### Step 3: Inspect Replay Comparison & Generate Clinical Diff
Open the **Ward · Replay & Reprocessing (v1 vs v2)** dashboard in Grafana to inspect side-by-side trajectories:
- Observe patient `P031` (COPD): score drops by 2-3 points, eliminating chronic false alerts.
- Observe patient `P014` (Control): score is 100% bit-identical (0 divergence).

Generate the clinical audit diff report:

```bash
make diff
```

The markdown report is saved to `reports/replay_diff_v1_vs_v2.md`.

### Step 4: Execute Clinical Governance Cutover
Execute atomic cutover to activate v2 in the serving layer:

```bash
make cutover VERSION=v2
```

Verify that the API immediately reports active version `v2`:

```bash
curl -s http://localhost:8100/api/v1/pipeline/status | jq .active_scorer_version
```

If needed, test instant rollback to v1:

```bash
make rollback VERSION=v1
```

## 6. Running Tests Locally

Run the complete test suite on the host machine without Docker:

```bash
# Setup virtual environment and dependencies
make venv

# Run all 365 unit and contract tests
make test

# Run code formatting and strict type checks
make lint
```

## 7. Teardown & Reset

Stop the platform while preserving database and checkpoint data:

```bash
make down
```

Wipe all containers, volumes, Cassandra tables, Kafka topics, and simulation state for a clean fresh run:

```bash
make clean
```
