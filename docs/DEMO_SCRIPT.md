# Demonstration Video Choreography & Viva Examination Guide

This document provides the exact timestamped screenplay, terminal commands, presenter cues, and clinical talking points for the EC8203 Applied Big Data Engineering coursework demonstration. It provides choreography for both the **comprehensive 10-minute viva video** and a **rapid 2-minute examiner walkthrough**.

## Technical Pre-Flight Checklist
Before recording or presenting live, ensure all containers and services are booted:
```bash
# 1. Start core data platform
make up

# 2. Start observability stack
make up-obs

# 3. Verify port availability
make ports
# Expected: API 8100, Grafana 3100, Airflow 8180, Prometheus 9190, Alertmanager 9193, Kafka 9092
```

## Screenplay: 10-Minute Comprehensive Viva Video

### 0:00 to 1:15: Introduction & Clinical Architectural Rationale
* **Screen Display:** Architecture Diagram D2 (`docs/diagrams/D2-layered-architecture.pdf`) and University title slide.
* **Presenter Dialogue (Munsif):**
  > "Welcome. We present Use Case 2: Hospital Patient Vital Signs Monitoring. Our platform answers the core clinical question: *Which patients show concerning vital-sign trends right now, and how do yesterday's lab results change their risk picture going forward?*
  > 
  > The brief required us to decide between a Lambda or Kappa architecture. We chose a **Pure Kappa Architecture**. In acute medical care, the dual-codebase drift inherent to Lambda is a clinical safety hazard: if a streaming speed layer and an overnight batch layer compute National Early Warning Scores (NEWS2) with even minor divergence, a patient classified as Critical at midnight could be downgraded on the morning ward report. 
  > 
  > Under pure Kappa, exactly one pure functional clinical risk scorer exists across the entire repository. This invariant is enforced by an automated Abstract Syntax Tree test. Bedside vital telemetry and 30-day historical replay execute the exact same Python scoring logic."

### 1:15 to 2:45: Live 40-Bed Ward Clinical Monitor (Worst-First Triage)
* **Screen Display:** Browser on Grafana Ward Clinical Monitor (`http://localhost:3100/d/ward-clinical-monitor`).
* **Terminal Command:**
  ```bash
  curl -s http://localhost:8100/api/v1/ward/monitor | jq '.[0:3]'
  ```
* **Presenter Dialogue (Ashfaq):**
  > "Here is our live 40-bed ward monitor running on Grafana, backed by FastAPI on port 8100. Notice that clinicians never search beds sequentially; they require a worst-first triage view.
  > 
  > Bed 31 (Patient P031) and Bed 14 (Patient P014) are automatically clustered at the top with composite risks of 10.0 and 9.5 in the critical red tier. 
  > 
  > How is this achieved at scale? In our Apache Cassandra storage layer, table Q2 (`ward_risk_snapshot`) defines `max_composite_risk DESC` as its clustering key. Cassandra physically stores rows sorted by risk on disk. When the dashboard queries the ward, retrieval is a sequential slice with zero application-level sorting overhead."

### 2:45 to 4:00: Pathology Lab Daily Ingestion & Risk Recalibration
* **Screen Display:** Airflow UI (`http://localhost:8180`) showing DAG `ward_lab_ingest` and the rendered daily PDF report.
* **Terminal Command:**
  ```bash
  # Trigger daily lab ingestion DAG
  docker compose exec airflow-scheduler airflow dags trigger ward_lab_ingest
  ```
* **Presenter Dialogue (Lareef):**
  > "While bedside sensors emit continuous telemetry every 3 seconds, the pathology laboratory uploads blood panels once daily. Airflow DAG `ward_lab_ingest` polls the inbox, parses Avro records, and updates Cassandra table Q6.
  > 
  > Here, Patient P014 presented with bacterial pneumonia. Yesterday's lab results reveal a serum lactate of 3.2 mmol/L. Our streaming engine dynamically applies a 1.25x risk multiplier, elevating the patient from moderate risk to critical septic shock alert.
  > 
  > Notice also Airflow DAG `ward_daily_report`: using Jinja2 and WeasyPrint, it generates an official daily clinical PDF report compiling patient summaries and pathology correlations without performing any duplicate scoring."

### 4:00 to 6:30: The Pure Kappa Replay & Guideline Migration Proof
* **Screen Display:** Split screen: Kafka UI consumer groups on the left, Grafana Replay Comparison dashboard on the right (`http://localhost:3100/d/ward-replay-comparison`).
* **Terminal Command:**
  ```bash
  # Start the historical log replay under NEWS2 Scale 2
  make replay VERSION=v2
  ```
* **Presenter Dialogue (Munsif):**
  > "Now we demonstrate the defining proof of the Kappa architecture: historical data reprocessing via log replay.
  > 
  > In 2017, the Royal College of Physicians updated NEWS2 to introduce SpO2 Scale 2 for patients with chronic hypercapnic respiratory failure (COPD). Under Scale 1, COPD patients with normal baseline saturations of 88 to 92 percent were constantly penalized, generating continuous false alarms and dangerous alarm fatigue.
  > 
  > To migrate our platform, we do not touch a batch database. We spawn a parallel streaming consumer group `ward-stream-v2-replay` configured with `auto.offset.reset=earliest`. It consumes all 30 days of raw immutable history from Kafka topic `vitals.raw` at full I/O throughput, executing the updated Scale 2 logic into versioned keyspace `hospital_vitals_v2`.
  > 
  > We then run our clinical audit comparator:
  > ```bash
  > python -m ward.replay.compare_versions --sim-date 2026-03-05
  > ```
  > Look at the results: for the COPD cohort, 180 false alarms are eliminated. Crucially, look at the 34 non-COPD control patients: their risk trajectory delta is exactly 0.00 percent. The recomputed history is mathematically bit-identical."

### 6:30 to 7:45: Zero-Downtime Atomic Serving Cutover
* **Screen Display:** Terminal executing cutover and live Grafana dashboard updating instantly.
* **Terminal Command:**
  ```bash
  curl -X POST http://localhost:8100/api/v1/pipeline/cutover -H "Content-Type: application/json" -d '{"target_version": "v2"}'
  ```
* **Presenter Dialogue (Ashfaq):**
  > "With clinical governance sign-off achieved, how do we cut over? We send a POST request to `/api/v1/pipeline/cutover`. 
  > 
  > In under 2 milliseconds, FastAPI atomically redirects its internal prepared statements from `hospital_vitals` to `hospital_vitals_v2`. The ward monitor immediately reflects the updated COPD baselines without a single dropped WebSocket connection or server restart."

### 7:45 to 9:00: Observability & Clinical Safety Chaos Test
* **Screen Display:** Prometheus Alerts page (`http://localhost:9190/alerts`) and Alertmanager (`http://localhost:9193`).
* **Terminal Command:**
  ```bash
  # Chaos test: kill the vital telemetry producer
  docker compose stop producer
  ```
* **Presenter Dialogue (Lareef):**
  > "We conclude with our observability design. In e-commerce, alerts fire when error rates rise. In acute clinical monitoring, the most lethal failure mode is silent pipeline death: if a sensor cable unplugs or PySpark crashes, error rates remain zero, but clinicians assume silence means health.
  > 
  > We enforce the doctrine: *Absence of Alerts is NOT Absence of Risk*. We stop the telemetry producer. Within 3 minutes, our Prometheus rule `WardMonitoringSilent` triggers because the write rate drops to zero. Alertmanager immediately routes this emergency alert with zero delay to the ward charge nurse pager, preventing unmonitored patient catastrophe."

### 9:00 to 10:00: Conclusion & Summary
* **Screen Display:** Academic report PDF (`docs/report/main.pdf`) and test suite execution.
* **Terminal Command:**
  ```bash
  make test && make lint
  ```
* **Presenter Dialogue (Munsif):**
  > "In summary: our 19-page report, 365 passing automated tests, vector TikZ diagrams, and pure Kappa pipeline demonstrate that a single stream processing engine is the safest, cleanest, and most robust architecture for acute hospital vital sign monitoring. Thank you."

## Fast 2-Minute Examiner Walkthrough Script
When an examiner requests a rapid summary:
1. **0:00 - 0:30 (The Core Idea):** Open `docs/diagrams/D2-layered-architecture.pdf`. Explain pure Kappa vs Lambda: single NEWS2 scorer eliminates dual-codebase clinical drift.
2. **0:30 - 1:00 (Worst-First Ward):** Open `http://localhost:3100/d/ward-clinical-monitor`. Point to Bed 31 and Bed 14 at the top. Explain Cassandra Q2 clustering order `max_composite_risk DESC`.
3. **1:00 - 1:30 (Replay & Invariance):** Run `python -m ward.replay.compare_versions`. Show COPD cohort dropping 2 points (180 false alarms eliminated) and control cohort at 0.00% delta.
4. **1:30 - 2:00 (Clinical Safety Alert):** Show `observability/prometheus/alerts.yml` rule `WardMonitoringSilent` ("Absence of alerts != absence of risk") and run `make test` (365 tests pass).
