# 10 — Infrastructure & Docker Compose

> **PDF:** *"A README with architecture summary, setup/run instructions, and how to reproduce results (**Docker Compose strongly recommended**)."*
> **Rubric:** Code Quality — *"configuration management, and **reproducibility** (e.g. via Docker/Compose)."*

---

## 0. Prerequisite: enable Docker in WSL

Docker Desktop is installed on Windows but **WSL integration is not enabled for this Ubuntu distro**. Before day 1:

> Docker Desktop → **Settings → Resources → WSL Integration** → enable this Ubuntu distro → **Apply & Restart**

Verify with `docker run --rm hello-world` and `docker compose version`.

Then create `C:\Users\<you>\.wslconfig` and run `wsl --shutdown`:
```ini
[wsl2]
memory=12GB
processors=12
swap=4GB
```
Without this, WSL2 claims up to half the host RAM by default and Docker Desktop's `vmmem` can balloon until the machine stalls mid-demo.

---

## 1. Service topology

Two files: `compose.yaml` (pipeline) and `compose.observability.yaml`, combined via `COMPOSE_FILE` in `.env`.

> **Note the filename** — this project uses the modern `compose.yaml`; the sibling ride-hailing project uses `docker-compose.yml`. Together with the distinct port range below, this keeps the two stacks unambiguous.

### 1.1 Core pipeline

| Service | Image | Host port | Mem limit | Healthcheck |
|---|---|---|---|---|
| `kafka` | `confluentinc/cp-kafka:7.6.1` (KRaft) | 9192 | 1.5 G | `kafka-broker-api-versions` |
| `schema-registry` | `confluentinc/cp-schema-registry:7.6.1` | 8181 | 512 M | `curl /subjects` |
| `kafka-ui` | `provectuslabs/kafka-ui:v0.7.2` | **8180** | 384 M | `curl /actuator/health` |
| **`cassandra`** | `cassandra:4.1` | 9142 | **2.0 G** | `cqlsh -e "describe keyspaces"` |
| `postgres-airflow` | `postgres:16-alpine` | 5533 | 256 M | `pg_isready` |
| `spark-master` | `bitnami/spark:3.5.1` | 7177, **8190** | 512 M | port check |
| `spark-worker-1/2` | `bitnami/spark:3.5.1` | — | 1.5 G each | port check |
| `airflow-webserver` | `apache/airflow:2.9.3-python3.11` | **8182** | 768 M | `curl /health` |
| `airflow-scheduler` | `apache/airflow:2.9.3-python3.11` | — | 768 M | `airflow jobs check` |
| `bedside-monitor` | build `./docker/producer` | 8101 | 256 M | `curl /health` |
| `lab-uploader` | build `./docker/producer` | 8102 | 256 M | `curl /health` |
| `ward-stream` | build `./docker/spark-app` | 4140 | 1.0 G | driver port |
| `api` | build `./docker/api` | **8100** | 256 M | `curl /health/live` |
| `init` (one-shot) | build `./docker/init` | — | 128 M | — |

`init` creates topics **with their cleanup policies**, registers schemas, applies the CQL schema, seeds the admissions topic, and **writes the simulated-clock epoch**. Everything else waits on it.

### 1.2 Observability

| Service | Image | Host port | Mem |
|---|---|---|---|
| `prometheus` | `prom/prometheus:v2.53.0` | **9190** | 512 M |
| `pushgateway` | `prom/pushgateway:v1.9.0` | 9191 | 128 M |
| `alertmanager` | `prom/alertmanager:v0.27.0` | **9193** | 128 M |
| `grafana` | `grafana/grafana:11.1.0` | **3100** | 384 M |
| `kafka-exporter` | `danielqsj/kafka-exporter:v1.7.0` | 9308 | 128 M |
| `cassandra-jmx-exporter` | `bitnami/jmx-exporter` sidecar | 9404 | 192 M |
| `statsd-exporter` | `prom/statsd-exporter:v0.27.0` | 9202 | 128 M |
| `otel-collector` | `otel/opentelemetry-collector-contrib:0.104.0` | 4417, 4418 | 256 M |
| `jaeger` | `jaegertracing/all-in-one:1.58` | **16786** | 384 M |
| `alert-sink` | build | 8103 | 128 M |
| `loki` + `promtail` *(stretch)* | `grafana/loki:3.0.0` | 3200 | 512 M |

### 1.3 Memory budget

| Group | Total |
|---|---|
| Kafka + Schema Registry + UI + exporter | ~2.5 G |
| **Cassandra (+ JMX exporter)** | ~2.2 G |
| Spark (master + 2 workers + app) | ~4.5 G |
| Airflow (web + scheduler + metadata DB) | ~1.8 G |
| Producers + API + init | ~0.9 G |
| Observability | ~1.9 G |
| **Total (no Loki, the default)** | **≈ 13.8 G** |

Against 14 GB with WSL capped at 12 GB this is **over budget**, so the defaults are tuned down:

1. **Run only one project's stack at a time.** `make down` before switching to `../ride-hailing-lambda`.
2. **`make up` uses a single Spark worker (2 G)** rather than two — saving 1.5 G. Two workers are available via `make up-full` for a throughput demonstration.
3. Loki is off by default (`COMPOSE_PROFILES`).
4. Every service has an explicit `mem_limit`. **Without limits, Cassandra and the Spark/Kafka JVMs size their heaps from *host* RAM and collectively over-commit** — this is the most common cause of a stack that runs for ten minutes and then dies.

Tuned default: **≈ 12.3 G**.

### 1.4 Cassandra tuning — do this on day 3, not on demo day

```yaml
cassandra:
  image: cassandra:4.1
  environment:
    MAX_HEAP_SIZE: 1G          # else Cassandra takes 1/4 of HOST ram
    HEAP_NEWSIZE: 256M
    CASSANDRA_CLUSTER_NAME: ward
    CASSANDRA_DC: dc1
    CASSANDRA_ENDPOINT_SNITCH: GossipingPropertyFileSnitch
  healthcheck:
    test: ["CMD-SHELL", "cqlsh -e 'describe keyspaces' || exit 1"]
    interval: 15s
    timeout: 10s
    retries: 10
    start_period: 90s          # Cassandra genuinely takes 60-90s to be ready
  mem_limit: 2g
```

`start_period: 90s` is essential. Without it the healthcheck fails during normal start-up, compose marks the service unhealthy, and every dependent service refuses to start.

---

## 2. Start-up ordering

```
kafka ─┬─▶ schema-registry ─┐
       └─▶ kafka-ui         │
cassandra ──────────────────┼──▶ init ──┬──▶ bedside-monitor
postgres-airflow ─▶ airflow-*           ├──▶ lab-uploader
spark-master ─▶ spark-worker-* ─────────┼──▶ ward-stream
                                        ├──▶ api
                                        └──▶ (observability)
```

`init` is the synchronisation point. Health-check ordering alone is still not sufficient for Kafka (a broker can report healthy a moment before accepting producer connections), so producers also retry with backoff (`03 §4.5`). Belt and braces — it is the difference between a stack that comes up first time and one that needs `docker compose up` run twice.

---

## 3. WSL2-specific issues to expect

| Issue | Symptom | Fix |
|---|---|---|
| **Files on `/mnt/c/`** | 10–50× slower; Spark and Cassandra crawl | Keep the project in the Linux filesystem (already correct at `/home/munsif/…`). **Never** bind-mount from `/mnt/c`. |
| **Cassandra slow start** | Dependents refuse to start | `start_period: 90s` (§1.4). Be patient on first `make up`. |
| **Cassandra heap grab** | Container OOM-killed, or the host stalls | `MAX_HEAP_SIZE=1G` — without it Cassandra sizes from host RAM |
| **Stale KRaft cluster id** | *"The Cluster ID … doesn't match"* | `make clean` removes the volume |
| **Stale Cassandra `system` keyspace** after a cluster-name change | Startup failure | `make clean` |
| **`vmmem` balloon** | Machine stalls | `.wslconfig` cap (§0) |
| **Spark on host Java 21** | `UnsupportedClassVersionError` | Never run Spark on the host JVM; `bitnami/spark:3.5.1` ships Java 17 |
| **PySpark ↔ Python mismatch** | *"Python in worker has different version"* | Pin both to 3.11; set `PYSPARK_PYTHON` |
| **Cassandra driver from Spark** | `ClassNotFoundException` | Needs the `spark-cassandra-connector` package, **or** — our choice — write via the pure-Python driver inside `foreachBatch`, avoiding the connector entirely. Simpler and easier to defend line by line. |
| **Clock skew after laptop sleep** | Simulated clock jumps; watermarks drop everything | `SimClockDrift` alert; `make restart-sim` re-anchors |

---

## 4. Configuration management

Single source of truth: **`.env`** (`.env.example` committed, `.env` git-ignored). No secrets or hostnames in code.

```bash
# Simulation
SIM_DAY_SECONDS=300
SIM_EPOCH_SIM=2026-04-01T00:00:00Z
WARD_SIZE=40
WARD_ID=WARD-A
VITALS_INTERVAL_SIM_MINUTES=15
DEFECT_RATE=0.017
RANDOM_SEED=42

# Kafka
KAFKA_BOOTSTRAP=kafka:9092
SCHEMA_REGISTRY_URL=http://schema-registry:8081
VITALS_TOPIC=vitals.readings.v1
VITALS_PARTITIONS=6
VITALS_RETENTION_MS=10800000        # 30 simulated days = 3 real hours

# Processing
WATERMARK_SIM_MINUTES=60
TREND_WINDOW_SIM_HOURS=4
TREND_SLIDE_SIM_HOURS=1
SCORER_VERSION=v2
CONSUMER_GROUP=ward-stream-v1

# Storage
CASSANDRA_HOSTS=cassandra
CASSANDRA_KEYSPACE=ward
SNAPSHOT_TTL_SECONDS=120

# Serving
ACTIVE_SCORER_VERSION=v2

# Observability
OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4317
PUSHGATEWAY_URL=http://pushgateway:9091
LOG_LEVEL=INFO
```

`ward/settings.py` uses a single frozen settings object with validators that fail fast:

```python
@model_validator(mode="after")
def watermark_must_survive_real_jitter(self):
    # 60 sim-min / 288x = 12.5 real seconds. See plan/03 §3.3.
    real = self.watermark_sim_minutes * 60 / (86400 / self.sim_day_seconds)
    if real < 10.0:
        raise ValueError(f"Watermark is only {real:.2f} real seconds - too tight.")
    return self

@model_validator(mode="after")
def snapshot_ttl_must_exceed_trigger(self):
    # An expiring snapshot must be refreshed before it expires, or the ward
    # monitor flickers. TTL must comfortably exceed the micro-batch interval.
    if self.snapshot_ttl_seconds < self.trigger_seconds * 10:
        raise ValueError("SNAPSHOT_TTL_SECONDS too close to the trigger interval.")
    return self
```

**Both validators encode a subtle coupling as a startup guard rather than a comment.** Worth showing in the viva.

---

## 5. Makefile

```
make setup              # check docker, copy .env.example, pull images
make up                 # full stack (1 spark worker)
make up-full            # 2 spark workers
make down               # stop, keep volumes
make clean              # stop and DELETE volumes (fixes KRaft/Cassandra id issues)
make logs SVC=api
make ps · make ports

make demo               # scripted end-to-end demo run (see 13)
make restart-sim        # re-anchor the simulated clock

make topics · make peek TOPIC=... · make lag · make offsets
make cqlsh              # into the ward keyspace
make trigger-dag DAG=ward_lab_ingest

make replay VERSION=v2  # ★ launch a replay run
make replay-status      # progress + diff so far
make cutover VERSION=v2 # ★ switch the active scorer version

make test · make test-int · make lint
make chaos-kill-monitors / chaos-kill-stream / chaos-bad-data /
     chaos-device-fault / chaos-skip-labs

make report             # regenerate the daily report for the latest simulated day
```

`make demo`, `make replay` and `make cutover` are the three commands the marker will run.

---

## 6. Reproducibility checklist

- [ ] `git clone` → `make setup` → `make up` → healthy within 4 minutes (Cassandra is the long pole)
- [ ] No manual UI steps (no hand-created Airflow connections, no hand-added Grafana dashboards)
- [ ] Every image tag pinned — **no `:latest`**
- [ ] `.env.example` covers every variable the code reads
- [ ] `make clean && make up` works twice in a row
- [ ] Grafana dashboards and datasources appear automatically
- [ ] `make demo` produces a report PDF without intervention
- [ ] `make replay VERSION=v2` completes and the diff matches the expected COPD-only result
- [ ] README quick start is ≤ 5 commands
- [ ] `RANDOM_SEED` fixed → `P014` deteriorates and `P031` behaves identically every run
- [ ] Disk footprint documented (~8 GB of images)

---

## 7. README structure

1. **What this is** — one paragraph + architecture diagram
2. **⚠ Safety disclaimer** — simulated data, not a medical device (must be prominent)
3. **The business question and how the system answers it**
4. **Architecture at a glance** — Kappa, one processing path, the log as system of record
5. **Quick start** — 5 commands
6. **What to look at** — the URL table
7. **The demo** — what to watch and when (`P014` deteriorating on simulated day 2; the replay)
8. **The replay** — `make replay` / `make cutover`, explained
9. **Simulated clock** — the compression, stated clearly
10. **Repository layout** — annotated tree
11. **Configuration** — the `.env` table
12. **Running the tests**
13. **Troubleshooting** — the WSL2/Cassandra table from §3
14. **Limitations** — pointer to the report
15. **Individual contributions** — required for group submissions
