# 11 — Code Quality, Testing & Repository Structure

> **Rubric weight: 5 marks** — *"readability, modularity, README/setup instructions, configuration management, and reproducibility."*
>
> The cheapest 5 marks in the rubric, and what makes the other 95 defensible in a viva.

---

## 1. Repository structure

Organised **by pipeline stage**, because under Kappa there is one path and the directories should read like a journey through it. (The sibling Lambda project is organised by *architecture layer* — a deliberate structural difference reflecting a genuinely different architecture.)

```
hospital-vitals-kappa/
├── README.md
├── Makefile
├── compose.yaml
├── compose.observability.yaml
├── .env.example
├── pyproject.toml
├── plan/                          # ← this planning set
├── docs/
│   ├── adr/                       # architecture decision records
│   │   ├── 001-kappa-over-lambda.md
│   │   ├── 002-patient-id-partition-key.md
│   │   ├── 003-cassandra-query-first.md
│   │   ├── 004-ttl-instead-of-tombstones.md
│   │   ├── 005-news2-as-the-clinical-rule.md
│   │   └── 006-versioned-scores-for-replay.md
│   ├── diagrams/                  # draw.io sources + exported SVG/PDF
│   └── report/
│
├── ward/                          # flat package, stage-ordered
│   ├── settings.py                # single frozen settings object + validators
│   ├── simclock.py                # THE shared simulated clock
│   ├── obs/
│   │   ├── log.py                 # structlog JSON
│   │   ├── metrics.py             # Prometheus registry + Pushgateway
│   │   ├── tracing.py             # OTel bootstrap
│   │   └── kafka_client.py
│   ├── contracts/                 # *.avsc + pydantic models + serialization
│   │
│   ├── producers/                 # ── stage 1: ingest
│   │   ├── bedside_monitor.py
│   │   ├── lab_uploader.py
│   │   ├── admissions.py
│   │   └── physiology/            # AR(1) model, baselines, scripted narratives
│   │
│   ├── stream/                    # ── stage 2: process (THE single path)
│   │   ├── pipeline.py            # the one streaming job
│   │   ├── clean.py
│   │   ├── enrich.py
│   │   ├── windows.py
│   │   ├── lab_join.py
│   │   ├── alerts.py
│   │   ├── sinks.py
│   │   ├── dlq.py
│   │   ├── late.py
│   │   └── listener.py            # StreamingQueryListener -> Pushgateway
│   │
│   ├── clinical/                  # ★ THE ONE CLINICAL RULE ★
│   │   ├── news2.py               # scoring table as data, v1 + v2
│   │   ├── lab_rules.py
│   │   └── composite_risk.py
│   │
│   ├── store/                     # ── stage 3: store
│   │   ├── schema.cql
│   │   ├── dao.py                 # prepared statements only
│   │   └── session.py
│   │
│   ├── api/                       # ── stage 4: serve
│   │   ├── app.py
│   │   ├── routes/{ward,patients,alerts,reports,replay,pipeline,health}.py
│   │   ├── models.py
│   │   └── deps.py
│   │
│   ├── replay/                    # ── the Kappa capability
│   │   ├── replay_runner.py
│   │   └── compare_versions.py
│   │
│   ├── reporting/
│   │   ├── render.py
│   │   └── templates/ward_risk_report.html.j2
│   └── cli.py
│
├── orchestration/dags/            # (not "airflow/" — see §7)
├── observability/{prometheus,alertmanager,grafana,otel}/
├── tests/{unit,integration,contract,fixtures}/
├── scripts/
├── docker/{producer,spark-app,api,init}/Dockerfile
└── .github/workflows/ci.yml
```

**The structure encodes the architecture:** one `stream/` directory, one `clinical/` directory, no `batch/` anywhere. A marker opening this tree sees Kappa before reading a line of code.

---

## 2. The modularity rules that matter

### 2.1 `clinical/` must be pure

No Spark, no Kafka, no Cassandra, no I/O. Plain Python taking primitives and returning dataclasses. Enforced:

```python
def test_clinical_is_pure():
    forbidden = {"pyspark", "cassandra", "confluent_kafka", "requests", "os.environ"}
    for module in walk_modules("ward/clinical"):
        assert not (collect_imports(module) & forbidden)
```

This is what makes NEWS2 exhaustively testable against published reference values, and it is why the clinical rule can be reasoned about independently of the pipeline.

### 2.2 Only one scorer exists

```python
def test_only_one_scorer_exists():
    """The Kappa argument, enforced. Nothing outside ward/clinical/ computes a score."""
    offenders = grep_tree(root="ward/", exclude="ward/clinical/",
                          patterns=[r"news2", r"risk_score\s*=", r"NEWS2_TABLE"])
    assert not offenders, f"Second scoring implementation found: {offenders}"
```

**Run this on screen in the viva** when asked to prove the single-rule claim.

### 2.3 No `ALLOW FILTERING`, no string-built CQL

```python
def test_no_allow_filtering():
    assert not grep_tree("ward/", patterns=[r"ALLOW\s+FILTERING"])

def test_prepared_statements_only():
    """No f-string or %-formatted CQL anywhere - injection safety and performance."""
    assert not grep_tree("ward/store/", patterns=[r'f".*SELECT', r'".*%s.*"\s*%'])
```

---

## 3. Testing strategy

### 3.1 The pyramid

| Level | Count | Runtime | Needs |
|---|---|---|---|
| **Unit — pure Python** (clinical, simclock, parsing) | ~55 | < 2 s | nothing |
| **Unit — local Spark** (transforms on static DataFrames) | ~12 | ~40 s | `SparkSession(local[2])` |
| **Contract** (Avro round-trip, evolution) | ~6 | < 5 s | nothing |
| **Integration / smoke** | ~8 | ~3 min | the stack up |

### 3.2 The tests that carry argumentative weight

Per-document test tables are in `03 §6`, `05 §9`, `06 §8`, `07 §8`, `08 §9`, `09 §8`. The five that matter most:

1. **`test_news2.py`** — the published NEWS2 scoring table as fixtures, parameter by parameter, every threshold boundary, the `any_parameter_is_3` escalation, Scale 1 vs Scale 2. **The best test in the repository**; it validates against external reference data rather than against our own assumptions.
2. **`test_only_one_scorer_exists.py`** — the architecture argument, mechanically enforced.
3. **`test_replay_expected_diff.py`** — v1→v2 changes only `copd_scale2` patients, always downward, zero change elsewhere. **A replay with a predicted result is a verification; one without is a leap of faith.**
4. **`test_deterioration_e2e.py`** — `P014`'s scripted trajectory through the real transform chain reaches NEWS2 ≥ 7. CI proves the demo works.
5. **`test_watermark_safety.py`** — fails if the speed-up and watermark are changed inconsistently.

### 3.3 Spark test fixture

```python
@pytest.fixture(scope="session")
def spark():
    return (SparkSession.builder
            .master("local[2]").appName("tests")
            .config("spark.sql.shuffle.partitions", "2")   # 200 default = slow tests
            .config("spark.ui.enabled", "false")
            .getOrCreate())
```

Streaming logic is tested through the same pure functions on static DataFrames, so almost nothing needs `MemoryStream`. The exception is `test_windows.py`, which verifies watermark drop and late-routing behaviour.

### 3.4 Integration smoke test

`scripts/smoke_test.py`, run by `make test-int`:

1. Produce 200 synthetic readings with a known signature
2. Poll `/api/v1/ward/WARD-A/monitor` until they appear (timeout 90 s)
3. Assert Cassandra rows exist in all five tables with correct TTLs
4. Assert the DLQ received the injected invalid readings with correct reason codes
5. Drop a lab file; trigger `ward_lab_ingest`; assert the compacted topic and `labs_by_patient`
6. Assert a scored patient's `composite_risk` differs from `news2_total` (the join worked)
7. Trigger `ward_daily_report`; assert the PDF exists
8. Run a short replay; assert v1 and v2 rows coexist
9. Assert `/metrics` exposes the expected names

**Testcontainers was considered and rejected**: spinning Cassandra per test run costs 90 s each time on WSL2. A compose-based smoke test gives the same confidence at this scale. Stated in the report as a conscious trade-off.

---

## 4. Code standards

| Concern | Tool / rule |
|---|---|
| Format + lint | `ruff`, `ruff format` (line length 100) |
| Types | `mypy --strict` on `ward/clinical/`, `ward/simclock.py`, `ward/store/dao.py`; permissive elsewhere (Spark stubs are poor) |
| Docstrings | Every public function: what, **why**, and **units** (`sim_minutes` vs `real_seconds`, `mmol/L` vs `mg/dL`). This project has two genuine units hazards — time and clinical measurements. |
| Magic numbers | Banned. Clinical thresholds live in `clinical/` as named constants with a source comment citing the NEWS2 specification. |
| Comments | *Why*, not *what*. The TTL-not-tombstone and `collect()`-is-safe comments are the model. |
| Errors | Typed exceptions in `ward/errors.py`; never a bare `except:` |

---

## 5. CI (GitHub Actions)

```yaml
jobs:
  quality:  ruff check · ruff format --check · mypy
  test:     pytest tests/unit tests/contract --cov=ward
            coverage gate: 90% on ward/clinical/ (the clinical rule must be
            near-exhaustively covered), 85% on ward/simclock.py
  validate: promtool check rules · DAG import check · docker compose config
  archrules: test_only_one_scorer_exists · test_dag_purity ·
             test_no_allow_filtering · test_clinical_is_pure
```

The **`archrules` job is separate on purpose** — architectural invariants are gated independently of ordinary tests, so a failure there is unmistakable. That is a nice detail to point at.

No Spark or compose in CI (too slow, too flaky on hosted runners); integration runs locally via `make test-int` with a screenshot in the report. Stating that trade-off beats a CI file that silently skips the real tests.

---

## 6. Git

- `main` + short-lived `feat/*`; conventional commits
- **ADRs in `docs/adr/`** — six short records capturing each decision when it was made. Raw material for report §3 and §5, and strong viva evidence that decisions preceded code.
- Tag `v1.0-submission`
- `.gitignore`: `.env`, `state/`, `labs/inbox/`, `__pycache__`, `.pytest_cache`, `checkpoints/`, generated `reports/*.pdf`

---

## 7. Structural differences from the sibling project

`../ride-hailing-lambda` is a different group's submission. Differences are architectural first, structural second:

| | This project (Kappa) | Ride-hailing (Lambda) |
|---|---|---|
| Package layout | `ward/` (flat) | `src/fleet/` (src layout) |
| Directory taxonomy | By **pipeline stage** | By **architecture layer** |
| Compose file | `compose.yaml` | `docker-compose.yml` |
| Host ports | 8100/8180/8182/8190/9190/3100/16786 | 8000/8080/8082/8090/9090/3000/16686 |
| Orchestration dir | `orchestration/` | `airflow/` |
| Domain logic home | `clinical/` — one rule, one path | `transforms/` — shared by two layers |
| Serving store | Cassandra | Redis + PostgreSQL |
| Settings style | one frozen module-level object | `BaseSettings` classes per layer |
| Signature test | `test_only_one_scorer_exists` | `test_shared_transforms` |

Both use Python, `confluent-kafka`, PySpark and `structlog` — because those are the right choices for both. Deliberately picking a worse tool in one repo to look different would be a bad decision a viva would expose. **The differentiation that matters is architectural, and it is total.**
