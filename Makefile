# Hospital Patient Vital Signs Monitoring - Kappa architecture.
#
# ONE STACK AT A TIME. The host has 15.6 GB and Docker gets ~11 GB; the sibling
# ride-hailing project peaks near 10 GB. Bring it down before starting this one.
# The port ranges are disjoint (this project 91xx/81xx/31xx) so they cannot
# collide, but they cannot coexist in memory.

SHELL := /bin/bash
.DEFAULT_GOAL := help
COMPOSE := docker compose

# ---------------------------------------------------------------------------
##@ Setup

.PHONY: setup
setup: ## Check docker, create .env
	@command -v docker >/dev/null || { echo "docker not found - enable Docker Desktop WSL integration (plan/10 section 0.1)"; exit 1; }
	@docker info >/dev/null 2>&1 || { echo "docker daemon unreachable - is Docker Desktop running?"; exit 1; }
	@test -f .env || { cp .env.example .env; echo "created .env from .env.example"; }
	@echo "OK. Next: make up"

.PHONY: venv
venv: ## Create the local dev environment (tests and linting on the host)
	uv venv --python 3.12 .venv
	uv pip install --python .venv/bin/python -e ".[dev]"

# ---------------------------------------------------------------------------
##@ Running

.PHONY: up
up: ## Start the stack (Kafka, registry, UI, producers)
	RESUME=$${RESUME:-} $(COMPOSE) up -d --build
	@$(MAKE) --no-print-directory wait

.PHONY: wait
wait: ## Block until every service reports healthy
	@echo "waiting for services..."
	@for i in $$(seq 1 40); do \
	  unhealthy=$$($(COMPOSE) ps --format '{{.Service}} {{.Health}}' 2>/dev/null | awk '$$2!="healthy" && $$2!="" {print $$1}'); \
	  if [ -z "$$unhealthy" ]; then echo "all healthy"; break; fi; \
	  sleep 3; \
	done
	@$(MAKE) --no-print-directory ps

.PHONY: down
down: ## Stop, KEEP data
	$(COMPOSE) down

.PHONY: clean
clean: ## Stop and DELETE all data. The normal way to start a fresh run.
	$(COMPOSE) down -v
	@rm -f state/sim_epoch.json
	@rm -rf labs/inbox/*.json labs/inbox/*.sha256 labs/inbox/.tmp
	@echo "wiped. next 'make up' starts a fresh simulation from simulated day 1"

# ---------------------------------------------------------------------------
##@ Inspecting

.PHONY: ps
ps: ## Status of every service
	@$(COMPOSE) ps --format 'table {{.Service}}\t{{.Status}}\t{{.Ports}}'

.PHONY: logs
logs: ## Follow one service's JSON logs.  make logs SVC=bedside-monitor
	$(COMPOSE) logs -f --tail=100 $(SVC)

.PHONY: topics
topics: ## ★ Topics WITH THEIR CLEANUP POLICIES - the thing that must be right
	@echo "topic                     partitions  cleanup.policy  retention"
	@for t in $$(docker exec ward-kafka kafka-topics --bootstrap-server localhost:9192 --list 2>/dev/null | grep -v '^_'); do \
	  parts=$$(docker exec ward-kafka kafka-topics --bootstrap-server localhost:9192 --describe --topic $$t 2>/dev/null | head -1 | grep -oE 'PartitionCount: [0-9]+' | awk '{print $$2}'); \
	  cfg=$$(docker exec ward-kafka kafka-configs --bootstrap-server localhost:9192 --entity-type topics --entity-name $$t --describe 2>/dev/null); \
	  pol=$$(echo "$$cfg" | grep -oE 'cleanup\.policy=[a-z]+' | head -1 | cut -d= -f2); \
	  ret=$$(echo "$$cfg" | grep -oE 'retention\.ms=[0-9]+' | head -1 | cut -d= -f2); \
	  printf '%-25s %-11s %-15s %s\n' "$$t" "$${parts:-?}" "$${pol:-delete}" "$${ret:-infinite}"; \
	done
	@echo ""
	@echo "labs.results.v1 and ward.admissions.v1 MUST read 'compact'."
	@echo "A compacted topic silently created with 'delete' loses lab results weeks"
	@echo "later with nothing to point at - see scripts/create_topics.py."

.PHONY: simclock
simclock: ## Where the simulated clock is now
	@cat state/sim_epoch.json 2>/dev/null | python3 -m json.tool || echo "no anchor yet - run make up"

.PHONY: keys
keys: ## Distinct patient keys on the vitals topic (must be exactly 40)
	@docker exec ward-kafka timeout 25 kafka-console-consumer \
	  --bootstrap-server localhost:9192 --topic vitals.readings.v1 --from-beginning \
	  --property print.key=true --property print.value=false --timeout-ms 20000 2>/dev/null \
	  | sort -u | grep -c '^P' | xargs -I{} echo "  {} distinct patient keys (WARD_BEDS=40)"

.PHONY: db-init
db-init: ## Apply schema.cql to Cassandra idempotently
	.venv/bin/python scripts/init_cassandra.py

.PHONY: cqlsh
cqlsh: ## Open cqlsh in the Cassandra container
	@docker exec -it ward-cassandra cqlsh -k ward

.PHONY: ports
ports: ## What is listening where
	@echo "  Kafka UI      http://localhost:8180"
	@echo "  Schema Reg    http://localhost:8181/subjects"
	@echo "  Kafka         localhost:9192"
	@echo "  Cassandra     localhost:9142"
	@echo "  FastAPI Docs  http://localhost:8100/docs"
	@echo "  Airflow UI    http://localhost:8182"
	@echo "  Prometheus    http://localhost:9190"
	@echo "  Alertmanager  http://localhost:9193"
	@echo "  Grafana       http://localhost:3100"
	@echo "  monitors      http://localhost:8101/metrics"
	@echo "  lab uploader  http://localhost:8102/metrics"
	@echo "  Spark UI      http://localhost:4140   (replay v2: 4141)"
	@echo "  stream        http://localhost:8104/metrics (replay v2: 8105)"

.PHONY: grafana
grafana: ## Open Grafana dashboards in browser (http://localhost:3100)
	@echo "Opening Grafana at http://localhost:3100 ..."
	@python3 -m webbrowser "http://localhost:3100" 2>/dev/null || echo "Navigate to http://localhost:3100"

# ---------------------------------------------------------------------------
##@ Replay & Reprocessing (Kappa)

VERSION ?= v2
# The replay tools run INSIDE the stream container: .env names services by their
# compose hostnames (kafka:29092, cassandra), which the host cannot resolve.
IN_STREAM := docker exec ward-stream python -m

.PHONY: replay
replay: ## ★ Start ward-stream-v2 from offset 0 and watch it catch up (make replay VERSION=v2)
	$(COMPOSE) --profile replay up -d --build ward-stream-v2
	$(IN_STREAM) ward.replay.replay_runner --target-version $(VERSION) \
	  --metrics-url http://ward-stream-v2:8105/metrics

.PHONY: replay-status
replay-status: ## Progress of a running replay
	$(IN_STREAM) ward.replay.replay_runner --target-version $(VERSION) \
	  --metrics-url http://ward-stream-v2:8105/metrics --timeout 5 || true

.PHONY: diff
diff: ## Compare v1 and the replayed version, reading by reading
	$(IN_STREAM) ward.replay.compare_versions --v1 v1 --v2 $(VERSION) --out-dir /tmp/diff
	docker cp ward-stream:/tmp/diff/. reports/

.PHONY: cutover
cutover: ## Switch the served version through the API (make cutover VERSION=v2)
	curl -fsS -X POST localhost:8100/api/v1/pipeline/cutover \
	  -H 'Content-Type: application/json' -d '{"target_version":"$(VERSION)"}'; echo

.PHONY: rollback
rollback: ## Switch the served version back to v1
	$(MAKE) --no-print-directory cutover VERSION=v1

.PHONY: chaos-stop-stream
chaos-stop-stream: ## Stop the v1 stream so WardMonitoringSilent fires; restart with 'make chaos-heal'
	$(COMPOSE) stop ward-stream
	@date -u +"stream stopped at %H:%M:%SZ"

.PHONY: chaos-heal
chaos-heal: ## Bring the v1 stream back
	$(COMPOSE) up -d ward-stream
	@date -u +"stream restarted at %H:%M:%SZ"

# ---------------------------------------------------------------------------
##@ Verifying

.PHONY: test
test: ## Unit and contract tests (no Docker needed)
	.venv/bin/pytest tests/unit tests/contract -q

.PHONY: lint
lint: ## ruff + format check + mypy on the strict modules
	.venv/bin/ruff check ward tests scripts
	.venv/bin/ruff format --check ward tests scripts
	.venv/bin/mypy ward/clinical ward/simclock.py ward/store/dao.py ward/stream

.PHONY: fmt
fmt: ## Auto-format
	.venv/bin/ruff check --fix ward tests scripts
	.venv/bin/ruff format ward tests scripts

.PHONY: e2e
e2e: ## ★ End-to-end: consume real readings and score them with the real scorer
	.venv/bin/python scripts/verify_end_to_end.py

# ---------------------------------------------------------------------------
##@ Report and Artifacts

.PHONY: diagrams
diagrams: ## Compile TikZ vector architecture diagrams (D1-D4)
	$(MAKE) -C docs/diagrams

.PHONY: figures
figures: ## Synthesize or capture report evidence figures (R1-R9b)
	.venv/bin/python scripts/capture_figures.py

.PHONY: report
report: diagrams figures ## Build the complete academic coursework report PDF
	$(MAKE) -C docs/report

.PHONY: report-check
report-check: ## Check that the coursework report contains no unresolved placeholders
	$(MAKE) -C docs/report check

.PHONY: report-clean
report-clean: ## Clean LaTeX build intermediate files
	$(MAKE) -C docs/report clean

# ---------------------------------------------------------------------------
##@ Help

.PHONY: help
help: ## Show this help
	@awk 'BEGIN {FS = "[:#]+ *"} \
	  /^##@/ {printf "\n\033[1m%s\033[0m\n", substr($$0, 5); next} \
	  /^[a-zA-Z_-]+:.*##/ {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$3}' $(MAKEFILE_LIST)
