# F7 tasks — Production setup (deploy + monitoring + alerts)

> Decomposição atômica. Cada task = 1 commit `conventional-commit`. Tests verdes obrigatórios antes de fechar a task.
> Ordem é dependência-respeitada; tasks paralelas marcadas como tal.
> Status: ⚪ pending · 🟡 in-progress · ✅ done.
>
> **Escopo:** empacotar pipeline para rodar 24/7 fora do laptop. Config tipada, container, métricas Prometheus, alertas com runbook.
> **Fora do escopo:** F5 (CLI watch + observabilidade pública de Gold), F6 (Databricks runner). F7 cobre infra de operação do agent loop.

---

## Config + secrets (1.x)

- ✅ **F7.1** — `feat(F7): typed Settings via pydantic-settings`. Shipped (code in commit 2da6965 — message accidentally swept by concurrent agent commit; intent + diff are F7.1). `src/pipeline/config/settings.py` `Settings(BaseSettings)` cobre 11 envs (`DASHSCOPE_API_KEY` SecretStr, `AGENT_RETRY_BUDGET`, `AGENT_DIAGNOSE_BUDGET`, `AGENT_LOOP_INTERVAL`, `AGENT_LOCK_PATH`, `MANIFEST_PATH`, `BRONZE_ROOT`, `SILVER_ROOT`, `GOLD_ROOT`, `LOG_LEVEL`, `LOG_FORMAT`); validators ge≥1, interval ≥1.0s, Literal-checked LOG_LEVEL/LOG_FORMAT; SecretStr no-leak; 12 tests em `tests/unit/test_config_settings.py` (renomeado para coexistir com legacy F1 `test_settings.py`). Co-existe com legacy `pipeline.settings` até F7.2 collapse.
- ✅ **F7.2** — `feat(F7): wire Settings into CLI agent subcommand`. Shipped e1366f6 — `pipeline.cli.agent` lê `pipeline.config.Settings` via `_field_default()` (no instantiation needed for `--help`); CLI flag final precedence (envvar mapping intact); missing `DASHSCOPE_API_KEY` → one-line `click.UsageError` (no Pydantic stack trace). Legacy `pipeline.settings.Settings.load()` still wires runner internals (anthropic key, lead secret). 12 tests in `tests/integration/test_cli_agent.py` (3 new: friendly error, settings-default in help, flag-overrides-env).
- ⚪ **F7.3** — `feat(F7): secret redaction in structlog processor`. Novo processor em `pipeline.agent._logging` mascara qualquer valor que case `*_KEY|*_TOKEN|*_SECRET|*_PASSWORD` (chave OR valor matching `sk-...`). Tests: dict nested, list, value-pattern, key-pattern, plaintext untouched.
- ✅ **F7.4** — `docs(F7): .env.example with every required var`. Shipped — `.env.example` na raiz com chaves vazias + comentário 1-line por var. README Setup section updated.

## Container + runtime (2.x)

- ✅ **F7.5** — `feat(F7): multi-stage Dockerfile (uv + non-root)`. Shipped 095afb4 — builder/runtime stages, non-root uid 10001, HEALTHCHECK, .dockerignore; `docker build --check` clean. Stage 1 `uv sync --frozen --no-dev` em `/app/.venv`. Stage 2 `python:3.12-slim` copia venv + src; non-root user `pipeline:pipeline` (uid 10001); HEALTHCHECK roda `pipeline agent --help`; ENTRYPOINT `["python","-m","pipeline"]`. `.dockerignore` exclui `data/`, `logs/`, `.venv/`, `__pycache__`. Tests: `docker build` smoke em CI lane (deferred to F7.19).
- ⚪ **F7.6** — `feat(F7): docker-compose.yml for local prod-mode`. Service `agent` rodando `agent run-forever`; volumes nomeados `pipeline-data` `pipeline-logs` `pipeline-state`; `env_file: .env`; `restart: unless-stopped`; healthcheck `CMD pipeline agent --help`. Sem deps externos (Prometheus/Grafana entram em F7.13).
- ⚪ **F7.7** — `feat(F7): graceful shutdown wires SIGTERM → INTERRUPTED`. `pipeline.agent.loop.run_forever` instala signal handler (SIGTERM + SIGINT) que set `stop_event` + flag in-progress run para `INTERRUPTED` (já existe em F4.22 fix; F7.7 expande para SIGTERM e garante manifest update antes de `sys.exit(0)`). Tests: subprocess send SIGTERM mid-loop → exit 0 + manifest row INTERRUPTED.

## Healthchecks + métricas Prometheus (3.x)

- ⚪ **F7.8** — `feat(F7): Prometheus client integration + counter/gauge primitives`. Add `prometheus-client` dep. `src/pipeline/obs/metrics.py` declara registry + métricas: `pipeline_agent_iterations_total{status}`, `pipeline_agent_failures_total{layer,error_kind}`, `pipeline_agent_escalations_total{layer,error_kind}`, `pipeline_agent_fix_attempts_total{layer,error_kind,outcome}`, `pipeline_layer_duration_seconds{layer}` (histogram), `pipeline_lock_held{instance}` (gauge 0/1). Tests: counters increment, histogram observes, registry isolated per test.
- ⚪ **F7.9** — `feat(F7): instrument agent loop + executor with metrics`. Hook em `loop.run_once` (iter counter, lock gauge), `executor.run_with_recovery` (failure/escalation/fix counters, layer histogram). Sem mudança de side-effects observáveis. Tests: run_once happy path → expected counter values; failure path → escalation counter +1.
- ⚪ **F7.10** — `feat(F7): /metrics + /healthz + /readyz HTTP endpoints`. `src/pipeline/obs/server.py` com `aiohttp` (já indireto?) ou `http.server` stdlib (preferir stdlib para zero deps novas). `/metrics` exporta `prometheus_client.generate_latest`. `/healthz` 200 sempre. `/readyz` checa: manifest reachable (SELECT 1), lock dir writable, source root exists; 503 se qualquer falhar. CLI flag `--obs-port=9100` em `agent run-forever` start server thread. Tests: endpoints respond, /readyz fails se manifest path bad.
- ⚪ **F7.11** — `feat(F7): structlog correlation ids (agent_run_id + batch_id)`. `bind_contextvars` em `loop.run_once` (agent_run_id) + `loop._process_batch` (batch_id). Toda linha de log dentro do escopo carrega ambos. Tests: capture log output → fields presentes em failure_detected, fix_applied, escalation, layer_completed.
- ⚪ **F7.12** — `feat(F7): logs/agent.jsonl rotation via RotatingFileHandler`. `AgentEventLogger` aceita `max_bytes` + `backup_count` (defaults 100 MB / 7 backups); rotation atomic (rename current → `.1`, `.1` → `.2`, etc); rotated files mantém JSONL valido (uma linha por record, sem split). Tests: write até cap → rotation triggers, backup_count respected, oldest dropped.

## Prometheus + Grafana stack (4.x)

- ⚪ **F7.13** — `feat(F7): docker-compose profile `monitoring` adds Prometheus + Grafana`. `compose.monitoring.yml` (extends base) com `prometheus:v2.55` + `grafana:11.3` + named volumes; Prometheus scrape config aponta para `agent:9100/metrics` (5s interval); Grafana provisioned datasource (Prometheus) + folder `Pipeline`. `make obs-up` shortcut.
- ⚪ **F7.14** — `feat(F7): Grafana dashboard JSON (Agent Overview)`. `deploy/grafana/dashboards/agent-overview.json` com 6 painéis: iter rate, lock gauge, escalations rate by layer/error_kind, layer duration p50/p95/p99, failure-to-fix ratio, top error_kinds (table). Provisioned via `dashboards.yaml`. Sem tests (config-only); validação manual em F7.19.

## Alerts (5.x)

- ⚪ **F7.15** — `feat(F7): Prometheus alert rules`. `deploy/prometheus/alerts.yml` com regras: `EscalationSpike` (rate(escalations_total[5m]) > 0.1), `LockHeldTooLong` (lock_held == 1 for 1h), `LoopStalled` (rate(iterations_total[10m]) == 0), `LayerLatencyHigh` (histogram_quantile(0.95) > 30s for 15m), `ManifestUnreachable` (probe_success{job="readyz"} == 0). Cada regra com `severity` + `runbook_url` annotation.
- ⚪ **F7.16** — `feat(F7): Alertmanager config + routing`. `deploy/alertmanager/config.yml` com receivers: `slack-critical`, `slack-warn`, `email-fallback`; routing tree por severity; inhibit rules (ManifestUnreachable inibe LoopStalled). Webhook URLs via env (`SLACK_WEBHOOK_CRITICAL`, etc); compose `monitoring` profile inclui Alertmanager service.
- ⚪ **F7.17** — `feat(F7): JSONL → webhook bridge for escalation events`. `src/pipeline/obs/escalation_bridge.py` tail `logs/agent.jsonl`, filter `event=="escalation"`, POST to webhook URL (env `ESCALATION_WEBHOOK_URL`); retry 3x exponencial; idempotência por `agent_run_id+batch_id+layer` em SQLite tracker (evita re-fire em restart). CLI subcommand `pipeline obs bridge`. Tests: 3 escalations → 3 POSTs; restart → no duplicate.
- ✅ **F7.18** — `docs(F7): runbooks per alert`. 5 runbooks shipped: escalation_spike, lock_held, loop_stalled, layer_latency, manifest_unreachable. Each: Symptom / Diagnosis / Mitigation / Verification / Postmortem-template. Linkable via runbook_url in F7.15 PromQL rules.

## Closeout (6.x)

- ⚪ **F7.19** — `test(F7): integration smoke via docker-compose`. Script `scripts/smoke_prod.sh`: `docker compose --profile monitoring up -d`, espera `/readyz` 200, dispara `pipeline agent run-once` em-container, scrape `/metrics`, valida 4 counters não-zero, `docker compose down -v`. Documenta tempo em `.specs/features/F7/SMOKE.md`.
- ⚪ **F7.20** — `docs(F7): production deployment guide`. `docs/production-deploy.md` cobre: pré-requisitos, build, env setup, first-run, monitoring URL, alert tuning, rollback, troubleshooting. Liga runbooks F7.18.
- ⚪ **F7.21** — Lane de review (code-reviewer + security-reviewer + critic) em `.specs/features/F7/REVIEWS/`. Bloqueia close se critical/high não-resolvidos.
- ⚪ **F7.22** — `docs(F7): close review lane and flip roadmap status`. Atualiza `ROADMAP.md` com F7 ✅ shipped + métricas (test count, smoke time, container size).

---

## Dependências internas

```
F7.1 → F7.2 → F7.5 → F7.6 → F7.7
F7.1 → F7.3 → F7.11
F7.8 → F7.9 → F7.10 → F7.13 → F7.14 → F7.15 → F7.16
F7.10 → F7.17
F7.{12} paralelo após F7.11
F7.18 paralelo após F7.15
F7.19 depende de F7.{6,10,13,16}
F7.20 depende de F7.{5,6,13,15,16,18}
F7.21 → F7.22 (sequencial closeout)
```

## Notas de escopo

- **Sem K8s.** Compose + systemd cobrem 95% dos casos; helm chart fica para F7-extension se necessário.
- **Stdlib HTTP server** preferido em F7.10 para não adicionar `aiohttp`/`fastapi` dep (NFR-05 análogo a F4).
- **prometheus-client** é a única dep nova obrigatória (~50KB, BSD-2).
- **Webhook bridge (F7.17)** é alternativa leve a Alertmanager para deploy minimal sem stack monitoring; ambos coexistem.
- **F5 overlap:** F5 cobre CLI watch + Gold dashboards públicos; F7 cobre infra/ops do agent. Sem duplicação.
