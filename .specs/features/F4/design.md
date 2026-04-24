# F4 — Design: Agent core (loop + auto-correção)

> Status: **draft** — segue `spec.md` assinado.
> Última atualização: 2026-04-24.

---

## 1. Module layout

```
src/pipeline/agent/
├── __init__.py          # public API: run_once, run_forever, AgentResult
├── loop.py              # orchestrator: planner -> executor -> reporter
├── observer.py          # detect new/failed batches from manifest + source
├── planner.py           # decide which (batch_id, layer) tuples need work
├── executor.py          # invoke layer entrypoints, wrap with retry
├── diagnoser.py         # classify exception -> ErrorKind, build Fix
├── escalator.py         # structured alert when budget exhausted
├── state.py             # agent_runs / agent_failures CRUD + lockfile
├── fixes/               # per-class deterministic fixes
│   ├── __init__.py
│   ├── schema_drift.py
│   ├── regex_break.py
│   ├── partition_missing.py
│   └── out_of_range.py
└── lock.py              # state/agent.lock with PID + stale detection

src/pipeline/cli/
└── agent.py             # `python -m pipeline agent {run-once,run-forever}`

scripts/
└── inject_fault.py      # demo: synthetic fault injection per ErrorKind

logs/
└── agent.jsonl          # append-only JSON event log (created on first run)
```

Public surface (re-exported from `pipeline.agent`):

```python
from pipeline.agent import run_once, run_forever, AgentResult, ErrorKind, Fix
```

---

## 2. Data flow

```
                ┌──────────────────────────────────────┐
                │        run_forever(interval)         │
                │   ┌────────────────────────────┐     │
                │   │       run_once()           │     │
                │   │                            │     │
                │   │  1. acquire lock           │     │
                │   │  2. start agent_run        │     │
                │   │  3. observer.scan()        │     │
                │   │       → pending_batches    │     │
                │   │  4. for batch_id:          │     │
                │   │       planner.plan()       │     │
                │   │         → [(layer, fn)]    │     │
                │   │       for (layer, fn):     │     │
                │   │         executor.run_with_recovery(...)
                │   │  5. close agent_run        │     │
                │   │  6. release lock           │     │
                │   └────────────────────────────┘     │
                │             │                        │
                │             ▼                        │
                │       sleep(interval)                │
                └──────────────────────────────────────┘
```

`run_once()` é o átomo testável. `run_forever()` é só o açoite.

---

## 3. Tipos / contratos internos

```python
# pipeline/agent/types.py
from enum import StrEnum
from dataclasses import dataclass

class ErrorKind(StrEnum):
    SCHEMA_DRIFT = "schema_drift"
    REGEX_BREAK = "regex_break"
    PARTITION_MISSING = "partition_missing"
    OUT_OF_RANGE = "out_of_range"
    UNKNOWN = "unknown"

class Layer(StrEnum):
    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"

@dataclass(frozen=True)
class Fix:
    kind: str                # "regenerate_regex", "recreate_partition", ...
    description: str
    apply: callable          # idempotent zero-arg function
    requires_llm: bool = False

@dataclass(frozen=True)
class FailureRecord:
    batch_id: str
    layer: Layer
    error_class: ErrorKind
    attempts: int
    last_fix_kind: str | None
    last_error_msg: str
    escalated: bool

@dataclass(frozen=True)
class AgentResult:
    agent_run_id: str
    batches_processed: int
    failures_recovered: int
    escalations: int
    status: str               # "COMPLETED" | "INTERRUPTED" | "FAILED"
```

---

## 4. Observer

```python
# pipeline/agent/observer.py
def scan(manifest, source) -> list[str]:
    """
    Returns list[batch_id] needing work.
    Pending if:
      - present in source AND
      - missing in runs OR runs.status IN ('FAILED', 'IN_PROGRESS' stale)
    """
```

- Source = parquet em `data/source/` (já gerenciado por F1).
- "Stale IN_PROGRESS" = `started_at < now() - 1h` (configurável). Limpa fantasmas de SIGKILL.
- Saída ordenada por `batch_id` lexicográfico para determinismo.

---

## 5. Planner

```python
# pipeline/agent/planner.py
def plan(batch_id, manifest) -> list[tuple[Layer, callable]]:
    """
    Sequential plan. Skips layers já COMPLETED.
    Order fixed: bronze -> silver -> gold.
    """
```

- Entrypoints invocados:
  - `pipeline.bronze.ingest.run(batch_id)`
  - `pipeline.silver.transform.run(batch_id)`
  - `pipeline.gold.transform.run(batch_id)`
- Cada entrypoint **deve** ser idempotente (já é, F1/F2/F3).

---

## 6. Executor + retry budget

```python
# pipeline/agent/executor.py
def run_with_recovery(layer, batch_id, fn, *, retry_budget=3) -> Outcome:
    attempt = 0
    while attempt < retry_budget:
        try:
            fn()
            return Outcome.COMPLETED
        except Exception as exc:
            attempt += 1
            kind = diagnoser.classify(exc, layer=layer, batch_id=batch_id)
            state.record_failure(batch_id, layer, kind, attempt, exc)
            if kind is ErrorKind.UNKNOWN:
                escalator.escalate(batch_id, layer, exc, kind)
                return Outcome.ESCALATED
            fix = diagnoser.build_fix(exc, kind, layer, batch_id)
            try:
                fix.apply()
                state.record_fix(batch_id, layer, kind, fix.kind)
            except Exception as fix_exc:
                state.record_failure(batch_id, layer, kind, attempt, fix_exc, fix_failed=True)
                # next iteration retries the layer; fix may have partial effect
    escalator.escalate(batch_id, layer, last_exc, kind)
    return Outcome.ESCALATED
```

Regras:

- Budget conta por `(batch_id, layer, error_class)`. Se Silver falhar com `regex_break` 3x, escalona; mas Gold do mesmo batch_id ainda roda.
- `fn()` é idempotente. Re-execução após `Fix` é seguro.
- `last_exc` é capturado para mensagem de escalação.

---

## 7. Diagnoser

Two-stage:

1. **Determinístico** — pattern match em `type(exc)` + `str(exc)`:
   - `polars.SchemaError` → `SCHEMA_DRIFT`
   - `pipeline.silver.regex.RegexMissError` → `REGEX_BREAK`
   - `FileNotFoundError` em path Bronze parquet → `PARTITION_MISSING`
   - `pipeline.silver.range.OutOfRangeError` → `OUT_OF_RANGE`
2. **LLM fallback** — se nada bate, chama `LLMClient.diagnose(error_ctx)` (PRD §18.3) com budget global de 10/run_once. LLM responde JSON com campos `kind, fix_kind, fix_payload`. Se `kind` inválido → `UNKNOWN` → escalação.

Diagnose budget:

```python
# pipeline/agent/diagnoser.py
class _DiagnoseBudget:
    def __init__(self, cap=10): self.cap = cap; self.used = 0
    def consume(self) -> bool:
        if self.used >= self.cap: return False
        self.used += 1; return True
```

Cap configurável via `AGENT_DIAGNOSE_BUDGET`.

---

## 8. Fixes (per-class)

### 8.1. `schema_drift`
- Detecta delta entre schema esperado (em `schemas/bronze.py`) e recebido.
- Fix: re-emite parquet Bronze com colunas extras dropadas / colunas faltantes preenchidas com NULL e cast forçado para schema canônico.
- Registra delta em `agent_failures.last_error_msg` (≤512 chars).

### 8.2. `regex_break`
- LLM regenera regex a partir de fixture mínima de mensagens-amostra do `batch_id`.
- Valida: roda regex em fixture canônica de Silver; se ≥1 match cai abaixo de baseline (`silver.regex_baseline.json`), Fix falha.
- Persiste novo regex em `state/regex_overrides.json` versionado por `batch_id`. F2 lê esse override antes de cair no default.

### 8.3. `partition_missing`
- Detecta path ausente.
- Fix: re-emite Bronze do `batch_id` (chama `pipeline.bronze.ingest.run(batch_id, force=True)`).
- Idempotente: se path existir após retry, no-op.

### 8.4. `out_of_range`
- Linha já em `bronze.rejected` (F1 quarentena).
- Fix: marca `runs.status='COMPLETED'` para a layer afetada com flag `had_quarantine=1`. Não tenta "consertar" valor; apenas registra que o batch teve quarentena e prossegue.
- Validação: `bronze.rejected` tem ≥1 linha do `batch_id`.

---

## 9. Escalator

```python
# pipeline/agent/escalator.py
def escalate(batch_id, layer, exc, kind):
    state.mark_escalated(batch_id, layer, kind)
    state.set_run_status(batch_id, layer, "FAILED")
    payload = {
        "event": "escalation",
        "batch_id": batch_id,
        "layer": layer.value,
        "error_class": kind.value,
        "last_error_msg": str(exc)[:512],
        "suggested_fix": _suggest(kind),  # human-readable
        "ts": utcnow_iso(),
    }
    log.json("logs/agent.jsonl", payload)
    log.stdout(f"[ESCALATION] batch={batch_id} layer={layer.value} kind={kind.value}")
```

Suggested-fix table (deterministic):

| `kind` | Suggested fix (operator) |
|---|---|
| `schema_drift` | `Verifique delta de schema em logs/agent.jsonl; ajuste schemas/bronze.py se delta for legítimo.` |
| `regex_break` | `Inspecione state/regex_overrides.json; rode pytest tests/unit/test_silver_regex.py.` |
| `partition_missing` | `Cheque permissões em data/bronze/; rode python -m pipeline ingest --batch-id <id>.` |
| `out_of_range` | `Inspecione bronze.rejected; ajuste range em pipeline.silver.range.` |
| `unknown` | `Anexe last_error_msg ao ticket; agent não classificou esta falha.` |

---

## 10. Lockfile (`state/agent.lock`)

```python
# pipeline/agent/lock.py
class AgentLock:
    def __init__(self, path="state/agent.lock", stale_after_s=3600):
        ...
    def acquire(self) -> None:
        # 1. if file missing -> write PID -> done
        # 2. if file exists, read PID:
        #    a. if process alive (os.kill(pid, 0)) -> raise AgentBusy
        #    b. if process dead -> overwrite (stale lock)
        # 3. if file exists but mtime > stale_after_s ago AND pid dead -> overwrite
    def release(self) -> None:
        # remove file if PID matches own
```

Acquired em `run_once` start; released em `finally`. Signal handlers (SIGINT/SIGTERM) chamam `release()` antes de exit 130.

---

## 11. State persistence

Migration nova em `manifest.py`:

```sql
CREATE TABLE IF NOT EXISTS agent_runs (
  agent_run_id TEXT PRIMARY KEY,
  started_at TEXT NOT NULL,
  ended_at TEXT,
  status TEXT NOT NULL,
  batches_processed INTEGER NOT NULL DEFAULT 0,
  failures_recovered INTEGER NOT NULL DEFAULT 0,
  escalations INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS agent_failures (
  failure_id TEXT PRIMARY KEY,
  agent_run_id TEXT NOT NULL,
  batch_id TEXT NOT NULL,
  layer TEXT NOT NULL,
  error_class TEXT NOT NULL,
  attempts INTEGER NOT NULL,
  last_fix_kind TEXT,
  escalated INTEGER NOT NULL DEFAULT 0,
  last_error_msg TEXT,
  ts TEXT NOT NULL,
  FOREIGN KEY (agent_run_id) REFERENCES agent_runs(agent_run_id)
);
CREATE INDEX IF NOT EXISTS ix_agent_failures_run ON agent_failures(agent_run_id);
CREATE INDEX IF NOT EXISTS ix_agent_failures_kind ON agent_failures(batch_id, layer, error_class);
```

`state.py` expõe: `start_run()`, `end_run(status)`, `record_failure(...)`, `record_fix(...)`, `mark_escalated(...)`, `count_attempts(batch_id, layer, error_class) -> int`.

---

## 12. CLI

```python
# pipeline/cli/agent.py
@click.group("agent")
def agent_cli(): ...

@agent_cli.command("run-once")
@click.option("--retry-budget", default=3, type=int)
@click.option("--diagnose-budget", default=10, type=int)
def run_once_cmd(retry_budget, diagnose_budget):
    res: AgentResult = run_once(retry_budget=retry_budget, diagnose_budget=diagnose_budget)
    click.echo(json.dumps(asdict(res)))
    sys.exit(0 if res.status == "COMPLETED" else 1)

@agent_cli.command("run-forever")
@click.option("--interval", default=60, type=int)
@click.option("--max-iters", default=None, type=int)
def run_forever_cmd(interval, max_iters):
    run_forever(interval=interval, max_iters=max_iters)
```

Env vars sobrescrevem flags: `AGENT_RETRY_BUDGET`, `AGENT_DIAGNOSE_BUDGET`, `AGENT_LOOP_INTERVAL`, `AGENT_LOCK_PATH`.

Wired em `pipeline/cli/__init__.py` ao lado de `ingest`, `transform-silver`, `gold`.

---

## 13. Logging

`structlog` JSON renderer. Sink: `logs/agent.jsonl` (append) + stdout (linha curta).

Eventos canônicos:

| event | campos extras |
|---|---|
| `loop_started` | `interval`, `max_iters` |
| `loop_iteration` | `iter`, `pending_count` |
| `batch_started` | `batch_id` |
| `layer_started` | `batch_id`, `layer` |
| `layer_completed` | `batch_id`, `layer`, `duration_ms` |
| `failure_detected` | `batch_id`, `layer`, `error_class`, `attempt` |
| `fix_applied` | `batch_id`, `layer`, `fix_kind` |
| `escalation` | `batch_id`, `layer`, `error_class`, `last_error_msg`, `suggested_fix` |
| `loop_stopped` | `reason` (`completed`/`interrupted`/`max_iters`) |

Determinismo: `ts` mockável via `pipeline.clock`. Em testes, clock fixo → log byte-idêntico.

---

## 14. Demo de injeção (`scripts/inject_fault.py`)

```bash
python scripts/inject_fault.py --kind schema_drift --batch-id demo01
python -m pipeline agent run-once
# expected: log mostra schema_drift detectado, fix_applied, layer_completed
```

Kinds suportados:
- `schema_drift` — escreve parquet Bronze com coluna extra `injected_col`.
- `regex_break` — substitui amostra de mensagem por formato novo "❌ R$ 1.500,00".
- `partition_missing` — deleta `data/bronze/<batch_id>/`.
- `out_of_range` — injeta linha com `valor_pago_atual_brl=-999`.

Cada kind tem fixture isolada em `tests/fixtures/agent/<kind>/`.

---

## 15. Testing strategy

- **Unit**:
  - `test_observer.py` — pending detection, stale IN_PROGRESS cleanup.
  - `test_planner.py` — skip COMPLETED, order bronze→silver→gold.
  - `test_diagnoser.py` — pattern match por exceção; LLM fallback mockado.
  - `test_executor.py` — retry budget exhaustion; fix success/failure paths.
  - `test_escalator.py` — log JSON shape, manifest update.
  - `test_lock.py` — acquire/release, stale detection, concurrent attempt raises.
  - `test_state.py` — CRUD + idempotência.
  - 1 test file por `fixes/<kind>.py` — golden in/out.
- **Integration**:
  - `test_agent_run_once_clean.py` — no-op em pipeline COMPLETED.
  - `test_agent_run_once_full.py` — Bronze→Gold from scratch.
  - `test_agent_per_kind.py` — parametrize por `ErrorKind`, injeta falha, verifica fix + log.
  - `test_agent_budget_exhausted.py` — força 4 falhas → 1 escalação.
  - `test_agent_run_forever_max_iters.py` — `max_iters=2`, valida 2 iterações.
- **Property** (Hypothesis):
  - `count_attempts` é monotônico não-decrescente por `(batch_id, layer, error_class)`.

Coverage gate: ≥90% em `src/pipeline/agent/**` (NFR-03).

---

## 16. ADRs derivados

### ADR-007 — Diagnoser two-stage (deterministic-first, LLM-fallback)
- **Decisão:** classificação por pattern match em type(exc) primeiro; LLM só quando determinístico falha.
- **Por quê:** custo + latência. 90%+ dos casos transientes têm exceção tipada.
- **Trade-off:** pattern match precisa cobrir tipos de exceção do F1/F2/F3. Refatorações em layers têm que atualizar diagnoser. Aceitável: contrato explícito.

### ADR-008 — Lockfile filesystem (não SQLite advisory lock)
- **Decisão:** `state/agent.lock` com PID + mtime. Não usar SQLite `BEGIN EXCLUSIVE`.
- **Por quê:** SIGKILL de processo Python solta lock SQLite só após restart de DB; arquivo + PID-check é mais robusto e debuggável.
- **Trade-off:** requer cleanup em SIGINT/SIGTERM handlers. Aceitável.

### ADR-009 — `regex_break` persistido como override versionado
- **Decisão:** novo regex regenerado por LLM vai para `state/regex_overrides.json` indexado por `batch_id`, não substitui o regex hardcoded em F2.
- **Por quê:** rastreabilidade + reversibilidade. Operador pode descartar override se LLM regenerou regex ruim.
- **Trade-off:** F2 precisa ler override. Cria acoplamento; mas é único hook agêntico em F2.

---

## 17. Open items resolvidos durante design

| # | Item | Resolução |
|---|---|---|
| O1 | F1 expõe API para requeue de `bronze.rejected`? | Verificado: F1 tem `pipeline.bronze.rejected` mas sem `requeue()`. F4.OUT_OF_RANGE não requeue; apenas marca COMPLETED com `had_quarantine=1`. F5 pode adicionar requeue se necessário. |
| O2 | Como F2 lê regex override? | Adicionar `pipeline.silver.regex.load_override(batch_id)` em F2 (mudança trivial, escopo F4). |
| O3 | `time.sleep(interval)` é cancelável por SIGINT? | Sim em CPython sob a maioria dos sinais; usar `threading.Event().wait(interval)` para garantir cancelamento limpo. |
| O4 | Logs JSON rotacionam? | Não em F4. Append-only. F5 trata rotação se necessário. |

---

## 18. Próximos passos

1. **Tasks.md** com decomposição atômica (12–18 tasks estimadas).
2. Implementação em ordem: `state.py` → `lock.py` → `observer.py` → `planner.py` → `diagnoser.py` (deterministic) → `fixes/*` → `executor.py` → `escalator.py` → `loop.py` → CLI → `inject_fault.py`.
3. Lane de review (code-reviewer + security-reviewer + critic) ao fim, mesmo padrão M2.
