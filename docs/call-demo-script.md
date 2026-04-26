# Monday Call — Live Demo Script

The exact click-by-click plan for the screen-share. Designed for a 30-minute call with ~15 minutes of demo + Q&A. Adjust pacing to the audience.

Print this. Or pin it on a second monitor. Do not improvise the order — improvising during a demo is when things break.

---

## Pre-flight checklist (do 30 minutes BEFORE the call)

- [ ] Pull latest: `git pull origin main` (verify F5 commits are visible).
- [ ] `.env` populated with `DASHSCOPE_API_KEY`. Sanity-test:
  ```bash
  uv run python -c "from pipeline.config import Settings; print('key prefix:', Settings().DASHSCOPE_API_KEY[:6])"
  ```
- [ ] `data/raw/conversations_bronze.parquet` exists. Sanity-test:
  ```bash
  uv run python -c "import polars as pl; print(pl.read_parquet('data/raw/conversations_bronze.parquet').height, 'rows')"
  ```
- [ ] State + bronze + silver + gold dirs from previous runs cleared so the demo runs from a clean slate. Wipe with:
  ```bash
  rm -rf data/bronze data/silver data/gold state/manifest.db logs/*.jsonl
  mkdir -p data/bronze data/silver data/gold state logs
  ```
- [ ] All tests pass: `uv run pytest tests/unit/ -k gold --no-cov -q` (expect 292 green).
- [ ] Lint + types clean: `uv run ruff check src/pipeline/gold/ src/pipeline/schemas/ && uv run mypy src/pipeline/gold/ src/pipeline/schemas/`.
- [ ] Open these tabs in your editor in this exact order, ready to switch:
  1. `Teste Técnico de Data & AI Engineering.md`
  2. `src/pipeline/__main__.py`
  3. `src/pipeline/cli/agent.py`
  4. `src/pipeline/agent/loop.py`
  5. `src/pipeline/gold/persona.py`
  6. `src/pipeline/gold/sentiment.py`
  7. `src/pipeline/gold/transform.py`
  8. `src/pipeline/silver/pii.py`
  9. `tests/unit/test_gold_sentiment.py`
- [ ] Open one terminal in the repo root. Make the font big.
- [ ] Close Slack/Discord/notifications. Mute phone.

---

## Stage 1 — 60-second pitch (no screen)

Read the **Quick Pitch** from `docs/learn-design-decisions.md` Part 1. Eye contact, no notes.

Aim for 60-75 seconds. End on:

> "I can show you the code, the data, and a live recovery if you want."

Wait. Let them ask.

---

## Stage 2 — Show the spec is met (3 minutes)

**Goal**: prove every mandatory requirement is satisfied. Frame it as a checklist tour.

**Tab to**: `Teste Técnico de Data & AI Engineering.md`

Read these lines aloud in order, then jump to the file that satisfies each:

| Spec line | Tab to |
|---|---|
| "Pipeline em código Python" | `src/pipeline/__main__.py` — point at the Click groups |
| "AI Agent: o agente cria E gerencia" | `src/pipeline/agent/loop.py:59` — show `run_once` |
| "Pipeline vivo: atualização automática" | `src/pipeline/agent/loop.py:174` — show `run_forever` with `interval=60.0` |
| "3 camadas Bronze → Silver → Gold" | `src/pipeline/` tree — point at `ingest/`, `silver/`, `gold/` |
| "Camada Bronze: dados não estruturados" | `src/pipeline/ingest/ingest.py` |
| "Camada Silver: limpos, mascarados" | `src/pipeline/silver/pii.py:99` (`_mask_email`), `:111` (`_mask_cpf`), `:130` (`_mask_phone`) |
| "Camada Gold: classificações, audiências" | `src/pipeline/gold/persona.py` + `gold/sentiment.py` + `gold/lead_profile.py` |
| "Auto-correção em caso de falha" | `src/pipeline/agent/executor.py:run_with_recovery` — explain classify → fix → retry → escalate |
| "Repositório público no GitHub" | terminal: `git remote -v` |

Talking-line: "Every mandatory requirement maps to a specific file. Differentials live in `gold/`, `silver/`, and the agent loop."

If asked which differentials you skipped: **Databricks** (optional, time trade-off — see `docs/learn-design-decisions.md` Part 9).

---

## Stage 3 — Walk the call chain (3 minutes)

**Goal**: show you understand the orchestration end-to-end.

Use `docs/learn-agent-flow.md` as your script. The order:

1. `__main__.py:50` — entry point, Click `cli()` group.
2. `cli/agent.py:171` `run_once_cmd` — setup + dispatch.
3. `agent/loop.py:59` `run_once` — observer → planner → executor.
4. `agent/observer.py` — discovers pending batches.
5. `agent/planner.py` — orders layers per batch.
6. `agent/executor.py` — try / classify / fix / retry / escalate (max 3 attempts).
7. `gold/transform.py:transform_gold` — orchestrates 4 parquet builders.
8. `gold/persona.py:classify_with_overrides` — hard rules → LLM fallback.
9. `gold/sentiment.py:evaluate_sentiment_rules` — SR1/SR2/SR3 priority.

Talking-line: "Dependency injection means the loop never hard-codes its collaborators — tests inject fakes, production injects real implementations. Same code path."

---

## Stage 4 — Live run on real data (4 minutes)

**Goal**: demonstrate it works end-to-end on the real parquet.

```bash
# 1. confirm clean slate
ls data/bronze data/silver data/gold 2>/dev/null
ls state/manifest.db 2>/dev/null

# 2. ingest the source (Bronze)
uv run python -m pipeline ingest run --source data/raw/conversations_bronze.parquet
```

Talking-line: "Bronze is bit-identical to source — that is the audit trail."

```bash
# 3. ls the bronze partition we just wrote
ls data/bronze/
```

```bash
# 4. silver pass — clean + mask
uv run python -m pipeline silver run-once
```

Talking-line while it runs: "Silver does dedup, normalize, extract, and PII mask. About 153K rows in this dataset."

```bash
# 5. gold pass — analytics + LLM persona/sentiment
uv run python -m pipeline gold run-once
```

Talking-line: "This is the only stage that pays for LLM calls. Half the leads hit hard rules and skip the LLM entirely. The combined prompt returns persona AND sentiment in one round-trip."

```bash
# 6. show the result for GABRIELA
uv run python <<'PY'
import polars as pl, glob
parquets = sorted(glob.glob('data/gold/lead_profile/batch_id=*/*.parquet'))
df = pl.concat([pl.read_parquet(p) for p in parquets])
print(df.filter(pl.col('sender_name_normalized') == 'gabriela barbosa').select(
    'lead_id', 'persona', 'persona_source', 'sentiment', 'sentiment_source',
    'engagement_profile', 'intent_score'
))
PY
```

Talking-line: "Here is GABRIELA from `conv_00000001`. She mentioned HDI Seguros and didn't close. Persona is `refem_de_concorrente` (LLM), sentiment is `negativo` from a hard rule (SR3 — competitor mention without close). Engagement bucket `warm`."

If GABRIELA is not in the partition you happened to write, run on the full source first (don't slice during a demo).

---

## Stage 5 — Show self-healing (3 minutes)

**Goal**: prove the agent recovers from a real-world failure mode.

```bash
# 1. corrupt a Silver parquet to simulate a partial write
ls data/silver/batch_id=*/part-*.parquet | head -1 | xargs -I {} mv {} {}.broken
ls data/silver/batch_id=*/

# 2. run the agent loop — it should detect missing file, classify, fix, retry
uv run python -m pipeline agent run-once 2>&1 | tail -20
```

Talking-line: "The executor catches the `FileNotFoundError`, the diagnoser classifies it as `MISSING_FILE`, the fix builder returns a no-op (we just retry the runner) — third attempt re-runs the whole Silver pass cleanly."

If the JSON output shows `failures_recovered: 1`, point at it. Talking-line: "Loud accounting. Silent recovery would be the worst outcome."

```bash
# 3. inspect logs for structured events
grep "failures_recovered\|escalated\|layer_completed" logs/*.jsonl | tail -10
```

Talking-line: "Every state change is a structured event. structlog with secret redaction. Production drains this to Loki/Datadog."

---

## Stage 6 — Show the F5 sentiment work (2 minutes)

**Goal**: signal you understand iteration / feature lanes.

```bash
git log --oneline -3
```

Show the three F5 commits.

**Tab to**: `.specs/features/F5/DESIGN.md` — read out the goal paragraph.

**Tab to**: `src/pipeline/gold/sentiment.py:70` — point at SR1/SR2/SR3 rule predicates.

**Tab to**: `src/pipeline/gold/persona.py:333` — show the modified `SYSTEM_PROMPT` requesting JSON.

**Tab to**: `tests/unit/test_gold_sentiment.py` — scroll fast, mention "25 unit tests, 22 covering hard rules and parser edge cases".

```bash
uv run pytest tests/unit/test_gold_sentiment.py --no-cov -q
```

Talking-line: "Combined prompt halves the LLM cost vs running two separate calls. The cache key includes the system prompt text, so bumping `PROMPT_VERSION_PERSONA` from v2 to v3 invalidates everything — first batch after deploy is a one-time cost spike I documented in the backfill runbook."

---

## Stage 7 — Q&A buffer (5+ minutes)

Use answers from `docs/learn-design-decisions.md`. The most likely questions are pre-rehearsed there.

If you do not know an answer:

> "I don't have that committed to memory. The relevant file is `<best guess>`. Want me to open it and we can reason about it together?"

Honest > confident-wrong. Always.

---

## Demo failure modes — what to do live

| Failure | Quick recovery |
|---|---|
| `pipeline agent run-once` hangs > 30s | Hit Ctrl-C. The lock will release. Talk through what would happen normally; pivot to showing a unit test instead. |
| LLM 429 / timeout | "DashScope is rate-limited from this IP — that is exactly why we have the retry budget. Let me show the test that simulates this." → run `pytest tests/unit/test_gold_persona_llm.py::test_llm_call_error_returns_skipped_sentinel`. |
| `DASHSCOPE_API_KEY` missing | Friendly UsageError prints. "This is the F7.2 friendly-fail path. Let me show that path in `cli/agent.py:_load_runtime_settings`." |
| Schema drift in source parquet | Run `pytest tests/unit/test_schemas_gold.py` to show the drift guard in action. |
| Tests are red | Show the failure, talk through what the test expected. Honest is better than panicked. |

The general rule: **a live failure is a teaching moment, not a disaster**. Every error class has a story you can tell from the docs.

---

## Closer (last 60 seconds)

End with a pivot to next steps:

> "If I had another week I'd close the observability triad with Prometheus metrics, add jittered exponential backoff in the executor, and write the prompt-version migration helper that re-uses cached user prompts under a new system prefix. Architecture-wise, the medallion contract and the port-and-adapter shape mean none of those changes touch the loop or the transforms."

Stop. Hand back to them.
