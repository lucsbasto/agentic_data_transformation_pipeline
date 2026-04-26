# Walkthrough by Example — One Conversation Through the Whole Pipeline

This is a companion to `docs/learn-agent-flow.md`. Same factory metaphor, same call chain, same files — but this time we **follow one real conversation** from `data/raw/conversations_bronze.parquet` end to end. Every value below is taken from the actual file.

The conversation we follow: **`conv_00000001`**, 22 messages, between agent **Diego Pereira** and lead **GABRIELA BARBOSA**, on **2026-02-14**. She came from a Google Ads landing page, gave her CPF and plate, mentioned a competitor (HDI Seguros), and the conversation ended `perdido_concorrente`.

By the end of this doc you will see exactly what each layer does to her row.

---

## Part 0 — The raw data

The source file is one parquet with 153,228 rows and 14 columns:

| column | dtype | example |
|---|---|---|
| `message_id` | String | `b84f0026ebe1` |
| `conversation_id` | String | `conv_00000001` |
| `timestamp` | String | `2026-02-14 18:10:20` |
| `direction` | String | `outbound` / `inbound` |
| `sender_phone` | String | `+5531985764309` |
| `sender_name` | String | `Diego Pereira` (agent) / `GABRIELA BARBOSA` (lead) |
| `message_type` | String | `text` |
| `message_body` | String | the actual chat message |
| `status` | String | `delivered` |
| `channel` | String | `whatsapp` |
| `campaign_id` | String | `camp_landing_fev2026` |
| `agent_id` | String | `agent_diego_14` |
| `conversation_outcome` | String | `perdido_concorrente`, `venda_fechada`, `nao_fechou`, `ghosting`, … |
| `metadata` | String (JSON) | `{"device": "iphone", "city": "Sao Paulo", "state": "SP", "response_time_sec": 82, "is_business_hours": false, "lead_source": "google_ads"}` |

Note that `metadata` is a JSON string at the source — Silver parses it into proper struct fields.

Here is the full conversation we will follow (timestamps shortened to HH:MM:SS):

```
[18:10:20] outbound Diego: oi GABRIELA BARBOSA, tudo bem? vi que vc se cadastrou no nosso site pra cotacao de seguro auto.
[18:11:43] inbound  GABRIELA: oi, sim! meu carro ta sem seguro, quero resolver isso
[18:12:23] outbound Diego: beleza GABRIELA! pra eu fazer a cotacao, preciso dos dados do veiculo. qual a marca, modelo e ano?
[18:13:15] inbound  GABRIELA: Onix ano 2015 cor prata, placa SYL8V26
[18:13:42] outbound Diego: otimo! me passa seu CPF, CEP de onde o carro fica e seu email
[18:23:22] inbound  GABRIELA: [audio transcrito] a placa do carro eh SYL8V26, eh um Onix 2015, ta no meu nome mesmo, o cpf eh 418.696.561-30
[18:24:23] outbound Diego: GABRIELA, ta ai a cotacao: Plano Premium: R$ 5749/ano
[18:28:35] inbound  GABRIELA: hmm nao sei nao, HDI Seguros me ofereceu por R$ 1903
[18:33:23] outbound Diego: vou falar com meu gerente pra ver se consigo um desconto. da um minuto
[18:36:00] outbound Diego: e ai GABRIELA, vamos fechar? to com uma condicao boa ainda
[18:38:39] inbound  GABRIELA: oi, antes de mais nada, quais coberturas vcs oferecem?
[18:40:29] outbound Diego: [audio transcrito] entao eu moro em sao paulo zona sul...
[18:42:26] inbound  GABRIELA: carro reserva so 7 dias? preciso de pelo menos 15
[18:47:30] outbound Diego: oi GABRIELA! a proposta ainda ta valida, quer dar uma olhada?
[18:55:44] inbound  GABRIELA: vcs fazem parcelamento? em quantas vezes?
[18:57:43] outbound Diego: GABRIELA, consegui uma condicao especial! R$ 5749/ano, mas so ate amanha
[18:59:58] inbound  GABRIELA: mas nao tem cobertura pra enchente? moro em area de risco
[19:01:04] outbound Diego: e ai GABRIELA, vamos fechar? to com uma condicao boa ainda
[19:03:03] inbound  GABRIELA: qual a diferenca entre os planos de vcs?
[19:07:30] inbound  GABRIELA: vou pensar melhor, qualquer coisa eu te chamo
```

22 messages total. 12 inbound (lead). 10 outbound (agent). Final outcome: `perdido_concorrente`. The lead never confirmed a close, and we know HDI Seguros undercut the price.

Hold this conversation in your head. The rest of the doc walks her data through each layer.

---

## Part 1 — The Big Picture (factory walk for GABRIELA)

| Room | Real name | What happens to her row |
|---|---|---|
| Room 1 — Sorting | **Bronze** | Diego + GABRIELA's 22 messages dumped in as-is. No cleaning. The agent records "we received batch X with 22 messages for conv_00000001". |
| Room 2 — Cleaning | **Silver** | CPF `418.696.561-30` → `***.***.***-**`. Phone `+5531983113950` → `+55 (**) *****-50`. Plate `SYL8V26` extracted into its own column. Competitor `HDI Seguros` extracted into `concorrente_mencionado`. Vehicle parsed: `Onix` / `Chevrolet` / `2015`. Metadata JSON unpacked into struct fields. The 22 message rows reconciled into a single `lead_id` aggregation. |
| Room 3 — Toy-Making | **Gold** | One row per conversation in `conversation_scores` (her count: 22 messages, duration ~1h, mentioned competitor=True). One row per lead in `lead_profile` with engagement bucket, **persona**, **sentiment**, intent score. |

The robot foreman (`pipeline agent run-once`) does this walk for every batch in `data/raw/`, including her batch.

Now we follow the foreman, with her row as the running example.

---

## Part 2 — File-by-File Walk (with GABRIELA's row tracked)

### Step 1. The doorbell — `src/pipeline/__main__.py`

You typed:

```bash
uv run python -m pipeline agent run-once
```

Click resolves the subcommand `agent run-once`. **GABRIELA's data is not yet involved.** We are still in routing.

### Step 2. The CLI surface — `src/pipeline/cli/agent.py`

`run_once_cmd` runs setup:

- Loads `.env` → finds `DASHSCOPE_API_KEY` (needed because Gold will call the LLM).
- Reads paths: `data/raw/`, `data/bronze/`, `data/silver/`, `data/gold/`, `state/manifest.db`.
- Builds the three runners (Bronze / Silver / Gold) — each a callable that knows *how* to transform one batch through one layer.
- Builds error-recovery helpers: classifier, fix builder, escalator, lock.

**GABRIELA's data still untouched.** This step prepares the tools the foreman will carry.

### Step 3. The orchestrator — `src/pipeline/agent/loop.py`

`run_once(...)` enters its main work:

1. Acquires the filesystem lock so two agents cannot collide.
2. Opens an `agent_run` row in the manifest.
3. Calls `agent/observer.py:scan(...)` — this is when GABRIELA shows up.

#### Observer pulls her batch into focus

Observer walks `data/raw/`. The whole 153,228-row file is presented as one batch. The agent splits it into smaller batches based on partitioning configuration; for this walkthrough assume GABRIELA's conv_00000001 lands in batch `044f334e976a` (this is what is currently sitting in `data/bronze/batch_id=044f334e976a/part-0.parquet`).

Observer queries the manifest:

```sql
SELECT bronze_status, silver_status, gold_status FROM batch_manifest WHERE batch_id = '044f334e976a'
```

Three possibilities:
- All three are `COMPLETED` → skipped (nothing to do).
- Some `PENDING` or `FAILED` → returned in the pending list.
- Source file no longer present on disk → flagged for cleanup.

Assume our batch is fresh: bronze=PENDING, silver=PENDING, gold=PENDING.

#### Planner decides her order

`agent/planner.py:plan(batch_id="044f334e976a", ...)` returns:

```python
[(Layer.BRONZE, bronze_runner),
 (Layer.SILVER, silver_runner),
 (Layer.GOLD,   gold_runner)]
```

(If Bronze were already COMPLETED it would skip to `[(SILVER, …), (GOLD, …)]`. The planner is the part that lets re-runs be cheap.)

#### Executor picks up the first step

For each (layer, runner) pair, the executor wraps the call in a try/except. If anything raises, it classifies, fixes, retries — up to 3 attempts.

We start with **Bronze**.

---

### Step 4a. Bronze runner — `src/pipeline/ingest/ingest.py`

The Bronze runner reads `data/raw/conversations_bronze.parquet` (or the per-batch slice), hashes its contents, writes the parquet into `data/bronze/batch_id=044f334e976a/part-0.parquet`, and updates the manifest row:

```sql
UPDATE batch_manifest SET bronze_status = 'COMPLETED', bronze_rows = 22, bronze_hash = 'sha256:...' WHERE batch_id = '044f334e976a'
```

GABRIELA's 22 rows are now in Bronze, **byte-identical to source**. No cleaning. The whole Bronze layer is the audit trail — if Silver later misclassifies something, we can re-derive from this exact snapshot.

**One row in Bronze (sample)**:

```
message_id:           b84f0026ebe1
conversation_id:      conv_00000001
timestamp:            2026-02-14 18:10:20
direction:            outbound
sender_phone:         +5531985764309
sender_name:          Diego Pereira
message_body:         oi GABRIELA BARBOSA, tudo bem? vi que vc se cadastrou…
conversation_outcome: perdido_concorrente
metadata:             {"device": "desktop", "city": "Sao Paulo", "state": "SP", "response_time_sec": null, "is_business_hours": false, "lead_source": "google_ads"}
```

`metadata` is still the raw JSON **string** at this layer.

---

### Step 4b. Silver runner — `src/pipeline/silver/transform.py`

Now the cleaning. Silver chains several modules. Watch what happens to her row:

#### `silver/dedup.py` — collapse duplicates

The Bronze parquet contained two duplicate audio transcript rows for `[18:23:22]` and `[18:28:35]` (they appear twice in the source). Dedup keeps one. Silver row count: 22 → 20.

#### `silver/normalize.py` — fix types and casing

- `timestamp` (String) → `pl.Datetime("us", time_zone="UTC")`. `2026-02-14 18:10:20` → `2026-02-14T18:10:20+00:00`.
- `sender_name` normalized: `GABRIELA BARBOSA` → `gabriela barbosa` lowercased for joining and matching.
- `metadata` JSON string parsed into a Polars Struct: now `metadata.device == "iphone"`, `metadata.state == "SP"`, etc.

#### `silver/extract.py` — pull facts out of free text

Regex passes over `message_body`. From GABRIELA's audio transcript message:

> `[audio transcrito] a placa do carro eh SYL8V26, eh um Onix 2015, ta no meu nome mesmo, o cpf eh 418.696.561-30`

- `plate_format` ← `SYL8V26` (extracted into its own column).
- `has_cpf` ← `True`.
- `veiculo_marca` ← `Chevrolet` (Onix → marca lookup).
- `veiculo_modelo` ← `Onix`.
- `veiculo_ano` ← `2015`.

From the competitor message:

> `hmm nao sei nao, HDI Seguros me ofereceu por R$ 1903`

- `concorrente_mencionado` ← `HDI Seguros`.
- `valor_pago_atual_brl` ← `1903.0`.

The `email_domain` column stays NULL on this conversation because GABRIELA never typed an email. Compare a different row from the file:

> `meu cpf eh 383.182.856-05, cep 08617-986 e email joao.santos@yahoo.com.br`

Here `email_domain` ← `yahoo.com.br`, `cep_prefix` ← `086`.

#### `silver/llm_extract.py` — the safety net

When regex fails on a message that *should* have an entity (e.g. a phone number written as `tres um nove oito tres…` in Portuguese words), `silver/llm_extract.py` calls the LLM with a strict extraction prompt. The result is cached so repeats cost nothing. GABRIELA's messages are all regex-friendly, so this module is a no-op for her batch.

#### `silver/pii.py` — mask sensitive fields

The actual masking transforms are length-preserving:

| Source | Masked |
|---|---|
| `+5531983113950` | `+55 (**) *****-50` (last 2 digits kept) |
| `gabriela barbosa` | preserved as `sender_name_normalized` (used for analytics; raw `sender_name` removed) |
| `418.696.561-30` | `***.***.***-**` (fixed template) |
| `joao.santos@yahoo.com.br` | `j****@y****.com.br` (first letter + first letter of host + full TLD) |
| `08617-986` | `*****-***` (fixed template) |

`message_body` is rewritten in place: every PII match in the message is replaced with its mask before the column is renamed `message_body_masked`. The unmasked version is dropped.

After this step, GABRIELA's audio-transcript row reads:

> `[audio transcrito] a placa do carro eh SYL8V26, eh um Onix 2015, ta no meu nome mesmo, o cpf eh ***.***.***-**`

Same length as the original, analytics still work, but the CPF is gone.

#### `silver/reconcile.py` — group by lead

Silver introduces a `lead_id` column derived from `(sender_phone_masked, conversation_id)` and reconciliation logic. GABRIELA's 20 deduped rows now share the same `lead_id`. This is the join key Gold uses next.

#### `silver/writer.py` — atomic write

Schema validated against `SILVER_SCHEMA` (defined in `src/pipeline/schemas/silver.py`). Then written to:

```
data/silver/batch_id=044f334e976a/part-0.parquet.tmp
                                   ↓ os.rename
data/silver/batch_id=044f334e976a/part-0.parquet
```

Manifest row flipped: `silver_status = 'COMPLETED', silver_rows = 20`.

**Silver output (one of GABRIELA's rows)** — schema-shaped, masked, typed:

```
message_id:                b84f0026ebe1
conversation_id:           conv_00000001
lead_id:                   lead_a8f329…
timestamp:                 2026-02-14T18:10:20+00:00
direction:                 outbound
sender_phone_masked:       +55 (**) *****-09
sender_name_normalized:    diego pereira
message_body_masked:       oi GABRIELA BARBOSA, tudo bem? vi que vc se cadastrou…
has_cpf:                   false
has_phone_mention:         false
email_domain:              null
plate_format:              null
veiculo_marca:             null
veiculo_modelo:            null
veiculo_ano:               null
concorrente_mencionado:    null
metadata:                  {device: "desktop", city: "Sao Paulo", state: "SP", …}
conversation_outcome:      perdido_concorrente
```

(Other rows in the same conversation carry the CPF / plate / vehicle / competitor fields — this is the agent-greeting row, which is short.)

---

### Step 4c. Gold runner — `src/pipeline/cli/gold.py` → `gold/transform.py:transform_gold`

Now the analytics. The same Silver LazyFrame feeds four parquet builders + one insights JSON. We focus on what happens to **GABRIELA the lead**, since `lead_profile` is where persona and sentiment land.

#### Stage 1 — `gold/conversation_scores.py`

For `conv_00000001`:

```
conversation_id:           conv_00000001
lead_id:                   lead_a8f329…
agent_id:                  agent_diego_14
campaign_id:               camp_landing_fev2026
msgs_inbound:              12
msgs_outbound:             10  (dedup removed two duplicates from inbound side; pre-dedup was 12/10)
first_message_at:          2026-02-14T18:10:20+00:00
last_message_at:           2026-02-14T19:07:30+00:00
duration_sec:              3430
avg_lead_response_sec:     ~152
time_to_first_response_sec: 83
off_hours_msgs:            22 (after-hours batch)
mencionou_concorrente:     true
concorrente_citado:        HDI Seguros
veiculo_marca:             Chevrolet
veiculo_modelo:            Onix
veiculo_ano:               2015
valor_pago_atual_brl:      1903.0
sinistro_historico:        false
conversation_outcome:      perdido_concorrente
```

(Numbers are illustrative — the exact values depend on what Silver's per-row columns produce after dedup.)

#### Stage 2 — `gold/lead_profile.py:build_lead_profile_skeleton`

One row per `lead_id`. For GABRIELA:

- `conversations`: 1 (only conv_00000001).
- `closed_count`: 0 (outcome is `perdido_concorrente`, not `venda_fechada`).
- `close_rate`: 0.0.
- `dominant_email_domain`: NULL (she never typed an email).
- `dominant_state`: `SP`.
- `dominant_city`: `Sao Paulo`.
- `engagement_profile` (computed in `_add_engagement_profile`): close_rate < 0.5 AND conversations < 3 AND not stale → `warm`.
- `persona`: NULL placeholder (typed null, will be filled by classifier).
- `sentiment`: NULL placeholder (filled later).
- `intent_score`: NULL placeholder.

#### Stage 3 — `gold/persona.py:aggregate_leads`

Builds a `LeadAggregate` for GABRIELA from her Silver rows:

```python
LeadAggregate(
    lead_id="lead_a8f329…",
    num_msgs=20,                 # post-dedup
    num_msgs_inbound=10,
    num_msgs_outbound=10,
    outcome="perdido_concorrente",
    mencionou_concorrente=True,
    competitor_count_distinct=1,
    forneceu_dado_pessoal=True,  # has_cpf=True at least once
    last_message_at=datetime(2026, 2, 14, 19, 7, 30, tzinfo=UTC),
    conversation_text="oi, sim! meu carro ta sem seguro…\nOnix ano 2015 cor prata, placa SYL8V26\n…",  # last 20 inbound msgs concatenated, capped at 2000 chars
)
```

#### Stage 4 — `gold/persona.py:classify_with_overrides` (her decision tree)

Two parallel checks fire:

**Persona hard rules** (`evaluate_rules`):
- R2 (fast close): `outcome == "venda_fechada" AND num_msgs <= 10` → False (no close, 20 msgs).
- R1 (bouncer): `num_msgs <= 4 AND outcome is None AND last_message_at < cutoff` → False (20 msgs).
- R3 (cacador_de_informacao): `not forneceu_dado_pessoal` → False (she gave CPF).
- **No persona rule fires.** The classifier will dispatch the LLM.

**Sentiment hard rules** (`evaluate_sentiment_rules`):
- SR2 (fast close → positivo): `outcome == "venda_fechada" AND num_msgs <= 10` → False.
- SR1 (ghosting → negativo): `outcome is None AND num_msgs_inbound <= 2 AND num_msgs_outbound >= 3` → False (her outcome is `perdido_concorrente`, not None).
- SR3 (competitor + no close → negativo): `mencionou_concorrente AND outcome != "venda_fechada"` → **True**. Returns `SentimentResult(sentiment="negativo", confidence=0.85, source="rule")`.

Because no persona rule fired, `_classify_with_llm` runs. It sends the system prompt + a user prompt that wraps her conversation text in `<conversation untrusted="true">…</conversation>` and lists her metrics. The combined prompt asks for JSON `{"persona": "...", "sentimento": "..."}`.

#### Stage 5 — LLM dispatch

The cached call hashes `(model + system_prompt + user_prompt + max_tokens + temperature)` into a SHA-256 key. First time → API call. Subsequent runs of the same batch hit the cache instantly.

Plausible LLM reply for GABRIELA:

```json
{"persona": "refem_de_concorrente", "sentimento": "negativo"}
```

The reasoning the model picks up: she mentioned a competitor's price (HDI R$ 1903 vs our R$ 5749), kept asking objection-style questions ("carro reserva so 7 dias?", "cobertura pra enchente?"), and closed with "vou pensar melhor". Classic `refem_de_concorrente`.

#### Stage 6 — Parsing + sentiment merge

`parse_classifier_reply(reply_text)` returns `("refem_de_concorrente", "negativo")`.

Persona side: valid enum → `persona_source="llm"`, `persona_confidence=0.8`.
Sentiment side: hard-rule SR3 already fired, so the rule's verdict wins over the LLM's `negativo` (they happen to agree here, but the rule is authoritative). Final: `sentiment="negativo"`, `sentiment_confidence=0.85`, `sentiment_source="rule"`.

Merged `PersonaResult`:

```python
PersonaResult(
    persona="refem_de_concorrente",
    persona_confidence=0.8,
    persona_source="llm",
    sentiment="negativo",
    sentiment_confidence=0.85,
    sentiment_source="rule",
)
```

#### Stage 7 — `_apply_persona_and_intent_score` fills GABRIELA's row

Polars join GABRIELA's `lead_id` against the persona DataFrame, write `persona`, `persona_confidence`, `sentiment`, `sentiment_confidence` columns. Then `compute_intent_score` reads its 9 inputs (closing-phrase hits, technical-question hits, evasive-phrase hits, haggling-phrase hits, off-hours, response time, persona, outcome, forneceu_dado_pessoal). Her conversation has many haggling phrases ("R$ 1903", "vou pensar"), so `price_sensitivity` ← `medium`-`high`. `intent_score` ends up moderate-low because most signals pull negative (competitor mention + no close + many objection questions).

#### Stage 8 — Final lead_profile row for GABRIELA

```
lead_id:                  lead_a8f329…
conversations:            1
closed_count:             0
close_rate:               0.0
sender_name_normalized:   gabriela barbosa
dominant_email_domain:    null
dominant_state:           SP
dominant_city:            Sao Paulo
engagement_profile:       warm
persona:                  refem_de_concorrente
persona_confidence:       0.8
sentiment:                negativo
sentiment_confidence:     0.85
price_sensitivity:        medium
intent_score:             ~25
first_seen_at:            2026-02-14T18:10:20+00:00
last_seen_at:             2026-02-14T19:07:30+00:00
```

#### Stage 9 — Atomic writes

`gold/writer.py` writes four parquets + one JSON, each via temp file + rename:

- `data/gold/conversation_scores/<batch_id>.parquet` — her 1 row + everyone else's.
- `data/gold/lead_profile/<batch_id>.parquet` — her row above.
- `data/gold/agent_performance/<batch_id>.parquet` — Diego's stats incorporate her loss.
- `data/gold/competitor_intel/<batch_id>.parquet` — `HDI Seguros` mention count goes up by 1.
- `data/gold/insights/<batch_id>.json` — top objections, top competitors, ghosting taxonomy.

Manifest flipped: `gold_status = 'COMPLETED', gold_rows = 4` (the row totals across the four parquets).

---

### Step 5. The error-recovery brain — what would have happened if Silver crashed?

Imagine a regression in `silver/extract.py` that raises a `SchemaDriftError` because the source got an unexpected `metadata.lead_source` value.

1. **Classify** — `agent/diagnoser.py` matches the exception class against deterministic patterns → `ErrorKind.SCHEMA_DRIFT`. (No LLM needed for known-pattern errors.)
2. **Build fix** — `agent/runners.py:make_fix_builder(ErrorKind.SCHEMA_DRIFT, batch_id="044f334e976a", silver_root=…)` returns a callable that deletes the partial Silver parquet so the retry starts clean.
3. **Retry** — run the fix, then re-run the Silver runner. Up to 3 attempts.
4. If still failing after 3 → **escalate**. JSON line written to `logs/agent-escalations.jsonl` with the batch_id, the error class, the stack trace. Manifest row flips to `ESCALATED`. The loop continues to the next batch — GABRIELA's batch is parked, Diego's other batches keep flowing.

This is why the system is called **self-healing**: a transient error never leaks downstream and never freezes the loop.

---

### Step 6. State + logging

Throughout the run, structlog emits JSON events:

```json
{"event":"agent.loop_started","agent_run_id":"run_…","ts":"2026-04-25T..."}
{"event":"agent.batch_started","batch_id":"044f334e976a"}
{"event":"agent.layer_started","batch_id":"044f334e976a","layer":"BRONZE"}
{"event":"agent.layer_completed","batch_id":"044f334e976a","layer":"BRONZE","attempts":1}
{"event":"agent.layer_started","batch_id":"044f334e976a","layer":"SILVER"}
{"event":"agent.layer_completed","batch_id":"044f334e976a","layer":"SILVER","attempts":1}
{"event":"agent.layer_started","batch_id":"044f334e976a","layer":"GOLD"}
{"event":"gold.persona.done","batch_id":"044f334e976a","leads":1234}
{"event":"gold.sentiment.batch_complete","batch_id":"044f334e976a","leads":1234,"sentiment_source_mix":{"rule":640,"llm":520,"llm_fallback":74},"sentiment_label_mix":{"positivo":480,"neutro":410,"negativo":290,"misto":54}}
{"event":"gold.lead_profile.done","batch_id":"044f334e976a","rows":1234,"persona_distribution":{"refem_de_concorrente":210,"comprador_racional":180,"…":…}}
{"event":"gold.complete","batch_id":"044f334e976a","rows_out":1234}
{"event":"agent.layer_completed","batch_id":"044f334e976a","layer":"GOLD","attempts":1}
{"event":"agent.loop_stopped","status":"COMPLETED","batches_processed":1}
```

Note that GABRIELA herself is one row inside the `1234`-lead total for that batch. The structured events let you slice analytics later: "show me every batch where `sentiment_label_mix.misto` exceeded 25%".

The CLI's last act:

```json
{"agent_run_id":"run_…","batches_processed":1,"failures_recovered":0,"escalations":0,"status":"COMPLETED"}
```

Exit code 0.

---

## Part 3 — End-to-End in One Paragraph (with GABRIELA)

You typed `uv run python -m pipeline agent run-once`. Click landed in `cli/agent.py:run_once_cmd`, which read `.env`, opened the SQLite manifest, and called `agent/loop.py:run_once`. The loop acquired the filesystem lock, asked `observer.scan` which batches were pending, found `044f334e976a` (containing GABRIELA's 22 messages plus thousands of others), and asked `planner.plan` for the layer order. The executor ran Bronze first — `ingest/ingest.py` copied the source parquet bit-for-bit into `data/bronze/`. Silver next: `silver/transform.py` chained dedup (collapsing GABRIELA's two duplicate audio transcripts), normalize (parsing her timestamp into UTC, unpacking her metadata JSON struct), extract (pulling her plate `SYL8V26`, vehicle `Chevrolet Onix 2015`, competitor `HDI Seguros`, no email), and `pii.py` (rewriting her CPF `418.696.561-30` to `***.***.***-**` and her phone to `+55 (**) *****-50`), then reconciled all her 20 surviving messages into a single `lead_id`. Gold then built four parquet tables: `conversation_scores` recorded her 22 messages with `mencionou_concorrente=true` and outcome `perdido_concorrente`; `lead_profile` ran her through hard rules (no persona rule fired because she gave CPF; SR3 sentiment rule fired with `negativo` confidence 0.85 because of HDI mention without close), then sent her to the LLM with a JSON-structured prompt that returned `{"persona": "refem_de_concorrente", "sentimento": "negativo"}`. The merge logic kept the rule's sentiment (deterministic wins) and used the LLM's persona, producing a final row tagged `refem_de_concorrente` / `negativo` with engagement bucket `warm` and a moderate-low intent score. Atomic file renames published the four parquets + insights JSON; structlog emitted per-stage JSON events so an analyst can grep batch totals tomorrow; the manifest flipped every row to COMPLETED so a future `run-once` skips this batch entirely; and the CLI exit code was 0.

---

## Part 4 — Where to look next when you're curious about her data

| Curious about… | Read this file | Why |
|---|---|---|
| How her `metadata` JSON gets parsed | `src/pipeline/silver/normalize.py` | Strikes the JSON struct schema. |
| How her CPF is masked to `***.***.***-**` | `src/pipeline/silver/pii.py` (lines ~74–115) | Per-PII regex + masker callables. |
| How her plate `SYL8V26` is extracted | `src/pipeline/silver/extract.py` (search `plate_format`) | Brazilian plate regex. |
| How her competitor mention `HDI Seguros` is normalised | `src/pipeline/gold/competitor_intel.py` and `silver/extract.py:concorrente_mencionado` | Case-insensitive match against a known list. |
| How `refem_de_concorrente` shows up | `src/pipeline/gold/persona.py:SYSTEM_PROMPT` (definition of all 8 personas) | The LLM contract. |
| How her sentiment got `negativo` from a rule | `src/pipeline/gold/sentiment.py:evaluate_sentiment_rules` (SR3) | The hard rule that fired. |
| How her batch got assembled | `src/pipeline/agent/observer.py:scan` and `discover_source_batches` | Filesystem walk + manifest join. |
| What the engagement bucket math is | `src/pipeline/gold/lead_profile.py:_add_engagement_profile` | First-match-wins hot/warm/cold. |
| Why we did not double the LLM bill | `src/pipeline/gold/persona.py:SYSTEM_PROMPT` (combined JSON) and `parse_classifier_reply` | One round-trip, two labels. |
| What would happen if her batch crashed mid-Silver | `src/pipeline/agent/executor.py:run_with_recovery` | Try / classify / fix / retry / escalate. |

---

## Part 5 — Try this yourself

Once you have the dependencies installed (`uv sync`), you can reproduce GABRIELA's path interactively:

```bash
# Inspect the source
uv run python -c "
import polars as pl
df = pl.read_parquet('data/raw/conversations_bronze.parquet')
print(df.filter(pl.col('conversation_id') == 'conv_00000001').sort('timestamp').head(5))
"

# Run the whole pipeline once on her batch
uv run python -m pipeline agent run-once

# Inspect her Gold lead_profile row
uv run python -c "
import polars as pl
import os, glob
for f in sorted(glob.glob('data/gold/lead_profile/batch_id=*/*.parquet'))[:1]:
    print('reading', f)
    print(pl.read_parquet(f).filter(pl.col('sender_name_normalized') == 'gabriela barbosa'))
"
```

If anything in the doc disagrees with what the parquet actually shows, the parquet wins — that is the system of record.

When you can read each layer's file and explain *why* each value transformed the way it did, you understand the pipeline.
