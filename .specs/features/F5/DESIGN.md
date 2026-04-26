# F5 — Per-Lead Sentiment Classification (Gold Layer)

## Goal

Surface "Análise de sentimento do cliente" (assessment §"Camada Gold")
as a first-class column in `gold.lead_profile`. Reuses the persona-lane
architecture: hard rules first, LLM fallback for ambiguous leads.

## Schema delta

`src/pipeline/schemas/gold.py`

```python
SENTIMENT_VALUES: tuple[str, ...] = ("positivo", "neutro", "negativo", "misto")

# _LEAD_PROFILE_FIELDS gains, between persona_confidence and price_sensitivity:
"sentiment": pl.Enum(list(SENTIMENT_VALUES)),
"sentiment_confidence": pl.Float64(),
```

`assert_lead_profile_schema` is auto-derived from `_LEAD_PROFILE_FIELDS`,
so existing parquets fail drift validation. Backfill via re-run.

## Hard rules (sentiment.py)

Priority order: SR2 → SR1 → SR3. First hit wins.

| Rule | Predicate | Outcome | Confidence |
|------|-----------|---------|-----------|
| SR2 | `outcome=="venda_fechada" AND num_msgs<=10` | positivo | 1.0 |
| SR1 | `outcome is None AND num_msgs_inbound<=2 AND num_msgs_outbound>=3` | negativo | 0.9 |
| SR3 | `mencionou_concorrente AND outcome != "venda_fechada"` | negativo | 0.85 |

SR2 priority over SR3 means a fast close still resolves to positivo
even if competitor was mentioned (closed-fast is a stronger signal).

## LLM combined prompt

`SYSTEM_PROMPT` v3 instructs the model to return JSON:

```json
{"persona": "<chave>", "sentimento": "<chave>"}
```

Single round-trip → no LLM cost increase per lead. Cache key includes
the system prompt text, so the v2→v3 bump invalidates all cached
entries (one-time cost on first post-deploy batch).

`parse_classifier_reply` strips markdown code fences (some providers
wrap JSON in ` ```json ... ``` ` despite explicit prohibition), then
`json.loads`. Each label is validated independently against its enum;
unknown label → `None` for that field only. Both `None` → fallback
(`comprador_racional` + `neutro`) with `*_source="llm_fallback"` for
audit-trail clarity.

## Wiring path

```
LeadAggregate
  → evaluate_rules (persona R1-R3)
  → evaluate_sentiment_rules (sentiment SR1-SR3)
  → if persona-rule hit: _attach_sentiment(rule_persona, sentiment_hit)
    else: _classify_with_llm(combined prompt) — sentiment_hit overrides LLM sentiment if rule fired
  → PersonaResult(persona, persona_confidence, persona_source,
                  sentiment, sentiment_confidence, sentiment_source)
  → transform_gold._apply_persona_and_intent_score
    → fills sentiment + sentiment_confidence columns
    → .select() includes both columns position-correctly
  → write_gold_lead_profile (atomic rename, schema-asserted)
```

## Observability

`gold.sentiment.batch_complete` structlog event per batch:

```python
{
  "leads": int,
  "sentiment_source_mix": {"rule": int, "llm": int, "llm_fallback": int, "skipped": int},
  "sentiment_label_mix": {"positivo": int, "neutro": int, "negativo": int, "misto": int, "_null": int}
}
```

Calibration gate: `misto` should be ≤25% of LLM-classified leads
post-deploy. Above threshold → file follow-up issue (do not block).

## Tests

22 unit tests in `tests/unit/test_gold_sentiment.py`:
- T-S1..T-S4: hard-rule predicates + non-firing cases.
- T-S5..T-S7 + T-S6b/T-S6c: parser variants (valid, malformed, unknown
  label, markdown fence, half-valid, non-dict, mixed case).
- SentimentResult.skipped sentinel.

E2E coverage in `tests/unit/test_gold_transform.py`:
- Sentiment columns populated through the writer with correct enum dtype.
- Skipped classifier yields null columns, schema still validates.

Schema drift test in `tests/unit/test_schemas_gold.py` updated for
new column order.

## Backfill

Phase 2 — re-run gold lane per existing batch:

```bash
pipeline manifest list --layer silver --status completed | \
  xargs -I{} pipeline gold run-once --batch-id {}
```

Cost: full LLM re-classification (cache flush from v3 prompt bump).
Atomic writer makes re-runs safe.

## Non-goals (out of scope for F5)

- Per-conversation sentiment in `conversation_scores` (future F5.1+).
- Numeric polarity score [-1, 1].
- Multilingual support.
- A/B harness for prompt v2 vs v3.
- Real-time / streaming sentiment.
