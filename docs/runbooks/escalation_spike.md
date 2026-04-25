# Runbook: Escalation Spike

**Severity:** warning  
**Source alert:** EscalationSpike (rate(escalations_total[5m]) > 0.1)

## Symptom

Agent is escalating batches faster than normal. Alert fires when escalation rate exceeds 0.1/sec averaged over 5 minutes. Oncall should check if this is a cascading failure or isolated spike.

## Diagnosis

1. **Check recent escalations:**  
   Run: `tail -n 20 logs/agent.jsonl | grep '"event":"escalation"'`  
   Look for `error_kind` and `layer` fields. Are most escalations from the same layer or error type?

2. **Check batch distribution:**  
   Run: `grep '"event":"escalation"' logs/agent.jsonl | jq -s 'group_by(.error_kind) | map({error_kind: .[0].error_kind, count: length})' | sort_by(-.count)`  
   If 80%+ from one error_kind, investigate that root cause. If spread across multiple layers/errors, check for bad data.

3. **Check for looping batch:**  
   Run: `grep '"event":"escalation"' logs/agent.jsonl | jq '.batch_id' | sort | uniq -c | sort -rn | head -5`  
   If one batch_id appears 3+ times, it is looping and continuously escalating.

## Mitigation

- **If single error_kind dominates** (e.g., all `manifest_mismatch`):  
  Check Silver schema. Run `sqlite3 state/manifest.db "SELECT COUNT(*) as cnt, error_kind FROM escalations GROUP BY error_kind LIMIT 5"`. Fix the root cause in code.

- **If single batch looping** (same batch_id repeated):  
  Stop agent, inspect batch in manifest (`SELECT * FROM batches WHERE id = '<batch_id>'`), identify blocker, mark batch `FAILED` or `SKIPPED` manually in manifest, restart agent.

- **If spread across batches/layers:**  
  Check if source data was recently updated or corrupted. Verify Bronze files are readable: `ls -la data/bronze/ | head -10`. If files truncated or zero-sized, restore from backup.

## Verification

Escalation rate should drop below 0.1/sec within 10m. Check Grafana dashboard:  
`Escalations (5m rate)` panel should show rate decreasing.

## Postmortem template

- **Trigger time (UTC):**
- **Root cause:** (single error_kind / looping batch / bad source data)
- **Impact (batches affected):**
- **Mitigation applied:**
- **Follow-up actions:** (code fix / data correction / monitoring threshold tuning)
- **Prevention:** (automated detector for repeated batch_id / schema validation pre-flight)
