# Runbook: Layer Latency High

**Severity:** warning  
**Source alert:** LayerLatencyHigh (histogram_quantile(0.95, layer_duration) > 30s for 15m)

## Symptom

A data transformation layer (Bronze, Silver, or Gold) is taking longer than expected. The 95th percentile of layer execution time has exceeded 30 seconds for 15 minutes. This causes slower batch throughput and may back up the queue.

## Diagnosis

1. **Identify which layer:**  
   Check Grafana dashboard "Layer Duration" panel or run:  
   `curl -s http://localhost:9100/metrics | grep 'pipeline_layer_duration_seconds' | grep '_bucket'`  
   Look for which `layer="Bronze|Silver|Gold"` has high p95 values.

2. **Check input batch volume:**  
   Run: `grep '"layer":"<LAYER>"' logs/agent.jsonl | grep '"event":"layer_started"' | wc -l`  
   Count batches processed in last hour. Large volume + high latency = throughput issue, not a bug.

3. **Check if Silver layer (LLM-dependent):**  
   For Silver only, check LLM cache hit rate and token usage:  
   `grep '"layer":"Silver"' logs/agent.jsonl | jq '.llm_cache_hit' | grep -c 'true'` vs total Silver events.  
   Low cache hit rate (< 50%) combined with high latency suggests LLM is slow or quotas being hit.

4. **Check for layer-specific errors:**  
   Run: `grep '"layer":"<LAYER>"' logs/agent.jsonl | grep '"event":"failure_detected"' | jq '.error_message' | sort | uniq -c | sort -rn`  
   If repeated errors, investigate error root cause.

## Mitigation

- **If high volume (legitimate throughput):**  
   No action needed. This is expected behavior. Monitor for sustained growth that exceeds capacity.

- **If Silver layer + low cache hit + high latency:**  
   LLM calls are slow. Check DashScope API quota/rate limits. If near quota: either increase quota or throttle batch concurrency.

- **If Bronze/Gold layer + repeated errors:**  
   Fix the layer logic bug. Most common: I/O bottleneck (disk reads) or memory exhaustion for large batches.  
   Check system resources: `free -h` (memory), `iostat -x 1 3` (disk).

- **If unsustainable latency (30s+) even at normal volume:**  
   Optimize layer code. Profile with: `python -m cProfile -s cumtime -m pipeline agent run-once 2>&1 | head -30`

## Verification

Layer p95 latency should return under 30 seconds. Check Grafana within 15 minutes of mitigation.  
Batch throughput should normalize (iterations_total counter increasing steadily).

## Postmortem template

- **Trigger time (UTC):**
- **Root cause:** (high input volume / LLM quota / code bottleneck / resource exhaustion)
- **Layer affected:** (Bronze / Silver / Gold)
- **Impact (latency in seconds / batches processed):**
- **Mitigation applied:**
- **Follow-up actions:** (quota increase / code optimization / resource tuning)
- **Prevention:** (per-layer SLA threshold / capacity planning / cache hit monitoring)
