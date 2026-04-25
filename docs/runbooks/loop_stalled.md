# Runbook: Loop Stalled

**Severity:** critical  
**Source alert:** LoopStalled (rate(iterations_total[10m]) == 0)

## Symptom

Agent loop is not making progress. No new iterations started in the last 10 minutes. Pipeline is blocked and batches are not being processed.

## Diagnosis

1. **Check if agent container is running:**  
   Run: `docker ps | grep agent`  
   If not in output, container crashed. Check logs: `docker logs <container_id> | tail -50`.

2. **Check readyz endpoint:**  
   Run: `curl -s http://localhost:9100/readyz`  
   Expected: HTTP 200.  
   If 503, one or more health checks failed (manifest, lock, source root).

3. **Check for stuck lock:**  
   Run: `cat state/agent.lock 2>/dev/null || echo "NO LOCK"`  
   If lock exists and is very old (> 10m), follow lock_held runbook.

4. **Check source root mount:**  
   Run: `ls -la data/ | head -5`  
   If `data/` is empty or inaccessible, source root mount failed or was unmounted.

## Mitigation

- **If container not running:**  
   Restart: `docker-compose restart agent`. Wait 30s for readyz to report 200.

- **If readyz failing:**  
   Run full readyz check manually and fix the underlying issue:  
   - Manifest unreachable: Check disk space (`df -h state/`). SQLite file locked? (`lsof state/manifest.db`)  
   - Lock dir not writable: Check permissions (`ls -ld state/`)  
   - Source root missing: Verify mount: `mount | grep data/`

- **If source root unmounted:**  
   Remount the volume: `mount <device> data/` or restart container to trigger mount hooks.

- **If everything responsive but no new iterations:**  
   Agent may be in deep think on a batch (legitimate long-running layer).  
   Check `/metrics` counter: `pipeline_agent_iterations_total` should increase within 5 minutes.  
   If still zero after 15m, kill and restart agent.

## Verification

New iteration should start within 5 minutes. Monitor:  
`curl -s http://localhost:9100/metrics | grep pipeline_agent_iterations_total`  
Counter value should increase every 1-2 minutes during normal operation.

## Postmortem template

- **Trigger time (UTC):**
- **Root cause:** (container crash / mount failure / disk full / lock stuck / manifest corrupted)
- **Impact (processing halt duration):**
- **Mitigation applied:**
- **Follow-up actions:** (container restart / volume remount / disk cleanup)
- **Prevention:** (persistent volume healthcheck / mount health monitor / disk utilization alert)
