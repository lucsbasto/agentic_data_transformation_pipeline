# Runbook: Manifest Unreachable

**Severity:** critical  
**Source alert:** ManifestUnreachable (probe_success{job="readyz"} == 0)

## Symptom

Agent cannot connect to the manifest database (state/manifest.db). The /readyz health check is failing, indicating the SQLite file is inaccessible, corrupted, or locked by an external process. Pipeline cannot proceed without manifest access.

## Diagnosis

1. **Check disk space:**  
   Run: `df -h state/` and `du -sh state/manifest.db`  
   If disk is full (0% free), manifest cannot be written. If manifest.db is huge (> 1GB), may indicate runaway growth.

2. **Check file permissions:**  
   Run: `ls -la state/manifest.db`  
   Expected: `-rw-r--r-- pipeline:pipeline`. If permissions are restrictive or owner is wrong, fix them.

3. **Check if locked by external process:**  
   Run: `lsof state/manifest.db 2>/dev/null | grep -v agent`  
   If any process other than agent holds the file, it may be locked. Common culprits: backup tools, rsync, external SQLite tools.

4. **Check SQLite file integrity:**  
   Run: `sqlite3 state/manifest.db "PRAGMA integrity_check;"`  
   If output is not "ok", database is corrupted.

## Mitigation

- **If disk is full:**  
   Free up space. Check what is consuming disk:  
   `du -sh data/* logs/* state/* | sort -rh | head -10`  
   Rotate or delete old logs: `find logs/ -name '*.json*' -mtime +7 -delete`

- **If external process locked file:**  
   Identify and stop the process: `lsof state/manifest.db` → identify PID → `kill -9 <PID>`  
   Then restart agent: `python -m pipeline agent run-forever`.

- **If database corrupted:**  
   Restore from backup if available: `cp state/manifest.db.backup state/manifest.db`  
   If no backup, reinitialize (data loss): `rm state/manifest.db && python -m pipeline agent run-once`  
   **This will lose manifest state. Use only as last resort.**

- **If permissions wrong:**  
   Fix: `chown pipeline:pipeline state/manifest.db && chmod 644 state/manifest.db`  
   Restart agent.

## Verification

/readyz endpoint should return HTTP 200 within 30 seconds of mitigation:  
`curl -v http://localhost:9100/readyz`  
Agent should resume processing batches.

## Postmortem template

- **Trigger time (UTC):**
- **Root cause:** (disk full / external lock / corruption / permission error)
- **Impact (processing halt duration / data loss):**
- **Mitigation applied:**
- **Follow-up actions:** (disk cleanup / backup restore / schema repair)
- **Prevention:** (disk space alert / backup automation / WAL mode for SQLite / periodic integrity check)
