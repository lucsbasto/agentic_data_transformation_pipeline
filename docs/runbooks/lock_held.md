# Runbook: Lock Held Too Long

**Severity:** critical  
**Source alert:** LockHeldTooLong (lock_held == 1 for 1h)

## Symptom

Agent process is holding the lock file for over 1 hour. This blocks other agent instances from acquiring the lock, preventing the pipeline from processing batches. Usually indicates a hung or dead agent process.

## Diagnosis

1. **Check lock file:**  
   Run: `cat state/agent.lock`  
   Output should show PID and acquisition timestamp. Note the PID.

2. **Check if PID is alive:**  
   Run: `ps -p <PID> -o pid,cmd,etime`  
   If PID not found, the process died without releasing the lock.  
   If PID found, check elapsed time. Is it unusually long (> 10m for a single run)?

3. **Check recent logs for that PID:**  
   Run: `grep '"pid":<PID>' logs/agent.jsonl | tail -20`  
   Look for last event timestamp. Is it hours old? Is the last event a failure or escalation?

## Mitigation

- **If process is dead:**  
   Run: `rm state/agent.lock` to release the lock. Then restart agent: `python -m pipeline agent run-forever`.

- **If process is hung (alive but no recent logs):**  
   Check if it is stuck in LLM call or I/O. Run `strace -p <PID>` briefly to see what syscall it is blocked on.  
   If confirmed hung: Kill the process (`kill -9 <PID>`), remove lock file, restart.

- **If process is alive and producing logs normally:**  
   Check if there is a legitimate reason for long lock hold (e.g., very large batch, network latency).  
   Monitor next few iterations. If lock release time normalizes, no action needed.  
   If lock continues held for >30m per iteration, suspect infinite loop in executor or layer logic.

## Verification

Lock should be released when agent completes current iteration. Run:  
`watch -n 5 'test -f state/agent.lock && echo "LOCKED" || echo "FREE"'`  
Lock file should disappear within 5 minutes after mitigation.

## Postmortem template

- **Trigger time (UTC):**
- **Root cause:** (dead process / hung LLM call / infinite loop / slow layer)
- **Impact (batches affected / processing delay):**
- **Mitigation applied:**
- **Follow-up actions:** (timeout tuning / strace investigation / code fix)
- **Prevention:** (healthcheck heartbeat to detect hung process / layer timeout / lock timeout watchdog)
