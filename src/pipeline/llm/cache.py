"""SQLite-backed cache for LLM responses.

The cache table (`llm_cache`) lives in the same `state/manifest.db` file
declared in F1.3. A cache key is the sha256 of the request signature
(model, system prompt, user prompt, max_tokens, temperature). Hits are
deterministic replays; misses round-trip to the provider and are stored.

Determinism is the whole point of the cache: replaying the exact same
request against the same prompt version must return the same row. That
is what lets pipeline runs be reproducible across Bronze re-builds and
across Silver/Gold re-materializations.
"""

from __future__ import annotations

import hashlib
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from pipeline.errors import LLMCacheError
from pipeline.schemas.manifest import LLM_CACHE_DDL


@dataclass(frozen=True, slots=True, kw_only=True)
class CachedResponse:
    """Projection of one row from the `llm_cache` table."""

    cache_key: str
    model: str
    response_text: str
    input_tokens: int
    output_tokens: int
    created_at: str


def compute_cache_key(
    *,
    model: str,
    system: str,
    user: str,
    max_tokens: int,
    temperature: float,
) -> str:
    """Stable sha256 key for a request signature.

    Temperature is normalized with ``+ 0.0`` so that a caller passing
    ``-0.0`` hashes to the same key as ``0.0`` — ``f"{-0.0:.6f}"``
    produces ``"-0.000000"`` in Python, which would otherwise leak a
    separate cache bucket for a semantically identical request.
    """
    payload = "\n".join(
        [
            f"model={model}",
            f"system={system}",
            f"user={user}",
            f"max_tokens={max_tokens}",
            f"temperature={temperature + 0.0:.6f}",
        ]
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


class LLMCache:
    """Thin wrapper over the `llm_cache` table.

    Opens its own SQLite connection — the cache is independent of
    :class:`pipeline.state.manifest.ManifestDB` so the two can be used
    without ordering constraints. The DDL is idempotent.
    """

    def __init__(self, db_path: Path | str) -> None:
        self._db_path = db_path if db_path == ":memory:" else Path(db_path)
        self._conn: sqlite3.Connection | None = None

    def open(self) -> LLMCache:
        if self._conn is not None:
            return self
        if isinstance(self._db_path, Path):
            self._db_path.parent.mkdir(parents=True, exist_ok=True)
            target: str = str(self._db_path)
        else:
            target = self._db_path
        conn = sqlite3.connect(target, isolation_level=None)
        conn.row_factory = sqlite3.Row
        if target != ":memory:":
            conn.execute("PRAGMA journal_mode = WAL;")
        conn.execute(LLM_CACHE_DDL)
        self._conn = conn
        return self

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def __enter__(self) -> LLMCache:
        return self.open()

    def __exit__(self, *_exc: object) -> None:
        self.close()

    def get(self, cache_key: str) -> CachedResponse | None:
        conn = self._require_conn()
        row = conn.execute(
            "SELECT cache_key, model, response_text, input_tokens, "
            "output_tokens, created_at FROM llm_cache WHERE cache_key = ?;",
            (cache_key,),
        ).fetchone()
        if row is None:
            return None
        return CachedResponse(**dict(row))

    def put(
        self,
        *,
        cache_key: str,
        model: str,
        response_text: str,
        input_tokens: int,
        output_tokens: int,
    ) -> None:
        """Insert a new row.

        Uses ``INSERT OR IGNORE`` by design: once a key is stored, the
        row is frozen. Even if a later network call reports different
        token counts or latency, the stored values represent the *first*
        observed response and stay canonical. Callers who need to replace
        a poisoned row must call :meth:`invalidate` with the prefix and
        re-store.
        """
        conn = self._require_conn()
        conn.execute(
            "INSERT OR IGNORE INTO llm_cache "
            "(cache_key, model, response_text, input_tokens, output_tokens, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?);",
            (
                cache_key,
                model,
                response_text,
                input_tokens,
                output_tokens,
                _utcnow_iso(),
            ),
        )

    def invalidate(self, *, prefix: str | None = None) -> int:
        """Delete rows whose ``cache_key`` starts with ``prefix``.

        ``None`` clears the entire cache — use only to force a full re-run.
        Returns the number of rows deleted. The prefix is run through a
        LIKE query after escaping SQL-LIKE metacharacters (``%`` and
        ``_``); otherwise a caller passing ``"%"`` would clear everything.
        """
        conn = self._require_conn()
        if prefix is None:
            cur = conn.execute("DELETE FROM llm_cache;")
        else:
            safe = (
                prefix.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
            )
            cur = conn.execute(
                "DELETE FROM llm_cache WHERE cache_key LIKE ? ESCAPE '\\';",
                (f"{safe}%",),
            )
        return cur.rowcount

    def _require_conn(self) -> sqlite3.Connection:
        if self._conn is None:
            raise LLMCacheError(
                "LLMCache connection is not open; use as a context manager "
                "or call .open() first."
            )
        return self._conn


def _utcnow_iso() -> str:
    return datetime.now(tz=UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
