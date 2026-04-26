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


# LEARN: ``@dataclass(frozen=True, slots=True, kw_only=True)`` — three
# flags that define this as a strict value object:
#   - ``frozen=True``   instances are immutable after construction;
#   - ``slots=True``    faster attribute access, forbids ad-hoc fields;
#   - ``kw_only=True``  callers MUST use keyword arguments. Future
#                       fields can be added without breaking ordering.
# The class just names what one ``llm_cache`` row looks like when we
# return it from ``.get()`` — Python side only; does not touch SQL.
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
    # LEARN: the cache-key idea is central to LLM engineering. Two
    # identical requests MUST produce the same key so the second one is
    # a cheap DB lookup instead of a paid API round-trip. Every input
    # that changes the *answer* must be in the key; inputs that do not
    # (request id, timestamp) must NOT be.
    #
    # We join each field on a newline and hash the bytes. Python-level
    # benefits:
    #   - ``"\n".join([...])`` builds a readable payload we could print
    #     at debug time to see exactly what was hashed;
    #   - ``.encode("utf-8")`` turns the string into bytes because
    #     ``hashlib`` only accepts bytes;
    #   - ``f"{temperature + 0.0:.6f}"`` normalizes ``-0.0`` to ``0.0``
    #     (IEEE-754 has two zeros). Six decimal places is enough
    #     precision for any temperature a caller would realistically
    #     set; more would add no entropy and risk float-rendering churn.
    payload = "\n".join(
        [
            f"model={model}",
            f"system={system}",
            f"user={user}",
            f"max_tokens={max_tokens}",
            f"temperature={temperature + 0.0:.6f}",
        ]
    ).encode("utf-8")
    # LEARN: ``sha256().hexdigest()`` returns a 64-char hex string.
    # Hex is URL-safe, grep-friendly, and stable across Python versions.
    return hashlib.sha256(payload).hexdigest()


class LLMCache:
    """Thin wrapper over the `llm_cache` table.

    Opens its own SQLite connection — the cache is independent of
    :class:`pipeline.state.manifest.ManifestDB` so the two can be used
    without ordering constraints. The DDL is idempotent.
    """

    def __init__(self, db_path: Path | str) -> None:
        # LEARN: same dual-path trick as ManifestDB — real files are
        # ``Path`` objects, the in-memory SQLite sentinel ``":memory:"``
        # stays a string. Branching on that lets us skip the
        # ``mkdir`` and WAL pragmas that would be meaningless for the
        # in-memory backend.
        self._db_path = db_path if db_path == ":memory:" else Path(db_path)
        self._conn: sqlite3.Connection | None = None

    def open(self) -> LLMCache:
        """Open the SQLite connection and apply the DDL. Idempotent — safe to call twice."""
        if self._conn is not None:
            return self
        if isinstance(self._db_path, Path):
            self._db_path.parent.mkdir(parents=True, exist_ok=True)
            target: str = str(self._db_path)
        else:
            target = self._db_path
        conn = sqlite3.connect(target, isolation_level=None)
        conn.row_factory = sqlite3.Row
        # LEARN: ``busy_timeout`` tells SQLite to wait up to 5000 ms for
        # another writer before raising ``SQLITE_BUSY``. Both this cache
        # and ``ManifestDB`` hit the same DB file under Silver load, so
        # a sane timeout prevents spurious failures.
        conn.execute("PRAGMA busy_timeout = 5000;")
        if target != ":memory:":
            conn.execute("PRAGMA journal_mode = WAL;")
        # LEARN: ``LLM_CACHE_DDL`` is the ``CREATE TABLE IF NOT EXISTS``
        # string from ``schemas/manifest.py``. Idempotent — safe on
        # every open.
        conn.execute(LLM_CACHE_DDL)
        self._conn = conn
        return self

    def close(self) -> None:
        """Close the SQLite connection. Safe to call when already closed."""
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def __enter__(self) -> LLMCache:
        """Support ``with LLMCache(...) as cache:`` — delegates to :meth:`open`."""
        return self.open()

    def __exit__(self, *_exc: object) -> None:
        """Close the connection on context exit, even if the body raised."""
        self.close()

    def get(self, cache_key: str) -> CachedResponse | None:
        """Look up a cached response by key. Returns ``None`` on a cache miss."""
        # LEARN: ``fetchone()`` returns ``None`` when the row does not
        # exist — perfect for our ``Optional`` return type. Callers
        # interpret ``None`` as "cache miss, go call the provider".
        conn = self._require_conn()
        row = conn.execute(
            "SELECT cache_key, model, response_text, input_tokens, "
            "output_tokens, created_at FROM llm_cache WHERE cache_key = ?;",
            (cache_key,),
        ).fetchone()
        if row is None:
            return None
        # LEARN: ``CachedResponse(**dict(row))`` explodes the dict-like
        # ``sqlite3.Row`` into keyword arguments. The ``kw_only=True``
        # on the dataclass forces every caller — including this one —
        # to use this explicit form, which catches column renames at
        # type-check time instead of producing garbage at runtime.
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
        # LEARN: ``INSERT OR IGNORE`` is SQLite syntax. On a primary-key
        # collision it silently drops the new row instead of raising.
        # For a cache that should be "first writer wins" that is exactly
        # the contract we want. ``INSERT OR REPLACE`` would be the
        # opposite: last-writer-wins.
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
            # LEARN: unbounded ``DELETE FROM table;`` wipes every row.
            # SQLite recognizes this as "truncate" and is fast.
            cur = conn.execute("DELETE FROM llm_cache;")
        else:
            # LEARN: in SQL LIKE, ``%`` matches any sequence and ``_``
            # matches any single character. Those are the equivalent of
            # regex ``*`` and ``?``. If a caller passes a prefix
            # containing one of these, the match would widen silently.
            # We escape them with a chosen escape character (``\``) and
            # tell LIKE which character is the escape via ``ESCAPE``.
            # The four ``.replace`` calls escape the escape character
            # itself first, then the two metacharacters.
            safe = (
                prefix.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
            )
            cur = conn.execute(
                "DELETE FROM llm_cache WHERE cache_key LIKE ? ESCAPE '\\';",
                (f"{safe}%",),
            )
        return cur.rowcount

    def _require_conn(self) -> sqlite3.Connection:
        """Return the open connection or raise :class:`LLMCacheError` if not opened yet."""
        if self._conn is None:
            raise LLMCacheError(
                "LLMCache connection is not open; use as a context manager "
                "or call .open() first."
            )
        return self._conn


def _utcnow_iso() -> str:
    """Return the current UTC time formatted as ``YYYY-MM-DDTHH:MM:SSZ``."""
    return datetime.now(tz=UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
