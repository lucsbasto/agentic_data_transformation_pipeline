"""Entry point for `python -m pipeline`."""

from __future__ import annotations

import sys


def main(argv: list[str] | None = None) -> int:
    """CLI dispatch. Real subcommands land in pipeline/cli/ (see F1.6)."""
    args = sys.argv[1:] if argv is None else argv
    if not args:
        print("usage: python -m pipeline <subcommand> [options]", file=sys.stderr)
        print("subcommands: (none registered yet)", file=sys.stderr)
        return 2
    sys.stderr.write(f"unknown subcommand: {args[0]}\n")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
