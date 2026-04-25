from __future__ import annotations

import argparse
import asyncio
import sys

from sync_app.cli.parser import build_parser


def windows_selector_loop_factory():
    return asyncio.SelectorEventLoop()

def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    normalized_argv = _normalize_legacy_command_aliases(list(argv) if argv is not None else sys.argv[1:])
    args = parser.parse_args(normalized_argv)
    return int(args.handler(args))

def _normalize_legacy_command_aliases(argv: list[str]) -> list[str]:
    if not argv:
        return []
    normalized = list(argv)
    if normalized[0] == "test-wecom":
        normalized[0] = "test-source"
    return normalized
