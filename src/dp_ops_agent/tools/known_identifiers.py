"""Formatting for the identifiers an "unknown" tool result lists.

A tool that finds no data for the identifier it was given names the ones
that do exist, so the model's one allowed retry (system prompt rule 5) can
land on a real one instead of a guess. See ADR-0009. Lists are capped so a
large cluster's topic list can't flood the context.
"""

from __future__ import annotations

MAX_LISTED = 20


def capped[T](items: list[T]) -> list[T]:
    return items[:MAX_LISTED]


def describe(names: list[str]) -> str:
    """ "a, b, c (+4 more)", or "none" for an empty list."""
    if not names:
        return "none"
    shown = ", ".join(names[:MAX_LISTED])
    extra = len(names) - MAX_LISTED
    return f"{shown} (+{extra} more)" if extra > 0 else shown
