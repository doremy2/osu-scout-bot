"""Round name normalization + importance weights.

Weights are deliberately small: they nudge the rating, they never dominate it.
"""
from __future__ import annotations

import re

# code -> (display name, sort order, weight)
ROUNDS: dict[str, tuple[str, int, float]] = {
    "Q":     ("Qualifiers",     0, 0.90),
    "GS":    ("Group Stage",    1, 1.00),
    "RO128": ("Round of 128",   2, 1.00),
    "RO64":  ("Round of 64",    3, 1.00),
    "RO32":  ("Round of 32",    4, 1.00),
    "RO16":  ("Round of 16",    5, 1.03),
    "QF":    ("Quarterfinals",  6, 1.06),
    "SF":    ("Semifinals",     7, 1.10),
    "F":     ("Finals",         8, 1.15),
    "GF":    ("Grand Finals",   9, 1.20),
}

# Order matters: more specific patterns first ("grand final" before "final", "semi" before "final").
_PATTERNS: list[tuple[str, str]] = [
    ("Q",     r"\bqual(ifier|ifiers|s)?\b"),
    ("GS",    r"\bgroup(s| stage)?\b|\bgs\b"),
    ("RO128", r"\bro\s?128\b|round of 128"),
    ("RO64",  r"\bro\s?64\b|round of 64"),
    ("RO32",  r"\bro\s?32\b|round of 32"),
    ("RO16",  r"\bro\s?16\b|round of 16"),
    ("GF",    r"\bgrand\s?finals?\b|\bgf\b"),
    ("SF",    r"\bsemi\s?-?\s?finals?\b|\bsf\b"),
    ("QF",    r"\bquarter\s?-?\s?finals?\b|\bqf\b"),
    ("F",     r"\bfinals?\b"),
]
_COMPILED = [(code, re.compile(p, re.IGNORECASE)) for code, p in _PATTERNS]


def normalize_round(text: str | None) -> str | None:
    """Map free text ("Losers Quarterfinals", "RO16 - Day 2") to a canonical code."""
    if not text:
        return None
    for code, rx in _COMPILED:
        if rx.search(text):
            return code
    return None


def round_weight(code: str | None) -> float:
    return ROUNDS.get(code or "", ("", 0, 1.0))[2]


def round_order(code: str | None) -> int:
    return ROUNDS.get(code or "", ("", -1, 1.0))[1]


def round_name(code: str | None) -> str:
    return ROUNDS.get(code or "", (code or "Unknown", 0, 1.0))[0]


# Rounds whose lobbies are not head-to-head matches (qualifier lobbies score players against the
# field), so they produce no match/map win-loss records.
NON_VERSUS_ROUNDS = {"Q"}


def is_versus_round(code: str | None) -> bool:
    return code not in NON_VERSUS_ROUNDS
