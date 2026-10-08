"""Which osu! client a tournament was played on. It decides how matches are fetched and how scores are read:

  stable  classic multiplayer matches (osu.ppy.sh/community/matches/<id>), legacy score / scoreV2
  lazer   multiplayer rooms (osu.ppy.sh/multiplayer/rooms/<id>), standardised score, lazer-only mods (DA, ...)
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class OsuClientKind:
    key: str
    label: str
    description: str


CLIENTS: dict[str, OsuClientKind] = {c.key: c for c in (
    OsuClientKind("stable", "Stable", "Classic multiplayer matches (osu.ppy.sh/community/matches/…). Legacy scoring, standard mods."),
    OsuClientKind("lazer", "Lazer", "Multiplayer rooms (osu.ppy.sh/multiplayer/rooms/…). Standardised scoring and lazer-only mods."),
)}
DEFAULT_CLIENT = "stable"

# Mods that exist in stable. Anything else in a lazer score (DA, AC, BL, ...) puts the map in the "LM" (lazer mod) bucket.
STABLE_MODS = {"NF", "EZ", "HD", "HR", "SD", "PF", "DT", "NC", "HT", "FL", "SO", "TD", "RX", "AP", "MR"}
# Mods that never change what a map "is" for pool purposes. DC (daycore) is a half-time variant; CL is the classic-scoring mod.
NEUTRAL_MODS = {"NF", "SD", "PF", "TD", "SO", "MR", "CL", "RX0"}


def validate_client(key: str | None) -> str:
    if key not in CLIENTS:
        raise ValueError(f"Unknown osu! client {key!r}; expected one of {', '.join(CLIENTS)}")
    return key


def lazer_only(mods) -> set[str]:
    """Acronyms in `mods` that stable doesn't have."""
    return {m for m in mods if m not in STABLE_MODS and m not in NEUTRAL_MODS and m != "DC"}
