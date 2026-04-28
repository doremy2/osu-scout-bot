"""Ban/pick suggestion engine for osu! scout.

Generates data-driven draft advice based on per-slot performance data.
Every suggestion includes an explanation and confidence score so captains
know *why* a ban or pick is recommended and how much to trust it.

Inputs:  slot-level aggregated stats (from database.fetch_player_slot_stats
         or analysis.build_slot_stats).
Outputs: ranked lists of suggested bans, picks, and comfort zones with
         explanations and confidence ratings.

Confidence model:
  - sample_confidence: based on how many maps were played on that slot
    (1 map = very low, 3-5 = moderate, 8+ = high)
  - recency_weight: optional future extension — recent maps weigh more
  - consistency: low score variance = more predictable = higher confidence
  - final confidence = min(sample_confidence, consistency_confidence)

The engine is source-agnostic: it works on the same shape of slot stats
whether they come from OWC, ROMAI, or any future source.
"""

from __future__ import annotations

import math
import statistics
from dataclasses import dataclass, field
from typing import Any


# ─── Configuration ────────────────────────────────────────────────

# Minimum maps on a slot to produce any suggestion at all
MIN_SAMPLE = 2
# Maps needed for "high confidence"
HIGH_SAMPLE = 8
# Maps needed for "moderate confidence"
MID_SAMPLE = 4


@dataclass
class SlotSuggestion:
    """One ban/pick suggestion for a single slot."""
    slot: str
    action: str              # "ban", "pick", "comfort", "avoid"
    reason: str              # human-readable explanation
    confidence: float        # 0.0–1.0
    confidence_label: str    # "high", "moderate", "low", "very low"
    metrics: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "slot": self.slot,
            "action": self.action,
            "reason": self.reason,
            "confidence": round(self.confidence, 2),
            "confidence_label": self.confidence_label,
            "metrics": self.metrics,
        }


@dataclass
class DraftAdvice:
    """Complete ban/pick advice for one player or team."""
    player: str
    suggested_bans: list[SlotSuggestion] = field(default_factory=list)
    suggested_picks: list[SlotSuggestion] = field(default_factory=list)
    comfort_picks: list[SlotSuggestion] = field(default_factory=list)
    risky_slots: list[SlotSuggestion] = field(default_factory=list)
    slot_rankings: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "player": self.player,
            "suggested_bans": [s.to_dict() for s in self.suggested_bans],
            "suggested_picks": [s.to_dict() for s in self.suggested_picks],
            "comfort_picks": [s.to_dict() for s in self.comfort_picks],
            "risky_slots": [s.to_dict() for s in self.risky_slots],
            "slot_rankings": self.slot_rankings,
        }


# ─── Confidence helpers ──────────────────────────────────────────

def _sample_confidence(n: int) -> float:
    """Maps sample count to 0–1 confidence."""
    if n <= 0:
        return 0.0
    if n >= HIGH_SAMPLE:
        return 1.0
    # Logarithmic ramp: grows fast from 1→4, then diminishing returns
    return min(1.0, math.log2(n + 1) / math.log2(HIGH_SAMPLE + 1))


def _consistency_confidence(scores: list[int | float]) -> float:
    """Low CV (coefficient of variation) = high consistency = higher confidence."""
    if len(scores) < 2:
        return 0.3  # can't assess with 1 data point
    mean = statistics.mean(scores)
    if mean == 0:
        return 0.5
    cv = statistics.stdev(scores) / mean
    # CV < 0.05 = very consistent (conf 1.0), CV > 0.30 = inconsistent (conf 0.3)
    return max(0.3, min(1.0, 1.0 - (cv - 0.05) / 0.25))


def _combined_confidence(n: int, scores: list[int | float]) -> tuple[float, str]:
    """Return (confidence_float, confidence_label)."""
    sc = _sample_confidence(n)
    cc = _consistency_confidence(scores) if scores else 0.3
    combined = min(sc, cc)
    if combined >= 0.75:
        label = "high"
    elif combined >= 0.50:
        label = "moderate"
    elif combined >= 0.30:
        label = "low"
    else:
        label = "very low"
    return combined, label


# ─── Core analysis ───────────────────────────────────────────────

def _build_enriched_slots(
    slot_stats: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    """Enrich raw slot stats with computed metrics."""
    enriched = []
    for slot, stats in slot_stats.items():
        played = stats.get("played") or stats.get("matches") or 0
        if played < MIN_SAMPLE:
            continue

        scores = stats.get("scores", [])
        if not scores and stats.get("avg_score") is not None:
            scores = [stats["avg_score"]] * played  # approximate

        avg_score = stats.get("avg_score")
        if avg_score is None and scores:
            avg_score = round(sum(scores) / len(scores))

        winrate = stats.get("winrate")
        if winrate is None:
            wins = stats.get("wins", 0)
            losses = stats.get("losses", 0)
            total_wl = wins + losses
            winrate = round(wins / total_wl * 100, 1) if total_wl > 0 else None

        avg_acc = stats.get("avg_accuracy")
        conf, conf_label = _combined_confidence(played, scores)

        # Score consistency (stdev / mean)
        consistency = None
        if len(scores) >= 2:
            mean = statistics.mean(scores)
            if mean > 0:
                consistency = round(statistics.stdev(scores) / mean * 100, 1)

        avg_sr = stats.get("avg_star_rating")
        if isinstance(avg_sr, str):
            avg_sr = None

        enriched.append({
            "slot": slot,
            "played": played,
            "avg_score": avg_score,
            "winrate": winrate,
            "avg_accuracy": avg_acc,
            "avg_star_rating": avg_sr,
            "effective_sr": stats.get("effective_sr"),
            "star_efficiency": stats.get("star_efficiency"),
            "consistency_cv": consistency,
            "confidence": conf,
            "confidence_label": conf_label,
            "scores": scores,
        })

    return enriched


def generate_ban_suggestions(
    player: str,
    opponent_slot_stats: dict[str, dict[str, Any]],
    *,
    top_n: int = 3,
) -> list[SlotSuggestion]:
    """What to BAN against this opponent: their strongest / most comfortable slots."""
    enriched = _build_enriched_slots(opponent_slot_stats)
    if not enriched:
        return []

    # Sort by: high winrate first, then high avg_score, then high confidence
    enriched.sort(key=lambda e: (
        -(e["winrate"] or 0),
        -(e["avg_score"] or 0),
        -e["confidence"],
    ))

    suggestions = []
    for e in enriched[:top_n]:
        wr_part = f"{e['winrate']}% WR" if e['winrate'] is not None else "strong scores"
        score_part = f"avg {e['avg_score']:,}" if e['avg_score'] else ""
        consistency_part = ""
        if e.get("consistency_cv") is not None and e["consistency_cv"] < 10:
            consistency_part = ", very consistent"

        reason = (
            f"Ban {e['slot']}: opponent has {wr_part}"
            + (f" ({score_part}{consistency_part})" if score_part else "")
            + f" across {e['played']} maps"
        )

        suggestions.append(SlotSuggestion(
            slot=e["slot"],
            action="ban",
            reason=reason,
            confidence=e["confidence"],
            confidence_label=e["confidence_label"],
            metrics={
                "winrate": e["winrate"],
                "avg_score": e["avg_score"],
                "played": e["played"],
                "consistency_cv": e.get("consistency_cv"),
            },
        ))

    return suggestions


def generate_pick_suggestions(
    player: str,
    own_slot_stats: dict[str, dict[str, Any]],
    opponent_slot_stats: dict[str, dict[str, Any]] | None = None,
    *,
    top_n: int = 3,
) -> list[SlotSuggestion]:
    """What to PICK: slots where we're strong and/or opponent is weak."""
    own = {e["slot"]: e for e in _build_enriched_slots(own_slot_stats)}
    opp = {e["slot"]: e for e in _build_enriched_slots(opponent_slot_stats)} if opponent_slot_stats else {}

    if not own:
        return []

    def _num(val: Any, default: float = 0) -> float:
        if isinstance(val, (int, float)):
            return float(val)
        return default

    scored: list[tuple[float, dict]] = []
    for slot, e in own.items():
        our_wr = _num(e["winrate"], 50)
        our_score = _num(e["avg_score"], 0)
        opp_wr = _num(opp.get(slot, {}).get("winrate"), 50)
        opp_score = _num(opp.get(slot, {}).get("avg_score"), 0)

        # Value = our strength + their weakness
        value = (our_wr - 50) + max(0, 50 - opp_wr) + (our_score - opp_score) / 10000
        scored.append((value, e))

    scored.sort(key=lambda x: -x[0])

    suggestions = []
    for value, e in scored[:top_n]:
        slot = e["slot"]
        parts = [f"our avg {e['avg_score']:,}" if e['avg_score'] else ""]
        if e["winrate"] is not None:
            parts.append(f"{e['winrate']}% WR")
        if slot in opp and opp[slot].get("winrate") is not None:
            parts.append(f"opponent only {opp[slot]['winrate']}% WR there")

        reason = f"Pick {slot}: " + ", ".join(p for p in parts if p)

        suggestions.append(SlotSuggestion(
            slot=slot,
            action="pick",
            reason=reason,
            confidence=e["confidence"],
            confidence_label=e["confidence_label"],
            metrics={
                "our_winrate": e["winrate"],
                "our_avg_score": e["avg_score"],
                "opponent_winrate": opp.get(slot, {}).get("winrate"),
                "opponent_avg_score": opp.get(slot, {}).get("avg_score"),
                "value_score": round(value, 1),
                "played": e["played"],
            },
        ))

    return suggestions


def generate_comfort_picks(
    player: str,
    slot_stats: dict[str, dict[str, Any]],
    *,
    top_n: int = 3,
) -> list[SlotSuggestion]:
    """Slots where the player is most comfortable: high play count + consistency."""
    enriched = _build_enriched_slots(slot_stats)
    if not enriched:
        return []

    # Comfort = played a lot + consistent + good scores
    enriched.sort(key=lambda e: (
        -e["played"],
        -(1.0 - (e.get("consistency_cv") or 50) / 100),  # low CV = good
        -(e["avg_score"] or 0),
    ))

    suggestions = []
    for e in enriched[:top_n]:
        cv_text = f"{e['consistency_cv']}% CV" if e.get("consistency_cv") is not None else "unknown consistency"
        reason = f"Comfort {e['slot']}: {e['played']} maps played, {cv_text}, avg {e['avg_score']:,}" if e['avg_score'] else f"Comfort {e['slot']}: {e['played']} maps played"

        suggestions.append(SlotSuggestion(
            slot=e["slot"],
            action="comfort",
            reason=reason,
            confidence=e["confidence"],
            confidence_label=e["confidence_label"],
            metrics={
                "played": e["played"],
                "consistency_cv": e.get("consistency_cv"),
                "avg_score": e["avg_score"],
                "winrate": e["winrate"],
            },
        ))

    return suggestions


def find_risky_slots(
    player: str,
    slot_stats: dict[str, dict[str, Any]],
    *,
    top_n: int = 3,
) -> list[SlotSuggestion]:
    """Slots to AVOID: low sample size or high variance = can't trust the data."""
    enriched = _build_enriched_slots(slot_stats)

    # Also include slots with very low sample that were filtered out
    for slot, stats in slot_stats.items():
        played = stats.get("played") or stats.get("matches") or 0
        if played > 0 and played < MIN_SAMPLE:
            enriched.append({
                "slot": slot,
                "played": played,
                "avg_score": stats.get("avg_score"),
                "winrate": None,
                "consistency_cv": None,
                "confidence": 0.1,
                "confidence_label": "very low",
            })

    if not enriched:
        return []

    # Risky = low confidence
    enriched.sort(key=lambda e: e["confidence"])

    suggestions = []
    for e in enriched[:top_n]:
        if e["confidence"] >= 0.6:
            continue  # not risky enough
        parts = []
        if e["played"] < MID_SAMPLE:
            parts.append(f"only {e['played']} maps played")
        if e.get("consistency_cv") is not None and e["consistency_cv"] > 20:
            parts.append(f"high variance ({e['consistency_cv']}% CV)")
        if not parts:
            parts.append("insufficient data")

        reason = f"Avoid {e['slot']}: {', '.join(parts)}"
        suggestions.append(SlotSuggestion(
            slot=e["slot"],
            action="avoid",
            reason=reason,
            confidence=e["confidence"],
            confidence_label=e["confidence_label"],
            metrics={
                "played": e["played"],
                "consistency_cv": e.get("consistency_cv"),
                "avg_score": e.get("avg_score"),
            },
        ))

    return suggestions


# ─── Top-level API ───────────────────────────────────────────────

def generate_draft_advice(
    player: str,
    own_slot_stats: dict[str, dict[str, Any]],
    opponent_slot_stats: dict[str, dict[str, Any]] | None = None,
    *,
    ban_count: int = 2,
    pick_count: int = 3,
) -> DraftAdvice:
    """Generate complete draft advice for a player against an opponent.

    If opponent_slot_stats is None, generates self-scouting advice only
    (comfort picks, risky slots, strongest/weakest areas).
    """
    advice = DraftAdvice(player=player)

    # Slot rankings (all enriched slots sorted by strength)
    enriched = _build_enriched_slots(own_slot_stats)
    enriched.sort(key=lambda e: -(e["avg_score"] or 0))
    advice.slot_rankings = [
        {
            "slot": e["slot"],
            "avg_score": e["avg_score"],
            "winrate": e["winrate"],
            "avg_star_rating": e.get("avg_star_rating"),
            "effective_sr": e.get("effective_sr"),
            "star_efficiency": e.get("star_efficiency"),
            "played": e["played"],
            "confidence": e["confidence_label"],
        }
        for e in enriched
    ]

    # Self-assessment
    advice.comfort_picks = generate_comfort_picks(player, own_slot_stats)
    advice.risky_slots = find_risky_slots(player, own_slot_stats)

    # Opponent-relative suggestions
    if opponent_slot_stats:
        advice.suggested_bans = generate_ban_suggestions(
            player, opponent_slot_stats, top_n=ban_count,
        )
        advice.suggested_picks = generate_pick_suggestions(
            player, own_slot_stats, opponent_slot_stats, top_n=pick_count,
        )
    else:
        # Without opponent data, just pick own strongest
        advice.suggested_picks = generate_pick_suggestions(
            player, own_slot_stats, top_n=pick_count,
        )

    return advice
