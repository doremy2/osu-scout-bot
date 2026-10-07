"""Generate osu!-API-shaped match payloads for a fake 2v2 tournament with known skill levels."""
from __future__ import annotations

import random

POOL = (
    [(1000 + i, f"NM{i}", []) for i in range(1, 5)]
    + [(2000 + i, f"HD{i}", ["HD"]) for i in range(1, 3)]
    + [(3000 + i, f"HR{i}", ["HR"]) for i in range(1, 3)]
    + [(4000 + i, f"DT{i}", ["DT"]) for i in range(1, 4)]
    + [(5000 + i, f"FM{i}", []) for i in range(1, 3)]
    + [(6000, "TB", [])]
)
WARMUP_MAP = 9999
# map difficulty: average score scale (hard maps -> low average)
MAP_MEAN = {bid: random.Random(bid).uniform(450_000, 900_000) for bid, _, _ in POOL}
MAP_MEAN[WARMUP_MAP] = 800_000

# user_id -> (name, base skill, mod bonus)
PLAYERS = {
    1: ("Alpha", 1.6, {"DT": 0.6}),
    2: ("Bravo", 0.8, {"HR": 1.6}),
    3: ("Charlie", 0.0, {}),
    4: ("Delta", -0.4, {"HD": 1.0}),
    5: ("Echo", 0.4, {}),
    6: ("Foxtrot", -1.0, {}),
    7: ("Golf", 0.2, {"DT": -1.0}),
    8: ("Hotel", -0.6, {}),
}
TEAMS = {"Team A": [1, 2], "Team B": [3, 4], "Team C": [5, 6], "Team D": [7, 8]}


def _score(rng, uid, bid, slot):
    _, skill, bonus = PLAYERS[uid]
    mod = slot[:2]
    perf = skill + bonus.get(mod, 0) + rng.gauss(0, 0.5)
    mean = MAP_MEAN[bid]
    return int(max(50_000, min(1_000_000, mean + perf * 90_000)))


def make_match(match_id: int, red: str, blue: str, rng: random.Random, n_maps: int = 9) -> dict:
    events, ev_id = [], match_id * 1000
    maps = [(WARMUP_MAP, "WU", [])] + rng.sample(POOL[:-1], n_maps - 1) + [POOL[-1]]
    for gi, (bid, slot, mods) in enumerate(maps):
        ev_id += 1
        scores = []
        for team, color in ((red, "red"), (blue, "blue")):
            for uid in TEAMS[team]:
                pmods = list(mods) + ["NF"]
                if slot.startswith("FM"):
                    pmods = rng.choice([["HD"], ["HR"], []]) + ["NF"]
                scores.append({
                    "user_id": uid, "score": _score(rng, uid, bid, slot), "accuracy": rng.uniform(0.9, 0.995),
                    "max_combo": rng.randint(300, 1500), "mods": pmods,
                    "statistics": {"count_300": 900, "count_100": 30, "count_50": 2, "count_miss": rng.randint(0, 6)},
                    "passed": True, "match": {"slot": len(scores), "team": color, "pass": True},
                })
        events.append({"id": ev_id, "detail": {"type": "other"}, "timestamp": "2027-01-01T00:00:00Z", "game": {
            "id": match_id * 100 + gi, "beatmap_id": bid, "mods": list(mods) + ["NF"], "mode": "osu",
            "scoring_type": "scorev2", "team_type": "team-vs",
            "start_time": f"2027-01-0{1 + match_id % 9}T12:{gi:02d}:00Z",
            "end_time": f"2027-01-0{1 + match_id % 9}T12:{gi:02d}:59Z",
            "beatmap": {"id": bid, "beatmapset_id": bid * 10, "version": slot, "difficulty_rating": 6.5,
                        "beatmapset": {"artist": "Artist", "title": f"Song {bid}"}},
            "scores": scores,
        }})
    return {
        "match": {"id": match_id, "name": f"FAKE: ({red}) vs ({blue})",
                  "start_time": f"2027-01-0{1 + match_id % 9}T12:00:00Z", "end_time": None},
        "events": events,
        "users": [{"id": u, "username": PLAYERS[u][0], "country_code": "US"} for u in PLAYERS],
        "first_event_id": events[0]["id"], "latest_event_id": events[-1]["id"],
    }


class FakeClient:
    def __init__(self, payloads: dict[int, dict]):
        self.payloads = payloads
        self.calls = 0

    def get_match_full(self, match_id: int) -> dict:
        self.calls += 1
        from scout.osu import MatchNotFound
        if match_id not in self.payloads:
            raise MatchNotFound(str(match_id))
        return self.payloads[match_id]


def build(seed: int = 7):
    """Round-robin RO16-ish + semis + final. Returns (payloads, {match_id: round})."""
    rng = random.Random(seed)
    names = list(TEAMS)
    payloads, rounds, mid = {}, {}, 111000
    for rnd in ("Group Stage", "Group Stage", "Semifinals"):
        for i in range(len(names)):
            for j in range(i + 1, len(names)):
                mid += 1
                payloads[mid] = make_match(mid, names[i], names[j], rng)
                rounds[mid] = rnd
    mid += 1
    payloads[mid] = make_match(mid, "Team A", "Team B", rng, n_maps=11)
    rounds[mid] = "Grand Finals"
    return payloads, rounds
