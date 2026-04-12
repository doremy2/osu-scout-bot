
from __future__ import annotations

import sys
from collections import Counter
from pathlib import Path

from importers.owc_csv import parse_owc_player_leaderboard_csv
from storage import DB_PATH, insert_matches


DEFAULT_CSV_PATH = Path("data/raw/owc_2025_grand_finals_leaderboards.csv")


def main() -> None:
    csv_path = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_CSV_PATH

    if not csv_path.exists():
        raise FileNotFoundError(
            f"Missing CSV file: {csv_path}\n"
            f"Put the OWC CSV there or run:\n"
            f"python -m scripts.import_owc_csv \"path/to/file.csv\""
        )

    rows = parse_owc_player_leaderboard_csv(csv_path)
    inserted = insert_matches(rows, source_file=str(csv_path), source_type="csv")

    players = Counter(row["player"] for row in rows)
    slots = Counter(row["slot"] for row in rows)

    print(f"Imported {inserted} rows into {DB_PATH}")
    print(f"Unique players: {len(players)}")
    print("Top players by imported rows:")
    for player, count in players.most_common(10):
        print(f"  - {player}: {count}")

    print("Slots detected:")
    for slot, count in slots.items():
        print(f"  - {slot}: {count}")


if __name__ == "__main__":
    main()
    