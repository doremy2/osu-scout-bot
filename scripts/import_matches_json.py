import json
from pathlib import Path

from storage import DB_PATH, insert_matches

JSON_PATH = Path("data/matches.json")


def main() -> None:
    if not JSON_PATH.exists():
        raise FileNotFoundError(f"Missing file: {JSON_PATH}")

    raw_matches = json.loads(JSON_PATH.read_text(encoding="utf-8"))
    inserted = insert_matches(
        raw_matches,
        source_file=str(JSON_PATH),
        source_type="json",
    )

    print(f"Imported {inserted} rows into {DB_PATH}")


if __name__ == "__main__":
    main()

