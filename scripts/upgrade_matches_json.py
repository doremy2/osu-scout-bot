import json
from pathlib import Path

from storage import normalize_match

JSON_PATH = Path("data/matches.json")
BACKUP_PATH = Path("data/matches.backup.json")


def main() -> None:
    if not JSON_PATH.exists():
        raise FileNotFoundError(f"Missing file: {JSON_PATH}")

    original_rows = json.loads(JSON_PATH.read_text(encoding="utf-8"))

    if not BACKUP_PATH.exists():
        BACKUP_PATH.write_text(
            json.dumps(original_rows, indent=2),
            encoding="utf-8",
        )

    upgraded_rows = [
        normalize_match(
            row,
            source_file=str(JSON_PATH),
            source_type="json",
        )
        for row in original_rows
    ]

    JSON_PATH.write_text(
        json.dumps(upgraded_rows, indent=2),
        encoding="utf-8",
    )

    print(f"Upgraded JSON written to {JSON_PATH}")
    print(f"Backup created at {BACKUP_PATH}")


if __name__ == "__main__":
    main()

