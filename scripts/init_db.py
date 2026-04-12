from storage import DB_PATH, init_db


def main() -> None:
    init_db()
    print(f"Database ready: {DB_PATH}")


if __name__ == "__main__":
    main()
