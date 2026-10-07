#!/usr/bin/env bash
# Consistent online backup of the SQLite database (safe while the API is running).
# Cron example (as the scout user):  15 4 * * * /opt/osu-scout/deploy/backup.sh
set -euo pipefail

DB="${SCOUT_DB:-/opt/osu-scout/data/scout.db}"
DEST="${BACKUP_DIR:-/opt/osu-scout/backups}"
KEEP_DAYS="${KEEP_DAYS:-14}"

mkdir -p "$DEST"
STAMP="$(date +%Y%m%d-%H%M%S)"
sqlite3 "$DB" ".backup '$DEST/scout-$STAMP.db'"
gzip -f "$DEST/scout-$STAMP.db"
find "$DEST" -name 'scout-*.db.gz' -mtime +"$KEEP_DAYS" -delete
echo "backup written: $DEST/scout-$STAMP.db.gz"
