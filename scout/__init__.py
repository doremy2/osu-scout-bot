"""osu! scout — tournament ingestion + analytics engine.

Layers (each only depends on the ones above it):
  sources/   tournament source -> MatchLink list           (sheets, CSV, XLSX, text; later forum/website)
  osu/       osu! API client + payload parser -> ParsedMatch
  db/        SQLite schema + repository (the only code that writes)
  ingest     orchestrates sources -> API -> db, resumable
  classify   mod bucket / warmup / exclusion per game (re-runnable)
  analytics/ ratings, awards, report dict (consumed by CLI, Discord bot, website)
"""
