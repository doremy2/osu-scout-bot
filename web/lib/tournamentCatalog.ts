import { existsSync, readFileSync } from "node:fs";
import path from "node:path";
import type { TournamentCatalog, TournamentEntry } from "./types";

type RawStageRow = Record<string, unknown> & {
  tournament_key?: string;
  tournament_name?: string;
  year?: number;
  source_url?: string | null;
  forum_url?: string | null;
  wiki_url?: string | null;
  rank_range?: string | null;
  team_size?: string | number | null;
  format?: string | null;
  data_quality?: string | null;
  start_date?: string | null;
  end_date?: string | null;
  game_mode?: string | null;
  player_count?: number | null;
  match_count?: number | null;
  verified_ratio?: number | null;
  stage_url?: string | null;
  classification?: TournamentEntry["classification"];
  metadata_json?: {
    stage?: {
      raw?: {
        abbreviation?: string | null;
        rankRangeLowerBound?: number | null;
      };
    };
  };
};

type RawStageDiscovery = {
  rows?: RawStageRow[];
};

type RawImportQueue = {
  items?: RawStageRow[];
};

type RawPowerEvent = {
  metadata?: {
    event?: string | null;
    tournament_player_count?: number | null;
    tournament_tier?: string | null;
  };
};

type CatalogQuery = {
  year?: number;
  game_mode?: string;
  classification?: string;
  import_status?: string;
  limit?: number;
};

const PROJECT_ROOT = existsSync(path.join(process.cwd(), "data"))
  ? process.cwd()
  : path.resolve(process.cwd(), "..");
const STAGE_DISCOVERY_PATH = path.join(PROJECT_ROOT, "data", "reports", "stage_tournament_discovery.json");
const STAGE_QUEUE_PATH = path.join(PROJECT_ROOT, "data", "import_queue_stage.json");
const POWER_EVENTS_PATH = path.join(PROJECT_ROOT, "data", "power_events_multi_event.json");
const SUPPORTED_YEARS = new Set([2024, 2025, 2026]);

function readJson<T>(filePath: string, fallback: T): T {
  try {
    return JSON.parse(readFileSync(filePath, "utf8")) as T;
  } catch {
    return fallback;
  }
}

function slugify(value: string): string {
  return value
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 96);
}

function compactKey(value: string | null | undefined): string {
  return (value || "").toLowerCase().replace(/[^a-z0-9]/g, "");
}

function expandShortYearKey(value: string): string {
  return value.replace(/(\D)(\d{2})$/, (_match, prefix: string, year: string) => {
    const fullYear = Number(year) >= 70 ? `19${year}` : `20${year}`;
    return `${prefix}${fullYear}`;
  });
}

function numberOrNull(value: unknown): number | null {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string" && value.trim()) {
    const parsed = Number(value);
    if (Number.isFinite(parsed)) return parsed;
  }
  return null;
}

function stringOrNull(value: unknown): string | null {
  if (typeof value === "string" && value.trim()) return value.trim();
  if (typeof value === "number" && Number.isFinite(value)) return String(value);
  return null;
}

function acronymFromName(name: string): string {
  const words = name.match(/[A-Za-z0-9]+/g) || [];
  const acronym = words
    .filter((word) => !["osu", "standard", "the", "of", "and"].includes(word.toLowerCase()))
    .map((word) => word[0])
    .join("")
    .slice(0, 8);
  return acronym || name.slice(0, 6).toUpperCase();
}

function rankRange(row: RawStageRow): { rank_range: string | null; rank_type: TournamentEntry["rank_type"] } {
  const explicit = stringOrNull(row.rank_range);
  if (explicit) {
    return {
      rank_range: explicit,
      rank_type: /open/i.test(explicit) ? "open" : "restricted",
    };
  }

  const lowerBound = numberOrNull(row.metadata_json?.stage?.raw?.rankRangeLowerBound);
  if (lowerBound === null) return { rank_range: null, rank_type: "unknown" };
  if (lowerBound <= 1) return { rank_range: "Open rank", rank_type: "open" };
  return { rank_range: `Rank #${lowerBound.toLocaleString()}+`, rank_type: "restricted" };
}

function importedEventKeys(): Set<string> {
  const events = readJson<RawPowerEvent[]>(POWER_EVENTS_PATH, []);
  const keys = new Set<string>();
  for (const row of events) {
    const event = row.metadata?.event;
    if (!event) continue;
    keys.add(compactKey(event));
    keys.add(compactKey(event.replace(/\s+/g, "")));
  }
  return keys;
}

function isImported(row: RawStageRow, importedKeys: Set<string>): boolean {
  const name = row.tournament_name || "";
  const acronym = row.metadata_json?.stage?.raw?.abbreviation || "";
  const year = row.year ? String(row.year) : "";
  const candidates = [name, acronym, expandShortYearKey(compactKey(acronym)), `${acronym} ${year}`, `${name} ${year}`]
    .map(compactKey);

  for (const candidate of candidates) {
    if (!candidate) continue;
    for (const key of importedKeys) {
      if (candidate === key) return true;
    }
  }
  return false;
}

function importQueueKeys(): Set<string> {
  const queue = readJson<RawImportQueue>(STAGE_QUEUE_PATH, { items: [] });
  return new Set(
    (queue.items || [])
      .flatMap((item) => [item.tournament_name, item.stage_url, item.source_url, item.tournament_key])
      .map((value) => compactKey(String(value || "")))
      .filter(Boolean),
  );
}

function isQueued(row: RawStageRow, queueKeys: Set<string>): boolean {
  return [row.tournament_name, row.stage_url, row.source_url, row.tournament_key]
    .map((value) => compactKey(String(value || "")))
    .some((key) => key && queueKeys.has(key));
}

function toEntry(row: RawStageRow, importedKeys: Set<string>, queueKeys: Set<string>): TournamentEntry | null {
  const name = stringOrNull(row.tournament_name);
  const year = numberOrNull(row.year);
  if (!name || !year || !SUPPORTED_YEARS.has(year)) return null;
  if ((row.game_mode || "").toLowerCase() !== "osu") return null;

  const acronym = stringOrNull(row.metadata_json?.stage?.raw?.abbreviation) || acronymFromName(name);
  const imported = isImported(row, importedKeys);
  const queued = isQueued(row, queueKeys);
  const { rank_range, rank_type } = rankRange(row);
  const classification = imported
    ? "imported"
    : queued
      ? "likely_importable"
      : row.classification || "partial";

  return {
    slug: slugify(`${year}-${acronym}-${name}-${row.tournament_key || row.stage_url || ""}`),
    name,
    acronym,
    year,
    game_mode: "osu",
    format: stringOrNull(row.format),
    rank_range,
    rank_type,
    team_size: stringOrNull(row.team_size),
    start_date: stringOrNull(row.start_date),
    end_date: stringOrNull(row.end_date),
    stage_url: stringOrNull(row.stage_url) || stringOrNull(row.source_url),
    forum_url: stringOrNull(row.forum_url),
    wiki_url: stringOrNull(row.wiki_url),
    source_url: stringOrNull(row.source_url),
    player_count: numberOrNull(row.player_count),
    match_count: numberOrNull(row.match_count),
    map_score_count: null,
    verified_ratio: numberOrNull(row.verified_ratio),
    classification,
    import_status: imported ? "imported" : "discovered",
    tier: null,
    data_quality: stringOrNull(row.data_quality),
  };
}

function supplementalImportedRows(existing: TournamentEntry[], importedKeys: Set<string>): TournamentEntry[] {
  const events = readJson<RawPowerEvent[]>(POWER_EVENTS_PATH, []);
  const existingKeys = new Set(
    existing.flatMap((entry) => [
      compactKey(entry.name),
      compactKey(entry.acronym),
      expandShortYearKey(compactKey(entry.acronym)),
    ]),
  );
  const byEvent = new Map<string, { player_count: number | null; tier: string | null }>();

  for (const row of events) {
    const event = row.metadata?.event;
    if (!event) continue;
    const current = byEvent.get(event) || { player_count: null, tier: null };
    current.player_count = Math.max(current.player_count || 0, row.metadata?.tournament_player_count || 0) || null;
    current.tier = current.tier || row.metadata?.tournament_tier || null;
    byEvent.set(event, current);
  }

  return [...byEvent.entries()]
    .filter(([event]) => importedKeys.has(compactKey(event)) && !existingKeys.has(compactKey(event)))
    .map(([event, meta]) => {
      const year = numberOrNull(event.match(/20\d{2}/)?.[0]) || 2025;
      return {
        slug: slugify(`${year}-${event}`),
        name: event,
        acronym: event.replace(/\s*20\d{2}.*/, ""),
        year,
        game_mode: "osu",
        format: null,
        rank_range: null,
        rank_type: "unknown",
        team_size: null,
        start_date: null,
        end_date: null,
        stage_url: null,
        forum_url: null,
        wiki_url: null,
        source_url: null,
        player_count: meta.player_count,
        match_count: null,
        map_score_count: null,
        verified_ratio: null,
        classification: "imported",
        import_status: "imported",
        tier: meta.tier,
        data_quality: "imported",
      } satisfies TournamentEntry;
    });
}

export function loadTournamentCatalog(query: CatalogQuery = {}): TournamentCatalog {
  const importedKeys = importedEventKeys();
  const queueKeys = importQueueKeys();
  const discovery = readJson<RawStageDiscovery>(STAGE_DISCOVERY_PATH, { rows: [] });
  const stageRows = (discovery.rows || [])
    .map((row) => toEntry(row, importedKeys, queueKeys))
    .filter((row): row is TournamentEntry => row !== null);
  const rows = [...stageRows, ...supplementalImportedRows(stageRows, importedKeys)]
    .filter((row) => !query.year || row.year === query.year)
    .filter((row) => !query.game_mode || row.game_mode === query.game_mode)
    .filter((row) => !query.classification || row.classification === query.classification)
    .filter((row) => !query.import_status || row.import_status === query.import_status)
    .sort((a, b) => {
      if (b.year !== a.year) return b.year - a.year;
      const bDate = b.start_date || b.end_date || "";
      const aDate = a.start_date || a.end_date || "";
      if (bDate !== aDate) return bDate.localeCompare(aDate);
      return a.name.localeCompare(b.name);
    });

  const limitedRows = rows.slice(0, query.limit ?? rows.length);
  return {
    total: rows.length,
    imported_count: rows.filter((row) => row.import_status === "imported").length,
    discovered_count: rows.filter((row) => row.import_status !== "imported").length,
    rows: limitedRows,
  };
}

export function loadTournamentDetail(slug: string): TournamentEntry | null {
  return loadTournamentCatalog().rows.find((row) => row.slug === slug) || null;
}
