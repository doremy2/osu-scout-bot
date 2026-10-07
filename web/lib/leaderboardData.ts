import { existsSync, readFileSync, readdirSync } from "node:fs";
import path from "node:path";
import type {
  CountryPowerRow,
  LeaderboardFormat,
  LeaderboardRow,
  PlayerPower,
  RecentMatch,
  RecentTournamentEvent,
  TournamentStat,
} from "./types";

type RawLeaderboardRow = Record<string, unknown> & {
  rank?: number;
  username?: string;
  profile_username?: string | null;
  aliases?: string[];
  user_id?: number | null;
  country?: string | null;
  country_code?: string | null;
  tier?: string;
  final_power_score?: number;
  recent_tournament_form?: number;
  consistency_score?: number;
  reliability_multiplier?: number;
  activity_multiplier?: number;
  tournaments_played_last_12m?: number;
  unique_tournaments_count?: number;
  dominant_event?: string | null;
  dominant_event_score_share?: number;
  team_world_cup_score_share?: number;
  previous_rank?: number | null;
  rank_jump?: number | null;
  confidence_label?: "high" | "medium" | "low";
  warning_flags?: string[];
  provisional?: boolean;
  activity_status?: string | null;
  days_since_last_event?: number | null;
  top_recent_events?: RecentTournamentEvent[];
  explanation?: string;
  bancho_rank?: number | null;
  country_rank?: number | null;
  pp?: number | null;
};

type RawPowerEvent = RecentTournamentEvent & {
  username?: string;
  metadata?: {
    event?: string | null;
    stage?: string | null;
    source?: string | null;
    stage_url?: string | null;
    forum_url?: string | null;
    abbreviation?: string | null;
    matches_played?: number | null;
    matches_won?: number | null;
    matches_lost?: number | null;
    games_played?: number | null;
    games_won?: number | null;
    games_lost?: number | null;
    average_rating_delta?: number | null;
    rating_before?: number | null;
    rating_after?: number | null;
    stage_osu_id?: number | null;
  };
};

type PackagePlayer = {
  event?: string;
  player?: string;
  user_id?: number | null;
  team_code?: string | null;
};

type PackageMatch = {
  event?: string;
  stage?: string | null;
  team?: string | null;
  team_code?: string | null;
  opponent_team?: string | null;
  team_score?: number | null;
  opponent_score?: number | null;
  result?: string | null;
  match_link?: string | null;
  date?: string | null;
  source?: string | null;
};

type PackageScore = {
  player?: string;
  user_id?: number | null;
  event?: string;
  stage?: string | null;
  date?: string | null;
  score?: number | null;
  accuracy?: number | null;
  result?: string | null;
  player_team?: string | null;
  opponent_team?: string | null;
};

type VerifiedPackage = {
  players?: PackagePlayer[];
  matches?: PackageMatch[];
  map_scores?: PackageScore[];
};

type StageHistoryRow = {
  tournament_id?: number | null;
  tournament_name?: string | null;
  abbreviation?: string | null;
  year?: number | null;
  forum_url?: string | null;
  stage_url?: string | null;
  rank_range_lower_bound?: number | null;
  lobby_size?: number | null;
  end_date?: string | null;
  start_date?: string | null;
  player_id?: number | null;
  osu_id?: number | null;
  username?: string | null;
  country?: string | null;
  average_rating_delta?: number | null;
  average_match_cost?: number | null;
  average_score?: number | null;
  average_placement?: number | null;
  average_accuracy?: number | null;
  matches_played?: number | null;
  matches_won?: number | null;
  matches_lost?: number | null;
  games_played?: number | null;
  games_won?: number | null;
  games_lost?: number | null;
  match_win_rate?: number | null;
  rating_before?: number | null;
  rating_after?: number | null;
};

type PackageData = {
  playerTeams: Map<string, PackagePlayer[]>;
  playerScores: Map<string, PackageScore[]>;
  userIdToNames: Map<number, Set<string>>;
  matches: PackageMatch[];
};

type LeaderboardQuery = {
  tier?: string;
  country?: string;
  format?: LeaderboardFormat;
  limit?: number;
  offset?: number;
};

const PROJECT_ROOT = existsSync(path.join(process.cwd(), "data"))
  ? process.cwd()
  : path.resolve(process.cwd(), "..");
const LEADERBOARD_PATH = path.join(PROJECT_ROOT, "data", "leaderboard_multi_event.json");
const POWER_EVENTS_PATH = path.join(PROJECT_ROOT, "data", "power_events_multi_event.json");
const VERIFIED_PACKAGES_DIR = path.join(PROJECT_ROOT, "data", "packages", "verified");
const STAGE_HISTORY_PATH = path.join(PROJECT_ROOT, "data", "stage_player_tournament_history.json");

const EVENT_FORMATS: Record<string, LeaderboardFormat> = {
  "OIT 2025": "1v1",
  "FDC 2025": "2v2",
  "3WC 2025": "3v3",
  "RESC 2025": "3v3",
  "OWC 2025": "4v4",
  "4WC 2025": "4v4",
};

function readJson<T>(filePath: string, fallback: T): T {
  try {
    return JSON.parse(readFileSync(filePath, "utf8")) as T;
  } catch {
    return fallback;
  }
}

function packageEventName(fileName: string): string {
  return fileName
    .replace(/\.json$/i, "")
    .split("_")
    .map((part) => part.toUpperCase())
    .join(" ");
}

function countryName(countryCode: string | null): string {
  if (!countryCode) return "Unknown country";
  try {
    return new Intl.DisplayNames(["en"], { type: "region" }).of(countryCode.toUpperCase()) || countryCode;
  } catch {
    return countryCode.toUpperCase();
  }
}

function countryFlagUrl(countryCode: string | null): string | null {
  if (!countryCode) return null;
  return `https://flagcdn.com/w40/${countryCode.toLowerCase()}.png`;
}

function eventFormat(event: string | null | undefined): LeaderboardFormat | null {
  if (!event) return null;
  if (EVENT_FORMATS[event]) return EVENT_FORMATS[event];
  const compact = event.toLowerCase().replace(/[^a-z0-9]/g, "");
  if (compact.includes("oit2025")) return "1v1";
  if (compact.includes("fdc2025") || compact.includes("finnishduocup2025")) return "2v2";
  if (compact.includes("3wc2025") || compact.includes("resc2025")) return "3v3";
  if (compact.includes("owc2025") || compact.includes("4wc2025")) return "4v4";
  return null;
}

function compactEventKey(value: string | null | undefined): string {
  return String(value || "").toLowerCase().replace(/[^a-z0-9]/g, "");
}

function importedEventAliasKeys(eventName: string): string[] {
  const key = compactEventKey(eventName);
  const aliases: Record<string, string[]> = {
    owc2025: ["osuworldcup2025"],
    resc2025: ["resurrectioncup2025"],
    "3wc2025": ["3digitworldcup2025"],
    "4wc2025": ["4digitworldcup2025"],
    fdc2025: ["finnishduocup2025"],
  };
  return [key, ...(aliases[key] || [])];
}

function rowFormats(row: RawLeaderboardRow): LeaderboardFormat[] {
  const formats = new Set<LeaderboardFormat>();
  for (const event of row.top_recent_events || []) {
    const format = eventFormat(event.event || event.event_name);
    if (format) formats.add(format);
  }
  return [...formats];
}

function toPublicRow(row: RawLeaderboardRow): LeaderboardRow {
  const countryCode = (row.country_code || row.country || null)?.toUpperCase() || null;
  const userId = typeof row.user_id === "number" ? row.user_id : null;
  return {
    rank: Number(row.rank || 0),
    username: String(row.username || "Unknown"),
    user_id: userId,
    avatar_url: userId ? `https://a.ppy.sh/${userId}` : null,
    aliases: Array.isArray(row.aliases) ? row.aliases : [],
    country_code: countryCode,
    country_name: countryName(countryCode),
    country_flag_url: countryFlagUrl(countryCode),
    formats: rowFormats(row),
    tier: row.tier === "Tier 1" || row.tier === "Tier 2" || row.tier === "Tier 3" ? row.tier : "Tier 3",
    final_power_score: Number(row.final_power_score || 0),
    recent_tournament_form: Number(row.recent_tournament_form || 0),
    consistency_score: Number(row.consistency_score || 0),
    reliability_multiplier: Number(row.reliability_multiplier || 0),
    activity_multiplier: Number(row.activity_multiplier || 0),
    unique_tournaments_count: Number(row.unique_tournaments_count || 0),
    dominant_event: row.dominant_event || null,
    dominant_event_score_share: Number(row.dominant_event_score_share || 0),
    team_world_cup_score_share: Number(row.team_world_cup_score_share || 0),
    previous_rank: typeof row.previous_rank === "number" ? row.previous_rank : null,
    rank_jump: typeof row.rank_jump === "number" ? row.rank_jump : null,
    confidence_label: row.confidence_label || "low",
    warning_flags: Array.isArray(row.warning_flags) ? row.warning_flags : [],
    provisional: Boolean(row.provisional),
    top_recent_events: row.top_recent_events || [],
    explanation: row.explanation || "",
  };
}

export function loadAllLeaderboardRows(): LeaderboardRow[] {
  return readJson<RawLeaderboardRow[]>(LEADERBOARD_PATH, []).map(toPublicRow);
}

export function loadLeaderboardRows(query: LeaderboardQuery = {}): LeaderboardRow[] {
  const format = query.format && query.format !== "overall" ? query.format : null;
  const offset = query.offset || 0;
  const rows = loadAllLeaderboardRows()
    .filter((row) => !query.tier || row.tier === query.tier)
    .filter((row) => !query.country || row.country_code === query.country.toUpperCase())
    .filter((row) => !format || row.formats?.includes(format));

  const rankedRows = format ? rows.map((row, index) => ({ ...row, rank: index + 1 })) : rows;
  return rankedRows.slice(offset, offset + (query.limit || 100));
}

export function loadCountryPowerRows(format: LeaderboardFormat = "overall", limit = 20): CountryPowerRow[] {
  const rows = loadLeaderboardRows({ format, limit: 10000 }).filter((row) => row.country_code);
  const groups = new Map<string, LeaderboardRow[]>();
  for (const row of rows) {
    const code = row.country_code;
    if (!code) continue;
    groups.set(code, [...(groups.get(code) || []), row]);
  }

  return [...groups.entries()]
    .map(([countryCode, countryRows]) => {
      const topPlayers = countryRows
        .sort((a, b) => b.final_power_score - a.final_power_score)
        .slice(0, 5);
      const powerScore =
        topPlayers.reduce((sum, row) => sum + row.final_power_score, 0) / Math.max(1, topPlayers.length);
      return {
        rank: 0,
        country_code: countryCode,
        country_name: countryName(countryCode),
        country_flag_url: countryFlagUrl(countryCode) || "",
        power_score: Number(powerScore.toFixed(2)),
        player_count: countryRows.length,
        top_players: topPlayers.map((row) => ({
          username: row.username,
          final_power_score: row.final_power_score,
          rank: row.rank,
        })),
      };
    })
    .sort((a, b) => b.power_score - a.power_score)
    .slice(0, limit)
    .map((row, index) => ({ ...row, rank: index + 1 }));
}

function normalizedName(value: string): string {
  return value.toLowerCase().replace(/\s+/g, " ").trim();
}

function loadPackageData(): PackageData {
  const playerTeams = new Map<string, PackagePlayer[]>();
  const playerScores = new Map<string, PackageScore[]>();
  const userIdToNames = new Map<number, Set<string>>();
  const matches: PackageMatch[] = [];

  if (!existsSync(VERIFIED_PACKAGES_DIR)) {
    return { playerTeams, playerScores, userIdToNames, matches };
  }

  for (const file of readdirSync(VERIFIED_PACKAGES_DIR).filter((name) => name.endsWith(".json")).sort()) {
    const packagePath = path.join(VERIFIED_PACKAGES_DIR, file);
    const pkg = readJson<VerifiedPackage>(packagePath, {});
    const fallbackEvent = packageEventName(file);

    for (const player of pkg.players || []) {
      const name = player.player || "";
      if (!name) continue;
      const key = normalizedName(name);
      const entry = { ...player, event: player.event || fallbackEvent };
      playerTeams.set(key, [...(playerTeams.get(key) || []), entry]);
      if (entry.user_id) {
        const names = userIdToNames.get(entry.user_id) || new Set<string>();
        names.add(key);
        userIdToNames.set(entry.user_id, names);
      }
    }

    for (const match of pkg.matches || []) {
      matches.push({ ...match, event: match.event || fallbackEvent });
    }

    for (const score of pkg.map_scores || []) {
      const name = score.player || "";
      if (!name) continue;
      const key = normalizedName(name);
      const entry = { ...score, event: score.event || fallbackEvent };
      playerScores.set(key, [...(playerScores.get(key) || []), entry]);
      if (entry.user_id) {
        const names = userIdToNames.get(entry.user_id) || new Set<string>();
        names.add(key);
        userIdToNames.set(entry.user_id, names);
      }
    }
  }

  return { playerTeams, playerScores, userIdToNames, matches };
}

function resolvePlayerNames(username: string): Set<string> {
  const data = loadPackageData();
  const names = new Set<string>([normalizedName(username)]);
  const leaderboardRow = findLeaderboardRow(username);
  if (leaderboardRow) {
    names.add(normalizedName(leaderboardRow.username));
    for (const alias of leaderboardRow.aliases) names.add(normalizedName(alias));
  }

  for (const name of [...names]) {
    for (const teamEntry of data.playerTeams.get(name) || []) {
      if (!teamEntry.user_id) continue;
      for (const linkedName of data.userIdToNames.get(teamEntry.user_id) || []) {
        names.add(linkedName);
      }
    }
  }
  return names;
}

function resolvePlayerUserIds(username: string): Set<number> {
  const data = loadPackageData();
  const ids = new Set<number>();
  const leaderboardRow = findLeaderboardRow(username);
  if (leaderboardRow?.user_id) ids.add(leaderboardRow.user_id);

  for (const name of resolvePlayerNames(username)) {
    for (const teamEntry of data.playerTeams.get(name) || []) {
      if (teamEntry.user_id) ids.add(teamEntry.user_id);
    }
    for (const score of data.playerScores.get(name) || []) {
      if (score.user_id) ids.add(score.user_id);
    }
  }
  return ids;
}

function loadStageHistoryRows(): StageHistoryRow[] {
  const payload = readJson<{ rows?: StageHistoryRow[] } | StageHistoryRow[]>(STAGE_HISTORY_PATH, []);
  if (Array.isArray(payload)) return payload;
  return Array.isArray(payload.rows) ? payload.rows : [];
}

function loadPlayerStageRows(username: string): StageHistoryRow[] {
  const names = resolvePlayerNames(username);
  const userIds = resolvePlayerUserIds(username);
  return loadStageHistoryRows()
    .filter((row) => {
      const rowName = normalizedName(String(row.username || ""));
      if (rowName && names.has(rowName)) return true;
      return typeof row.osu_id === "number" && userIds.has(row.osu_id);
    })
    .sort((a, b) => (b.end_date || b.start_date || "").localeCompare(a.end_date || a.start_date || ""));
}

const COMMON_COUNTRIES: Record<string, string> = {
  US: "United States",
  KR: "South Korea",
  JP: "Japan",
  PL: "Poland",
  AU: "Australia",
  DE: "Germany",
  GB: "United Kingdom",
  CA: "Canada",
  BR: "Brazil",
  FR: "France",
  CN: "China",
  TW: "Taiwan",
  PH: "Philippines",
  ID: "Indonesia",
  CL: "Chile",
  SE: "Sweden",
  FI: "Finland",
  RO: "Romania",
  HK: "Hong Kong",
  MY: "Malaysia",
  SG: "Singapore",
  TH: "Thailand",
  NL: "Netherlands",
  NO: "Norway",
  DK: "Denmark",
  MX: "Mexico",
  AR: "Argentina",
  ES: "Spain",
  IT: "Italy",
  PT: "Portugal",
  CZ: "Czech Republic",
  VN: "Vietnam",
  TR: "Turkey",
};

function teamDisplayName(team: string | null | undefined): string {
  if (!team) return "Unknown";
  return team.length === 2 ? COMMON_COUNTRIES[team.toUpperCase()] || team : team;
}

function loadPlayerPerformanceEvents(username: string): RecentTournamentEvent[] {
  const names = resolvePlayerNames(username);
  const userIds = resolvePlayerUserIds(username);
  return readJson<RawPowerEvent[]>(POWER_EVENTS_PATH, [])
    .filter((event) => {
      if (names.has(normalizedName(String(event.username || "")))) return true;
      const stageOsuId = event.metadata?.stage_osu_id;
      return typeof stageOsuId === "number" && userIds.has(stageOsuId);
    })
    .map((event) => ({
      event_name: event.event_name,
      event: event.metadata?.event || event.event || null,
      stage: event.metadata?.stage || event.stage || null,
      event_date: event.event_date,
      days_since_event: event.days_since_event,
      impact_score: event.impact_score,
      match_cost: event.match_cost,
      win_rate: event.win_rate,
      placement_percentile: event.placement_percentile,
      strength_of_schedule: event.strength_of_schedule,
      event_tier_weight: event.event_tier_weight,
      map_total: event.map_total,
      map_wins: event.map_wins,
      metadata: event.metadata,
    }))
    .sort((a, b) => (b.event_date || "").localeCompare(a.event_date || ""));
}

function loadRecentPackageMatches(username: string, limit = 20): RecentMatch[] {
  const data = loadPackageData();
  const names = resolvePlayerNames(username);
  const teamsByEvent = new Map<string, Set<string>>();

  for (const name of names) {
    for (const teamEntry of data.playerTeams.get(name) || []) {
      if (!teamEntry.event || !teamEntry.team_code) continue;
      const teams = teamsByEvent.get(teamEntry.event) || new Set<string>();
      teams.add(teamEntry.team_code);
      teamsByEvent.set(teamEntry.event, teams);
    }
    for (const score of data.playerScores.get(name) || []) {
      if (!score.event || !score.player_team) continue;
      const teams = teamsByEvent.get(score.event) || new Set<string>();
      teams.add(score.player_team);
      teamsByEvent.set(score.event, teams);
    }
  }

  const seen = new Set<string>();
  const recentMatches: RecentMatch[] = [];
  for (const match of data.matches) {
    const event = match.event || "";
    const teams = teamsByEvent.get(event);
    if (!teams) continue;
    const team = match.team_code || match.team || "";
    if (!teams.has(team)) continue;

    const key = match.match_link || `${event}|${match.stage}|${match.opponent_team}|${match.date}|${team}`;
    if (seen.has(key)) continue;
    seen.add(key);

    recentMatches.push({
      match_date: match.date || null,
      tournament_name: event,
      stage: match.stage || null,
      team_name: teamDisplayName(team),
      opponent_name: teamDisplayName(match.opponent_team),
      opponent_team_name: teamDisplayName(match.opponent_team),
      result: match.result || null,
      player_score: typeof match.team_score === "number" ? match.team_score : null,
      opponent_score: typeof match.opponent_score === "number" ? match.opponent_score : null,
      match_link: match.match_link || null,
      match_id: null,
      source: match.source || "verified_package",
      data_quality: "verified",
    });
  }

  return recentMatches.sort((a, b) => (b.match_date || "").localeCompare(a.match_date || "")).slice(0, limit);
}

function loadRecentStageSummaries(username: string, limit = 20): RecentMatch[] {
  return loadPlayerStageRows(username)
    .map((row) => {
      const wins = typeof row.matches_won === "number" ? row.matches_won : null;
      const losses = typeof row.matches_lost === "number" ? row.matches_lost : null;
      return {
        match_date: row.end_date || row.start_date || null,
        tournament_name: row.tournament_name || null,
        stage: "Tournament summary",
        team_name: row.country ? teamDisplayName(row.country) : null,
        opponent_name: "tournament field",
        opponent_team_name: "tournament field",
        result: wins !== null && losses !== null ? `${wins}W-${losses}L` : "stage summary",
        player_score: wins,
        opponent_score: losses,
        match_link: row.stage_url || row.forum_url || null,
        match_id: row.tournament_id || null,
        source: "stage_player_tournament_stats",
        data_quality: "stage_verified_summary",
      };
    })
    .slice(0, limit);
}

export function findLeaderboardRow(username: string): LeaderboardRow | null {
  const target = normalizedName(username);
  return (
    loadAllLeaderboardRows().find((row) => {
      if (normalizedName(row.username) === target) return true;
      return row.aliases.some((alias) => normalizedName(alias) === target);
    }) || null
  );
}

function scoreBreakdown(row: RawLeaderboardRow, publicRow: LeaderboardRow): PlayerPower["score_breakdown"] {
  return {
    final_power_score: publicRow.final_power_score,
    base_power_score: typeof row.base_power_score === "number" ? row.base_power_score : null,
    elitebotix_score: typeof row.elitebotix_score === "number" ? row.elitebotix_score : null,
    skill_issue_score: typeof row.skill_issue_score === "number" ? row.skill_issue_score : null,
    bancho_score: typeof row.bancho_score === "number" ? row.bancho_score : null,
    lazer_score: typeof row.lazer_score === "number" ? row.lazer_score : null,
    recent_tournament_form: publicRow.recent_tournament_form,
    consistency_score: publicRow.consistency_score,
    reliability_multiplier: publicRow.reliability_multiplier,
    activity_multiplier: publicRow.activity_multiplier,
    tournaments_played_last_12m: Number(row.tournaments_played_last_12m || publicRow.unique_tournaments_count),
    unique_tournaments_count: publicRow.unique_tournaments_count,
    dominant_event: publicRow.dominant_event,
    dominant_event_score_share: publicRow.dominant_event_score_share,
    team_world_cup_score_share: publicRow.team_world_cup_score_share,
    previous_rank: publicRow.previous_rank,
    rank_jump: publicRow.rank_jump,
    confidence_label: publicRow.confidence_label,
    warning_flags: publicRow.warning_flags,
    provisional: publicRow.provisional,
    days_since_last_event: typeof row.days_since_last_event === "number" ? row.days_since_last_event : null,
    activity_status: typeof row.activity_status === "string" ? row.activity_status : null,
    bancho_rank: typeof row.bancho_rank === "number" ? row.bancho_rank : null,
    country_rank: typeof row.country_rank === "number" ? row.country_rank : null,
    pp: typeof row.pp === "number" ? row.pp : null,
  };
}

export function loadPlayerPower(username: string): PlayerPower | null {
  const rawRows = readJson<RawLeaderboardRow[]>(LEADERBOARD_PATH, []);
  const target = normalizedName(username);
  const raw = rawRows.find((row) => {
    if (normalizedName(String(row.username || "")) === target) return true;
    return (row.aliases || []).some((alias) => normalizedName(alias) === target);
  });
  if (!raw) return null;

  const publicRow = toPublicRow(raw);
  const recentMatches = [
    ...loadRecentPackageMatches(publicRow.username, 50),
    ...loadRecentStageSummaries(publicRow.username, 50),
  ]
    .sort((a, b) => (b.match_date || "").localeCompare(a.match_date || ""))
    .slice(0, 20);
  return {
    username: publicRow.username,
    profile_username: typeof raw.profile_username === "string" ? raw.profile_username : publicRow.username,
    user_id: publicRow.user_id,
    avatar_url: publicRow.avatar_url,
    aliases: publicRow.aliases,
    rank: publicRow.rank,
    tier: publicRow.tier,
    country_code: publicRow.country_code,
    country_flag_url: publicRow.country_flag_url || undefined,
    score_breakdown: scoreBreakdown(raw, publicRow),
    recent_tournament_events: loadPlayerPerformanceEvents(publicRow.username),
    recent_matches: recentMatches,
    explanation: publicRow.explanation,
  };
}

export function loadTournamentStats(username: string): TournamentStat[] {
  const data = loadPackageData();
  const names = resolvePlayerNames(username);
  const scores = [...names].flatMap((name) => data.playerScores.get(name) || []);
  const grouped = new Map<string, PackageScore[]>();
  for (const score of scores) {
    const eventName = score.event || "Unknown";
    grouped.set(eventName, [...(grouped.get(eventName) || []), score]);
  }

  const packageStats = [...grouped.entries()]
    .map(([eventName, eventScores]) => {
      const dates = eventScores.map((row) => row.date).filter((date): date is string => Boolean(date)).sort();
      const mapTotal = eventScores.length;
      const mapWins = eventScores.filter((row) => row.result === "win").length;
      const stages = [...new Set(eventScores.map((row) => row.stage).filter(Boolean) as string[])].sort();
      const avgAccuracy =
        eventScores.reduce((sum, row) => sum + Number(row.accuracy || 0), 0) / Math.max(1, eventScores.length);
      const avgScore = eventScores.reduce((sum, row) => sum + Number(row.score || 0), 0) / Math.max(1, eventScores.length);
      return {
        tournament: eventName,
        maps_played: mapTotal,
        map_wins: mapWins,
        map_losses: Math.max(0, mapTotal - mapWins),
        win_rate: mapTotal ? Number(((mapWins / mapTotal) * 100).toFixed(1)) : 0,
        avg_accuracy: Number(avgAccuracy.toFixed(2)),
        avg_score: Math.round(avgScore),
        stages,
        first_date: dates[0] || null,
        last_date: dates[dates.length - 1] || null,
      };
    })
  const packageEvents = new Set(packageStats.flatMap((stat) => importedEventAliasKeys(stat.tournament)));
  const stageStats: TournamentStat[] = loadPlayerStageRows(username)
    .filter((row) => row.tournament_name && !packageEvents.has(compactEventKey(row.tournament_name)))
    .map((row) => {
      const matchesPlayed = Number(row.matches_played || 0);
      const matchesWon = Number(row.matches_won || 0);
      const accuracy = Number(row.average_accuracy || 0);
      return {
        tournament: String(row.tournament_name),
        maps_played: matchesPlayed,
        map_wins: matchesWon,
        map_losses: Math.max(0, matchesPlayed - matchesWon),
        win_rate: matchesPlayed ? Number(((matchesWon / matchesPlayed) * 100).toFixed(1)) : 0,
        avg_accuracy: Number((accuracy <= 1 ? accuracy * 100 : accuracy).toFixed(2)),
        avg_score: Math.round(Number(row.average_score || 0)),
        stages: ["Tournament summary", "Stage verified"],
        first_date: row.start_date || null,
        last_date: row.end_date || row.start_date || null,
      };
    });

  return [...packageStats, ...stageStats].sort((a, b) => (b.last_date || "").localeCompare(a.last_date || ""));
}
