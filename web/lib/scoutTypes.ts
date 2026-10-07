// Shapes returned by the Python service (scout.server). All numbers are computed there.

export type Format = "1v1" | "team";

export type TeamRef = { name: string; slug: string; country: string | null };

export type Tournament = {
  id: number;
  slug: string;
  name: string;
  acronym: string | null;
  format: Format;
  format_label: string;
  has_teams: boolean;
  start_date: string | null;
  end_date: string | null;
};

export type TournamentListItem = {
  id: number;
  slug: string;
  name: string;
  acronym: string | null;
  format: Format;
  format_label: string;
  has_teams: boolean;
  start_date: string | null;
  end_date: string | null;
  matches: number;
  pending: number;
  players: number;
  teams: number;
};

export type Summary = {
  matches: number;
  games: number;
  scores: number;
  players: number;
  teams: number;
  beatmaps: number;
  empty_matches: number;
  mods: string[];
  rounds: string[];
  round_names: Record<string, string>;
};

export type RankedRating = {
  rating: number;
  maps: number;
  rank: number;
  rank_of: number;
  confidence: number;
  low_confidence?: boolean;
};

export type PlayerSummary = {
  rank: number;
  user_id: number;
  username: string;
  slug: string;
  avatar_url: string;
  country: string | null;
  country_name: string | null;
  team: TeamRef | null;
  rating: number; // Tournament Rating (default ranking)
  performance_rating: number;
  performance_rank: number;
  rating_raw: number;
  confidence: number;
  deepest_round: string | null;
  deepest_round_name: string | null;
  qualifier: { rating: number; maps: number; rank: number; rank_of: number } | null;
  maps_played: number;
  qualified: boolean | null;
  consistency_sigma: number | null;
  mod_ratings: Record<string, RankedRating>;
  round_ratings: Record<string, RankedRating>;
  avg_score: number | null;
  avg_accuracy: number | null;
  map_winrate: number | null;
  map_record: [number, number];
  match_record: [number, number];
};

export type Performance = {
  z: number;
  rating: number;
  score: number;
  accuracy: number | null;
  mod: string;
  round: string | null;
  round_name: string;
  beatmap_id: number | null;
  beatmapset_id: number | null;
  map: string;
  osu_match_id: number;
  match_name: string;
};

export type MatchSide = {
  name: string;
  slug: string | null;
  kind: "team" | "player" | "label";
  country: string | null;
  side: string;
  map_wins: number;
};

export type MatchItem = {
  osu_match_id: number;
  link: string;
  round: string | null;
  round_name: string;
  kind: "match" | "qualifier";
  name: string;
  start_time: string | null;
  sides: MatchSide[];
  winner: string | null;
  winner_side: string | null;
  result?: "W" | "L" | null;
};

export type HistoryItem = {
  osu_match_id: number;
  round: string | null;
  round_name: string;
  kind: "match" | "qualifier";
  name: string;
  start_time: string | null;
  result: "W" | "L" | null;
  score: string | null;
  sides: MatchSide[];
  maps: number;
  rating: number;
};

export type RosterRow = {
  team_rank: number;
  user_id: number;
  username: string;
  slug: string;
  avatar_url: string;
  country: string | null;
  rating: number;
  rank: number;
  rank_of: number;
  maps: number;
};

export type PlayerPage = PlayerSummary & {
  rank_of: number;
  breakdown: {
    tournament: { rating: number; rank: number };
    performance: { rating: number; rank: number };
    confidence: number;
    maps: number;
    observed_rating: number;
    deepest_round: string | null;
    deepest_round_name: string | null;
    qualifier: { rating: number; maps: number; rank: number; rank_of: number } | null;
  };
  carry_index: number | null;
  by_round: { round: string; round_name: string; rating: number; maps: number; confidence: number; rank: number; rank_of: number }[];
  by_mod: { mod: string; rating: number; maps: number; confidence: number; low_confidence: boolean; rank: number; rank_of: number }[];
  best_performances: Performance[];
  worst_performances: Performance[];
  match_history: HistoryItem[];
  teammates: RosterRow[];
  team: (TeamRef & { flag_url: string | null; rank: number; rating: number }) | null;
};

export type Award = {
  key: string;
  title: string;
  rule: string;
  winner: { user_id: number; username: string; slug: string; avatar_url: string; value: string | number } | null;
};

export type TeamListItem = {
  rank: number;
  name: string;
  slug: string;
  country: string | null;
  flag_url: string | null;
  rating: number;
  match_record: [number, number];
  map_record: [number, number];
  roster_size: number;
  maps: number;
  furthest_round_name: string | null;
};

export type BestPlayer = { user_id: number; username: string; slug: string; avatar_url: string; rating: number; maps: number };

export type TeamPage = {
  slug: string;
  name: string;
  country: string | null;
  country_name: string | null;
  flag_url: string | null;
  rank: number;
  rank_of: number;
  rating: number;
  match_record: [number, number];
  map_record: [number, number];
  maps_won: number;
  maps_played: number;
  players: number;
  avg_player_rating: number;
  furthest_round: string | null;
  furthest_round_name: string | null;
  best_round: { round: string; round_name: string; rating: number; maps: number } | null;
  worst_round: { round: string; round_name: string; rating: number; maps: number } | null;
  best_player: RosterRow | null;
  best_by_mod: Record<string, BestPlayer | null>;
  by_round: { round: string; round_name: string; rating: number; maps: number }[];
  roster: RosterRow[];
  matches: MatchItem[];
};

export type Overview = {
  tournament: Tournament;
  summary: Summary;
  mvp: PlayerSummary | null;
  mod_leaders: { mod: string; user_id: number; username: string; slug: string; avatar_url: string; rating: number; maps: number }[];
  top_players: PlayerSummary[];
  top_teams: TeamListItem[];
  awards: Award[];
  recent_matches: MatchItem[];
};

export type LeaderboardRow = {
  rank: number;
  user_id: number;
  username: string;
  slug: string;
  avatar_url: string | null;
  country: string | null;
  team: TeamRef | null;
  rating: number;
  maps: number;
  extra?: number;
  confidence?: number;
  low_confidence?: boolean;
  qualified?: boolean | null;
  tournament_rating?: number;
  performance_rating?: number;
  deepest_round_name?: string | null;
};

export type LeaderboardMode = "tournament" | "performance" | "round" | "mod" | "consistency" | "maps";

export type LeaderboardData = {
  mode: LeaderboardMode;
  key: string | null;
  total: number;
  note: string | null;
  qualified_cutoff: number | null; // number of players above the qualification line (tournament mode)
  columns: { value_label: string; extra_label?: string };
  rows: LeaderboardRow[];
  options: { rounds: { key: string; name: string }[]; mods: string[] };
};

export type SearchHit = {
  user_id: number;
  username: string;
  slug: string;
  avatar_url: string;
  country: string | null;
  team: TeamRef | null;
  rating: number;
  rank: number;
  rank_of: number;
  maps_played: number;
  mod_ratings: Record<string, number>;
};

export type MatchGameScore = {
  user_id: number;
  username: string;
  slug: string | null;
  avatar_url: string;
  score: number;
  accuracy: number | null;
  max_combo: number | null;
  misses: number | null;
  mods: string;
  side: string | null;
  rating: number | null;
};

export type MatchGame = {
  order: number;
  osu_game_id: number;
  beatmap_id: number | null;
  map: string;
  beatmapset_id: number | null;
  star_rating: number | null;
  mod: string | null;
  excluded: boolean;
  exclude_reason: string | null;
  winner_side: string | null;
  side_totals: Record<string, number>;
  scores: MatchGameScore[];
};

export type ImportStatus = {
  id: string;
  slug: string;
  phase: "queued" | "scanning" | "importing" | "calculating" | "done" | "error";
  message: string;
  found: number;
  done: number;
  total: number;
  failed: number;
  not_found: number;
  error: string | null;
  existing?: boolean;
  mode?: "thread" | "step" | "inline";
};

// ---- draft simulator ----
export type DraftSide = { slug: string; name: string; country?: string | null; avatar_url?: string; rating: number; detail: string };

export type DraftSetup = {
  tournament: Tournament;
  sides: DraftSide[];
  rounds: { round: string; name: string; maps: number }[];
  side_kind: "team" | "player";
};

export type DraftMap = {
  beatmap_id: number;
  mod: string;
  slot: string | null;
  plays: number;
  beatmapset_id: number | null;
  star_rating: number | null;
  title: string;
  artist: string;
  version: string;
  p_a: number;
  rating_a: number;
  rating_b: number;
  plays_a: number;
  plays_b: number;
  reasons: string[];
  taken?: { type: "ban" | "pick"; side: "A" | "B"; order: number };
};

export type DraftAdvice = {
  a: { slug: string; name: string };
  b: { slug: string; name: string };
  round: string;
  round_name: string;
  sequence: { type: "ban" | "pick"; side: "A" | "B" }[];
  step: number;
  next: { type: "ban" | "pick"; side: "A" | "B" } | null;
  done: boolean;
  maps: DraftMap[];
  tiebreaker: number | null;
  suggestions: { beatmap_id: number; score: number; p_side: number; reasons: string[] }[];
  match_win_a: number;
  target: number;
};
