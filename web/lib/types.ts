export type Tier = "Tier 1" | "Tier 2" | "Tier 3";

export type LeaderboardRow = {
  rank: number;
  username: string;
  user_id: number | null;
  avatar_url: string | null;
  aliases: string[];
  country_code: string | null;
  country_name?: string | null;
  country_flag_url?: string | null;
  tier: Tier;
  final_power_score: number;
  recent_tournament_form: number;
  consistency_score: number;
  reliability_multiplier: number;
  activity_multiplier: number;
  unique_tournaments_count: number;
  dominant_event: string | null;
  dominant_event_score_share: number;
  team_world_cup_score_share: number;
  previous_rank: number | null;
  rank_jump: number | null;
  confidence_label: "high" | "medium" | "low";
  warning_flags: string[];
  provisional: boolean;
  top_recent_events: RecentTournamentEvent[];
  explanation: string;
};

export type RecentTournamentEvent = {
  event_name: string;
  event: string | null;
  stage: string | null;
  event_date: string | null;
  days_since_event: number | null;
  match_cost: number | null;
  win_rate: number | null;
  placement_percentile: number | null;
  strength_of_schedule: number | null;
  event_tier_weight: number | null;
  map_total: number | null;
  map_wins: number | null;
};

export type RecentMatch = {
  match_date: string | null;
  tournament_name: string | null;
  stage: string | null;
  team_name: string | null;
  opponent_name: string | null;
  opponent_team_name: string | null;
  result: string | null;
  player_score: number | null;
  opponent_score: number | null;
  match_link: string | null;
  match_id: number | null;
  source: string | null;
  data_quality: string | null;
};

export type ScoreBreakdown = {
  final_power_score: number;
  base_power_score: number | null;
  elitebotix_score: number | null;
  skill_issue_score: number | null;
  bancho_score: number | null;
  lazer_score: number | null;
  recent_tournament_form: number;
  consistency_score: number;
  reliability_multiplier: number;
  activity_multiplier: number;
  tournaments_played_last_12m: number;
  unique_tournaments_count: number;
  dominant_event: string | null;
  dominant_event_score_share: number;
  team_world_cup_score_share: number;
  previous_rank: number | null;
  rank_jump: number | null;
  confidence_label: "high" | "medium" | "low";
  warning_flags: string[];
  provisional: boolean;
  days_since_last_event: number | null;
  activity_status: string | null;
  bancho_rank: number | null;
  country_rank: number | null;
  pp: number | null;
};

export type PlayerPower = {
  username: string;
  profile_username: string | null;
  user_id: number | null;
  avatar_url: string | null;
  aliases: string[];
  rank: number;
  tier: Tier;
  country_code: string | null;
  country_flag_url?: string | null;
  score_breakdown: ScoreBreakdown;
  recent_tournament_events: RecentTournamentEvent[];
  recent_matches: RecentMatch[];
  explanation: string;
};

export type TournamentEntry = {
  slug: string;
  name: string;
  year: number;
  game_mode: string;
  format: string | null;
  rank_range: string | null;
  team_size: string | null;
  start_date: string | null;
  end_date: string | null;
  stage_url: string | null;
  forum_url: string | null;
  wiki_url: string | null;
  source_url: string | null;
  player_count: number | null;
  match_count: number | null;
  map_score_count: number | null;
  classification: "imported" | "production_safe" | "likely_importable" | "stage_only" | "partial" | "ignore";
  import_status: "imported" | "discovered";
  tier: string | null;
  data_quality: string | null;
};

export type TournamentCatalog = {
  total: number;
  imported_count: number;
  discovered_count: number;
  rows: TournamentEntry[];
};
