import Link from "next/link";
import { notFound } from "next/navigation";
import { fetchPlayerPower } from "@/lib/api";
import type { RecentMatch, RecentTournamentEvent } from "@/lib/types";

export const dynamic = "force-dynamic";

type PageProps = {
  params: Promise<{ username: string }>;
};

function fmt(value: number | null | undefined, digits = 2): string {
  if (value === null || value === undefined || Number.isNaN(value)) return "N/A";
  return value.toFixed(digits);
}

function countryName(countryCode: string | null): string {
  if (!countryCode) return "Unknown country";
  try {
    return new Intl.DisplayNames(["en"], { type: "region" }).of(countryCode.toUpperCase()) || countryCode;
  } catch {
    return countryCode.toUpperCase();
  }
}

function countryLabel(countryCode: string | null): string {
  return countryName(countryCode).toUpperCase();
}

function tierClass(tier: string): string {
  return `badge tier-${tier.replace(" ", "-").toLowerCase()}`;
}

function warningLabel(flag: string): string {
  const labels: Record<string, string> = {
    low_sample: "fewer than three unique tournaments",
    one_event: "score is concentrated in one tournament",
    team_wc_heavy: "majority contribution is from team world cups",
    unstable: "rank moved by more than 50 places in the last update",
    needs_formula_review: "marked for formula review"
  };
  return labels[flag] || flag.replaceAll("_", " ");
}

function confidenceTitle(breakdown: { 
  confidence_label: string;
  unique_tournaments_count: number;
  dominant_event: string | null;
  dominant_event_score_share: number;
  rank_jump: number | null;
  warning_flags: string[];
}): string {
  const parts = [
    `Confidence: ${breakdown.confidence_label}`,
    `Unique tournaments: ${breakdown.unique_tournaments_count}`,
    breakdown.dominant_event
      ? `Main source: ${breakdown.dominant_event} (${Math.round(breakdown.dominant_event_score_share * 100)}%)`
      : "Main source: unavailable"
  ];
  if (breakdown.rank_jump !== null) {
    parts.push(`Last movement: ${breakdown.rank_jump > 0 ? "+" : ""}${breakdown.rank_jump}`);
  }
  if (breakdown.warning_flags.length) {
    parts.push(`Flags: ${breakdown.warning_flags.map(warningLabel).join("; ")}`);
  }
  return parts.join("\n");
}

function cleanExplanation(value: string | null | undefined): string {
  return (value || "")
    .replace(/,?\s*provisional\.?/gi, "")
    .replace(/\s{2,}/g, " ")
    .trim();
}

function matchTitle(match: RecentMatch): string {
  const opponent = match.opponent_team_name || match.opponent_name || "Unknown opponent";
  const score =
    match.player_score !== null && match.opponent_score !== null
      ? ` ${match.player_score}-${match.opponent_score}`
      : "";
  return `vs ${opponent}${score}`;
}

function EventItem({ event }: { event: RecentTournamentEvent }) {
  return (
    <article className="event-item">
      <div className="item-top">
        <span>{event.event_name}</span>
        <span>{event.event_date || "Undated"}</span>
      </div>
      <div className="item-meta">
        <span>Cost {fmt(event.match_cost, 1)}</span>
        <span>WR {fmt(event.win_rate, 1)}</span>
        <span>Maps {event.map_wins ?? 0}/{event.map_total ?? 0}</span>
        <span>Weight {fmt(event.event_tier_weight, 2)}</span>
      </div>
    </article>
  );
}

function MatchItem({ match }: { match: RecentMatch }) {
  const content = (
    <article className="match-item">
      <div className="item-top">
        <span>{matchTitle(match)}</span>
        <span>{match.match_date || "Undated"}</span>
      </div>
      <div className="item-meta">
        <span>{match.tournament_name || "Unknown event"}</span>
        <span>{match.stage || "Unknown stage"}</span>
        <span>{match.result || "result pending"}</span>
        <span>{match.data_quality || "unlabeled"}</span>
      </div>
    </article>
  );

  if (!match.match_link) return content;
  return (
    <a href={match.match_link} target="_blank" rel="noreferrer">
      {content}
    </a>
  );
}

export default async function PlayerPowerPage({ params }: PageProps) {
  const { username } = await params;
  let player;
  try {
    player = await fetchPlayerPower(username);
  } catch {
    notFound();
  }
  if (!player || !player.score_breakdown) {
    notFound();
  }

  const breakdown = player.score_breakdown;

  return (
    <main className="page-shell">
      <div className="profile-header">
        <div>
          <Link className="back-link" href="/">
            Back to leaderboard
          </Link>
          <p className="eyebrow">Player Profile</p>
          <div className="profile-title">
            <span className="profile-avatar">
              {player.avatar_url ? (
                <img src={player.avatar_url} alt={`${player.username} avatar`} />
              ) : (
                <span>{player.username.slice(0, 2).toUpperCase()}</span>
              )}
            </span>
            <h1>{player.username}</h1>
            <span className={tierClass(player.tier)}>{player.tier}</span>
            {breakdown.provisional ? (
              <span className="mini-badge" title="Fewer than three unique tournaments in the last 12 months">
                Provisional
              </span>
            ) : null}
            {breakdown.confidence_label === "low" ? (
              <span className="mini-badge mini-badge-alert" title={confidenceTitle(breakdown)}>
                Low confidence
              </span>
            ) : null}
            {breakdown.warning_flags.includes("needs_formula_review") ? (
              <span className="review-mark" title={confidenceTitle(breakdown)} aria-label="Needs formula review">
                !
              </span>
            ) : null}
          </div>
          <p className="hero-copy">{cleanExplanation(player.explanation)}</p>
        </div>
        <div className="hero-card">
          <strong>#{player.rank}</strong>
          <span>global power rank</span>
        </div>
      </div>

      <section className="profile-grid">
        <div className="panel profile-card">
          <p className="eyebrow">Score breakdown</p>
          <h2>Power profile</h2>
          <div className="stat-grid">
            <div className="stat">
              <span>Power Score</span>
              <strong>{fmt(breakdown.final_power_score)}</strong>
            </div>
            <div className="stat">
              <span>Recent form</span>
              <strong>{fmt(breakdown.recent_tournament_form)}</strong>
            </div>
            <div className="stat">
              <span>Consistency</span>
              <strong>{fmt(breakdown.consistency_score)}</strong>
            </div>
            <div className="stat">
              <span>Activity</span>
              <strong>{fmt(breakdown.activity_multiplier, 3)}</strong>
            </div>
            <div className="stat">
              <span>Confidence</span>
              <strong>{fmt(breakdown.reliability_multiplier, 3)}</strong>
            </div>
            <div className="stat">
              <span>Bancho score</span>
              <strong>{fmt(breakdown.bancho_score)}</strong>
            </div>
            <div className="stat">
              <span>Bancho rank</span>
              <strong>{breakdown.bancho_rank ? `#${breakdown.bancho_rank}` : "N/A"}</strong>
            </div>
            <div className="stat">
              <span>Tournaments</span>
              <strong>{breakdown.unique_tournaments_count}</strong>
            </div>
            <div className="stat">
              <span>Main source</span>
              <strong>{breakdown.dominant_event || "N/A"}</strong>
            </div>
            <div className="stat">
              <span>Source share</span>
              <strong>{fmt((breakdown.dominant_event_score_share || 0) * 100, 0)}%</strong>
            </div>
          </div>

          {player.aliases.length ? (
            <p>
              <strong>Aliases:</strong> {player.aliases.join(", ")}
            </p>
          ) : null}
        </div>

        <aside className="panel profile-card">
          <p className="eyebrow">Formula note</p>
          <h2>Why this ranking?</h2>
          <p>
            Recent tournament form, event prestige, reliability, and activity
            are blended into the final score. External ratings remain optional
            context until stable public sources exist.
          </p>
          <div className="formula-list">
            <div className="formula-row">
              <span>Country</span>
              <span className="profile-country" title={countryLabel(player.country_code)}>
                {player.country_flag_url ? (
                  <img src={player.country_flag_url} alt={countryLabel(player.country_code)} />
                ) : (
                  player.country_code || "N/A"
                )}
                <span className="flag-tooltip" role="tooltip">
                  {countryLabel(player.country_code)}
                </span>
              </span>
            </div>
            <div className="formula-row">
              <span>Activity</span>
              <span>{breakdown.activity_status || "unknown"}</span>
            </div>
            <div className="formula-row">
              <span>Confidence</span>
              <span>{breakdown.confidence_label}</span>
            </div>
            <div className="formula-row">
              <span>Flags</span>
              <span>{breakdown.warning_flags.length ? breakdown.warning_flags.map(warningLabel).join(", ") : "none"}</span>
            </div>
            <div className="formula-row">
              <span>Days since event</span>
              <span>{breakdown.days_since_last_event ?? "N/A"}</span>
            </div>
          </div>
        </aside>

        <div className="panel profile-card">
          <p className="eyebrow">Recent events</p>
          <h2>Tournament form</h2>
          <div className="event-list">
            {player.recent_tournament_events.length ? (
              player.recent_tournament_events.map((event) => (
                <EventItem event={event} key={`${event.event_name}-${event.event_date}`} />
              ))
            ) : (
              <p>No recent tournament events available.</p>
            )}
          </div>
        </div>

        <div className="panel profile-card">
          <p className="eyebrow">Recent matches</p>
          <h2>Match history</h2>
          <div className="match-list">
            {player.recent_matches.length ? (
              player.recent_matches.map((match, index) => (
                <MatchItem match={match} key={`${match.match_id ?? index}-${match.match_date}`} />
              ))
            ) : (
              <p>No recent matches available.</p>
            )}
          </div>
        </div>
      </section>
    </main>
  );
}
