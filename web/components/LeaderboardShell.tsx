"use client";

import Link from "next/link";
import { useMemo, useState } from "react";
import { FormulaCard } from "./FormulaCard";
import type { CountryPowerRow, LeaderboardFormat, LeaderboardRow, Tier } from "@/lib/types";

type CountryOption = {
  code: string;
  name: string;
};

type LeaderboardShellProps = {
  initialRows?: LeaderboardRow[];
  initialAllRows?: LeaderboardRow[];
  initialCountryOptions?: CountryOption[];
  initialCountryRows?: CountryPowerRow[];
  initialTier?: Tier | "";
  initialCountry?: string;
  initialLimit?: number;
  selectedYear?: string;
  selectedFormat?: LeaderboardFormat;
};

const FORMAT_TABS: Array<{ value: LeaderboardFormat; label: string }> = [
  { value: "overall", label: "Overall" },
  { value: "1v1", label: "1v1" },
  { value: "2v2", label: "2v2" },
  { value: "3v3", label: "3v3" },
  { value: "4v4", label: "4v4" },
];

function formatScore(value: number | null | undefined, digits = 2): string {
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

function countryFlagUrl(countryCode: string): string {
  return `https://flagcdn.com/w40/${countryCode.toLowerCase()}.png`;
}

function rankClass(rank: number): string {
  if (rank === 1) return "rank rank-1";
  if (rank === 2) return "rank rank-2";
  if (rank === 3) return "rank rank-3";
  if (rank <= 10) return "rank rank-top-10";
  if (rank <= 25) return "rank rank-top-25";
  if (rank <= 50) return "rank rank-top-50";
  if (rank <= 100) return "rank rank-top-100";
  return "rank";
}

function tierClass(tier: Tier): string {
  return `tier-label tier-label-${tier.replace(" ", "-").toLowerCase()}`;
}

function confidenceBadge(row: LeaderboardRow): string {
  if (row.confidence_label === "high") return "High";
  if (row.confidence_label === "medium") return "Medium";
  return "Low confidence";
}

function activityLabel(value: number | null | undefined): string {
  if (value === null || value === undefined || Number.isNaN(value)) return "Pending";
  if (value >= 0.97) return "Active";
  if (value >= 0.93) return "Stable";
  return "Cooling down";
}

function formatPercent(value: number | null | undefined): string {
  if (value === null || value === undefined || Number.isNaN(value)) return "N/A";
  return `${Math.round(value * 100)}%`;
}

function warningLabel(flag: string): string {
  const labels: Record<string, string> = {
    low_sample: "fewer than three unique tournaments",
    one_event: "score is concentrated in one tournament",
    team_wc_heavy: "majority contribution is from team world cups",
    unstable: "rank moved by more than 50 places in the last update",
    needs_formula_review: "marked for formula review",
  };
  return labels[flag] || flag.replaceAll("_", " ");
}

function confidenceTitle(row: LeaderboardRow): string {
  const reasons = row.warning_flags.map(warningLabel);
  const parts = [
    `Confidence: ${row.confidence_label}`,
    `Unique tournaments: ${row.unique_tournaments_count}`,
    row.dominant_event
      ? `Main source: ${row.dominant_event} (${formatPercent(row.dominant_event_score_share)})`
      : "Main source: unavailable",
  ];
  if (row.rank_jump !== null && row.rank_jump !== undefined) {
    parts.push(`Last movement: ${row.rank_jump > 0 ? "+" : ""}${row.rank_jump}`);
  }
  if (reasons.length) parts.push(`Flags: ${reasons.join("; ")}`);
  return parts.join("\n");
}

function CountryFlag({ row }: { row: LeaderboardRow }) {
  const label = countryLabel(row.country_code);
  return (
    <span className="flag" title={label} aria-label={label}>
      {row.country_flag_url ? <img src={row.country_flag_url} alt={label} /> : <span>{row.country_code || "??"}</span>}
      <span className="flag-tooltip" role="tooltip">
        {label}
      </span>
    </span>
  );
}

function PlayerAvatar({ row }: { row: LeaderboardRow }) {
  const [imgError, setImgError] = useState(false);
  const initials = row.username.slice(0, 2).toUpperCase();
  return (
    <span className="avatar-frame">
      {row.avatar_url && !imgError ? (
        <img src={row.avatar_url} alt={`${row.username} avatar`} onError={() => setImgError(true)} />
      ) : (
        <span className="avatar-initials">{initials}</span>
      )}
    </span>
  );
}

function shortEventName(eventName: string): string {
  return eventName
    .replace(/^osu! /, "")
    .replace(/ 2025$/, "")
    .replace(/ 2026$/, "")
    .replace("World Cup", "WC")
    .replace("Invitational Tournament", "IT")
    .replace("Digit World Cup", "DWC")
    .replace("Resurrection Cup", "RESC")
    .replace("French Draft Cup", "FDC")
    .replace("Liveplay Global Arena", "LGA");
}

function TournamentBadges({ row }: { row: LeaderboardRow }) {
  const events = row.top_recent_events || [];
  const uniqueEvents = Array.from(new Set(events.map((e) => e.event || e.event_name).filter(Boolean)));
  if (uniqueEvents.length === 0) {
    return <span className="source-cell">{row.dominant_event || "N/A"}</span>;
  }
  return (
    <span className="tournament-badges-cell">
      {uniqueEvents.slice(0, 4).map((evt) => (
        <span className="tournament-pill" key={evt} title={evt}>
          {shortEventName(evt)}
        </span>
      ))}
      {uniqueEvents.length > 4 ? <span className="tournament-pill tournament-pill-more">+{uniqueEvents.length - 4}</span> : null}
    </span>
  );
}

function computeCountryRows(rows: LeaderboardRow[], format: LeaderboardFormat): CountryPowerRow[] {
  const sourceRows = rows.filter((row) => row.country_code && (format === "overall" || row.formats?.includes(format)));
  const groups = new Map<string, LeaderboardRow[]>();
  for (const row of sourceRows) {
    const code = row.country_code;
    if (!code) continue;
    groups.set(code, [...(groups.get(code) || []), row]);
  }

  return [...groups.entries()]
    .map(([countryCode, countryRows]) => {
      const topPlayers = countryRows.sort((a, b) => b.final_power_score - a.final_power_score).slice(0, 5);
      const powerScore =
        topPlayers.reduce((sum, row) => sum + row.final_power_score, 0) / Math.max(1, topPlayers.length);
      return {
        rank: 0,
        country_code: countryCode,
        country_name: countryName(countryCode),
        country_flag_url: countryFlagUrl(countryCode),
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
    .slice(0, 12)
    .map((row, index) => ({ ...row, rank: index + 1 }));
}

function CountryPowerTable({ rows }: { rows: CountryPowerRow[] }) {
  return (
    <section className="panel country-ranking-panel" id="country-rankings">
      <div className="section-heading">
        <p className="eyebrow">Country Power Ranking</p>
        <h2>Country form</h2>
        <p>Display-only country ranking based on each country&apos;s top active players in the current view.</p>
      </div>
      <div className="table-wrap">
        <table className="leaderboard-table country-table">
          <thead>
            <tr>
              <th>Rank</th>
              <th>Country</th>
              <th>Power</th>
              <th>Players</th>
              <th>Top contributors</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((row) => (
              <tr key={row.country_code}>
                <td className={rankClass(row.rank)}>#{row.rank}</td>
                <td>
                  <span className="country-cell">
                    <span className="flag" title={row.country_name.toUpperCase()} aria-label={row.country_name}>
                      <img src={row.country_flag_url} alt={row.country_name} />
                      <span className="flag-tooltip" role="tooltip">
                        {row.country_name.toUpperCase()}
                      </span>
                    </span>
                    <span>{row.country_name}</span>
                  </span>
                </td>
                <td className="score">{formatScore(row.power_score)}</td>
                <td className="metric">{row.player_count}</td>
                <td className="metric">
                  {row.top_players.map((player) => player.username).join(", ")}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  );
}

export function LeaderboardShell({
  initialRows = [],
  initialAllRows = [],
  initialCountryOptions = [],
  initialCountryRows = [],
  initialTier = "",
  initialCountry = "",
  initialLimit = 100,
  selectedYear = "2026",
  selectedFormat = "overall",
}: LeaderboardShellProps) {
  const allRows = initialAllRows.length ? initialAllRows : initialRows;
  const [tier, setTier] = useState<Tier | "">(initialTier);
  const [country, setCountry] = useState(initialCountry);
  const [limit, setLimit] = useState(initialLimit);
  const [format, setFormat] = useState<LeaderboardFormat>(selectedFormat);

  const rows = useMemo(() => {
    const filtered = allRows.filter((row) => {
      const matchesTier = !tier || row.tier === tier;
      const matchesCountry = !country || row.country_code?.toUpperCase() === country;
      const matchesFormat = format === "overall" || row.formats?.includes(format);
      return matchesTier && matchesCountry && matchesFormat;
    });
    return filtered.slice(0, limit);
  }, [allRows, country, format, limit, tier]);

  const countryRows = useMemo(() => {
    if (format === selectedFormat && initialCountryRows.length) return initialCountryRows;
    return computeCountryRows(allRows, format);
  }, [allRows, format, initialCountryRows, selectedFormat]);

  return (
    <main className="page-shell">
      <header className="top-nav">
        <Link href="/legacy" className="brand-mark">osu! scout</Link>
        <nav className="nav-links" aria-label="Primary">
          <a href="#leaderboard">Leaderboard</a>
          <a href="#country-rankings">Countries</a>
          <a href="#methodology">Methodology</a>
        </nav>
      </header>

      <section className="hero">
        <div>
          <p className="eyebrow">Current Performance</p>
          <h1>osu! Tournament Power Rankings</h1>
          <p className="hero-copy">A data-driven view of recent tournament performance.</p>
          <p className="hero-description">
            This project helps players, captains, and analysts understand trends in competitive play. It does not
            define absolute skill.
          </p>
          <div className="year-tabs" aria-label="Year filters">
            {["2026", "2025", "2024"].map((year) => (
              <Link className={selectedYear === year ? "year-tab year-tab-active" : "year-tab"} href={`/legacy?year=${year}&limit=${limit}`} key={year}>
                {year}
              </Link>
            ))}
          </div>
        </div>
        <div className="hero-card">
          <strong>10,000</strong>
          <span>players leaderboard target</span>
        </div>
      </section>

      <section className="format-tabs" aria-label="Leaderboard format views">
        {FORMAT_TABS.map((tab) => (
          <button
            className={format === tab.value ? "format-tab format-tab-active" : "format-tab"}
            key={tab.value}
            type="button"
            onClick={() => setFormat(tab.value)}
          >
            {tab.label}
          </button>
        ))}
      </section>

      <section className="dashboard-grid">
        <div className="panel" id="leaderboard">
          <form
            id="leaderboard-filters"
            className="controls controls-with-apply"
            onSubmit={(event) => event.preventDefault()}
          >
            <div className="field">
              <label htmlFor="tier">Tier</label>
              <select id="tier" value={tier} onChange={(event) => setTier(event.currentTarget.value as Tier | "")}>
                <option value="">All tiers</option>
                <option value="Tier 1">Tier 1</option>
                <option value="Tier 2">Tier 2</option>
                <option value="Tier 3">Tier 3</option>
              </select>
            </div>

            <div className="field">
              <label htmlFor="country">Country</label>
              <select id="country" value={country} onChange={(event) => setCountry(event.currentTarget.value)}>
                <option value="">All countries</option>
                {initialCountryOptions.map((option) => (
                  <option value={option.code} key={option.code}>
                    {option.name} ({option.code})
                  </option>
                ))}
              </select>
            </div>

            <div className="field">
              <label htmlFor="limit">Limit</label>
              <select id="limit" value={limit} onChange={(event) => setLimit(Number(event.currentTarget.value))}>
                <option value={20}>Top 20</option>
                <option value={50}>Top 50</option>
                <option value={100}>Top 100</option>
                <option value={250}>Top 250</option>
              </select>
            </div>

            <div className="view-label">
              {format === "overall" ? "Overall leaderboard" : `${format} leaderboard`}
            </div>
          </form>

          {rows.length === 0 ? <div className="state">No players match the selected filters.</div> : null}

          <div className="table-wrap">
            <table className="leaderboard-table">
              <thead>
                <tr>
                  <th>Rank</th>
                  <th>Change</th>
                  <th>Player</th>
                  <th>Country</th>
                  <th>Tier</th>
                  <th>Power Score</th>
                  <th>Recent Form</th>
                  <th>Tournaments</th>
                  <th>Activity</th>
                  <th>Confidence</th>
                </tr>
              </thead>
              <tbody>
                {rows.map((row, index) => {
                  const displayRank = format === "overall" ? row.rank : index + 1;
                  return (
                    <tr key={`${format}-${row.username}`}>
                      <td className={rankClass(displayRank)}>#{displayRank}</td>
                      <td className="change-placeholder">--</td>
                      <td>
                        <Link className="player-cell" href={`/legacy/player/${encodeURIComponent(row.username)}`}>
                          <PlayerAvatar row={row} />
                          <span className="player-main">
                            <span className="username">{row.username}</span>
                            {row.aliases.length ? <span className="aliases">aka {row.aliases.join(", ")}</span> : null}
                            <span className="player-badges">
                              {row.provisional ? (
                                <span className="mini-badge" title="Fewer than three unique tournaments in the last 12 months">
                                  Provisional
                                </span>
                              ) : null}
                              {row.confidence_label === "low" ? (
                                <span className="mini-badge mini-badge-alert" title={confidenceTitle(row)}>
                                  Low confidence
                                </span>
                              ) : null}
                            </span>
                          </span>
                        </Link>
                      </td>
                      <td>
                        <CountryFlag row={row} />
                      </td>
                      <td>
                        <span className={tierClass(row.tier)}>{row.tier}</span>
                      </td>
                      <td className="score">{formatScore(row.final_power_score)}</td>
                      <td className="metric">{formatScore(row.recent_tournament_form)}</td>
                      <td className="metric">
                        <TournamentBadges row={row} />
                      </td>
                      <td className="metric">{activityLabel(row.activity_multiplier)}</td>
                      <td className="metric">
                        <span className={`confidence-pill confidence-${row.confidence_label}`} title={confidenceTitle(row)}>
                          {confidenceBadge(row)}
                        </span>
                        {row.warning_flags.includes("needs_formula_review") ? (
                          <span className="review-mark" title={confidenceTitle(row)} aria-label="Needs formula review">
                            !
                          </span>
                        ) : null}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </div>

        <FormulaCard />
      </section>

      <CountryPowerTable rows={countryRows} />
    </main>
  );
}
