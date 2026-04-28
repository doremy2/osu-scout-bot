"use client";

import Link from "next/link";
import { useEffect, useState, useTransition } from "react";
import { FormulaCard } from "./FormulaCard";
import { fetchLeaderboard } from "@/lib/api";
import type { LeaderboardRow, Tier } from "@/lib/types";

type CountryOption = {
  code: string;
  name: string;
};

type LeaderboardShellProps = {
  initialRows?: LeaderboardRow[];
  initialAllRows?: LeaderboardRow[];
  initialCountryOptions?: CountryOption[];
  initialTier?: Tier | "";
  initialCountry?: string;
  initialLimit?: number;
  selectedYear?: string;
};

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

function confidenceLabel(value: number | null | undefined): string {
  if (value === null || value === undefined || Number.isNaN(value)) return "Pending";
  const score = value * 100;
  if (score >= 92) return "High";
  if (score >= 86) return "Medium";
  return "Building";
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
    needs_formula_review: "marked for formula review"
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
      : "Main source: unavailable"
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
      {row.country_flag_url ? (
        <img src={row.country_flag_url} alt={label} />
      ) : (
        <span>{row.country_code || "??"}</span>
      )}
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
        <img
          src={row.avatar_url}
          alt={`${row.username} avatar`}
          onError={() => setImgError(true)}
        />
      ) : (
        <span className="avatar-initials">{initials}</span>
      )}
    </span>
  );
}

export function LeaderboardShell({
  initialRows = [],
  initialAllRows = [],
  initialCountryOptions = [],
  initialTier = "",
  initialCountry = "",
  initialLimit = 100,
  selectedYear = "2026"
}: LeaderboardShellProps) {
  const [rows, setRows] = useState<LeaderboardRow[]>(initialRows);
  const [allRows, setAllRows] = useState<LeaderboardRow[]>(
    initialAllRows.length ? initialAllRows : initialRows
  );
  const [countryOptions, setCountryOptions] = useState<CountryOption[]>(initialCountryOptions);
  const [tier, setTier] = useState<Tier | "">(initialTier);
  const [country, setCountry] = useState(initialCountry);
  const [limit, setLimit] = useState(initialLimit);
  const [error, setError] = useState<string | null>(null);
  const [isPending, startTransition] = useTransition();

  const handleTierChange = (value: string) => setTier(value as Tier | "");
  const handleCountryChange = (value: string) => setCountry(value);
  const handleLimitChange = (value: string) => setLimit(Number(value));

  useEffect(() => {
    const tierSelect = document.getElementById("tier") as HTMLSelectElement | null;
    const countrySelect = document.getElementById("country") as HTMLSelectElement | null;
    const limitSelect = document.getElementById("limit") as HTMLSelectElement | null;
    const filterForm = document.getElementById("leaderboard-filters") as HTMLFormElement | null;

    const syncFilters = () => {
      if (tierSelect) setTier(tierSelect.value as Tier | "");
      if (countrySelect) setCountry(countrySelect.value);
      if (limitSelect) setLimit(Number(limitSelect.value));
      filterForm?.requestSubmit();
    };

    tierSelect?.addEventListener("change", syncFilters);
    tierSelect?.addEventListener("input", syncFilters);
    countrySelect?.addEventListener("change", syncFilters);
    countrySelect?.addEventListener("input", syncFilters);
    limitSelect?.addEventListener("change", syncFilters);
    limitSelect?.addEventListener("input", syncFilters);

    return () => {
      tierSelect?.removeEventListener("change", syncFilters);
      tierSelect?.removeEventListener("input", syncFilters);
      countrySelect?.removeEventListener("change", syncFilters);
      countrySelect?.removeEventListener("input", syncFilters);
      limitSelect?.removeEventListener("change", syncFilters);
      limitSelect?.removeEventListener("input", syncFilters);
    };
  }, []);

  useEffect(() => {
    if (allRows.length > 0 || initialCountryOptions.length > 0) return;

    startTransition(async () => {
      try {
        const allRows = await fetchLeaderboard({ limit: 10000 });
        setAllRows(allRows);
        const countriesByCode = new Map<string, string>();
        for (const row of allRows) {
          if (!row.country_code) continue;
          const code = row.country_code.toUpperCase();
          countriesByCode.set(code, countryName(code));
        }
        setCountryOptions(
          Array.from(countriesByCode, ([code, name]) => ({ code, name })).sort((a, b) =>
            a.name.localeCompare(b.name)
          )
        );
      } catch {
        setCountryOptions([]);
      }
    });
  }, [allRows.length, initialCountryOptions.length]);

  useEffect(() => {
    if (allRows.length === 0) return;

    setError(null);
    setRows(
      allRows
        .filter((row) => {
          const matchesTier = !tier || row.tier === tier;
          const matchesCountry = !country || row.country_code?.toUpperCase() === country;
          return matchesTier && matchesCountry;
        })
        .slice(0, limit)
    );
  }, [allRows, country, limit, tier]);

  return (
    <main className="page-shell">
      <header className="top-nav">
        <Link href="/" className="brand-mark">osu! scout</Link>
        <nav className="nav-links" aria-label="Primary">
          <a href="#leaderboard">Leaderboard</a>
          <Link href="/tournaments">Tournaments</Link>
          <a href="#methodology">Methodology</a>
        </nav>
      </header>

      <section className="hero">
        <div>
          <p className="eyebrow">Current Performance</p>
          <h1>osu! Tournament Power Rankings</h1>
          <p className="hero-copy">
            A data-driven view of recent tournament performance.
          </p>
          <p className="hero-description">
            This project helps players, captains, and analysts understand
            trends in competitive play. It does not define absolute skill.
          </p>
          <div className="year-tabs" aria-label="Year filters">
            {["2026", "2025", "2024"].map((year) => (
              <Link
                className={selectedYear === year ? "year-tab year-tab-active" : "year-tab"}
                href={`/?year=${year}&limit=${limit}`}
                key={year}
              >
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

      <section className="dashboard-grid">
        <div className="panel" id="leaderboard">
          <form
            id="leaderboard-filters"
            className="controls controls-with-apply"
            action="/"
            method="get"
            onChange={(event) => event.currentTarget.requestSubmit()}
          >
            <input type="hidden" name="year" value={selectedYear} />
            <div className="field">
              <label htmlFor="tier">Tier</label>
              <select
                id="tier"
                name="tier"
                value={tier}
                onChange={(event) => handleTierChange(event.currentTarget.value)}
                onInput={(event) => handleTierChange(event.currentTarget.value)}
              >
                <option value="">All tiers</option>
                <option value="Tier 1">Tier 1</option>
                <option value="Tier 2">Tier 2</option>
                <option value="Tier 3">Tier 3</option>
              </select>
            </div>

            <div className="field">
              <label htmlFor="country">Country</label>
              <select
                id="country"
                name="country"
                value={country}
                onChange={(event) => handleCountryChange(event.currentTarget.value)}
                onInput={(event) => handleCountryChange(event.currentTarget.value)}
              >
                <option value="">All countries</option>
                {countryOptions.map((option) => (
                  <option value={option.code} key={option.code}>
                    {option.name} ({option.code})
                  </option>
                ))}
              </select>
            </div>

            <div className="field">
              <label htmlFor="limit">Limit</label>
              <select
                id="limit"
                name="limit"
                value={limit}
                onChange={(event) => handleLimitChange(event.currentTarget.value)}
                onInput={(event) => handleLimitChange(event.currentTarget.value)}
              >
                <option value={20}>Top 20</option>
                <option value={50}>Top 50</option>
                <option value={100}>Top 100</option>
                <option value={250}>Top 250</option>
              </select>
            </div>

            <button className="filter-submit" type="submit">
              Apply
            </button>
          </form>

          {error ? <div className="state">API error: {error}</div> : null}
          {isPending ? <div className="state">Updating leaderboard...</div> : null}
          {!error && !isPending && rows.length === 0 ? (
            <div className="state">
              {allRows.length
                ? "No players match the selected filters."
                : "No leaderboard rows loaded. Check that the API is running and refresh this page."}
            </div>
          ) : null}

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
                  <th>Main Source</th>
                  <th>Activity</th>
                  <th>Confidence</th>
                </tr>
              </thead>
              <tbody>
                {rows.map((row) => (
                  <tr key={`${row.rank}-${row.username}`}>
                    <td className={rankClass(row.rank)}>#{row.rank}</td>
                    <td className="change-placeholder">--</td>
                    <td>
                      <Link className="player-cell" href={`/player/${encodeURIComponent(row.username)}`}>
                        <PlayerAvatar row={row} />
                        <span className="player-main">
                          <span className="username">{row.username}</span>
                          {row.aliases.length ? (
                            <span className="aliases">aka {row.aliases.join(", ")}</span>
                          ) : null}
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
                      <span className="source-cell" title={confidenceTitle(row)}>
                        <span>{row.dominant_event || "N/A"}</span>
                        <small>{formatPercent(row.dominant_event_score_share)}</small>
                      </span>
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
                ))}
              </tbody>
            </table>
          </div>
        </div>

        <FormulaCard />
      </section>
    </main>
  );
}
