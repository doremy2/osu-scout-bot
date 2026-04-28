"use client";

import Link from "next/link";
import { useState } from "react";
import type { TournamentCatalog, TournamentEntry } from "@/lib/types";

type TournamentListingShellProps = {
  initialCatalog: TournamentCatalog;
  initialYear?: number;
  initialMode?: string;
  initialStatus?: string;
};

function classificationBadge(entry: TournamentEntry): { label: string; className: string } {
  if (entry.import_status === "imported") {
    return { label: "Imported", className: "cls-badge cls-imported" };
  }
  switch (entry.classification) {
    case "production_safe":
      return { label: "Ready to import", className: "cls-badge cls-production-safe" };
    case "likely_importable":
      return { label: "Likely importable", className: "cls-badge cls-likely" };
    case "partial":
      return { label: "Discovered", className: "cls-badge cls-partial" };
    case "stage_only":
      return { label: "Stage only", className: "cls-badge cls-stage-only" };
    case "ignore":
      return { label: "Skipped", className: "cls-badge cls-ignore" };
    default:
      return { label: entry.classification || "Unknown", className: "cls-badge cls-partial" };
  }
}

function modeBadge(mode: string): string {
  switch (mode.toLowerCase()) {
    case "osu": return "osu!standard";
    case "taiko": return "osu!taiko";
    case "catch": return "osu!catch";
    case "mania": return "osu!mania";
    default: return mode || "Unknown";
  }
}

function tierLabel(tier: string | null): string | null {
  if (!tier) return null;
  const labels: Record<string, string> = {
    world_cup: "World Cup",
    premier: "Premier",
    major: "Major",
    minor: "Minor",
  };
  return labels[tier] || tier;
}

function formatDate(d: string | null): string {
  if (!d) return "";
  try {
    return new Date(d).toLocaleDateString("en-US", { month: "short", day: "numeric", year: "numeric" });
  } catch {
    return d;
  }
}

function dateRange(entry: TournamentEntry): string {
  const start = formatDate(entry.start_date);
  const end = formatDate(entry.end_date);
  if (start && end) return `${start} — ${end}`;
  if (start) return `From ${start}`;
  if (end) return `Until ${end}`;
  return "";
}

export function TournamentListingShell({
  initialCatalog,
  initialYear,
  initialMode = "",
  initialStatus = "",
}: TournamentListingShellProps) {
  const [mode, setMode] = useState(initialMode);
  const [status, setStatus] = useState(initialStatus);
  const catalog = initialCatalog;

  const filtered = catalog.rows.filter((row) => {
    if (mode && row.game_mode.toLowerCase() !== mode.toLowerCase()) return false;
    if (status === "imported" && row.import_status !== "imported") return false;
    if (status === "discovered" && row.import_status !== "discovered") return false;
    return true;
  });

  const importedCount = filtered.filter((r) => r.import_status === "imported").length;
  const discoveredCount = filtered.length - importedCount;

  return (
    <main className="page-shell">
      <header className="top-nav">
        <Link href="/" className="brand-mark">osu! scout</Link>
        <nav className="nav-links" aria-label="Primary">
          <Link href="/">Leaderboard</Link>
          <Link href="/tournaments">Tournaments</Link>
        </nav>
      </header>

      <section className="hero">
        <div>
          <p className="eyebrow">Tournament Database</p>
          <h1>Tournament Discovery</h1>
          <p className="hero-copy">
            All tournaments tracked by osu! scout — imported into rankings or discovered from Stage and wiki sources.
          </p>
          <p className="hero-description">
            <strong>{importedCount}</strong> tournaments are imported into the power ranking.{" "}
            <strong>{discoveredCount}</strong> additional tournaments have been discovered but are not yet ranked.
          </p>
          <div className="year-tabs" aria-label="Year filters">
            {[undefined, 2026, 2025, 2024, 2023, 2022].map((y) => {
              const href = y ? `/tournaments/${y}` : "/tournaments";
              const active = y === initialYear || (y === undefined && !initialYear);
              return (
                <Link
                  className={active ? "year-tab year-tab-active" : "year-tab"}
                  href={href}
                  key={y ?? "all"}
                >
                  {y ?? "All"}
                </Link>
              );
            })}
          </div>
        </div>
        <div className="hero-card">
          <strong>{catalog.total.toLocaleString()}</strong>
          <span>tournaments tracked</span>
        </div>
      </section>

      <section className="dashboard-grid">
        <div className="panel" id="tournaments">
          <div className="controls">
            <div className="field">
              <label htmlFor="t-mode">Mode</label>
              <select id="t-mode" value={mode} onChange={(e) => setMode(e.target.value)}>
                <option value="">All modes</option>
                <option value="osu">osu!standard</option>
                <option value="taiko">osu!taiko</option>
                <option value="catch">osu!catch</option>
                <option value="mania">osu!mania</option>
              </select>
            </div>
            <div className="field">
              <label htmlFor="t-status">Status</label>
              <select id="t-status" value={status} onChange={(e) => setStatus(e.target.value)}>
                <option value="">All</option>
                <option value="imported">Imported into ranking</option>
                <option value="discovered">Discovered only</option>
              </select>
            </div>
          </div>

          {filtered.length === 0 ? (
            <div className="state">
              {catalog.total === 0
                ? "No tournaments loaded. Check that the API is running."
                : "No tournaments match the selected filters."}
            </div>
          ) : null}

          <div className="tournament-grid">
            {filtered.map((entry) => {
              const badge = classificationBadge(entry);
              const tier = tierLabel(entry.tier);
              const dates = dateRange(entry);
              return (
                <article className="tournament-card" key={entry.slug}>
                  <div className="tournament-card-header">
                    <h3>{entry.name}</h3>
                    <span className={badge.className}>{badge.label}</span>
                  </div>
                  <div className="tournament-card-meta">
                    <span className="tournament-year">{entry.year}</span>
                    <span className="tournament-mode">{modeBadge(entry.game_mode)}</span>
                    {entry.format ? <span>{entry.format}</span> : null}
                    {tier ? <span className="tournament-tier">{tier}</span> : null}
                    {entry.rank_range ? <span>{entry.rank_range}</span> : null}
                  </div>
                  {dates ? <p className="tournament-dates">{dates}</p> : null}
                  <div className="tournament-card-stats">
                    {entry.player_count ? <span>{entry.player_count} players</span> : null}
                    {entry.match_count ? <span>{entry.match_count} matches</span> : null}
                    {entry.map_score_count ? <span>{entry.map_score_count.toLocaleString()} scores</span> : null}
                  </div>
                  <div className="tournament-card-links">
                    {entry.wiki_url ? (
                      <a href={entry.wiki_url} target="_blank" rel="noreferrer">Wiki</a>
                    ) : null}
                    {entry.stage_url ? (
                      <a href={entry.stage_url} target="_blank" rel="noreferrer">Stage</a>
                    ) : null}
                    {entry.forum_url ? (
                      <a href={entry.forum_url} target="_blank" rel="noreferrer">Forum</a>
                    ) : null}
                  </div>
                </article>
              );
            })}
          </div>
        </div>
      </section>
    </main>
  );
}
