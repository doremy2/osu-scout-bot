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
    return { label: "Imported into ranking", className: "cls-badge cls-imported" };
  }
  switch (entry.classification) {
    case "production_safe":
      return { label: "Ready to import", className: "cls-badge cls-production-safe" };
    case "likely_importable":
      return { label: "Likely importable", className: "cls-badge cls-likely" };
    case "partial":
      return { label: "Discovered only", className: "cls-badge cls-partial" };
    case "stage_only":
      return { label: "Stage only", className: "cls-badge cls-stage-only" };
    case "ignore":
      return { label: "Skipped", className: "cls-badge cls-ignore" };
    default:
      return { label: entry.classification || "Unknown", className: "cls-badge cls-partial" };
  }
}

function modeBadge(mode: string): string {
  return mode.toLowerCase() === "osu" ? "osu!standard" : mode || "Unknown";
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
    const [year, month, day] = d.slice(0, 10).split("-").map(Number);
    if (year && month && day) {
      return new Date(year, month - 1, day).toLocaleDateString("en-US", {
        month: "short",
        day: "numeric",
        year: "numeric",
      });
    }
    return new Date(d).toLocaleDateString("en-US", { month: "short", day: "numeric", year: "numeric" });
  } catch {
    return d;
  }
}

function dateRange(entry: TournamentEntry): string {
  const start = formatDate(entry.start_date);
  const end = formatDate(entry.end_date);
  if (start && end) return `${start} - ${end}`;
  if (start) return `From ${start}`;
  if (end) return `Until ${end}`;
  return "";
}

function formatGroup(format: string | null): string {
  const value = (format || "").toLowerCase();
  if (["1v1", "2v2", "3v3", "4v4"].includes(value)) return value;
  if (value.includes("team")) return "team";
  return value || "unknown";
}

function matchesStatus(entry: TournamentEntry, status: string): boolean {
  if (!status) return true;
  if (status === "imported") return entry.import_status === "imported";
  if (status === "discovered") return entry.import_status !== "imported";
  if (status === "likely_importable") {
    return entry.classification === "likely_importable" || entry.classification === "production_safe";
  }
  return entry.classification === status;
}

export function TournamentListingShell({
  initialCatalog,
  initialYear,
  initialMode = "",
  initialStatus = "",
}: TournamentListingShellProps) {
  const [year, setYear] = useState(initialYear ? String(initialYear) : "");
  const [mode, setMode] = useState(initialMode);
  const [status, setStatus] = useState(initialStatus);
  const [rankType, setRankType] = useState("");
  const [format, setFormat] = useState("");
  const catalog = initialCatalog;

  const filtered = catalog.rows.filter((row) => {
    if (year && row.year !== Number(year)) return false;
    if (mode && row.game_mode.toLowerCase() !== mode.toLowerCase()) return false;
    if (!matchesStatus(row, status)) return false;
    if (rankType && row.rank_type !== rankType) return false;
    if (format && formatGroup(row.format) !== format) return false;
    return true;
  });

  const importedCount = filtered.filter((r) => r.import_status === "imported").length;
  const likelyCount = filtered.filter(
    (r) => r.classification === "production_safe" || r.classification === "likely_importable",
  ).length;
  const discoveredCount = filtered.length - importedCount;
  const grouped = [2026, 2025, 2024]
    .map((groupYear) => ({
      year: groupYear,
      rows: filtered.filter((entry) => entry.year === groupYear),
    }))
    .filter((group) => group.rows.length > 0);

  return (
    <main className="page-shell">
      <header className="top-nav">
        <Link href="/legacy" className="brand-mark">osu! scout</Link>
        <nav className="nav-links" aria-label="Primary">
          <Link href="/legacy">Leaderboard</Link>
          <Link href="/legacy/tournaments">Tournaments</Link>
        </nav>
      </header>

      <section className="hero">
        <div>
          <p className="eyebrow">Tournament Database</p>
          <h1>Tournament Discovery</h1>
          <p className="hero-copy">
            Full 2024-2026 osu!standard tournament visibility from Stage discovery and the import queue.
          </p>
          <p className="hero-description">
            <strong>{importedCount}</strong> tournaments are imported into the power ranking.{" "}
            <strong>{likelyCount}</strong> are marked likely importable.{" "}
            <strong>{discoveredCount}</strong> are discovered only and need review before ranking import.
          </p>
          <div className="year-tabs" aria-label="Year filters">
            {[undefined, 2026, 2025, 2024].map((y) => {
              const href = y ? `/legacy/tournaments/${y}` : "/legacy/tournaments";
              const active = y === initialYear || (y === undefined && !initialYear);
              return (
                <Link className={active ? "year-tab year-tab-active" : "year-tab"} href={href} key={y ?? "all"}>
                  {y ?? "All"}
                </Link>
              );
            })}
          </div>
        </div>
        <div className="hero-card">
          <strong>{filtered.length.toLocaleString()}</strong>
          <span>osu!standard tournaments shown</span>
        </div>
      </section>

      <section className="dashboard-grid">
        <div className="panel" id="tournaments">
          <div className="controls tournament-controls">
            <div className="field">
              <label htmlFor="t-year">Year</label>
              <select id="t-year" value={year} onChange={(e) => setYear(e.target.value)}>
                <option value="">All years</option>
                <option value="2026">2026</option>
                <option value="2025">2025</option>
                <option value="2024">2024</option>
              </select>
            </div>
            <div className="field">
              <label htmlFor="t-mode">Mode</label>
              <select id="t-mode" value={mode} onChange={(e) => setMode(e.target.value)}>
                <option value="">osu!standard only</option>
                <option value="osu">osu!standard</option>
              </select>
            </div>
            <div className="field">
              <label htmlFor="t-status">Status</label>
              <select id="t-status" value={status} onChange={(e) => setStatus(e.target.value)}>
                <option value="">All statuses</option>
                <option value="imported">Imported into ranking</option>
                <option value="likely_importable">Likely importable</option>
                <option value="stage_only">Stage-only</option>
                <option value="partial">Partial</option>
                <option value="discovered">Discovered only</option>
              </select>
            </div>
            <div className="field">
              <label htmlFor="t-rank">Rank range</label>
              <select id="t-rank" value={rankType} onChange={(e) => setRankType(e.target.value)}>
                <option value="">All rank ranges</option>
                <option value="open">Open rank</option>
                <option value="restricted">Rank-restricted</option>
                <option value="unknown">Unknown</option>
              </select>
            </div>
            <div className="field">
              <label htmlFor="t-format">Format</label>
              <select id="t-format" value={format} onChange={(e) => setFormat(e.target.value)}>
                <option value="">All formats</option>
                <option value="1v1">1v1</option>
                <option value="2v2">2v2</option>
                <option value="3v3">3v3</option>
                <option value="4v4">4v4</option>
                <option value="team">Team</option>
              </select>
            </div>
          </div>

          {filtered.length === 0 ? (
            <div className="state">
              {catalog.total === 0
                ? "No tournaments loaded. Check the Stage discovery report files."
                : "No tournaments match the selected filters."}
            </div>
          ) : null}

          <div className="tournament-groups">
            {grouped.map((group) => (
              <section className="tournament-year-group" key={group.year}>
                <div className="year-group-header">
                  <h2>{group.year}</h2>
                  <span>{group.rows.length.toLocaleString()} tournaments</span>
                </div>
                <div className="tournament-grid">
                  {group.rows.map((entry) => {
                    const badge = classificationBadge(entry);
                    const tier = tierLabel(entry.tier);
                    const dates = dateRange(entry);
                    return (
                      <article className="tournament-card" key={entry.slug}>
                        <div className="tournament-card-header">
                          <div>
                            <p className="tournament-acronym">{entry.acronym || "TBD"}</p>
                            <h3>
                              <Link href={`/legacy/tournaments/detail/${entry.slug}`}>{entry.name}</Link>
                            </h3>
                          </div>
                          <span className={badge.className}>{badge.label}</span>
                        </div>
                        <div className="tournament-card-meta">
                          <span className="tournament-year">{entry.year}</span>
                          <span className="tournament-mode">{modeBadge(entry.game_mode)}</span>
                          {entry.format ? <span>{entry.format}</span> : null}
                          {entry.team_size ? <span>{entry.team_size} per team</span> : null}
                          {tier ? <span className="tournament-tier">{tier}</span> : null}
                          {entry.rank_range ? <span>{entry.rank_range}</span> : <span>Rank range unknown</span>}
                        </div>
                        {dates ? <p className="tournament-dates">{dates}</p> : null}
                        <div className="tournament-card-stats">
                          {entry.player_count ? <span>{entry.player_count} players</span> : null}
                          {entry.match_count ? <span>{entry.match_count} matches</span> : null}
                          {entry.verified_ratio !== null ? (
                            <span>{Math.round(entry.verified_ratio * 100)}% verified</span>
                          ) : null}
                          <span>{entry.data_quality || "unreviewed"}</span>
                        </div>
                        <div className="tournament-card-links">
                          {entry.stage_url ? (
                            <a href={entry.stage_url} target="_blank" rel="noreferrer">Stage</a>
                          ) : null}
                          {entry.forum_url ? (
                            <a href={entry.forum_url} target="_blank" rel="noreferrer">Forum</a>
                          ) : null}
                          {entry.wiki_url ? (
                            <a href={entry.wiki_url} target="_blank" rel="noreferrer">Wiki</a>
                          ) : null}
                        </div>
                      </article>
                    );
                  })}
                </div>
              </section>
            ))}
          </div>
        </div>
      </section>
    </main>
  );
}
