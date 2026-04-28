import Link from "next/link";
import { notFound } from "next/navigation";
import { fetchTournamentDetail } from "@/lib/api";
import type { TournamentEntry } from "@/lib/types";

export const dynamic = "force-dynamic";

type PageProps = {
  params: Promise<{ slug: string }>;
};

function classificationLabel(entry: TournamentEntry): { label: string; description: string } {
  if (entry.import_status === "imported") {
    return {
      label: "Imported into ranking",
      description: "This tournament's match data has been imported and its players are included in the power ranking."
    };
  }
  switch (entry.classification) {
    case "production_safe":
      return {
        label: "Ready to import",
        description: "This tournament has been validated and is ready to be imported into the ranking system."
      };
    case "likely_importable":
      return {
        label: "Likely importable",
        description: "This tournament has enough data to likely be importable, but has not been validated yet."
      };
    case "partial":
      return {
        label: "Discovered — partial data",
        description: "This tournament was discovered from Stage or wiki sources. Not enough data to import into rankings yet."
      };
    case "stage_only":
      return {
        label: "Stage listing only",
        description: "This tournament is listed on Stage but has no additional data sources."
      };
    case "ignore":
      return {
        label: "Skipped",
        description: "This tournament was reviewed and excluded from the ranking system."
      };
    default:
      return { label: entry.classification || "Unknown", description: "" };
  }
}

function modeName(mode: string): string {
  switch (mode.toLowerCase()) {
    case "osu": return "osu!standard";
    case "taiko": return "osu!taiko";
    case "catch": return "osu!catch";
    case "mania": return "osu!mania";
    default: return mode || "Unknown";
  }
}

function formatDate(d: string | null): string {
  if (!d) return "Unknown";
  try {
    return new Date(d).toLocaleDateString("en-US", { month: "long", day: "numeric", year: "numeric" });
  } catch {
    return d;
  }
}

function tierLabel(tier: string | null): string {
  if (!tier) return "Unclassified";
  const labels: Record<string, string> = {
    world_cup: "World Cup",
    premier: "Premier",
    major: "Major",
    minor: "Minor",
  };
  return labels[tier] || tier;
}

export default async function TournamentDetailPage({ params }: PageProps) {
  const { slug } = await params;
  let entry: TournamentEntry;
  try {
    entry = await fetchTournamentDetail(slug);
  } catch {
    notFound();
  }

  const status = classificationLabel(entry);

  return (
    <main className="page-shell">
      <header className="top-nav">
        <Link href="/" className="brand-mark">osu! scout</Link>
        <nav className="nav-links" aria-label="Primary">
          <Link href="/">Leaderboard</Link>
          <Link href="/tournaments">Tournaments</Link>
        </nav>
      </header>

      <div className="profile-header">
        <div>
          <Link className="back-link" href={`/tournaments/${entry.year}`}>
            Back to {entry.year} tournaments
          </Link>
          <p className="eyebrow">Tournament Detail</p>
          <div className="profile-title">
            <h1>{entry.name}</h1>
            <span className={`cls-badge cls-${entry.import_status === "imported" ? "imported" : entry.classification}`}>
              {status.label}
            </span>
          </div>
          <p className="hero-copy">{status.description}</p>
        </div>
        <div className="hero-card">
          <strong>{entry.year}</strong>
          <span>{modeName(entry.game_mode)}</span>
        </div>
      </div>

      <section className="profile-grid">
        <div className="panel profile-card">
          <p className="eyebrow">Tournament info</p>
          <h2>Details</h2>
          <div className="formula-list">
            <div className="formula-row"><span>Year</span><span>{entry.year}</span></div>
            <div className="formula-row"><span>Mode</span><span>{modeName(entry.game_mode)}</span></div>
            {entry.format ? <div className="formula-row"><span>Format</span><span>{entry.format}</span></div> : null}
            {entry.team_size ? <div className="formula-row"><span>Team size</span><span>{entry.team_size}</span></div> : null}
            {entry.rank_range ? <div className="formula-row"><span>Rank range</span><span>{entry.rank_range}</span></div> : null}
            {entry.tier ? <div className="formula-row"><span>Tier</span><span>{tierLabel(entry.tier)}</span></div> : null}
            <div className="formula-row"><span>Start date</span><span>{formatDate(entry.start_date)}</span></div>
            <div className="formula-row"><span>End date</span><span>{formatDate(entry.end_date)}</span></div>
            <div className="formula-row"><span>Data quality</span><span>{entry.data_quality || "Unknown"}</span></div>
          </div>
        </div>

        <div className="panel profile-card">
          <p className="eyebrow">Statistics</p>
          <h2>Numbers</h2>
          <div className="stat-grid">
            <div className="stat">
              <span>Players</span>
              <strong>{entry.player_count ?? "Unknown"}</strong>
            </div>
            <div className="stat">
              <span>Matches</span>
              <strong>{entry.match_count ?? "Unknown"}</strong>
            </div>
            <div className="stat">
              <span>Map scores</span>
              <strong>{entry.map_score_count ? entry.map_score_count.toLocaleString() : "Unknown"}</strong>
            </div>
          </div>
        </div>

        <div className="panel profile-card">
          <p className="eyebrow">Sources</p>
          <h2>Links</h2>
          <div className="tournament-detail-links">
            {entry.wiki_url ? (
              <a href={entry.wiki_url} target="_blank" rel="noreferrer" className="detail-link">osu! Wiki page</a>
            ) : null}
            {entry.stage_url ? (
              <a href={entry.stage_url} target="_blank" rel="noreferrer" className="detail-link">o!TR Stage page</a>
            ) : null}
            {entry.forum_url ? (
              <a href={entry.forum_url} target="_blank" rel="noreferrer" className="detail-link">Forum thread</a>
            ) : null}
            {entry.source_url && entry.source_url !== entry.wiki_url && entry.source_url !== entry.stage_url ? (
              <a href={entry.source_url} target="_blank" rel="noreferrer" className="detail-link">Data source</a>
            ) : null}
            {!entry.wiki_url && !entry.stage_url && !entry.forum_url ? (
              <p>No external links available for this tournament.</p>
            ) : null}
          </div>
        </div>
      </section>
    </main>
  );
}
