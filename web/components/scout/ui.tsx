import Link from "next/link";
import type { ReactNode } from "react";
import { flagSrc, fmtRating, ratingTier, tournamentHref } from "@/lib/scoutFormat";
import type { MatchItem, TeamRef } from "@/lib/scoutTypes";

export function Avatar({ src, name, size = 36 }: { src?: string | null; name: string; size?: number }) {
  return (
    // eslint-disable-next-line @next/next/no-img-element
    <img className="sc-avatar" src={src || ""} alt="" width={size} height={size} loading="lazy"
         style={{ width: size, height: size }} data-name={name} />
  );
}

export function Flag({ country, size = 20 }: { country: string | null | undefined; size?: number }) {
  const src = flagSrc(country);
  if (!src) return null;
  return (
    // eslint-disable-next-line @next/next/no-img-element
    <img className="sc-flag" src={src} alt={country ?? ""} title={country ?? ""} width={size} height={Math.round(size * 0.75)} loading="lazy" />
  );
}

export function ModBadge({ mod }: { mod: string }) {
  return <span className={`sc-mod sc-mod-${mod.toLowerCase()}`}>{mod}</span>;
}

/** Rating number with a colour tier, so exceptional performances stand out. */
export function Rating({ value, big = false }: { value: number | null | undefined; big?: boolean }) {
  if (value == null) return <span className="sc-rating sc-tier-none">–</span>;
  return <span className={`sc-rating sc-tier-${ratingTier(value)}${big ? " sc-rating-big" : ""}`}>{fmtRating(value)}</span>;
}

/** Sample confidence as a percentage; flags thin samples. */
export function Confidence({ value, low }: { value: number | null | undefined; low?: boolean }) {
  if (value == null) return null;
  const pct = Math.round(value * 100);
  return <span className={`sc-conf${low || value < 0.5 ? " sc-conf-low" : ""}`} title="How much evidence the rating rests on">{pct}%{low ? " · low" : ""}</span>;
}

export function PlayerLink({ slug, tournament, name, avatar, country, size = 28 }: {
  slug: string; tournament: string; name: string; avatar?: string | null; country?: string | null; size?: number;
}) {
  return (
    <Link className="sc-player" href={tournamentHref(tournament, "players", slug)}>
      {avatar !== undefined && <Avatar src={avatar} name={name} size={size} />}
      <span className="sc-player-name">{name}</span>
      {country && <Flag country={country} size={16} />}
    </Link>
  );
}

export function TeamLink({ team, tournament }: { team: TeamRef | { name: string; slug: string; country: string | null }; tournament: string }) {
  return (
    <Link className="sc-team-link" href={tournamentHref(tournament, "teams", team.slug)}>
      <Flag country={team.country} size={18} />
      <span>{team.name}</span>
    </Link>
  );
}

export function Stat({ label, value, sub }: { label: string; value: ReactNode; sub?: ReactNode }) {
  return (
    <div className="sc-stat">
      <div className="sc-stat-label">{label}</div>
      <div className="sc-stat-value">{value}</div>
      {sub && <div className="sc-stat-sub">{sub}</div>}
    </div>
  );
}

export function Card({ title, action, children, className = "" }: {
  title?: ReactNode; action?: ReactNode; children: ReactNode; className?: string;
}) {
  return (
    <section className={`sc-card ${className}`}>
      {(title || action) && (
        <header className="sc-card-head">
          {title && <h2>{title}</h2>}
          {action}
        </header>
      )}
      {children}
    </section>
  );
}

/** "Vietnam 3-5 Ukraine" - clickable, links to the match page. */
export function MatchLine({ match, tournament }: { match: MatchItem; tournament: string }) {
  const [a, b] = match.sides;
  const result = match.result;
  return (
    <Link className="sc-match" href={tournamentHref(tournament, "matches", String(match.osu_match_id))}>
      <span className="sc-match-round">{match.round_name}</span>
      {match.kind === "qualifier" || !a || !b ? (
        <span className="sc-match-title">{match.name?.replace(/^[^:]+:\s*/, "") || `Match ${match.osu_match_id}`}</span>
      ) : (
        <span className="sc-match-title">
          <span className={a.name === match.winner ? "sc-win" : ""}>{a.name}</span>
          <b className="sc-match-score">{a.map_wins} – {b.map_wins}</b>
          <span className={b.name === match.winner ? "sc-win" : ""}>{b.name}</span>
        </span>
      )}
      {result && <span className={`sc-result sc-result-${result}`}>{result}</span>}
    </Link>
  );
}

/** The "import a tournament" call to action. When imports are switched off (read-only public deployment) it stays
 *  visible but greyed out, explains why on hover, and links to /import which says the same in full. */
export function ImportButton({ policy, children, variant = "button" }: {
  policy: "open" | "public" | "token" | "off"; children: ReactNode; variant?: "button" | "primary" | "nav";
}) {
  const off = policy === "off";
  const base = variant === "nav" ? "" : `sc-btn${variant === "primary" ? " sc-btn-primary" : ""}`;
  return (
    <Link href="/import" className={`${base}${off ? " sc-disabled" : ""}`.trim()}
          title={off ? "Imports currently run locally" : undefined} aria-disabled={off || undefined}>
      {children}
    </Link>
  );
}

export function EmptyState({ children }: { children: ReactNode }) {
  return <p className="sc-empty">{children}</p>;
}
