"use client";

import Link from "next/link";
import { useRouter } from "next/navigation";
import { useCallback, useEffect, useRef, useState } from "react";
import { scoutClient } from "@/lib/scout";
import { fmtRecord, tournamentHref } from "@/lib/scoutFormat";
import type { LeaderboardData, LeaderboardMode, TeamListItem } from "@/lib/scoutTypes";
import { Avatar, Flag, ModBadge, Rating } from "./ui";

const MODES: { key: LeaderboardMode; label: string }[] = [
  { key: "overall", label: "Overall" },
  { key: "round", label: "By Round" },
  { key: "mod", label: "By Mod" },
  { key: "consistency", label: "Consistency" },
  { key: "maps", label: "Maps Played" }
];

type Props = {
  slug: string;
  initial: LeaderboardData;
  pageSize?: number;
  hasTeams?: boolean;
  teams?: TeamListItem[];
  /** Hide the mode switcher (e.g. a compact overview card). */
  compact?: boolean;
  viewAllHref?: string;
};

export function LeaderboardPanel({ slug, initial, pageSize = 25, hasTeams = false, teams, compact = false, viewAllHref }: Props) {
  const router = useRouter();
  const [view, setView] = useState<"players" | "teams">("players");
  const [mode, setMode] = useState<LeaderboardMode>(initial.mode);
  const [key, setKey] = useState<string | null>(initial.key);
  const [q, setQ] = useState("");
  const [limit, setLimit] = useState(pageSize);
  const [data, setData] = useState<LeaderboardData>(initial);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const first = useRef(true);
  const { rounds, mods } = initial.options;

  const load = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const params = new URLSearchParams({ mode, limit: String(limit) });
      if (key) params.set("key", key);
      if (q.trim()) params.set("q", q.trim());
      setData(await scoutClient<LeaderboardData>(`/tournaments/${encodeURIComponent(slug)}/leaderboard?${params}`));
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to load leaderboard");
    } finally {
      setLoading(false);
    }
  }, [slug, mode, key, q, limit]);

  useEffect(() => {
    if (first.current) { first.current = false; return; }
    const t = setTimeout(load, q ? 200 : 0);
    return () => clearTimeout(t);
  }, [load, q]);

  function pickMode(next: LeaderboardMode) {
    setMode(next);
    setLimit(pageSize);
    if (next === "round") setKey(rounds[0]?.key ?? null);
    else if (next === "mod") setKey(mods[0] ?? null);
    else setKey(null);
  }

  const keyed = mode === "round" || mode === "mod";
  const showTeam = hasTeams && !compact;
  const open = (slugName: string) => router.push(tournamentHref(slug, "players", slugName));

  return (
    <div className="sc-lb">
      {hasTeams && teams && !compact && (
        <div className="sc-seg" role="tablist" aria-label="Leaderboard type">
          <button role="tab" aria-selected={view === "players"} className={view === "players" ? "on" : ""} onClick={() => setView("players")}>Players</button>
          <button role="tab" aria-selected={view === "teams"} className={view === "teams" ? "on" : ""} onClick={() => setView("teams")}>Teams</button>
        </div>
      )}

      {view === "teams" && teams ? (
        <TeamTable slug={slug} teams={teams} />
      ) : (
        <>
          {!compact && (
            <div className="sc-lb-controls">
              <div className="sc-field">
                <span className="sc-field-label">Ranking</span>
                <div className="sc-seg sc-seg-wrap" role="tablist" aria-label="Ranking mode">
                  {MODES.map((m) => (
                    <button key={m.key} role="tab" aria-selected={mode === m.key} className={mode === m.key ? "on" : ""}
                            onClick={() => pickMode(m.key)}>{m.label}</button>
                  ))}
                </div>
              </div>
              {keyed && (
                <div className="sc-field">
                  <span className="sc-field-label">{mode === "round" ? "Round" : "Mod"}</span>
                  <div className="sc-chips">
                    {mode === "round"
                      ? rounds.map((r) => (
                          <button key={r.key} className={`sc-chip${key === r.key ? " on" : ""}`} onClick={() => { setKey(r.key); setLimit(pageSize); }}>{r.name}</button>
                        ))
                      : mods.map((m) => (
                          <button key={m} className={`sc-chip sc-chip-mod${key === m ? " on" : ""}`} onClick={() => { setKey(m); setLimit(pageSize); }}>{m}</button>
                        ))}
                  </div>
                </div>
              )}
              <input className="sc-input sc-lb-filter" type="search" placeholder="Filter by username…" value={q}
                     onChange={(e) => { setQ(e.target.value); setLimit(pageSize); }} aria-label="Filter players" />
            </div>
          )}
          {data.note && <p className="sc-note">{data.note}</p>}
          {error && <p className="sc-error">{error}</p>}
          <div className={`sc-table-wrap${loading ? " sc-loading" : ""}`}>
            <table className="sc-table sc-lb-table">
              <thead>
                <tr>
                  <th className="sc-num">#</th>
                  <th>Player</th>
                  {showTeam && <th className="sc-hide-sm">Team</th>}
                  {data.columns.extra_label && <th className="sc-num">{data.columns.extra_label}</th>}
                  <th className="sc-num">Rating</th>
                  <th className="sc-num">Maps</th>
                </tr>
              </thead>
              <tbody>
                {data.rows.map((r) => (
                  <tr key={r.user_id} className="sc-row-link" onClick={() => open(r.slug)}>
                    <td className={`sc-num sc-rank sc-rank-${r.rank <= 3 ? r.rank : "n"}`}>{r.rank}</td>
                    <td>
                      <Link className="sc-player" href={tournamentHref(slug, "players", r.slug)} onClick={(e) => e.stopPropagation()}>
                        <Avatar src={r.avatar_url} name={r.username} size={30} />
                        <span className="sc-player-name">{r.username}</span>
                        {r.country && <Flag country={r.country} size={16} />}
                      </Link>
                    </td>
                    {showTeam && (
                      <td className="sc-hide-sm">
                        {r.team && (
                          <Link className="sc-team-link" href={tournamentHref(slug, "teams", r.team.slug)} onClick={(e) => e.stopPropagation()}>
                            {r.team.name}
                          </Link>
                        )}
                      </td>
                    )}
                    {data.columns.extra_label && <td className="sc-num sc-dim">{r.extra?.toFixed(2)}</td>}
                    <td className="sc-num"><Rating value={r.rating} /></td>
                    <td className="sc-num sc-dim">{r.maps}</td>
                  </tr>
                ))}
                {data.rows.length === 0 && !loading && (
                  <tr><td colSpan={6} className="sc-empty">No players match.</td></tr>
                )}
              </tbody>
            </table>
          </div>
          <div className="sc-lb-foot">
            <span className="sc-dim">
              Showing {data.rows.length} of {data.total}
              {mode === "mod" && key && <> · <ModBadge mod={key} /></>}
            </span>
            {data.rows.length < data.total && !compact && (
              <button className="sc-btn sc-btn-ghost" onClick={() => setLimit((l) => l + pageSize * 2)}>Show more</button>
            )}
            {viewAllHref && <Link className="sc-btn sc-btn-ghost" href={viewAllHref}>Full leaderboards →</Link>}
          </div>
        </>
      )}
    </div>
  );
}

export function TeamTable({ slug, teams }: { slug: string; teams: TeamListItem[] }) {
  const router = useRouter();
  return (
    <div className="sc-table-wrap">
      <table className="sc-table sc-lb-table">
        <thead>
          <tr>
            <th className="sc-num">#</th><th>Team</th><th className="sc-num">Rating</th>
            <th className="sc-num">Record</th><th className="sc-num sc-hide-sm">Maps</th>
          </tr>
        </thead>
        <tbody>
          {teams.map((t) => (
            <tr key={t.slug} className="sc-row-link" onClick={() => router.push(tournamentHref(slug, "teams", t.slug))}>
              <td className={`sc-num sc-rank sc-rank-${t.rank <= 3 ? t.rank : "n"}`}>{t.rank}</td>
              <td>
                <Link className="sc-team-link" href={tournamentHref(slug, "teams", t.slug)} onClick={(e) => e.stopPropagation()}>
                  <Flag country={t.country} size={22} /> <span>{t.name}</span>
                </Link>
              </td>
              <td className="sc-num"><Rating value={t.rating} /></td>
              <td className="sc-num">{fmtRecord(t.match_record)}</td>
              <td className="sc-num sc-dim sc-hide-sm">{t.maps}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
