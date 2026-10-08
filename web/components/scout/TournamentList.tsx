"use client";

import Link from "next/link";
import { useMemo, useState } from "react";
import { fmtDate, tournamentHref } from "@/lib/scoutFormat";
import type { TournamentListItem } from "@/lib/scoutTypes";

export function TournamentList({ tournaments, limit, initialQuery = "" }: { tournaments: TournamentListItem[]; limit?: number; initialQuery?: string }) {
  const [q, setQ] = useState(initialQuery);
  const rows = useMemo(() => {
    const needle = q.trim().toLowerCase();
    const hit = tournaments.filter((t) => !needle || `${t.name} ${t.acronym ?? ""} ${t.slug}`.toLowerCase().includes(needle));
    return limit && !needle ? hit.slice(0, limit) : hit;
  }, [tournaments, q, limit]);

  return (
    <div>
      <input className="sc-input" type="search" placeholder="Search tournaments…" value={q}
             onChange={(e) => setQ(e.target.value)} aria-label="Search tournaments" />
      <div className="sc-tcards">
        {rows.map((t) => (
          <Link key={t.slug} className="sc-tcard" href={tournamentHref(t.slug)}>
            <div className="sc-tcard-top">
              <span className="sc-tag">{t.acronym || t.slug}</span>
              <span className={`sc-tag sc-tag-${t.format}`}>{t.format_label}</span>
              {t.client === "lazer" && <span className="sc-tag sc-tag-lazer">Lazer</span>}
            </div>
            <h3>{t.name}</h3>
            <p className="sc-dim">
              {t.matches} matches · {t.players} players{t.has_teams ? ` · ${t.teams} teams` : ""}
            </p>
            {(t.start_date || t.end_date) && <p className="sc-dim">{fmtDate(t.start_date)}{t.end_date && t.end_date !== t.start_date ? ` – ${fmtDate(t.end_date)}` : ""}</p>}
            {t.pending > 0 && <p className="sc-warn">{t.pending} matches not imported yet</p>}
          </Link>
        ))}
        {rows.length === 0 && <p className="sc-empty">No tournaments found.</p>}
      </div>
    </div>
  );
}
