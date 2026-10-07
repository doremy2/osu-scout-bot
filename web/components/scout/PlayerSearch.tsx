"use client";

import Link from "next/link";
import { useRouter } from "next/navigation";
import { useEffect, useRef, useState } from "react";
import { scoutClient } from "@/lib/scout";
import { tournamentHref } from "@/lib/scoutFormat";
import type { SearchHit } from "@/lib/scoutTypes";
import { Avatar, Flag, ModBadge, Rating } from "./ui";

/** Searches only players who took part in THIS tournament (username, partial username or osu! user id). */
export function PlayerSearch({ slug }: { slug: string }) {
  const router = useRouter();
  const [q, setQ] = useState("");
  const [hits, setHits] = useState<SearchHit[]>([]);
  const [open, setOpen] = useState(false);
  const [busy, setBusy] = useState(false);
  const [active, setActive] = useState(0);
  const box = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const term = q.trim();
    if (!term) { setHits([]); return; }
    const ctrl = new AbortController();
    setBusy(true);
    const t = setTimeout(async () => {
      try {
        const res = await scoutClient<SearchHit[]>(`/tournaments/${encodeURIComponent(slug)}/search?q=${encodeURIComponent(term)}&limit=6`, { signal: ctrl.signal });
        setHits(res);
        setActive(0);
        setOpen(true);
      } catch { /* aborted or offline: keep previous hits */ }
      finally { setBusy(false); }
    }, 150);
    return () => { clearTimeout(t); ctrl.abort(); };
  }, [q, slug]);

  useEffect(() => {
    const close = (e: MouseEvent) => { if (box.current && !box.current.contains(e.target as Node)) setOpen(false); };
    document.addEventListener("mousedown", close);
    return () => document.removeEventListener("mousedown", close);
  }, []);

  function onKey(e: React.KeyboardEvent) {
    if (e.key === "ArrowDown") { e.preventDefault(); setActive((i) => Math.min(i + 1, hits.length - 1)); }
    else if (e.key === "ArrowUp") { e.preventDefault(); setActive((i) => Math.max(i - 1, 0)); }
    else if (e.key === "Enter" && hits[active]) router.push(tournamentHref(slug, "players", hits[active].slug));
    else if (e.key === "Escape") setOpen(false);
  }

  return (
    <div className="sc-search" ref={box}>
      <label className="sc-search-box">
        <span className="sc-search-icon" aria-hidden>🔍</span>
        <input className="sc-search-input" type="search" placeholder="Search players in this tournament…" value={q}
               onChange={(e) => setQ(e.target.value)} onFocus={() => hits.length && setOpen(true)} onKeyDown={onKey}
               aria-label="Search players" autoComplete="off" />
        {busy && <span className="sc-spinner" aria-hidden />}
      </label>
      {open && q.trim() && (
        <div className="sc-search-pop" role="listbox">
          {hits.length === 0 && !busy && <p className="sc-empty">No player in this tournament matches “{q.trim()}”.</p>}
          {hits.map((h, i) => (
            <div key={h.user_id} role="option" aria-selected={i === active}
                 className={`sc-hit${i === active ? " on" : ""}`} onMouseEnter={() => setActive(i)}>
              <Avatar src={h.avatar_url} name={h.username} size={48} />
              <div className="sc-hit-body">
                <div className="sc-hit-top">
                  <strong>{h.username}</strong>
                  {h.country && <Flag country={h.country} size={16} />}
                  {h.team && <span className="sc-dim">{h.team.name}</span>}
                </div>
                <div className="sc-hit-meta">
                  <span>Overall <Rating value={h.rating} /></span>
                  <span>Rank <b>#{h.rank}</b> / {h.rank_of}</span>
                  <span>{h.maps_played} maps</span>
                </div>
                <div className="sc-hit-mods">
                  {Object.entries(h.mod_ratings).map(([m, r]) => (
                    <span key={m} className="sc-hit-mod"><ModBadge mod={m} /> <Rating value={r} /></span>
                  ))}
                </div>
              </div>
              <Link className="sc-btn sc-btn-sm" href={tournamentHref(slug, "players", h.slug)} onClick={() => setOpen(false)}>View Full Profile</Link>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
