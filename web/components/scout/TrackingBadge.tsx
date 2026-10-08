"use client";

import { useRouter } from "next/navigation";
import { useEffect, useRef, useState } from "react";
import { scoutClient } from "@/lib/scout";
import { fmtInt } from "@/lib/scoutFormat";
import type { Tracking } from "@/lib/scoutTypes";

/** "just now", "18 minutes ago", "3 hours ago", "2 days ago" */
export function timeAgo(iso: string | null | undefined, now: number = Date.now()): string {
  if (!iso) return "never";
  const secs = Math.max(0, Math.round((now - new Date(iso).getTime()) / 1000));
  if (secs < 60) return "just now";
  const units: [number, string][] = [[86400, "day"], [3600, "hour"], [60, "minute"]];
  for (const [size, name] of units) {
    if (secs >= size) {
      const n = Math.floor(secs / size);
      return `${n} ${name}${n === 1 ? "" : "s"} ago`;
    }
  }
  return "just now";
}

/** Live tracking status of a tournament: when its source was last checked and whether new matches are being imported. */
export function TrackingBadge({ slug, initial }: { slug: string; initial: Tracking | null }) {
  const router = useRouter();
  const [t, setT] = useState<Tracking | null>(initial);
  const [now, setNow] = useState(() => Date.now());
  const wasUpdating = useRef(initial?.updating ?? false);

  // keep the "x minutes ago" text honest
  useEffect(() => {
    const id = setInterval(() => setNow(Date.now()), 30_000);
    return () => clearInterval(id);
  }, []);

  // poll quickly while matches are being imported, slowly otherwise
  useEffect(() => {
    if (!initial?.enabled) return;
    let stop = false;
    let timer: ReturnType<typeof setTimeout>;
    const poll = async () => {
      try {
        const next = await scoutClient<Tracking>(`/tournaments/${encodeURIComponent(slug)}/tracking`);
        if (stop) return;
        setT(next);
        if (wasUpdating.current && !next.updating) router.refresh();      // the update finished: show the new numbers
        wasUpdating.current = next.updating;
        timer = setTimeout(poll, next.updating ? 5_000 : 60_000);
      } catch {
        if (!stop) timer = setTimeout(poll, 60_000);
      }
    };
    timer = setTimeout(poll, initial.updating ? 5_000 : 60_000);
    return () => { stop = true; clearTimeout(timer); };
  }, [slug, initial?.enabled, initial?.updating, router]);

  if (!t || !t.enabled) return null;
  const progress = t.job && t.job.total > 0 ? ` ${t.job.done}/${t.job.total}` : "";

  return (
    <div className={`sc-tracking sc-tracking-${t.state}`} role="status" aria-live="polite">
      <div className="sc-tracking-main">
        <span className={`sc-tracking-dot${t.live || t.updating ? " pulse" : ""}`} aria-hidden="true" />
        <strong>{t.state === "error" ? "Tracking paused" : "Live tracking"}</strong>
        <span className="sc-tracking-sep">·</span>
        <span title={t.last_checked_at ?? undefined}>
          {t.last_checked_at ? `Last checked ${timeAgo(t.last_checked_at, now)}` : "Waiting for the first check"}
        </span>
        <span className="sc-tracking-sep">·</span>
        <span>{fmtInt(t.matches_tracked)} matches tracked</span>
        {t.live && !t.updating && <span className="sc-tag sc-tracking-run">Running now</span>}
      </div>
      {t.updating && (
        <div className="sc-tracking-update">
          {t.new_detected > 0 && <b>{fmtInt(t.new_detected)} new {t.new_detected === 1 ? "match" : "matches"} detected</b>}
          <span>Updating…{progress}</span>
        </div>
      )}
      {t.state === "error" && (
        <div className="sc-tracking-update sc-dim">Couldn&rsquo;t read the source sheet; it will retry automatically.</div>
      )}
    </div>
  );
}
