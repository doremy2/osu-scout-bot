"use client";

import Link from "next/link";
import { useRouter } from "next/navigation";
import { useEffect, useRef, useState } from "react";
import { scoutClient } from "@/lib/scout";
import { tournamentHref } from "@/lib/scoutFormat";
import type { Format, ImportStatus } from "@/lib/scoutTypes";

function slugify(text: string): string {
  return text.normalize("NFKD").replace(/[̀-ͯ]/g, "").toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-+|-+$/g, "");
}

const FORMAT_OPTIONS: { key: Format; label: string; blurb: string }[] = [
  { key: "1v1", label: "1v1", blurb: "Individual players · matches are Player A vs Player B" },
  { key: "team", label: "Team", blurb: "Teams or countries · matches are Team A vs Team B" }
];

const STEPS = [
  { phase: "scanning", label: "Scanning sheet" },
  { phase: "importing", label: "Importing matches" },
  { phase: "calculating", label: "Calculating ratings" },
  { phase: "done", label: "Tournament ready" }
] as const;

export function ImportForm() {
  const router = useRouter();
  const [name, setName] = useState("");
  const [acronym, setAcronym] = useState("");
  const [slug, setSlug] = useState("");
  const [slugTouched, setSlugTouched] = useState(false);
  const [sheet, setSheet] = useState("");
  const [format, setFormat] = useState<Format | "">("");
  const [error, setError] = useState<string | null>(null);
  const [policy, setPolicy] = useState<"open" | "token" | "off" | null>(null);
  const [token, setToken] = useState("");
  const [job, setJob] = useState<ImportStatus | null>(null);
  const timer = useRef<ReturnType<typeof setInterval> | null>(null);
  const cancelled = useRef(false);

  useEffect(() => {
    cancelled.current = false;       // (React StrictMode mounts twice in dev)
    return () => { cancelled.current = true; if (timer.current) clearInterval(timer.current); };
  }, []);

  // Public deployments protect imports: ask the server whether they are open, token-gated or off.
  useEffect(() => {
    scoutClient<{ imports: "open" | "token" | "off" }>("/health").then((h) => setPolicy(h.imports)).catch(() => setPolicy("open"));
  }, []);

  function onName(v: string) {
    setName(v);
    if (!slugTouched) setSlug(slugify(acronym ? `${acronym}` : v));
  }

  async function submit(e: React.FormEvent) {
    e.preventDefault();
    setError(null);
    if (!format) { setError("Choose a tournament format."); return; }
    try {
      const started = await scoutClient<ImportStatus>("/imports", {
        method: "POST",
        headers: { "Content-Type": "application/json", ...(token ? { "X-Admin-Token": token } : {}) },
        body: JSON.stringify({ name, acronym, slug, sheet_url: sheet, format })
      });
      setJob(started);
      const finish = (s: ImportStatus) => {
        if (s.phase === "done") setTimeout(() => router.push(tournamentHref(s.slug)), 1200);
      };
      if (started.mode === "step") {
        // Serverless host: nothing runs in the background, so keep asking the server to do the next slice of work.
        // Each request is short; the job lives in the database, so a dropped request just means asking again.
        let failures = 0;
        while (!cancelled.current) {
          try {
            const s = await scoutClient<ImportStatus>(`/imports/${started.id}/step`, {
              method: "POST",
              headers: token ? { "X-Admin-Token": token } : {}
            });
            failures = 0;
            setJob(s);
            if (s.phase === "done" || s.phase === "error") { finish(s); break; }
          } catch (err) {
            if (++failures >= 4) {
              setError(err instanceof Error ? err.message : "Lost contact with the server");
              break;
            }
            await new Promise((r) => setTimeout(r, 1500));
          }
        }
        return;
      }
      timer.current = setInterval(async () => {
        try {
          const s = await scoutClient<ImportStatus>(`/imports/${started.id}`);
          setJob(s);
          if (s.phase === "done" || s.phase === "error") {
            if (timer.current) clearInterval(timer.current);
            finish(s);
          }
        } catch (err) {
          if (timer.current) clearInterval(timer.current);
          setError(err instanceof Error ? err.message : "Lost contact with the server");
        }
      }, 700);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Could not start the import");
    }
  }

  if (job) return <Progress job={job} error={error} onRetry={() => { setJob(null); setError(null); }} />;
  if (policy === "off") {
    return (
      <p className="sc-empty">
        Imports currently run locally. To add a tournament, ask the site owner or run the importer yourself
        (<code>python -m scout import</code>).
      </p>
    );
  }

  return (
    <form className="sc-form" onSubmit={submit}>
      <label className="sc-field-block">
        <span>Tournament name</span>
        <input className="sc-input" required value={name} onChange={(e) => onName(e.target.value)} placeholder="4 Digit World Cup 2026" />
      </label>
      <div className="sc-form-row">
        <label className="sc-field-block">
          <span>Acronym</span>
          <input className="sc-input" value={acronym} onChange={(e) => { setAcronym(e.target.value); if (!slugTouched) setSlug(slugify(e.target.value || name)); }} placeholder="4WC" />
        </label>
        <label className="sc-field-block">
          <span>Slug <small>(used in the URL)</small></span>
          <input className="sc-input" required value={slug} pattern="[a-z0-9]+(-[a-z0-9]+)*" title="lowercase letters, numbers and dashes"
                 onChange={(e) => { setSlug(e.target.value); setSlugTouched(true); }} placeholder="4wc-2026" />
        </label>
      </div>
      <label className="sc-field-block">
        <span>Google Sheet URL</span>
        <input className="sc-input" required type="url" value={sheet} onChange={(e) => setSheet(e.target.value)}
               placeholder="https://docs.google.com/spreadsheets/d/…" />
        <small>Share it as “anyone with the link can view”. We read the MP links and round headers from every tab.</small>
      </label>
      <fieldset className="sc-field-block sc-format">
        <legend>Tournament format <em>required</em></legend>
        <div className="sc-format-grid">
          {FORMAT_OPTIONS.map((o) => (
            <label key={o.key} className={`sc-format-opt${format === o.key ? " on" : ""}`}>
              <input type="radio" name="format" value={o.key} checked={format === o.key} onChange={() => setFormat(o.key)} required />
              <strong>{o.label}</strong>
              <span>{o.blurb}</span>
            </label>
          ))}
        </div>
      </fieldset>
      {policy === "token" && (
        <label className="sc-field-block">
          <span>Invite code</span>
          <input className="sc-input" required type="password" autoComplete="off" value={token}
                 onChange={(e) => setToken(e.target.value)} placeholder="Invite code" />
          <small>Imports are invite-only to protect the osu! API quota. Enter the invite code you were given; it is only sent to this site.</small>
        </label>
      )}
      {error && <p className="sc-error" role="alert">{error}</p>}
      <button className="sc-btn sc-btn-primary sc-btn-lg" type="submit">Import tournament</button>
    </form>
  );
}

function Progress({ job, error, onRetry }: { job: ImportStatus; error: string | null; onRetry: () => void }) {
  const idx = job.phase === "error" ? -1 : STEPS.findIndex((s) => s.phase === job.phase);
  const pct = job.total ? Math.round((job.done / job.total) * 100) : job.phase === "done" ? 100 : 0;
  return (
    <div className="sc-progress">
      <h2>{job.phase === "done" ? "Tournament ready ✓" : job.phase === "error" ? "Import failed" : "Importing…"}</h2>
      <ol className="sc-steps">
        {STEPS.map((s, i) => (
          <li key={s.phase} className={i < idx || job.phase === "done" ? "done" : i === idx ? "now" : ""}>
            <span className="sc-step-dot" />
            <div>
              <strong>{s.label}</strong>
              {s.phase === "scanning" && job.found > 0 && <p>Found {job.found} multiplayer matches</p>}
              {s.phase === "importing" && job.total > 0 && (
                <>
                  <p>{job.done} / {job.total}{job.failed ? ` · ${job.failed} failed` : ""}{job.not_found ? ` · ${job.not_found} not found` : ""}</p>
                  <div className="sc-bar"><div style={{ width: `${pct}%` }} /></div>
                </>
              )}
            </div>
          </li>
        ))}
      </ol>
      {(job.error || error) && <p className="sc-error" role="alert">{job.error ?? error}</p>}
      {error && job.phase !== "error" && <button className="sc-btn" onClick={onRetry}>Back to the form</button>}
      {job.phase === "error" && <button className="sc-btn" onClick={onRetry}>Back to the form</button>}
      {job.phase === "done" && <Link className="sc-btn sc-btn-primary" href={tournamentHref(job.slug)}>Open tournament report</Link>}
    </div>
  );
}
