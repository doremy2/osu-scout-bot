"use client";

import Link from "next/link";
import { useCallback, useEffect, useRef, useState } from "react";
import { scoutClient } from "@/lib/scout";
import { fmtDate, tournamentHref } from "@/lib/scoutFormat";
import type { DiscoveryCandidate, DiscoverySource, Format, ImportStatus, OsuClient } from "@/lib/scoutTypes";
import { timeAgo } from "./TrackingBadge";

type Status = DiscoveryCandidate["status"];
type Edit = { name?: string; format?: Format; client?: OsuClient };
const STATUSES: { key: Status; label: string }[] = [
  { key: "pending", label: "Review" },
  { key: "approved", label: "Approved" },
  { key: "ignored", label: "Ignored" },
  { key: "duplicate", label: "Duplicates" }
];
const TOKEN_KEY = "scout-admin-token";

function savedToken(): string {
  try { return window.sessionStorage.getItem(TOKEN_KEY) ?? ""; } catch { return ""; }
}

/** The owner's discovery queue: tournaments found by the discovery sources, waiting to be approved or ignored. */
export function AdminQueue() {
  const [token, setToken] = useState("");
  const [auth, setAuth] = useState<"checking" | "yes" | "no">("checking");
  const [authError, setAuthError] = useState<string | null>(null);
  const [status, setStatus] = useState<Status>("pending");
  const [rows, setRows] = useState<DiscoveryCandidate[]>([]);
  const [sources, setSources] = useState<DiscoverySource[]>([]);
  const [edits, setEdits] = useState<Record<number, Edit>>({});
  const [busy, setBusy] = useState<Record<number, string>>({});
  const [note, setNote] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [scanning, setScanning] = useState(false);
  const [newSource, setNewSource] = useState({ name: "", kind: "page", url: "" });
  const tokenRef = useRef("");
  const cancelled = useRef(false);

  const call = useCallback(<T,>(path: string, init: RequestInit = {}) =>
    scoutClient<T>(path, {
      ...init,
      headers: { "Content-Type": "application/json", ...(tokenRef.current ? { "X-Admin-Token": tokenRef.current } : {}) }
    }), []);

  const reload = useCallback(async (which: Status) => {
    const [c, s] = await Promise.all([
      call<DiscoveryCandidate[]>(`/admin/discovery/candidates?status=${which}`),
      call<DiscoverySource[]>("/admin/discovery/sources")
    ]);
    if (!cancelled.current) { setRows(c); setSources(s); }
  }, [call]);

  const unlock = useCallback(async (value: string) => {
    tokenRef.current = value;
    setToken(value);
    try {
      await reload("pending");
      try { window.sessionStorage.setItem(TOKEN_KEY, value); } catch { /* private mode: just don't remember it */ }
      setAuth("yes");
      setAuthError(null);
    } catch (e) {
      setAuth("no");
      setAuthError(value ? (e instanceof Error ? e.message : "Could not unlock") : null);
    }
  }, [reload]);

  useEffect(() => {
    cancelled.current = false;
    void unlock(savedToken());
    return () => { cancelled.current = true; };
  }, [unlock]);

  async function show(which: Status) {
    setStatus(which);
    setError(null);
    try { await reload(which); } catch (e) { setError(e instanceof Error ? e.message : "Could not load the queue"); }
  }

  function edit(id: number, patch: Edit) {
    setEdits((all) => ({ ...all, [id]: { ...all[id], ...patch } }));
  }

  async function approve(c: DiscoveryCandidate) {
    setError(null);
    setBusy((b) => ({ ...b, [c.id]: "Starting import…" }));
    try {
      const e = edits[c.id] ?? {};
      const res = await call<{ job: ImportStatus }>(`/admin/discovery/candidates/${c.id}/approve`, {
        method: "POST",
        body: JSON.stringify({ name: e.name, format: e.format, client: e.client })
      });
      let job = res.job;
      let failures = 0;
      // The import is the ordinary import job. On serverless hosts it advances one short step per request.
      while (!cancelled.current && job.phase !== "done" && job.phase !== "error") {
        setBusy((b) => ({ ...b, [c.id]: job.total ? `Importing ${job.done}/${job.total}` : job.message }));
        try {
          job = job.mode === "step"
            ? await call<ImportStatus>(`/imports/${job.id}/step`, { method: "POST" })
            : await call<ImportStatus>(`/imports/${job.id}`);
          failures = 0;
        } catch (err) {
          if (++failures >= 4) throw err;
        }
        await new Promise((r) => setTimeout(r, job.mode === "step" ? 200 : 900));
      }
      if (job.phase === "error") throw new Error(job.error ?? "Import failed");
      setNote(`Imported “${c.name}” as ${job.slug}.`);
      await reload(status);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Could not approve");
    } finally {
      setBusy((b) => { const { [c.id]: _gone, ...rest } = b; return rest; });
    }
  }

  async function decide(c: DiscoveryCandidate, action: "ignore" | "restore") {
    setError(null);
    try {
      await call(`/admin/discovery/candidates/${c.id}/${action}`, { method: "POST" });
      await reload(status);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Could not update the candidate");
    }
  }

  async function scan(id?: number) {
    setScanning(true);
    setError(null);
    setNote(null);
    try {
      const res = await call<{ scanned: { created: number; known: number; duplicate: number; error?: string }[] }>(
        "/admin/discovery/scan", { method: "POST", body: JSON.stringify({ source_id: id ?? null }) });
      const created = res.scanned.reduce((n, s) => n + s.created, 0);
      const failed = res.scanned.find((s) => s.error);
      setNote(`Scan finished: ${created} new candidate${created === 1 ? "" : "s"}.`);
      if (failed) setError(`A source could not be read: ${failed.error}`);
      await reload(status);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Scan failed");
    } finally {
      setScanning(false);
    }
  }

  async function addSource(e: React.FormEvent) {
    e.preventDefault();
    setError(null);
    try {
      await call("/admin/discovery/sources", { method: "POST", body: JSON.stringify(newSource) });
      setNewSource({ name: "", kind: "page", url: "" });
      await reload(status);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Could not add the source");
    }
  }

  async function toggleSource(s: DiscoverySource) {
    try {
      await call(`/admin/discovery/sources/${s.id}/enabled`, { method: "POST", body: JSON.stringify({ enabled: !s.enabled }) });
      await reload(status);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Could not update the source");
    }
  }

  if (auth === "checking") return <p className="sc-dim">Checking access…</p>;
  if (auth === "no") {
    return (
      <form className="sc-form" onSubmit={(e) => { e.preventDefault(); void unlock(token); }}>
        <label className="sc-field-block">
          <span>Admin token</span>
          <input className="sc-input" type="password" autoComplete="off" value={token} onChange={(e) => setToken(e.target.value)} placeholder="SCOUT_ADMIN_TOKEN" />
          <small>The discovery queue is owner-only. The token is only sent to this site and kept for this browser tab.</small>
        </label>
        {authError && <p className="sc-error" role="alert">{authError}</p>}
        <button className="sc-btn sc-btn-primary" type="submit">Unlock</button>
      </form>
    );
  }

  return (
    <div className="sc-stack">
      {error && <p className="sc-error" role="alert">{error}</p>}
      {note && <p className="sc-note">{note}</p>}

      <div className="sc-card">
        <div className="sc-admin-head">
          <h2>Candidates</h2>
          <div className="sc-admin-tabs" role="tablist">
            {STATUSES.map((s) => (
              <button key={s.key} role="tab" aria-selected={status === s.key} className={`sc-btn sc-btn-sm${status === s.key ? " sc-btn-primary" : ""}`}
                      onClick={() => void show(s.key)}>{s.label}</button>
            ))}
          </div>
        </div>
        {rows.length === 0 ? (
          <p className="sc-empty">
            {status === "pending" ? "Nothing waiting for review. Scan a source below to look for new tournaments." : "Nothing here."}
          </p>
        ) : (
          <div className="sc-admin-scroll">
            <table className="sc-admin-table">
              <thead>
                <tr><th>Tournament</th><th>Format</th><th>MP links</th><th>Confidence</th><th>Discovered</th><th /></tr>
              </thead>
              <tbody>
                {rows.map((c) => {
                  const e = edits[c.id] ?? {};
                  const working = busy[c.id];
                  const pct = Math.round(c.confidence * 100);
                  return (
                    <tr key={c.id}>
                      <td>
                        {c.status === "pending" ? (
                          <input className="sc-input sc-admin-name" value={e.name ?? c.name} onChange={(ev) => edit(c.id, { name: ev.target.value })} aria-label="Tournament name" />
                        ) : <b>{c.name}</b>}
                        <div className="sc-admin-sub">
                          <a className="sc-link" href={c.source_url} target="_blank" rel="noreferrer">source ↗</a>
                          <span> · slug <code>{c.suggested_slug}</code></span>
                          {c.duplicate_of && <span> · duplicate of <Link className="sc-link" href={tournamentHref(c.duplicate_of)}>{c.duplicate_of}</Link></span>}
                          {c.tournament_slug && <span> · <Link className="sc-link" href={tournamentHref(c.tournament_slug)}>{c.tournament_slug}</Link></span>}
                        </div>
                        {c.job && c.status === "approved" && (
                          <div className="sc-admin-sub">{c.job.phase === "error" ? `Import failed: ${c.job.error}` : `Import ${c.job.phase}${c.job.total ? ` ${c.job.done}/${c.job.total}` : ""}`}</div>
                        )}
                        <details className="sc-admin-why"><summary>Why this score</summary>
                          <ul>{c.reasons.map((r) => <li key={r}>{r}</li>)}</ul>
                        </details>
                      </td>
                      <td>
                        {c.status === "pending" ? (
                          <div className="sc-admin-selects">
                            <select className="sc-input" value={e.format ?? c.detected_format} onChange={(ev) => edit(c.id, { format: ev.target.value as Format })} aria-label="Format">
                              <option value="1v1">1v1</option><option value="team">Team</option>
                            </select>
                            <select className="sc-input" value={e.client ?? c.detected_client} onChange={(ev) => edit(c.id, { client: ev.target.value as OsuClient })} aria-label="osu! client">
                              <option value="stable">Stable</option><option value="lazer">Lazer</option>
                            </select>
                          </div>
                        ) : <span>{c.detected_format === "team" ? "Team" : "1v1"} · {c.detected_client}</span>}
                      </td>
                      <td className="sc-num">{c.match_count}{c.room_count > 0 && <small className="sc-dim"> ({c.room_count} rooms)</small>}</td>
                      <td>
                        <div className="sc-confbar" title={c.reasons.join("\n")}><i style={{ width: `${pct}%` }} className={pct >= 75 ? "hi" : pct >= 45 ? "mid" : "lo"} /></div>
                        <small>{pct}%</small>
                      </td>
                      <td className="sc-dim" title={c.discovered_at ?? undefined}>{fmtDate(c.discovered_at)}</td>
                      <td className="sc-admin-actions">
                        {c.status === "pending" && (
                          <>
                            <button className="sc-btn sc-btn-sm sc-btn-primary" disabled={!!working} onClick={() => void approve(c)}>{working ?? "Approve"}</button>
                            <button className="sc-btn sc-btn-sm" disabled={!!working} onClick={() => void decide(c, "ignore")}>Ignore</button>
                          </>
                        )}
                        {(c.status === "ignored" || c.status === "duplicate") && (
                          <button className="sc-btn sc-btn-sm" onClick={() => void decide(c, "restore")}>Move to review</button>
                        )}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        )}
      </div>

      <div className="sc-card">
        <div className="sc-admin-head">
          <h2>Discovery sources</h2>
          <button className="sc-btn sc-btn-sm" disabled={scanning || sources.length === 0} onClick={() => void scan()}>{scanning ? "Scanning…" : "Scan all now"}</button>
        </div>
        <p className="sc-note">
          Public pages that list tournaments (a forum thread, a wiki page) or an index spreadsheet. A scan only finds Google Sheets
          that contain osu! multiplayer links and adds them to the queue above; nothing is published until you approve it.
        </p>
        {sources.length > 0 && (
          <ul className="sc-admin-sources">
            {sources.map((s) => (
              <li key={s.id} className={s.enabled ? "" : "off"}>
                <div>
                  <b>{s.name}</b> <span className="sc-tag">{s.kind}</span>
                  <div className="sc-admin-sub">
                    <a className="sc-link" href={s.url} target="_blank" rel="noreferrer">{s.url.length > 70 ? `${s.url.slice(0, 70)}…` : s.url}</a>
                    <span> · every {s.scan_interval_min >= 60 ? `${Math.round(s.scan_interval_min / 60)} h` : `${s.scan_interval_min} min`}</span>
                    <span> · {s.last_scanned_at ? `scanned ${timeAgo(s.last_scanned_at)}, ${s.last_found} sheets seen` : "not scanned yet"}</span>
                    {s.last_status === "error" && <span className="sc-warn"> · {s.last_error}</span>}
                  </div>
                </div>
                <div className="sc-admin-actions">
                  <button className="sc-btn sc-btn-sm" disabled={scanning} onClick={() => void scan(s.id)}>Scan</button>
                  <button className="sc-btn sc-btn-sm" onClick={() => void toggleSource(s)}>{s.enabled ? "Disable" : "Enable"}</button>
                </div>
              </li>
            ))}
          </ul>
        )}
        <form className="sc-admin-add" onSubmit={addSource}>
          <input className="sc-input" required placeholder="Name (e.g. osu! forum: tournaments)" value={newSource.name} onChange={(e) => setNewSource({ ...newSource, name: e.target.value })} aria-label="Source name" />
          <input className="sc-input" required type="url" placeholder="https://…" value={newSource.url} onChange={(e) => setNewSource({ ...newSource, url: e.target.value })} aria-label="Source URL" />
          <select className="sc-input" value={newSource.kind} onChange={(e) => setNewSource({ ...newSource, kind: e.target.value })} aria-label="Source kind">
            <option value="page">Web page</option><option value="sheet">Index sheet</option>
          </select>
          <button className="sc-btn sc-btn-primary" type="submit">Add source</button>
        </form>
      </div>
    </div>
  );
}
