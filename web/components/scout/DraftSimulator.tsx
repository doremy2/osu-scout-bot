"use client";

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { scoutClient } from "@/lib/scout";
import { coverSrc } from "@/lib/scoutFormat";
import type { DraftAdvice, DraftMap, DraftSetup, DraftSide } from "@/lib/scoutTypes";
import { Avatar, Card, Flag, ModBadge, Rating } from "./ui";

type Side = "A" | "B";
type Config = { a: string; b: string; round: string; best_of: number; bans_per_side: number; first_ban: Side; first_pick: Side };

const BEST_OF = [5, 7, 9, 11, 13];
const MOD_ROWS = ["NM", "HD", "HR", "DT", "EZ", "FL", "HT", "FM", "LM", "TB"];

export function DraftSimulator({ slug, setup }: { slug: string; setup: DraftSetup }) {
  const defaultRound = (setup.rounds.find((r) => r.round === "QF") ?? setup.rounds[setup.rounds.length - 1])?.round ?? "";
  const [cfg, setCfg] = useState<Config>({
    a: setup.sides[0]?.slug ?? "", b: setup.sides[1]?.slug ?? "", round: defaultRound,
    best_of: 9, bans_per_side: 2, first_ban: "A", first_pick: "B"
  });
  const [started, setStarted] = useState(false);
  const [taken, setTaken] = useState<number[]>([]);
  const [advice, setAdvice] = useState<DraftAdvice | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [auto, setAuto] = useState(false);
  const seq = useRef(0);

  const refresh = useCallback(async (list: number[]) => {
    const id = ++seq.current;
    try {
      const res = await scoutClient<DraftAdvice>(`/tournaments/${encodeURIComponent(slug)}/draft/advice`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ ...cfg, taken: list })
      });
      if (id === seq.current) { setAdvice(res); setError(null); }
      return res;
    } catch (e) {
      if (id === seq.current) setError(e instanceof Error ? e.message : "Could not load the draft");
      return null;
    }
  }, [slug, cfg]);

  useEffect(() => { if (started) refresh(taken); }, [started, taken, refresh]);

  // auto-draft: keep taking the advised map until the draft is complete
  useEffect(() => {
    if (!auto || !advice) return;
    if (advice.done || !advice.suggestions[0]) { setAuto(false); return; }
    const t = setTimeout(() => setTaken((cur) => [...cur, advice.suggestions[0].beatmap_id]), 450);
    return () => clearTimeout(t);
  }, [auto, advice]);

  const sideName = (s: Side) => (advice ? (s === "A" ? advice.a.name : advice.b.name) : "");
  const bySlug = useMemo(() => new Map(setup.sides.map((s) => [s.slug, s])), [setup.sides]);

  if (!started) {
    return (
      <SetupPanel setup={setup} cfg={cfg} setCfg={setCfg} error={error}
                  onStart={() => { setTaken([]); setAdvice(null); setStarted(true); }} />
    );
  }

  const maps = advice?.maps ?? [];   // already in official pool order
  const suggested = new Set(advice?.suggestions.map((s) => s.beatmap_id));
  const byId = new Map(maps.map((m) => [m.beatmap_id, m]));
  const pWin = advice ? advice.match_win_a : 0.5;
  const sa = bySlug.get(cfg.a), sb = bySlug.get(cfg.b);

  function take(id: number) {
    if (!advice || advice.done || advice.maps.find((m) => m.beatmap_id === id)?.taken || id === advice.tiebreaker) return;
    setTaken((t) => [...t, id]);
  }

  return (
    <div className="sc-stack">
      <Card className="sc-dv">
        <div className="sc-dv-side sc-side-a"><SideHead side={sa} name={advice?.a.name ?? sa?.name ?? ""} tag="A" /></div>
        <div className="sc-dv-mid">
          <div className="sc-dv-label">Projected match win</div>
          <div className="sc-dv-bar" aria-label={`Side A ${Math.round(pWin * 100)}%`}>
            <i style={{ width: `${pWin * 100}%` }} />
          </div>
          <div className="sc-dv-pct"><b>{Math.round(pWin * 100)}%</b><span>first to {advice?.target ?? Math.ceil(cfg.best_of / 2)}</span><b>{Math.round((1 - pWin) * 100)}%</b></div>
        </div>
        <div className="sc-dv-side sc-side-b right"><SideHead side={sb} name={advice?.b.name ?? sb?.name ?? ""} tag="B" /></div>
      </Card>

      {advice && (
        <div className="sc-rail" aria-label="Draft order">
          {advice.sequence.map((st, i) => {
            const id = taken[i];
            const m = id ? byId.get(id) : undefined;
            return (
              <span key={i} className={`sc-rail-step sc-${st.side === "A" ? "side-a" : "side-b"}${i === advice.step ? " now" : ""}${m ? " filled" : ""}`}
                    title={m ? `${st.type} · ${m.title}` : `${st.type} · ${sideName(st.side)}`}>
                {st.type === "ban" ? "BAN" : "PICK"}
              </span>
            );
          })}
          <span className={`sc-rail-step tb${advice.done ? " now" : ""}`}>TB</span>
        </div>
      )}

      <div className="sc-turn">
        {advice?.done ? (
          <span><b>Draft complete.</b> Projected: {sideName("A")} {Math.round(pWin * 100)}% · {sideName("B")} {Math.round((1 - pWin) * 100)}%</span>
        ) : advice?.next ? (
          <span>
            <b className={advice.next.side === "A" ? "sc-side-a-text" : "sc-side-b-text"}>{sideName(advice.next.side)}</b>{" "}
            to <b>{advice.next.type}</b> <span className="sc-dim">· step {advice.step + 1} of {advice.sequence.length}</span>
          </span>
        ) : <span className="sc-dim">Loading…</span>}
        <span className="sc-turn-actions">
          <button className="sc-btn sc-btn-sm" disabled={!taken.length || auto} onClick={() => setTaken((t) => t.slice(0, -1))}>Undo</button>
          <button className="sc-btn sc-btn-sm" disabled={!advice || advice.done} onClick={() => setAuto((a) => !a)}>{auto ? "Stop" : "Auto-draft"}</button>
          <button className="sc-btn sc-btn-sm" onClick={() => { setAuto(false); setTaken([]); }}>Reset</button>
          <button className="sc-btn sc-btn-sm" onClick={() => { setAuto(false); setStarted(false); }}>Change setup</button>
        </span>
      </div>
      {error && <p className="sc-error" role="alert">{error}</p>}

      <div className="sc-dgrid">
        <SideColumn tag="A" name={sideName("A")} advice={advice} byId={byId} taken={taken} />

        <div className="sc-pool">
          <p className="sc-eyebrow">{advice?.round_name ?? "Pool"} map pool</p>
          {MOD_ROWS.filter((mod) => maps.some((m) => m.mod === mod)).map((mod) => (
            <div key={mod} className="sc-pool-row">
              <ModBadge mod={mod} />
              <div className="sc-pool-maps">
                {maps.filter((m) => m.mod === mod).map((m) => (
                  <MapCard key={m.beatmap_id} m={m} advised={suggested.has(m.beatmap_id)} disabled={!!advice?.done || m.beatmap_id === advice?.tiebreaker}
                           nameA={sideName("A")} nameB={sideName("B")} onTake={() => take(m.beatmap_id)} />
                ))}
              </div>
            </div>
          ))}
        </div>

        <SideColumn tag="B" name={sideName("B")} advice={advice} byId={byId} taken={taken} />
      </div>

      {advice && !advice.done && advice.next && (
        <Card title={<>Advice for {sideName(advice.next.side)} · {advice.next.type}</>}>
          <ol className="sc-advice">
            {advice.suggestions.map((s, i) => {
              const m = byId.get(s.beatmap_id);
              if (!m) return null;
              return (
                <li key={s.beatmap_id}>
                  <button className="sc-advice-btn" onClick={() => take(s.beatmap_id)}>
                    <span className="sc-advice-n">{i + 1}</span>
                    <ModBadge mod={m.mod} />
                    <span className="sc-advice-map">{m.artist} - {m.title} <span className="sc-dim">[{m.version}]</span></span>
                    <span className="sc-advice-why sc-dim">{s.reasons.join(" · ")}</span>
                    <span className="sc-advice-p">{Math.round(s.p_side * 100)}%<small> to win</small></span>
                  </button>
                </li>
              );
            })}
          </ol>
          <p className="sc-explain">
            Odds come from each side&rsquo;s results in this tournament: how they perform on that mod, and on that exact map if they
            played it, compared with the other side. Ban what the opponent is most likely to win; pick what you are.
          </p>
        </Card>
      )}
    </div>
  );
}

function SideHead({ side, name, tag }: { side?: DraftSide; name: string; tag: Side }) {
  return (
    <>
      <span className="sc-dv-tag">{tag}</span>
      {side?.avatar_url ? <Avatar src={side.avatar_url} name={name} size={44} /> : side?.country ? <Flag country={side.country} size={36} /> : null}
      <div>
        <div className="sc-dv-name">{name}</div>
        {side && <div className="sc-dim"><Rating value={side.rating} /> · {side.detail}</div>}
      </div>
    </>
  );
}

function SideColumn({ tag, name, advice, byId, taken }: { tag: Side; name: string; advice: DraftAdvice | null; byId: Map<number, DraftMap>; taken: number[] }) {
  const mine = (advice?.sequence ?? []).map((st, i) => ({ st, i })).filter(({ st }) => st.side === tag);
  const render = (type: "ban" | "pick") => mine.filter(({ st }) => st.type === type).map(({ i }) => {
    const m = taken[i] ? byId.get(taken[i]) : undefined;
    return (
      <li key={i} className={m ? "filled" : advice?.step === i ? "now" : ""}>
        {m ? <><ModBadge mod={m.mod} /><span>{m.title}</span></> : <span className="sc-dim">{advice?.step === i ? "choosing…" : "—"}</span>}
      </li>
    );
  });
  return (
    <aside className={`sc-col ${tag === "A" ? "sc-side-a" : "sc-side-b"}`}>
      <div className="sc-col-title">{name || `Side ${tag}`}</div>
      <div className="sc-col-h">Bans</div>
      <ul>{render("ban")}</ul>
      <div className="sc-col-h">Picks</div>
      <ul>{render("pick")}</ul>
    </aside>
  );
}

function MapCard({ m, advised, disabled, nameA, nameB, onTake }: {
  m: DraftMap; advised: boolean; disabled: boolean; nameA: string; nameB: string; onTake: () => void;
}) {
  const cover = coverSrc(m.beatmapset_id);
  const t = m.taken;
  return (
    <button className={`sc-map${t ? ` taken ${t.type} ${t.side === "A" ? "sc-side-a" : "sc-side-b"}` : ""}${advised ? " advised" : ""}`}
            disabled={disabled || !!t} onClick={onTake}
            title={[`${nameA} ${Math.round(m.p_a * 100)}% · ${nameB} ${Math.round((1 - m.p_a) * 100)}%`, ...m.reasons].join("\n")}>
      {/* eslint-disable-next-line @next/next/no-img-element */}
      {cover && <img src={cover} alt="" loading="lazy" />}
      <span className="sc-map-body">
        {m.slot && <span className="sc-map-slot">{m.slot}</span>}
        <span className="sc-map-title">{m.title}</span>
        <span className="sc-map-ver">{m.version}</span>
        <span className="sc-map-odds" aria-hidden><i style={{ width: `${m.p_a * 100}%` }} /></span>
        <span className="sc-map-pct"><span>{Math.round(m.p_a * 100)}</span><span>{Math.round((1 - m.p_a) * 100)}</span></span>
      </span>
      {t && <span className="sc-map-flag">{t.type === "ban" ? "BAN" : `PICK ${t.side}`}</span>}
      {advised && !t && <span className="sc-map-star" aria-label="advised">★</span>}
    </button>
  );
}

function SetupPanel({ setup, cfg, setCfg, onStart, error }: {
  setup: DraftSetup; cfg: Config; setCfg: (c: Config) => void; onStart: () => void; error: string | null;
}) {
  const kind = setup.side_kind === "team" ? "team" : "player";
  const set = <K extends keyof Config>(k: K, v: Config[K]) => setCfg({ ...cfg, [k]: v });
  const poolSize = setup.rounds.find((r) => r.round === cfg.round)?.maps ?? 0;
  const bad = !cfg.a || !cfg.b || cfg.a === cfg.b || poolSize < 6;
  return (
    <div className="sc-narrow sc-wide">
      <p className="sc-eyebrow">Draft simulator</p>
      <h1>Pick &amp; ban preview</h1>
      <p className="sc-dim">
        Choose two {kind}s and a round&rsquo;s map pool, then run the draft. Win odds for every map come from how each side
        actually performed in this tournament, and the tool advises the best ban or pick at each step.
      </p>
      <Card>
        <div className="sc-setup">
          <SidePicker label={`Side A ${kind}`} tag="A" sides={setup.sides} value={cfg.a} other={cfg.b} onChange={(v) => set("a", v)} />
          <SidePicker label={`Side B ${kind}`} tag="B" sides={setup.sides} value={cfg.b} other={cfg.a} onChange={(v) => set("b", v)} />
        </div>
        <div className="sc-lb-controls" style={{ marginTop: 18 }}>
          <div className="sc-field">
            <span className="sc-field-label">Map pool (round)</span>
            <div className="sc-chips">
              {setup.rounds.map((r) => (
                <button key={r.round} className={`sc-chip${cfg.round === r.round ? " on" : ""}`} onClick={() => set("round", r.round)}>
                  {r.name} <span className="sc-dim">· {r.maps}</span>
                </button>
              ))}
            </div>
          </div>
          <div className="sc-field">
            <span className="sc-field-label">Best of</span>
            <div className="sc-chips">{BEST_OF.map((n) => <button key={n} className={`sc-chip${cfg.best_of === n ? " on" : ""}`} onClick={() => set("best_of", n)}>{n}</button>)}</div>
          </div>
          <div className="sc-field">
            <span className="sc-field-label">Bans per side</span>
            <div className="sc-chips">{[0, 1, 2, 3].map((n) => <button key={n} className={`sc-chip${cfg.bans_per_side === n ? " on" : ""}`} onClick={() => set("bans_per_side", n)}>{n}</button>)}</div>
          </div>
          <div className="sc-form-row">
            <div className="sc-field">
              <span className="sc-field-label">Bans first</span>
              <div className="sc-chips">{(["A", "B"] as Side[]).map((s) => <button key={s} className={`sc-chip${cfg.first_ban === s ? " on" : ""}`} onClick={() => set("first_ban", s)}>Side {s}</button>)}</div>
            </div>
            <div className="sc-field">
              <span className="sc-field-label">Picks first</span>
              <div className="sc-chips">{(["A", "B"] as Side[]).map((s) => <button key={s} className={`sc-chip${cfg.first_pick === s ? " on" : ""}`} onClick={() => set("first_pick", s)}>Side {s}</button>)}</div>
            </div>
          </div>
        </div>
        {error && <p className="sc-error">{error}</p>}
        {cfg.a === cfg.b && <p className="sc-warn">Choose two different sides.</p>}
        {poolSize < 6 && <p className="sc-warn">This round has too few maps to draft from.</p>}
        <button className="sc-btn sc-btn-primary sc-btn-lg" style={{ marginTop: 18 }} disabled={bad} onClick={onStart}>Start draft</button>
      </Card>
      <p className="sc-note">Beta: odds are estimates built from a small number of maps per side. Use them as a guide, not a prediction.</p>
    </div>
  );
}

function SidePicker({ label, tag, sides, value, other, onChange }: {
  label: string; tag: Side; sides: DraftSide[]; value: string; other: string; onChange: (v: string) => void;
}) {
  const [q, setQ] = useState("");
  const current = sides.find((s) => s.slug === value);
  const list = useMemo(() => {
    const needle = q.trim().toLowerCase();
    return sides.filter((s) => s.slug !== other && (!needle || s.name.toLowerCase().includes(needle))).slice(0, 8);
  }, [sides, q, other]);
  return (
    <div className={`sc-picker ${tag === "A" ? "sc-side-a" : "sc-side-b"}`}>
      <span className="sc-field-label">{label}</span>
      <div className="sc-picker-current">
        <span className="sc-dv-tag">{tag}</span>
        {current?.avatar_url && <Avatar src={current.avatar_url} name={current.name} size={32} />}
        {current && !current.avatar_url && current.country && <Flag country={current.country} size={26} />}
        <b>{current?.name ?? "—"}</b>
        {current && <Rating value={current.rating} />}
      </div>
      <input className="sc-input" type="search" placeholder="Search…" value={q} onChange={(e) => setQ(e.target.value)} aria-label={label} />
      <ul className="sc-picker-list">
        {list.map((s) => (
          <li key={s.slug}>
            <button className={s.slug === value ? "on" : ""} onClick={() => { onChange(s.slug); setQ(""); }}>
              <span>{s.name}</span><Rating value={s.rating} />
            </button>
          </li>
        ))}
      </ul>
    </div>
  );
}
