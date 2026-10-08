import Link from "next/link";
import type { ReactNode } from "react";
import { Card } from "@/components/scout/ui";
import { scoutGet } from "@/lib/scout";
import { tournamentHref } from "@/lib/scoutFormat";

export const dynamic = "force-dynamic";
export const metadata = { title: "Methodology" };

type Model = {
  client?: string;
  rating: Record<string, number | string | boolean | null>;
  awards: Record<string, number>;
  rounds: { code: string; name: string; order: number; weight: number }[];
  tournament: {
    slug: string;
    name: string;
    confidence_k: number;
    k_noise: number;
    k_workload: number;
    prior: number;
    field_strength: Record<string, number>;
    qualified_split: { enabled: boolean; qualified: number | null; eliminated: number | null };
  } | null;
};

function Formula({ children }: { children: string }) {
  return <pre className="sc-formula">{children}</pre>;
}

function Step({ n, title, children }: { n: string; title: string; children: ReactNode }) {
  return (
    <Card title={<><span className="sc-step-n">{n}</span> {title}</>}>
      <div className="sc-method">{children}</div>
    </Card>
  );
}

export default async function Methodology({ searchParams }: { searchParams: Promise<{ t?: string }> }) {
  const { t } = await searchParams;
  const m = await scoutGet<Model>(`/model${t ? `?slug=${encodeURIComponent(t)}` : ""}`);
  const r = m.rating;
  const a = m.awards;
  const tm = m.tournament;
  const weights = m.rounds.map((x) => `${x.name.padEnd(15)} ${x.weight.toFixed(2)}`).join("\n");

  return (
    <div className="sc-narrow sc-wide">
      <p className="sc-eyebrow">Methodology</p>
      <h1>How ratings are calculated</h1>
      <p className="sc-dim">
        Beta: this model is still being tuned, so constants and results can change. Every number below is read live from
        the scouting engine, so this page always matches what the site actually does.
      </p>
      {tm && (
        <p className="sc-note">
          Showing values for <Link className="sc-link" href={tournamentHref(tm.slug)}>{tm.name}</Link>.{" "}
          <Link className="sc-link" href="/methodology">Show defaults</Link>
        </p>
      )}

      <Step n="1" title="Normalize every score">
        {m.client === "lazer" && (
          <p>
            <b>Lazer tournament.</b> Matches are read from multiplayer rooms and scored with lazer&rsquo;s standardised total
            score (what the room history shows). It is bounded and roughly linear in performance, so it is used as is
            (<code>transform = raw</code>). Maps with lazer-only mods (DA and friends) form their own <code>LM</code> mod group.
          </p>
        )}
        <p>
          A score is only meaningful relative to the field that played the same map. Each score is turned into a
          z-score against everyone who played that beatmap (same mod, same stage). All qualifier lobbies are pooled
          together, so a strong or weak lobby cannot change anyone&rsquo;s rating.
        </p>
        <Formula>{`v     = ${r.transform}(score)               long right tail of scorev2 scores is tamed
z     = (v − mean) / std_eff            clipped to ±${r.z_clip}
std_eff² = ((n−1)·s² + ${r.std_prior_n}·σ_pool²) / ((n−1) + ${r.std_prior_n})     small groups borrow a pooled spread, so the
                                        margin of victory still matters on a map only two players played`}</Formula>
        <p>
          <b>Poor maps cost extra.</b> Below {String(-Number(r.low_score_start))}σ every further σ counts {(1 + Number(r.low_score_amp)).toFixed(0)}× :
        </p>
        <Formula>{`if z < −${r.low_score_start}:   z ← z − ${r.low_score_amp} · (−z − ${r.low_score_start})`}</Formula>
      </Step>

      <Step n="2" title="Performance Rating: how good were the scores?">
        <Formula>{`w  = round weight (later rounds count as slightly more evidence)
Z_perf = Σ(w·z) / (Σw + ${r.perf_k})            light shrinkage so one lucky map is not a 10.0
rating = ${r.base} + ${r.scale} · Z_perf          if Z_perf ≥ 0
         ${r.base} + ${r.scale_below} · Z_perf          if Z_perf < 0      (bad play falls faster than good play rises; floor ${r.min_rating})`}</Formula>
        <p>Round weights:</p>
        <Formula>{weights}</Formula>
      </Step>

      <Step n="3" title="Tournament Rating: the whole body of work">
        <p>
          Performance is not enough on its own: ten great qualifier maps are less evidence than fifty strong maps across
          a whole run. Tournament Rating adds sample confidence and credit for the strength of the field faced. There is
          no qualification bonus.
        </p>
        <Formula>{`z'   = z + ${r.field_strength_weight} · strength(round)        strength = how much better the players in that round were
                                          than the qualifier field (their average qualifier performance)
obs  = Σ(w·z') / Σw
k    = k_noise + ${r.confidence_workload_weight} · (average maps a participant plays)
conf = Σw / (Σw + k)                        sample confidence
Z_T  = conf · obs + (1 − conf) · prior      prior = the average participant
       (players below the prior are shrunk ${Number(r.below_average_k_factor) === 0 ? "not at all" : `with k × ${r.below_average_k_factor}`}, so a poor tournament is not rescued by a small sample)
rating = ${r.base} + ${r.scale} · Z_T   or   ${r.base} + ${r.scale_below} · Z_T   (same asymmetric scale)`}</Formula>
        <p>
          <b>k</b> has two parts. <code>k_noise</code> is estimated from the data (within-player noise ÷ between-player
          skill spread). <code>k_workload</code> grows with how many maps a typical participant plays, so a 10-map run
          counts for less in a tournament where most players play 50.
        </p>
        {tm && (
          <Formula>{`${tm.name}:
k = ${tm.k_noise.toFixed(2)} (noise) + ${tm.k_workload.toFixed(2)} (workload) = ${tm.confidence_k.toFixed(2)}
prior = ${tm.prior.toFixed(3)}
field strength: ${Object.entries(tm.field_strength).map(([k, v]) => `${k} ${v.toFixed(2)}`).join(" · ") || "n/a"}
example confidence: 10 maps → ${(100 * 10 / (10 + tm.confidence_k)).toFixed(0)}%   40 maps → ${(100 * 40 / (40 + tm.confidence_k)).toFixed(0)}%   70 maps → ${(100 * 70 / (70 + tm.confidence_k)).toFixed(0)}%`}</Formula>
        )}
      </Step>

      <Step n="4" title="Qualified players rank first">
        <p>
          {r.rank_qualified_first ? "When a tournament has qualifiers, players who reached a bracket round rank above players who did not, then each group is ordered by Tournament Rating." : "Players are ordered by Tournament Rating only."}{" "}
          A dashed line marks the cut. Performance Rating, Round and Mod views are never split this way, so a strong
          qualifier-only player still shows up there.
          {tm?.qualified_split.enabled && <> In {tm.name}: {tm.qualified_split.qualified} qualified, {tm.qualified_split.eliminated} did not.</>}
        </p>
      </Step>

      <Step n="5" title="Mod and round ratings">
        <p>
          These stay <i>skill</i> ratings: Performance Rating restricted to one mod or one round, with no progression
          credit. Each shows its own sample confidence and is marked low confidence under {Number(r.min_slice_confidence) * 100}%:
        </p>
        <Formula>{`conf_slice = Σw / (Σw + ${r.slice_confidence_k})`}</Formula>
      </Step>

      <Step n="6" title="Team rating">
        <p>
          The same Tournament Rating formula applied to every counted score of the team&rsquo;s players pooled together,
          so it reflects how far above the field they played, not bracket placement.
        </p>
      </Step>

      <Step n="7" title="Awards">
        <Formula>{`Tournament MVP        highest Tournament Rating, at least ${a.min_maps_overall} maps (qualified players first)
Best <mod> Player     at least ${a.min_maps_mod} maps in that mod and ≥ ${Number(r.min_slice_confidence) * 100}% slice confidence
Most Consistent       lowest σ of z, above-average players, ≥ ${a.min_maps_overall} maps
Best Finals Player    ≥ ${a.min_maps_finals} maps in the final rounds
Best Carry            ≥ ${a.min_team_games} team games`}</Formula>
      </Step>

      <Step n="8" title="Which games count?">
        <p>
          Warm-ups and aborted games are dropped. A map is treated as a replay only when the same beatmap is played again
          straight away in the same lobby by overlapping players. The same map in another lobby, or a lobby that runs its
          pool twice, is kept. Matches with no counted games never reach the analytics.
        </p>
      </Step>
    </div>
  );
}
