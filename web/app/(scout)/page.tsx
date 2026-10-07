import Link from "next/link";
import { Reveal } from "@/components/scout/Reveal";
import { TournamentList } from "@/components/scout/TournamentList";
import { importsPolicy, scoutGet } from "@/lib/scout";
import type { TournamentListItem } from "@/lib/scoutTypes";

export const dynamic = "force-dynamic";

export default async function Landing() {
  const [tournaments, imports] = await Promise.all([scoutGet<TournamentListItem[]>("/tournaments"), importsPolicy()]);
  const canImport = imports !== "off";
  return (
    <>
      <section className="sc-splash">
        <h1 className="sc-wordmark">osu!<b>scout</b></h1>
        <p className="sc-splash-sub">osu! tournament analytics &amp; scouting</p>
        <div className="sc-hero-cta">
          <Link className="sc-btn sc-btn-primary" href="/tournaments">Browse tournaments</Link>
          {canImport && <Link className="sc-btn" href="/import">Import a tournament</Link>}
        </div>
        <span className="sc-scrollhint" aria-hidden>↓</span>
      </section>

      <section className="sc-features">
        <Reveal><div className="sc-card"><h3>Player ratings</h3><p>Every score is compared to everyone who played the same map, then rolled into tournament, performance, per-mod and per-round ratings.</p></div></Reveal>
        <Reveal delay={120}><div className="sc-card"><h3>Teams &amp; countries</h3><p>Team tournaments get roster pages, team rankings and match records. Click from a team to a player to a single map.</p></div></Reveal>
        <Reveal delay={240}><div className="sc-card"><h3>Scouting profiles</h3><p>Best and worst maps, mod strengths, round-by-round form and full match history for every participant.</p></div></Reveal>
      </section>

      <Reveal>
        <section className="sc-card sc-beta">
          <p className="sc-eyebrow">Beta</p>
          <h2>This site and its ratings are a work in progress.</h2>
          <p className="sc-dim">
            The rating model is still being tuned, so numbers and rankings can change as it improves. Treat them as a
            scouting aid, not a verdict.{" "}
            {canImport
              ? "The best way to help is to add more tournaments: the more data the model sees, the better it gets."
              : "Imports currently run locally, so new tournaments are added by the site owner for now."}
          </p>
          {canImport && <Link className="sc-btn sc-btn-primary" href="/import">Add a tournament</Link>}
        </section>
      </Reveal>

      <Reveal>
        <section className="sc-section">
          <div className="sc-section-head">
            <h2>Tournaments</h2>
            <Link href="/tournaments" className="sc-link">View all →</Link>
          </div>
          {tournaments.length ? (
            <TournamentList tournaments={tournaments} limit={6} />
          ) : (
            <div className="sc-card sc-center">
              <p>No tournaments imported yet.</p>
              {canImport && <Link className="sc-btn sc-btn-primary" href="/import">Import your first tournament</Link>}
            </div>
          )}
        </section>
      </Reveal>
    </>
  );
}
