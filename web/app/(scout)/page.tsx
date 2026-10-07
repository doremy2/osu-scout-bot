import Link from "next/link";
import { TournamentList } from "@/components/scout/TournamentList";
import { scoutGet } from "@/lib/scout";
import type { TournamentListItem } from "@/lib/scoutTypes";

export const dynamic = "force-dynamic";

export default async function Landing() {
  const tournaments = await scoutGet<TournamentListItem[]>("/tournaments");
  return (
    <>
      <section className="sc-hero">
        <p className="sc-eyebrow">osu! tournament analytics</p>
        <h1>Scout any osu! tournament<br />in minutes.</h1>
        <p className="sc-hero-sub">
          Paste a tournament’s Google Sheet. We pull every multiplayer lobby from the osu! API and turn it into
          player ratings, mod rankings, team profiles and awards — normalized per map, so a great score on a
          hard map counts for more.
        </p>
        <div className="sc-hero-cta">
          <Link className="sc-btn sc-btn-primary sc-btn-lg" href="/import">Import a tournament</Link>
          <Link className="sc-btn sc-btn-lg" href="/tournaments">Browse tournaments</Link>
        </div>
      </section>

      <section className="sc-features">
        <div className="sc-card"><h3>Player ratings</h3><p>Every score is compared to everyone who played the same map, then rolled into overall, per-mod and per-round ratings.</p></div>
        <div className="sc-card"><h3>Teams &amp; countries</h3><p>Team tournaments get roster pages, team rankings and match records — click from a team to a player to a single map.</p></div>
        <div className="sc-card"><h3>Scouting profiles</h3><p>Best and worst maps, mod strengths, round-by-round form and full match history for every participant.</p></div>
      </section>

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
            <Link className="sc-btn sc-btn-primary" href="/import">Import your first tournament</Link>
          </div>
        )}
      </section>
    </>
  );
}
