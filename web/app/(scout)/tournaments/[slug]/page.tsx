import Link from "next/link";
import { LeaderboardPanel } from "@/components/scout/LeaderboardPanel";
import { Avatar, Card, Flag, MatchLine, ModBadge, PlayerLink, Rating, TeamLink } from "@/components/scout/ui";
import { scoutGet } from "@/lib/scout";
import { fmtRecord, tournamentHref } from "@/lib/scoutFormat";
import type { LeaderboardData, Overview } from "@/lib/scoutTypes";

export const dynamic = "force-dynamic";

export default async function OverviewPage({ params }: { params: Promise<{ slug: string }> }) {
  const { slug } = await params;
  const s = encodeURIComponent(slug);
  const [o, board] = await Promise.all([
    scoutGet<Overview>(`/tournaments/${s}`),
    scoutGet<LeaderboardData>(`/tournaments/${s}/leaderboard?mode=overall&limit=25`)
  ]);
  const { tournament: t, mvp } = o;
  const highlight = o.awards.filter((a) => a.winner && ["most_consistent", "best_carry", "best_accuracy", "best_finals", "best_performance"].includes(a.key)).slice(0, 4);

  return (
    <div className="sc-stack">
      <div className="sc-grid-hero">
        {mvp && (
          <Card className="sc-mvp">
            <p className="sc-eyebrow">Tournament MVP</p>
            <Link href={tournamentHref(slug, "players", mvp.slug)} className="sc-mvp-body">
              <Avatar src={mvp.avatar_url} name={mvp.username} size={96} />
              <div>
                <div className="sc-mvp-name">{mvp.username} <Flag country={mvp.country} size={22} /></div>
                {mvp.team && <div className="sc-dim">{mvp.team.name}</div>}
                <div className="sc-mvp-rating"><Rating value={mvp.rating} big /> <span className="sc-dim">Rating</span></div>
                <div className="sc-dim">Rank #{mvp.rank} · {mvp.maps_played} maps</div>
              </div>
            </Link>
          </Card>
        )}
        <Card title="Mod leaders">
          <div className="sc-modleaders">
            {o.mod_leaders.map((m) => (
              <Link key={m.mod} className="sc-modleader" href={tournamentHref(slug, "players", m.slug)}>
                <ModBadge mod={m.mod} />
                <Avatar src={m.avatar_url} name={m.username} size={40} />
                <span className="sc-player-name">{m.username}</span>
                <Rating value={m.rating} />
              </Link>
            ))}
            {o.mod_leaders.length === 0 && <p className="sc-empty">Not enough maps per mod yet.</p>}
          </div>
        </Card>
      </div>

      <Card title="Player leaderboard"
            action={<Link className="sc-link" href={tournamentHref(slug, "leaderboards")}>Rounds, mods &amp; more →</Link>}>
        <LeaderboardPanel slug={slug} initial={board} pageSize={25} hasTeams={t.has_teams} compact
                          viewAllHref={tournamentHref(slug, "players")} />
      </Card>

      <div className="sc-grid-2">
        {t.has_teams && o.top_teams.length > 0 && (
          <Card title="Top teams" action={<Link className="sc-link" href={tournamentHref(slug, "teams")}>All teams →</Link>}>
            <ol className="sc-toplist">
              {o.top_teams.map((tm) => (
                <li key={tm.slug}>
                  <span className={`sc-rank sc-rank-${tm.rank <= 3 ? tm.rank : "n"}`}>{tm.rank}</span>
                  <TeamLink team={tm} tournament={slug} />
                  <span className="sc-dim">{fmtRecord(tm.match_record)}</span>
                  <Rating value={tm.rating} />
                </li>
              ))}
            </ol>
          </Card>
        )}
        {highlight.length > 0 && (
          <Card title="Awards" action={<Link className="sc-link" href={tournamentHref(slug, "awards")}>All awards →</Link>}>
            <ul className="sc-awardlist">
              {highlight.map((a) => a.winner && (
                <li key={a.key}>
                  <span className="sc-dim">{a.title}</span>
                  <PlayerLink slug={a.winner.slug} tournament={slug} name={a.winner.username} avatar={a.winner.avatar_url} size={24} />
                  <span className="sc-award-value">{a.winner.value}</span>
                </li>
              ))}
            </ul>
          </Card>
        )}
      </div>

      <Card title="Recent matches" action={<Link className="sc-link" href={tournamentHref(slug, "matches")}>All matches →</Link>}>
        <div className="sc-matchlist">
          {o.recent_matches.map((m) => <MatchLine key={m.osu_match_id} match={m} tournament={slug} />)}
        </div>
      </Card>
    </div>
  );
}
