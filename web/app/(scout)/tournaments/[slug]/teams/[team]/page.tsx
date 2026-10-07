import Link from "next/link";
import { Avatar, Card, Flag, MatchLine, ModBadge, Rating, Stat } from "@/components/scout/ui";
import { scoutGet } from "@/lib/scout";
import { fmtRecord, tournamentHref } from "@/lib/scoutFormat";
import type { TeamPage, Tournament } from "@/lib/scoutTypes";

export const dynamic = "force-dynamic";

type Data = { tournament: Tournament; team: TeamPage; mods: string[] };

export async function generateMetadata({ params }: { params: Promise<{ slug: string; team: string }> }) {
  const { slug, team } = await params;
  const d = await scoutGet<Data>(`/tournaments/${encodeURIComponent(slug)}/teams/${encodeURIComponent(team)}`);
  return { title: `${d.team.name} · ${d.tournament.acronym ?? d.tournament.name}` };
}

export default async function TeamProfile({ params }: { params: Promise<{ slug: string; team: string }> }) {
  const { slug, team } = await params;
  const { team: t, mods } = await scoutGet<Data>(`/tournaments/${encodeURIComponent(slug)}/teams/${encodeURIComponent(team)}`);

  return (
    <div className="sc-stack">
      <Card className="sc-phead">
        <Flag country={t.country} size={72} />
        <div className="sc-phead-main">
          <h2>{t.name}</h2>
          <div className="sc-phead-sub">
            {t.country_name && <span className="sc-dim">{t.country_name}</span>}
            {t.furthest_round_name && <span>Furthest round: <b>{t.furthest_round_name}</b></span>}
          </div>
        </div>
        <div className="sc-phead-rating">
          <div className="sc-stat-label">Team rating</div>
          <Rating value={t.rating} big />
          <div className="sc-rankline">Rank <b>#{t.rank}</b> <span className="sc-dim">/ {t.rank_of}</span></div>
        </div>
      </Card>

      <div className="sc-grid-4">
        <Stat label="Match record" value={fmtRecord(t.match_record)} />
        <Stat label="Maps won" value={`${t.maps_won}`} sub={`Map record ${fmtRecord(t.map_record)}`} />
        <Stat label="Maps played" value={`${t.maps_played}`} sub={`${t.players} players`} />
        <Stat label="Avg player rating" value={<Rating value={t.avg_player_rating} />} />
      </div>

      <Card title="Roster">
        <div className="sc-table-wrap">
          <table className="sc-table">
            <thead><tr><th className="sc-num">#</th><th>Player</th><th className="sc-num">Rating</th><th className="sc-num">Tournament rank</th><th className="sc-num sc-hide-sm">Maps</th></tr></thead>
            <tbody>
              {t.roster.map((r) => (
                <tr key={r.user_id} className="sc-row-link-plain">
                  <td className="sc-num sc-dim">{r.team_rank}</td>
                  <td>
                    <Link className="sc-player" href={tournamentHref(slug, "players", r.slug)}>
                      <Avatar src={r.avatar_url} name={r.username} size={30} />
                      <span className="sc-player-name">{r.username}</span>
                      <Flag country={r.country} size={16} />
                    </Link>
                  </td>
                  <td className="sc-num"><Rating value={r.rating} /></td>
                  <td className="sc-num">#{r.rank} <span className="sc-dim">/ {r.rank_of}</span></td>
                  <td className="sc-num sc-dim sc-hide-sm">{r.maps}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </Card>

      <div className="sc-grid-2">
        <Card title="Best players">
          <ul className="sc-rows">
            {t.best_player && (
              <li>
                <span className="sc-round-name">Overall</span>
                <Link className="sc-player sc-grow" href={tournamentHref(slug, "players", t.best_player.slug)}>
                  <Avatar src={t.best_player.avatar_url} name={t.best_player.username} size={26} /><span className="sc-player-name">{t.best_player.username}</span>
                </Link>
                <Rating value={t.best_player.rating} />
              </li>
            )}
            {mods.map((m) => {
              const b = t.best_by_mod[m];
              return b && (
                <li key={m}>
                  <span className="sc-round-name"><ModBadge mod={m} /></span>
                  <Link className="sc-player sc-grow" href={tournamentHref(slug, "players", b.slug)}>
                    <Avatar src={b.avatar_url} name={b.username} size={26} /><span className="sc-player-name">{b.username}</span>
                  </Link>
                  <span className="sc-dim">{b.maps} maps</span>
                  <Rating value={b.rating} />
                </li>
              );
            })}
          </ul>
        </Card>
        <Card title="Round performance">
          <ul className="sc-rows">
            {t.by_round.map((r) => (
              <li key={r.round}>
                <span className="sc-round-name">{r.round_name}</span>
                <span className="sc-grow sc-dim">{r.maps} maps</span>
                <Rating value={r.rating} />
              </li>
            ))}
          </ul>
          {t.best_round && t.worst_round && t.best_round.round !== t.worst_round.round && (
            <p className="sc-note">Best round: <b>{t.best_round.round_name}</b> · Worst round: <b>{t.worst_round.round_name}</b></p>
          )}
        </Card>
      </div>

      <Card title="Matches">
        <div className="sc-matchlist">
          {t.matches.map((m) => <MatchLine key={m.osu_match_id} match={m} tournament={slug} />)}
          {t.matches.length === 0 && <p className="sc-empty">No head-to-head matches recorded for this team.</p>}
        </div>
      </Card>
    </div>
  );
}
