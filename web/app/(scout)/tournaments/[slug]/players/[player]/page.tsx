import Link from "next/link";
import { Avatar, Card, Flag, ModBadge, Rating, Stat, TeamLink } from "@/components/scout/ui";
import { scoutGet } from "@/lib/scout";
import { coverSrc, fmtInt, fmtPct, fmtRecord, tournamentHref } from "@/lib/scoutFormat";
import type { Performance, PlayerPage, Tournament } from "@/lib/scoutTypes";

export const dynamic = "force-dynamic";

type Data = { tournament: Tournament; player: PlayerPage; mods: string[] };

export async function generateMetadata({ params }: { params: Promise<{ slug: string; player: string }> }) {
  const { slug, player } = await params;
  const d = await scoutGet<Data>(`/tournaments/${encodeURIComponent(slug)}/players/${encodeURIComponent(player)}`);
  return { title: `${d.player.username} · ${d.tournament.acronym ?? d.tournament.name}` };
}

export default async function PlayerProfile({ params }: { params: Promise<{ slug: string; player: string }> }) {
  const { slug, player } = await params;
  const { player: p } = await scoutGet<Data>(`/tournaments/${encodeURIComponent(slug)}/players/${encodeURIComponent(player)}`);

  return (
    <div className="sc-stack">
      <Card className="sc-phead">
        <Avatar src={p.avatar_url} name={p.username} size={112} />
        <div className="sc-phead-main">
          <h2>{p.username}</h2>
          <div className="sc-phead-sub">
            {p.country && (!p.team || p.team.name !== p.country_name) && <><Flag country={p.country} size={22} /> <span>{p.country_name ?? p.country}</span></>}
            {p.team && <TeamLink team={p.team} tournament={slug} />}
            <a className="sc-link" href={`https://osu.ppy.sh/users/${p.user_id}`} target="_blank" rel="noreferrer">osu! profile ↗</a>
          </div>
        </div>
        <div className="sc-phead-rating">
          <div className="sc-stat-label">Overall tournament rating</div>
          <Rating value={p.rating} big />
          <div className="sc-rankline">Rank <b>#{p.rank}</b> <span className="sc-dim">/ {p.rank_of}</span></div>
        </div>
      </Card>

      <div className="sc-grid-4">
        <Stat label="Maps played" value={fmtInt(p.maps_played)} />
        <Stat label="Average score" value={fmtInt(p.avg_score)} />
        <Stat label="Average accuracy" value={fmtPct(p.avg_accuracy)} />
        <Stat label="Match record" value={fmtRecord(p.match_record)}
              sub={p.map_winrate != null ? `Map record ${fmtRecord(p.map_record)} · ${fmtPct(p.map_winrate, 0)}` : undefined} />
      </div>

      <div className="sc-grid-2">
        <Card title="Mod ratings">
          <ul className="sc-rows">
            {p.by_mod.map((m) => (
              <li key={m.mod}>
                <ModBadge mod={m.mod} />
                <Link className="sc-grow sc-link-plain" href={`${tournamentHref(slug, "leaderboards")}`}>
                  <span className="sc-dim">{m.maps} maps</span>
                </Link>
                <Rating value={m.rating} />
                <span className="sc-rankchip">#{m.rank} <small>/ {m.rank_of}</small></span>
              </li>
            ))}
          </ul>
        </Card>
        <Card title="Round performance">
          <ul className="sc-rows">
            {p.by_round.map((r) => (
              <li key={r.round}>
                <span className="sc-round-name">{r.round_name}</span>
                <span className="sc-grow sc-dim">{r.maps} maps</span>
                <Rating value={r.rating} />
                <span className="sc-rankchip">#{r.rank} <small>/ {r.rank_of}</small></span>
              </li>
            ))}
          </ul>
        </Card>
      </div>

      <div className="sc-grid-2">
        <Card title="Best performances"><PerfList items={p.best_performances} slug={slug} /></Card>
        {p.worst_performances.length > 0 && <Card title="Worst performances"><PerfList items={p.worst_performances} slug={slug} /></Card>}
      </div>

      {p.team && p.teammates.length > 0 && (
        <Card title={<>Teammates · {p.team.name}</>}>
          <ul className="sc-rows">
            {p.teammates.map((t) => (
              <li key={t.user_id}>
                <Link className="sc-player sc-grow" href={tournamentHref(slug, "players", t.slug)}>
                  <Avatar src={t.avatar_url} name={t.username} size={28} /><span className="sc-player-name">{t.username}</span>
                </Link>
                <Rating value={t.rating} />
                <span className="sc-rankchip">#{t.rank}</span>
              </li>
            ))}
          </ul>
        </Card>
      )}

      <Card title="Match history">
        <div className="sc-table-wrap">
          <table className="sc-table">
            <thead><tr><th>Round</th><th>Match</th><th className="sc-num">Score</th><th className="sc-num sc-hide-sm">Maps</th><th className="sc-num">Rating</th><th /></tr></thead>
            <tbody>
              {p.match_history.map((h) => {
                const [a, b] = h.sides;
                return (
                  <tr key={h.osu_match_id}>
                    <td className="sc-dim">{h.round_name}</td>
                    <td>
                      <Link className="sc-link-plain" href={tournamentHref(slug, "matches", String(h.osu_match_id))}>
                        {h.kind === "match" && a && b ? <>{a.name} <span className="sc-dim">vs</span> {b.name}</> : h.name.replace(/^[^:]+:\s*/, "")}
                      </Link>
                    </td>
                    <td className="sc-num">{h.score ?? "–"}</td>
                    <td className="sc-num sc-dim sc-hide-sm">{h.maps}</td>
                    <td className="sc-num"><Rating value={h.rating} /></td>
                    <td>{h.result && <span className={`sc-result sc-result-${h.result}`}>{h.result}</span>}</td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      </Card>
    </div>
  );
}

function PerfList({ items, slug }: { items: Performance[]; slug: string }) {
  return (
    <ul className="sc-perfs">
      {items.map((x, i) => {
        const cover = coverSrc(x.beatmapset_id);
        return (
          <li key={`${x.osu_match_id}-${x.beatmap_id}-${i}`}>
            {/* eslint-disable-next-line @next/next/no-img-element */}
            {cover ? <img className="sc-cover" src={cover} alt="" loading="lazy" /> : <span className="sc-cover" />}
            <div className="sc-grow">
              <div className="sc-perf-map">{x.map}</div>
              <div className="sc-dim">
                <ModBadge mod={x.mod} /> {fmtInt(x.score)}{x.accuracy != null && <> · {fmtPct(x.accuracy)}</>} ·{" "}
                <Link className="sc-link-plain" href={tournamentHref(slug, "matches", String(x.osu_match_id))}>{x.round_name}</Link>
              </div>
            </div>
            <Rating value={x.rating} />
          </li>
        );
      })}
    </ul>
  );
}
