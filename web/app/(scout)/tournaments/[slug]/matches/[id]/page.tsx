import Link from "next/link";
import { Avatar, Card, ModBadge, Rating } from "@/components/scout/ui";
import { scoutGet } from "@/lib/scout";
import { coverSrc, fmtInt, fmtPct, tournamentHref } from "@/lib/scoutFormat";
import type { MatchGame, MatchItem } from "@/lib/scoutTypes";

export const dynamic = "force-dynamic";

type Data = { match: MatchItem; games: MatchGame[] };

export default async function MatchPage({ params }: { params: Promise<{ slug: string; id: string }> }) {
  const { slug, id } = await params;
  const { match: m, games } = await scoutGet<Data>(`/tournaments/${encodeURIComponent(slug)}/matches/${encodeURIComponent(id)}`);
  const [a, b] = m.sides;
  const sideName = (side: string | null) => m.sides.find((s) => s.side === side)?.name ?? side ?? "";

  return (
    <div className="sc-stack">
      <Card className="sc-matchhead">
        <p className="sc-eyebrow">{m.round_name}</p>
        {m.kind === "match" && a && b ? (
          <div className="sc-versus">
            <SideLink slug={slug} side={a} win={a.name === m.winner} />
            <div className="sc-versus-score">{a.map_wins} – {b.map_wins}</div>
            <SideLink slug={slug} side={b} win={b.name === m.winner} right />
          </div>
        ) : (
          <h2>{m.name}</h2>
        )}
        <a className="sc-link" href={m.link} target="_blank" rel="noreferrer">osu! multiplayer link ↗</a>
      </Card>

      {games.map((g) => (
        <Card key={g.osu_game_id} className={g.excluded ? "sc-game sc-game-excluded" : "sc-game"}>
          <div className="sc-game-head">
            {/* eslint-disable-next-line @next/next/no-img-element */}
            {coverSrc(g.beatmapset_id) ? <img className="sc-cover" src={coverSrc(g.beatmapset_id)!} alt="" loading="lazy" /> : <span className="sc-cover" />}
            <div className="sc-grow">
              <div className="sc-perf-map">{g.order}. {g.map}</div>
              <div className="sc-dim">
                {g.mod && <ModBadge mod={g.mod} />} {g.star_rating ? `${g.star_rating.toFixed(2)}★` : ""}
                {g.winner_side && <> · won by <b>{sideName(g.winner_side)}</b></>}
                {g.excluded && <span className="sc-warn"> · not counted ({g.exclude_reason})</span>}
              </div>
            </div>
          </div>
          <table className="sc-table sc-table-tight">
            <tbody>
              {g.scores.map((s) => (
                <tr key={s.user_id} className={g.winner_side && s.side === g.winner_side ? "sc-winrow" : ""}>
                  <td>
                    {s.slug ? (
                      <Link className="sc-player" href={tournamentHref(slug, "players", s.slug)}><Avatar src={s.avatar_url} name={s.username} size={24} /><span className="sc-player-name">{s.username}</span></Link>
                    ) : <span className="sc-player"><Avatar src={s.avatar_url} name={s.username} size={24} /><span className="sc-player-name">{s.username}</span></span>}
                  </td>
                  <td className="sc-dim sc-hide-sm">{s.side && s.side !== "none" ? sideName(s.side) : ""}</td>
                  <td className="sc-num">{fmtInt(s.score)}</td>
                  <td className="sc-num sc-dim sc-hide-sm">{fmtPct(s.accuracy)}</td>
                  <td className="sc-num sc-dim sc-hide-sm">{s.misses ?? 0}✗</td>
                  <td className="sc-num"><Rating value={s.rating} /></td>
                </tr>
              ))}
            </tbody>
          </table>
        </Card>
      ))}
    </div>
  );
}

function SideLink({ slug, side, win, right = false }: { slug: string; side: MatchItem["sides"][number]; win: boolean; right?: boolean }) {
  const href = side.kind === "team" && side.slug ? tournamentHref(slug, "teams", side.slug)
    : side.kind === "player" && side.slug ? tournamentHref(slug, "players", side.slug) : null;
  const inner = <span className={`sc-versus-name${win ? " sc-win" : ""}${right ? " right" : ""}`}>{side.name}</span>;
  return href ? <Link href={href} className="sc-versus-side">{inner}</Link> : <span className="sc-versus-side">{inner}</span>;
}
