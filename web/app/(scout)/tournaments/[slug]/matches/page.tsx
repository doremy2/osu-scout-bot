import Link from "next/link";
import { Card, MatchLine } from "@/components/scout/ui";
import { scoutGet } from "@/lib/scout";
import { tournamentHref } from "@/lib/scoutFormat";
import type { MatchItem } from "@/lib/scoutTypes";

export const dynamic = "force-dynamic";

type Data = { rounds: string[]; round_names: Record<string, string>; matches: MatchItem[] };

export default async function MatchesPage({ params, searchParams }: {
  params: Promise<{ slug: string }>;
  searchParams: Promise<{ round?: string }>;
}) {
  const { slug } = await params;
  const { round } = await searchParams;
  const data = await scoutGet<Data>(`/tournaments/${encodeURIComponent(slug)}/matches${round ? `?round=${encodeURIComponent(round)}` : ""}`);
  const base = tournamentHref(slug, "matches");
  const groups = data.rounds.filter((r) => data.matches.some((m) => m.round === r));

  return (
    <div className="sc-stack">
      <div className="sc-chips">
        <Link className={`sc-chip${!round ? " on" : ""}`} href={base}>All rounds</Link>
        {data.rounds.map((r) => (
          <Link key={r} className={`sc-chip${round === r ? " on" : ""}`} href={`${base}?round=${encodeURIComponent(r)}`}>{data.round_names[r]}</Link>
        ))}
      </div>
      {groups.map((r) => (
        <Card key={r} title={data.round_names[r]}>
          <div className="sc-matchlist">
            {data.matches.filter((m) => m.round === r).map((m) => <MatchLine key={m.osu_match_id} match={m} tournament={slug} />)}
          </div>
        </Card>
      ))}
    </div>
  );
}
