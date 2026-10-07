import { Avatar, Card } from "@/components/scout/ui";
import Link from "next/link";
import { scoutGet } from "@/lib/scout";
import { tournamentHref } from "@/lib/scoutFormat";
import type { Award } from "@/lib/scoutTypes";

export const dynamic = "force-dynamic";

export default async function AwardsPage({ params }: { params: Promise<{ slug: string }> }) {
  const { slug } = await params;
  const { awards } = await scoutGet<{ awards: Award[] }>(`/tournaments/${encodeURIComponent(slug)}/awards`);
  return (
    <div className="sc-awards">
      {awards.map((a) => (
        <Card key={a.key} className="sc-award">
          <p className="sc-eyebrow">{a.title}</p>
          {a.winner ? (
            <Link className="sc-award-body" href={tournamentHref(slug, "players", a.winner.slug)}>
              <Avatar src={a.winner.avatar_url} name={a.winner.username} size={56} />
              <div>
                <div className="sc-award-name">{a.winner.username}</div>
                <div className="sc-award-value">{a.winner.value}</div>
              </div>
            </Link>
          ) : <p className="sc-empty">Nobody qualified.</p>}
          <p className="sc-dim sc-award-rule">{a.rule}</p>
        </Card>
      ))}
    </div>
  );
}
