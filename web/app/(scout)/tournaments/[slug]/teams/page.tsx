import { TeamTable } from "@/components/scout/LeaderboardPanel";
import { Card } from "@/components/scout/ui";
import { scoutGet } from "@/lib/scout";
import type { TeamListItem } from "@/lib/scoutTypes";

export const dynamic = "force-dynamic";

export default async function TeamsPage({ params }: { params: Promise<{ slug: string }> }) {
  const { slug } = await params;
  const { teams } = await scoutGet<{ teams: TeamListItem[] }>(`/tournaments/${encodeURIComponent(slug)}/teams`);
  return (
    <Card title="Team leaderboard">
      <p className="sc-note">
        Team rating pools every counted score of the team’s players, so it reflects how far above the field they
        performed — not bracket placement.
      </p>
      <TeamTable slug={slug} teams={teams} />
    </Card>
  );
}
