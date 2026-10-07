import { LeaderboardPanel } from "@/components/scout/LeaderboardPanel";
import { Card } from "@/components/scout/ui";
import { scoutGet } from "@/lib/scout";
import type { LeaderboardData } from "@/lib/scoutTypes";

export const dynamic = "force-dynamic";

export default async function PlayersPage({ params }: { params: Promise<{ slug: string }> }) {
  const { slug } = await params;
  const board = await scoutGet<LeaderboardData>(`/tournaments/${encodeURIComponent(slug)}/leaderboard?mode=tournament&limit=50`);
  const t = await scoutGet<{ tournament: { has_teams: boolean } }>(`/tournaments/${encodeURIComponent(slug)}`);
  return (
    <Card title="All players">
      <LeaderboardPanel slug={slug} initial={board} pageSize={50} hasTeams={t.tournament.has_teams} />
    </Card>
  );
}
