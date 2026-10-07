import { LeaderboardPanel } from "@/components/scout/LeaderboardPanel";
import { Card } from "@/components/scout/ui";
import { scoutGet } from "@/lib/scout";
import type { LeaderboardData, Overview, TeamListItem } from "@/lib/scoutTypes";

export const dynamic = "force-dynamic";

export default async function LeaderboardsPage({ params }: { params: Promise<{ slug: string }> }) {
  const { slug } = await params;
  const s = encodeURIComponent(slug);
  const [board, o] = await Promise.all([
    scoutGet<LeaderboardData>(`/tournaments/${s}/leaderboard?mode=tournament&limit=50`),
    scoutGet<Overview>(`/tournaments/${s}`)
  ]);
  const teams = o.tournament.has_teams ? (await scoutGet<{ teams: TeamListItem[] }>(`/tournaments/${s}/teams`)).teams : undefined;
  return (
    <Card title="Leaderboards">
      <LeaderboardPanel slug={slug} initial={board} pageSize={50} hasTeams={o.tournament.has_teams} teams={teams} />
    </Card>
  );
}
