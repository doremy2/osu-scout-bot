import { loadTournamentStats } from "@/lib/leaderboardData";

type RouteContext = {
  params: Promise<{ username: string }>;
};

export async function GET(request: Request, context: RouteContext) {
  const { username } = await context.params;
  const stats = loadTournamentStats(username);

  return Response.json(stats);
}
