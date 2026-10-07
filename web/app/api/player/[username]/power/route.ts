import { loadPlayerPower } from "@/lib/leaderboardData";

type RouteContext = {
  params: Promise<{ username: string }>;
};

export async function GET(request: Request, context: RouteContext) {
  const { username } = await context.params;
  const player = loadPlayerPower(username);
  if (!player) return Response.json({ detail: "Player not found" }, { status: 404 });

  return Response.json(player);
}
