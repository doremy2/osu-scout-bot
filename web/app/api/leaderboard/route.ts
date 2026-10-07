import { loadLeaderboardRows } from "@/lib/leaderboardData";
import type { LeaderboardFormat } from "@/lib/types";

export async function GET(request: Request) {
  const url = new URL(request.url);
  const format = url.searchParams.get("format") as LeaderboardFormat | null;
  const rows = loadLeaderboardRows({
    tier: url.searchParams.get("tier") || undefined,
    country: url.searchParams.get("country") || undefined,
    format: format || "overall",
    limit: Number(url.searchParams.get("limit")) || 100,
    offset: Number(url.searchParams.get("offset")) || 0,
  });

  return Response.json(rows);
}
