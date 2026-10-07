import { loadTournamentDetail } from "@/lib/tournamentCatalog";

type RouteContext = {
  params: Promise<{ slug: string }>;
};

export async function GET(request: Request, context: RouteContext) {
  const { slug } = await context.params;
  const entry = loadTournamentDetail(slug);
  if (!entry) return Response.json({ detail: "Tournament not found" }, { status: 404 });

  return Response.json(entry);
}
