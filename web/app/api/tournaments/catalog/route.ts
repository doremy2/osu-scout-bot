import { loadTournamentCatalog } from "@/lib/tournamentCatalog";

export async function GET(request: Request) {
  const url = new URL(request.url);
  const year = Number(url.searchParams.get("year"));
  const catalog = loadTournamentCatalog({
    year: Number.isFinite(year) ? year : undefined,
    game_mode: url.searchParams.get("game_mode") || undefined,
    classification: url.searchParams.get("classification") || undefined,
    import_status: url.searchParams.get("import_status") || undefined,
    limit: Number(url.searchParams.get("limit")) || undefined,
  });

  return Response.json(catalog);
}
