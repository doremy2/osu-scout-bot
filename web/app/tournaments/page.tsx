import Link from "next/link";
import { fetchTournamentCatalog } from "@/lib/api";
import { TournamentListingShell } from "@/components/TournamentListingShell";
import type { TournamentCatalog } from "@/lib/types";

export const dynamic = "force-dynamic";

type PageProps = {
  searchParams: Promise<{
    year?: string;
    mode?: string;
    status?: string;
  }>;
};

function cleanYear(value: string | undefined): number | undefined {
  const n = Number(value);
  if (n >= 2020 && n <= 2030) return n;
  return undefined;
}

export default async function TournamentsPage({ searchParams }: PageProps) {
  const params = await searchParams;
  const year = cleanYear(params.year);
  const mode = params.mode || "";
  const status = params.status || "";

  let catalog: TournamentCatalog = { total: 0, imported_count: 0, discovered_count: 0, rows: [] };
  try {
    catalog = await fetchTournamentCatalog({
      year,
      game_mode: mode || undefined,
      import_status: status || undefined,
      limit: 2000,
    });
  } catch {
    // Backend not reachable
  }

  return (
    <TournamentListingShell
      initialCatalog={catalog}
      initialYear={year}
      initialMode={mode}
      initialStatus={status}
    />
  );
}
