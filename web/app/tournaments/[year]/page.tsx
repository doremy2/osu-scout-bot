import Link from "next/link";
import { fetchTournamentCatalog } from "@/lib/api";
import { TournamentListingShell } from "@/components/TournamentListingShell";
import type { TournamentCatalog } from "@/lib/types";

export const dynamic = "force-dynamic";

type PageProps = {
  params: Promise<{ year: string }>;
  searchParams: Promise<{
    mode?: string;
    status?: string;
  }>;
};

export default async function TournamentsByYearPage({ params, searchParams }: PageProps) {
  const { year: yearStr } = await params;
  const sp = await searchParams;
  const year = Number(yearStr);
  if (Number.isNaN(year) || year < 2020 || year > 2030) {
    return (
      <main className="page-shell">
        <p>Invalid year: {yearStr}</p>
        <Link href="/tournaments">Back to all tournaments</Link>
      </main>
    );
  }

  const mode = sp.mode || "";
  const status = sp.status || "";

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
