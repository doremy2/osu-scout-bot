import { TournamentList } from "@/components/scout/TournamentList";
import { ImportButton } from "@/components/scout/ui";
import { importsPolicy, scoutGet } from "@/lib/scout";
import type { TournamentListItem } from "@/lib/scoutTypes";

export const dynamic = "force-dynamic";
export const metadata = { title: "Tournaments" };

export default async function TournamentsPage({ searchParams }: { searchParams: Promise<{ q?: string }> }) {
  const { q } = await searchParams;
  const [tournaments, imports] = await Promise.all([scoutGet<TournamentListItem[]>("/tournaments"), importsPolicy()]);
  return (
    <>
      <div className="sc-section-head">
        <h1>Tournaments</h1>
        <ImportButton policy={imports} variant="primary">Import tournament</ImportButton>
      </div>
      <TournamentList tournaments={tournaments} initialQuery={q ?? ""} />
    </>
  );
}
