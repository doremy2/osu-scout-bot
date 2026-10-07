import Link from "next/link";
import { TournamentList } from "@/components/scout/TournamentList";
import { scoutGet } from "@/lib/scout";
import type { TournamentListItem } from "@/lib/scoutTypes";

export const dynamic = "force-dynamic";
export const metadata = { title: "Tournaments" };

export default async function TournamentsPage() {
  const tournaments = await scoutGet<TournamentListItem[]>("/tournaments");
  return (
    <>
      <div className="sc-section-head">
        <h1>Tournaments</h1>
        <Link className="sc-btn sc-btn-primary" href="/import">Import tournament</Link>
      </div>
      <TournamentList tournaments={tournaments} />
    </>
  );
}
