import { redirect } from "next/navigation";
import { tournamentHref } from "@/lib/scoutFormat";

// The full player table lives under Leaderboards; keep old links working.
export default async function PlayersIndex({ params }: { params: Promise<{ slug: string }> }) {
  const { slug } = await params;
  redirect(tournamentHref(slug, "leaderboards"));
}
