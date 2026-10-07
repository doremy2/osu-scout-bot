import { DraftSimulator } from "@/components/scout/DraftSimulator";
import { scoutGet } from "@/lib/scout";
import type { DraftSetup } from "@/lib/scoutTypes";

export const dynamic = "force-dynamic";
export const metadata = { title: "Draft simulator" };

export default async function DraftPage({ params }: { params: Promise<{ slug: string }> }) {
  const { slug } = await params;
  const setup = await scoutGet<DraftSetup>(`/tournaments/${encodeURIComponent(slug)}/draft/setup`);
  return <DraftSimulator slug={slug} setup={setup} />;
}
