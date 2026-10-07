import { redirect } from "next/navigation";

export const dynamic = "force-dynamic";

type PageProps = {
  params: Promise<{ year: string }>;
  searchParams: Promise<{
    mode?: string;
    status?: string;
  }>;
};

export default async function TournamentsByYearPage() {
  redirect("/legacy");
}
