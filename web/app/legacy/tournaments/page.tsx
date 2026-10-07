import { redirect } from "next/navigation";

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

export default async function TournamentsPage() {
  redirect("/legacy");
}
