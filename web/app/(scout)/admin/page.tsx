import { AdminQueue } from "@/components/scout/AdminQueue";

export const dynamic = "force-dynamic";
export const metadata = { title: "Discovery queue", robots: { index: false, follow: false } };

export default function AdminPage() {
  return (
    <div className="sc-wide">
      <p className="sc-eyebrow">Admin</p>
      <h1>Discovery queue</h1>
      <p className="sc-dim">
        Tournaments found by the discovery sources wait here until you approve them. Approving starts the normal import, and the
        tournament is then kept up to date automatically.
      </p>
      <AdminQueue />
    </div>
  );
}
