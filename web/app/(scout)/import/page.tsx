import { ImportForm } from "@/components/scout/ImportForm";

export const metadata = { title: "Import tournament" };

export default function ImportPage() {
  return (
    <div className="sc-narrow">
      <p className="sc-eyebrow">Import</p>
      <h1>Import a tournament</h1>
      <p className="sc-dim">
        Paste the tournament’s Google Sheet. Importing runs on the server with the osu! API — it takes about a
        second per match, and re-importing the same slug only fetches matches that are new or failed.
      </p>
      <p className="sc-note">Beta: the more tournaments are added, the better the ratings get. Thanks for contributing.</p>
      <div className="sc-card"><ImportForm /></div>
    </div>
  );
}
