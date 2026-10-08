import { ImportForm } from "@/components/scout/ImportForm";
import { importsPolicy } from "@/lib/scout";

export const dynamic = "force-dynamic";
export const metadata = { title: "Import tournament" };

export default async function ImportPage() {
  const imports = await importsPolicy();
  return (
    <div className="sc-narrow">
      <p className="sc-eyebrow">Import</p>
      <h1>Import a tournament</h1>
      {imports === "off" ? (
        <div className="sc-card">
          <p><b>Imports currently run locally.</b></p>
          <p className="sc-dim">
            This public site shows a read-only snapshot of the database. Tournaments are imported on the owner&rsquo;s machine
            and published with the next update. Web imports will return once the site moves to a hosted database.
          </p>
        </div>
      ) : (
        <>
          <p className="sc-dim">
            Paste the tournament&rsquo;s Google Sheet. Importing runs on the server with the osu! API: it takes about a
            second per match, and re-importing the same slug only fetches matches that are new or failed.
          </p>
          <p className="sc-note">
            Beta: the more tournaments are added, the better the ratings get. Thanks for contributing.
          </p>
          <div className="sc-card"><ImportForm /></div>
        </>
      )}
    </div>
  );
}
