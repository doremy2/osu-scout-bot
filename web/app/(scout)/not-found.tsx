import Link from "next/link";

export default function NotFound() {
  return (
    <div className="sc-card sc-error-card">
      <h1>Not found</h1>
      <p>That tournament, player, team or match isn’t in the database.</p>
      <Link className="sc-btn" href="/tournaments">Browse tournaments</Link>
    </div>
  );
}
