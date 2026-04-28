const BACKEND_API_BASE = (process.env.API_BASE_URL || "http://127.0.0.1:8000").replace(/\/$/, "");

type RouteContext = {
  params: Promise<{ slug: string }>;
};

export async function GET(request: Request, context: RouteContext) {
  const { slug } = await context.params;
  const upstream = await fetch(
    `${BACKEND_API_BASE}/tournaments/catalog/${encodeURIComponent(slug)}`,
    { cache: "no-store" }
  );
  const body = await upstream.text();

  return new Response(body, {
    status: upstream.status,
    headers: {
      "content-type": upstream.headers.get("content-type") || "application/json"
    }
  });
}
