// Runtime proxy from the browser to the Python scouting service.
// Not a next.config rewrite: on Vercel the service address is injected at runtime (a service binding),
// which is not available while building. The browser never sees the service URL, the database or the osu! credentials.
import { scoutApiUrl } from "@/lib/scout";

export const dynamic = "force-dynamic";
export const maxDuration = 60;   // an import step can run for up to ~45 s

async function proxy(request: Request, ctx: { params: Promise<{ path: string[] }> }): Promise<Response> {
  const { path } = await ctx.params;
  const target = scoutApiUrl(`/${path.map(encodeURIComponent).join("/")}`);
  target.search = new URL(request.url).search;

  const headers = new Headers();
  // x-forwarded-for lets the API apply per-visitor import limits; authorization carries Vercel Cron's bearer secret
  for (const name of ["content-type", "x-admin-token", "authorization", "accept", "x-forwarded-for"]) {
    const value = request.headers.get(name);
    if (value) headers.set(name, value);
  }
  const hasBody = request.method !== "GET" && request.method !== "HEAD";
  try {
    const upstream = await fetch(target, {
      method: request.method,
      headers,
      body: hasBody ? await request.text() : undefined,
      cache: "no-store"
    });
    return new Response(upstream.body, {
      status: upstream.status,
      headers: { "content-type": upstream.headers.get("content-type") ?? "application/json" }
    });
  } catch {
    return Response.json({ detail: "The scouting service is not reachable." }, { status: 503 });
  }
}

export { proxy as GET, proxy as POST };
