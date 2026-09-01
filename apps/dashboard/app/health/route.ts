export const dynamic = "force-dynamic";

export function GET() {
  return Response.json({ status: "ok", service: "dashboard", at: new Date().toISOString() });
}
