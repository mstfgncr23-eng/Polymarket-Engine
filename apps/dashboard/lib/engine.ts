export const ENGINE_UNREACHABLE = "ENGINE_UNREACHABLE" as const;

export function engineInternalUrl(): string {
  return process.env.ENGINE_INTERNAL_URL ?? "http://127.0.0.1:8080";
}

export function engineReadHeaders(extra?: Record<string, string>): HeadersInit {
  const token = process.env.ENGINE_READ_TOKEN;
  const headers: Record<string, string> = { ...extra };
  if (token) headers.authorization = `Bearer ${token}`;
  return headers;
}

export function engineUnreachableResponse(status = 503): Response {
  return Response.json(
    { error: ENGINE_UNREACHABLE },
    { status, headers: { "cache-control": "no-store" } },
  );
}
