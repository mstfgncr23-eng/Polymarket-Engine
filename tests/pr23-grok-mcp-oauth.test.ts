import { createHash, randomBytes } from "node:crypto";
import { describe, expect, it } from "vitest";
import { createId, engineEnvSchema } from "@pm/core";
import { MemoryEventJournal } from "@pm/db";
import { EngineService } from "../apps/engine/src/engine.js";
import { EngineMetrics } from "../apps/engine/src/metrics.js";
import { SseHub, createApiServer } from "../apps/engine/src/server.js";
import { McpOAuthService, MCP_OAUTH_SCOPES, type OAuthSafeEvent } from "../apps/engine/src/mcp-oauth.js";

const ROLE = "market_microstructure_scanner" as const;
const ALPHA = "fair_probability_analyst" as const;
const ROLE_SCOPE = `mcp:${ROLE}`;

function verifier(): { value: string; challenge: string } {
  const value = randomBytes(32).toString("base64url");
  return { value, challenge: createHash("sha256").update(value).digest("base64url") };
}

function form(values: Record<string, string>): string { return new URLSearchParams(values).toString(); }

async function createOAuthServer() {
  const journal = new MemoryEventJournal();
  const env = engineEnvSchema.parse({
    NODE_ENV: "test", JOURNAL_MODE: "memory", TRADING_MODE: "paper", POLYMARKET_TOKEN_IDS: "up,down",
    GROK_MCP_BEARER_TOKENS: "market_microstructure_scanner:legacy-micro, fair_probability_analyst:legacy-alpha",
    GROK_MCP_OAUTH_ENABLED: "true", GROK_MCP_OAUTH_BOOTSTRAP_TOKENS: `${ROLE}:bootstrap-micro`,
  });
  const events: OAuthSafeEvent[] = [];
  const engine = new EngineService(env, journal, { error: () => undefined });
  const server = await createApiServer({
    state: engine.state, journal, metrics: new EngineMetrics(), sse: new SseHub(), dashboardOrigin: "http://localhost:3000", logger: false,
    mcp: {
      bearerTokens: env.GROK_MCP_BEARER_TOKENS, allowedOrigins: "", rateLimitPerMinute: 60,
      oauth: {
        enabled: true, issuer: "", bootstrapTokens: `${ROLE}:bootstrap-micro`, accessTokenTtlSeconds: 3_600, authorizationCodeTtlSeconds: 300,
      },
    },
    oauthObserve: (event) => events.push(event),
    submitGrokSignal: (signal, clientId) => engine.submitGrokSignal(signal, clientId),
    persistReportInput: (event) => engine.persistReportInput(event),
  });
  await server.listen({ host: "127.0.0.1", port: 0 });
  const address = server.server.address();
  const port = typeof address === "object" && address ? address.port : 0;
  return { server, engine, journal, events, base: `http://127.0.0.1:${port}` };
}

async function jsonFetch(url: string, init: RequestInit = {}) {
  const response = await fetch(url, init);
  const text = await response.text();
  let body: unknown = text;
  try { body = JSON.parse(text); } catch { /* html/form response */ }
  return { response, body, text };
}

describe("PR23 OAuth 2.1 compatibility for role-scoped MCP", () => {
  it("expires authorization codes and access tokens with bounded state", () => {
    let clock = 1_700_000_000_000;
    const service = new McpOAuthService({ enabled: true, issuer: "https://mcp.example", bootstrapTokens: `${ROLE}:bootstrap`, accessTokenTtlSeconds: 2, authorizationCodeTtlSeconds: 1 }, () => clock);
    const context = { origin: "https://mcp.example", endpoint: "/oauth/authorize", remoteAddress: "127.0.0.1" };
    const registration = service.register({ redirect_uris: ["http://127.0.0.1:9876/callback"] }, context);
    const clientId = String((registration.body as { client_id: string }).client_id);
    const pkce = verifier();
    const params = { client_id: clientId, redirect_uri: "http://127.0.0.1:9876/callback", response_type: "code", scope: ROLE_SCOPE, code_challenge: pkce.challenge, code_challenge_method: "S256", resource: `https://mcp.example/mcp/${ROLE}` };
    const approved = service.approve({ ...params, bootstrap_secret: "bootstrap" }, context, ROLE);
    expect(approved.status).toBe(302);
    const code = new URL(approved.headers?.location ?? "").searchParams.get("code");
    clock += 1_001;
    const expiredCode = service.token({ grant_type: "authorization_code", client_id: clientId, code: code ?? "expired", redirect_uri: params.redirect_uri, code_verifier: pkce.value, resource: params.resource }, context);
    expect(expiredCode.status).toBe(400);
    expect(service.footprint().codes).toBe(0);
  });

  it("reclaims inactive DCR clients at the hard cap and keeps the footprint bounded", () => {
    let clock = 1_700_000_000_000;
    const service = new McpOAuthService({ enabled: true, issuer: "https://mcp.example", bootstrapTokens: "", accessTokenTtlSeconds: 3_600, authorizationCodeTtlSeconds: 300 }, () => clock);
    const context = (index: number) => ({ origin: "https://mcp.example", remoteAddress: `registration-${index}` });
    for (let index = 0; index < 256; index += 1) {
      expect(service.register({ redirect_uris: ["http://127.0.0.1:9876/callback"], client_name: `client-${index}` }, context(index)).status).toBe(201);
    }
    expect(service.footprint().clients).toBe(256);
    expect(service.register({ redirect_uris: ["http://127.0.0.1:9876/callback"], client_name: "after-cap" }, context(256)).status).toBe(201);
    expect(service.footprint().clients).toBe(256);

    clock += 24 * 60 * 60 * 1_000 + 1;
    expect(service.footprint().clients).toBe(0);
    for (let index = 0; index < 600; index += 1) {
      expect(service.register({ redirect_uris: ["http://127.0.0.1:9876/callback"] }, context(1_000 + index)).status).toBe(201);
    }
    const footprint = service.footprint();
    expect(footprint.clients).toBeLessThanOrEqual(256);
    expect(footprint.codes).toBeLessThanOrEqual(512);
    expect(footprint.tokens).toBeLessThanOrEqual(1_024);
    expect(footprint.rateKeys).toBeLessThanOrEqual(512);
  });

  it("does not evict clients that own an unexpired code or access token", () => {
    let clock = 1_700_000_000_000;
    const service = new McpOAuthService({ enabled: true, issuer: "https://mcp.example", bootstrapTokens: `${ROLE}:bootstrap`, accessTokenTtlSeconds: 3_600, authorizationCodeTtlSeconds: 300 }, () => clock);
    const redirectUri = "http://127.0.0.1:9876/callback";
    const registered: string[] = [];
    for (let index = 0; index < 256; index += 1) {
      const result = service.register({ redirect_uris: [redirectUri] }, { origin: "https://mcp.example", remoteAddress: `active-${index}` });
      registered.push(String((result.body as { client_id: string }).client_id));
    }
    const pkce = verifier();
    const params = { client_id: registered[0]!, redirect_uri: redirectUri, response_type: "code", scope: ROLE_SCOPE, code_challenge: pkce.challenge, code_challenge_method: "S256", resource: `https://mcp.example/mcp/${ROLE}` };
    const approval = service.approve({ ...params, bootstrap_secret: "bootstrap" }, { origin: "https://mcp.example", endpoint: "/oauth/authorize", remoteAddress: "active-0" }, ROLE);
    const code = new URL(approval.headers?.location ?? "").searchParams.get("code");
    expect(code).toBeTruthy();
    expect(service.register({ redirect_uris: [redirectUri] }, { origin: "https://mcp.example", remoteAddress: "active-new-code" }).status).toBe(201);
    const tokenResult = service.token({ grant_type: "authorization_code", client_id: registered[0]!, code: code!, redirect_uri: redirectUri, code_verifier: pkce.value, resource: params.resource }, { origin: "https://mcp.example", endpoint: "/oauth/token", remoteAddress: "active-0" });
    expect(tokenResult.status).toBe(200);
    const accessToken = String(tokenResult.body.access_token);
    expect(service.register({ redirect_uris: [redirectUri] }, { origin: "https://mcp.example", remoteAddress: "active-new-token" }).status).toBe(201);
    expect(service.authenticate(`Bearer ${accessToken}`, ROLE, { origin: "https://mcp.example", endpoint: `/mcp/${ROLE}` }).ok).toBe(true);
  });

  it("publishes protected-resource and authorization-server discovery without authentication", async () => {
    const ctx = await createOAuthServer();
    try {
      const unauth = await fetch(`${ctx.base}/mcp/${ROLE}`, { method: "POST", headers: { accept: "application/json" }, body: JSON.stringify({ jsonrpc: "2.0", id: 1, method: "tools/list", params: {} }) });
      expect(unauth.status).toBe(401);
      const challenge = unauth.headers.get("www-authenticate") ?? "";
      expect(challenge).toContain("Bearer");
      expect(challenge).toContain(`resource_metadata=\"${ctx.base}/.well-known/oauth-protected-resource/mcp/${ROLE}\"`);
      const resource = await jsonFetch(`${ctx.base}/.well-known/oauth-protected-resource/mcp/${ROLE}`);
      expect(resource.response.status).toBe(200);
      expect(resource.body).toMatchObject({ resource: `${ctx.base}/mcp/${ROLE}`, authorization_servers: [ctx.base], scopes_supported: [ROLE_SCOPE] });
      const fallback = await jsonFetch(`${ctx.base}/mcp/${ROLE}/.well-known/oauth-protected-resource`);
      expect(fallback.response.status).toBe(200);
      const metadata = await jsonFetch(`${ctx.base}/.well-known/oauth-authorization-server`);
      expect(metadata.response.status).toBe(200);
      expect(metadata.body).toMatchObject({ issuer: ctx.base, authorization_endpoint: `${ctx.base}/oauth/authorize`, token_endpoint: `${ctx.base}/oauth/token`, registration_endpoint: `${ctx.base}/oauth/register`, code_challenge_methods_supported: ["S256"], token_endpoint_auth_methods_supported: ["none"] });
      expect((await fetch(`${ctx.base}/.well-known/openid-configuration`)).status).toBe(404);
      expect((await fetch(`${ctx.base}/oauth/.well-known/openid-configuration`)).status).toBe(404);
      expect(ctx.events.map((event) => event.event)).toEqual(expect.arrayContaining(["protected_resource_401", "resource_metadata_requested", "authorization_metadata_requested"]));
    } finally { await ctx.server.close(); }
  });

  it("accepts refresh-token metadata but normalizes the effective grant capability", async () => {
    const ctx = await createOAuthServer();
    try {
      for (const grantTypes of [["authorization_code"], ["authorization_code", "refresh_token"]]) {
        const registration = await jsonFetch(`${ctx.base}/oauth/register`, {
          method: "POST", headers: { "content-type": "application/json" },
          body: JSON.stringify({ redirect_uris: ["http://127.0.0.1:9876/callback"], grant_types: grantTypes }),
        });
        expect(registration.response.status).toBe(201);
        expect((registration.body as { grant_types: string[] }).grant_types).toEqual(["authorization_code"]);
      }
      const metadata = await jsonFetch(`${ctx.base}/.well-known/oauth-authorization-server`);
      expect(metadata.body).toMatchObject({ grant_types_supported: ["authorization_code"] });
    } finally { await ctx.server.close(); }
  });

  it("completes DCR, operator approval, PKCE token exchange, and Streamable HTTP tools/list", async () => {
    const ctx = await createOAuthServer();
    try {
      const redirectUri = "http://127.0.0.1:9876/callback";
      const registration = await jsonFetch(`${ctx.base}/oauth/register`, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ client_name: "local oauth test", redirect_uris: [redirectUri], token_endpoint_auth_method: "none", grant_types: ["authorization_code"], response_types: ["code"] }) });
      expect(registration.response.status).toBe(201);
      const client = registration.body as { client_id: string };
      expect(client.client_id).toBeTruthy();
      const pkce = verifier();
      const state = createId("state");
      const resource = `${ctx.base}/mcp/${ROLE}`;
      const params = { client_id: client.client_id, redirect_uri: redirectUri, response_type: "code", scope: ROLE_SCOPE, state, code_challenge: pkce.challenge, code_challenge_method: "S256", resource };
      const page = await jsonFetch(`${ctx.base}/oauth/authorize?${new URLSearchParams(params)}`);
      expect(page.response.status).toBe(200);
      expect(page.text).toContain("Market Microstructure Scanner");
      expect(page.text).not.toContain("bootstrap-micro");
      const approval = await fetch(`${ctx.base}/oauth/authorize`, { method: "POST", headers: { "content-type": "application/x-www-form-urlencoded" }, redirect: "manual", body: form({ ...params, bootstrap_secret: "bootstrap-micro" }) });
      expect(approval.status).toBe(302);
      const location = new URL(approval.headers.get("location") ?? "");
      expect(location.searchParams.get("state")).toBe(state);
      const code = location.searchParams.get("code");
      expect(code).toBeTruthy();
      const exchanged = await jsonFetch(`${ctx.base}/oauth/token`, { method: "POST", headers: { "content-type": "application/x-www-form-urlencoded" }, body: form({ grant_type: "authorization_code", client_id: client.client_id, code: code!, redirect_uri: redirectUri, code_verifier: pkce.value, resource }) });
      expect(exchanged.response.status).toBe(200);
      const token = exchanged.body as { access_token: string; token_type: string; scope: string; expires_in: number };
      expect(token).toMatchObject({ token_type: "Bearer", scope: ROLE_SCOPE });
      expect(token.access_token).not.toContain("bootstrap-micro");
      const mcp = await jsonFetch(resource, { method: "POST", headers: { authorization: `Bearer ${token.access_token}`, accept: "application/json, text/event-stream", "content-type": "application/json", "mcp-protocol-version": "2025-11-25" }, body: JSON.stringify({ jsonrpc: "2.0", id: 1, method: "tools/list", params: {} }) });
      expect(mcp.response.status).toBe(200);
      expect(mcp.text).toContain("get_markets");
      expect(ctx.events.map((event) => event.event)).toEqual(expect.arrayContaining(["dynamic_client_registered", "authorization_started", "authorization_approved", "token_issued", "mcp_authenticated"]));
    } finally { await ctx.server.close(); }
  });

  it("shows only escaped registration identity on the operator approval page", async () => {
    const ctx = await createOAuthServer();
    try {
      const redirectUri = "https://grok.com/oauth/callback";
      const clientName = `<img src=x onerror="alert(1)">`;
      const registration = await jsonFetch(`${ctx.base}/oauth/register`, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ client_name: clientName, redirect_uris: [redirectUri] }) });
      expect(registration.response.status).toBe(201);
      const clientId = (registration.body as { client_id: string }).client_id;
      const pkce = verifier();
      const resource = `${ctx.base}/mcp/${ROLE}`;
      const params = { client_id: clientId, redirect_uri: redirectUri, response_type: "code", scope: ROLE_SCOPE, state: "safe", code_challenge: pkce.challenge, code_challenge_method: "S256", resource };
      const page = await jsonFetch(`${ctx.base}/oauth/authorize?${new URLSearchParams(params)}`);
      expect(page.response.status).toBe(200);
      expect(page.text).toContain("Client: <strong>&lt;img src=x onerror=&quot;alert(1)&quot;&gt;</strong>");
      expect(page.text).not.toContain(clientName);
      expect(page.text).toContain("Redirect host: <code>grok.com</code>");
      expect(page.text).toContain(`Resource: <code>${resource}</code>`);
      expect(page.text).not.toContain("bootstrap-micro");
    } finally { await ctx.server.close(); }
  });

  it("enforces bootstrap, PKCE, one-time code, exact redirect, resource audience, role isolation, and legacy bearer compatibility", async () => {
    const ctx = await createOAuthServer();
    try {
      const redirectUri = "http://127.0.0.1:9876/callback";
      const registration = await jsonFetch(`${ctx.base}/oauth/register`, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ redirect_uris: [redirectUri], token_endpoint_auth_method: "none" }) });
      const clientId = (registration.body as { client_id: string }).client_id;
      const pkce = verifier();
      const resource = `${ctx.base}/mcp/${ROLE}`;
      const params = { client_id: clientId, redirect_uri: redirectUri, response_type: "code", scope: ROLE_SCOPE, state: "s", code_challenge: pkce.challenge, code_challenge_method: "S256", resource };
      const plain = await fetch(`${ctx.base}/oauth/authorize?${new URLSearchParams({ ...params, code_challenge_method: "plain" })}`);
      expect(plain.status).toBe(400);
      const wrongRedirect = await fetch(`${ctx.base}/oauth/authorize?${new URLSearchParams({ ...params, redirect_uri: "http://127.0.0.1:9876/other" })}`);
      expect(wrongRedirect.status).toBe(400);
      const wrongPath = await fetch(`${ctx.base}/mcp/${ALPHA}`, { method: "POST", headers: { authorization: "Bearer nope", "content-type": "application/json" }, body: "{}" });
      expect(wrongPath.status).toBe(401);
      expect((await fetch(`${ctx.base}/mcp/not-a-role`, { method: "POST", body: "{}" })).status).toBe(404);
      const wrongSecret = await fetch(`${ctx.base}/oauth/authorize`, { method: "POST", headers: { "content-type": "application/x-www-form-urlencoded" }, body: form({ ...params, bootstrap_secret: "wrong" }) });
      expect(wrongSecret.status).toBe(401);
      const approved = await fetch(`${ctx.base}/oauth/authorize`, { method: "POST", redirect: "manual", headers: { "content-type": "application/x-www-form-urlencoded" }, body: form({ ...params, bootstrap_secret: "bootstrap-micro" }) });
      const code = new URL(approved.headers.get("location") ?? "").searchParams.get("code")!;
      const wrongVerifier = await jsonFetch(`${ctx.base}/oauth/token`, { method: "POST", headers: { "content-type": "application/x-www-form-urlencoded" }, body: form({ grant_type: "authorization_code", client_id: clientId, code, redirect_uri: redirectUri, code_verifier: verifier().value, resource }) });
      expect(wrongVerifier.response.status).toBe(400);
      const exchanged = await jsonFetch(`${ctx.base}/oauth/token`, { method: "POST", headers: { "content-type": "application/x-www-form-urlencoded" }, body: form({ grant_type: "authorization_code", client_id: clientId, code, redirect_uri: redirectUri, code_verifier: pkce.value, resource }) });
      expect(exchanged.response.status).toBe(200);
      const token = (exchanged.body as { access_token: string }).access_token;
      const forbiddenAnalysis = await jsonFetch(resource, {
        method: "POST",
        headers: { authorization: `Bearer ${token}`, "content-type": "application/json", accept: "application/json, text/event-stream" },
        body: JSON.stringify({ jsonrpc: "2.0", id: 3, method: "tools/call", params: { name: "submit_analysis", arguments: {} } }),
      });
      expect(forbiddenAnalysis.response.status).toBe(200);
      expect(forbiddenAnalysis.text).toContain("WRITE_TOOL_FORBIDDEN");
      const forbiddenPortfolio = await jsonFetch(resource, {
        method: "POST",
        headers: { authorization: `Bearer ${token}`, "content-type": "application/json", accept: "application/json, text/event-stream" },
        body: JSON.stringify({ jsonrpc: "2.0", id: 4, method: "tools/call", params: { name: "get_portfolio", arguments: {} } }),
      });
      expect(forbiddenPortfolio.response.status).toBe(200);
      expect(forbiddenPortfolio.text).toContain("ROLE_TOOL_FORBIDDEN");
      const wrongAudience = await jsonFetch(`${ctx.base}/oauth/token`, { method: "POST", headers: { "content-type": "application/x-www-form-urlencoded" }, body: form({ grant_type: "authorization_code", client_id: clientId, code: "already-used", redirect_uri: redirectUri, code_verifier: pkce.value, resource: `${ctx.base}/mcp/${ALPHA}` }) });
      expect(wrongAudience.response.status).toBe(400);
      const replay = await jsonFetch(`${ctx.base}/oauth/token`, { method: "POST", headers: { "content-type": "application/x-www-form-urlencoded" }, body: form({ grant_type: "authorization_code", client_id: clientId, code, redirect_uri: redirectUri, code_verifier: pkce.value, resource }) });
      expect(replay.response.status).toBe(400);
      const alpha = await fetch(`${ctx.base}/mcp/${ALPHA}`, { method: "POST", headers: { authorization: `Bearer ${token}`, "content-type": "application/json" }, body: "{}" });
      expect(alpha.status).toBe(401);
      const legacy = await jsonFetch(`${ctx.base}/mcp`, { method: "POST", headers: { authorization: "Bearer legacy-micro", "content-type": "application/json", accept: "application/json, text/event-stream" }, body: JSON.stringify({ jsonrpc: "2.0", id: 2, method: "tools/list", params: {} }) });
      expect(legacy.response.status).toBe(200);
      expect(ctx.events.every((event) => !JSON.stringify(event).includes("bootstrap-micro"))).toBe(true);
    } finally { await ctx.server.close(); }
  });

  it("rejects unsupported registration and authorization capabilities without weakening defaults", async () => {
    const ctx = await createOAuthServer();
    try {
      const noOAuth = new McpOAuthService({ enabled: false, issuer: "", bootstrapTokens: "", accessTokenTtlSeconds: 3600, authorizationCodeTtlSeconds: 300 });
      expect(noOAuth.register({}, { origin: ctx.base }).status).toBe(404);
      const badRedirect = await jsonFetch(`${ctx.base}/oauth/register`, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ redirect_uris: ["javascript:alert(1)"] }) });
      expect(badRedirect.response.status).toBe(400);
      const badClientAuth = await jsonFetch(`${ctx.base}/oauth/register`, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ redirect_uris: ["https://client.example/cb"], token_endpoint_auth_method: "client_secret_post" }) });
      expect(badClientAuth.response.status).toBe(400);
      const unsupportedGrantTypes = ["client_credentials", "password", "urn:ietf:params:oauth:grant-type:jwt-bearer"];
      for (const grantType of unsupportedGrantTypes) {
        const rejected = await jsonFetch(`${ctx.base}/oauth/register`, {
          method: "POST", headers: { "content-type": "application/json" },
          body: JSON.stringify({
            client_name: "diagnostic-client",
            redirect_uris: ["https://grok.example/callback?secret=bootstrap-secret"],
            token_endpoint_auth_method: "none",
            grant_types: [grantType],
            scope: `${ROLE_SCOPE} mcp:unknown_scope`,
          }),
        });
        expect(rejected.response.status).toBe(400);
      }
      const rejection = [...ctx.events].reverse().find((event) => event.event === "dynamic_client_registration_rejected");
      expect(rejection).toMatchObject({
        event: "dynamic_client_registration_rejected",
        reason: "unsupported_grant_type",
        tokenEndpointAuthMethod: "none",
        requestedGrantTypes: [unsupportedGrantTypes.at(-1)],
        redirectUris: [{ scheme: "https", hostname: "grok.example" }],
        requestedScopes: [ROLE_SCOPE, "mcp:unknown_scope"],
      });
      expect(rejection?.metadataFields).toEqual(expect.arrayContaining(["client_name", "redirect_uris", "token_endpoint_auth_method", "grant_types", "scope"]));
      expect(JSON.stringify(rejection)).not.toContain("bootstrap-secret");
      expect(MCP_OAUTH_SCOPES).toHaveLength(6);
    } finally { await ctx.server.close(); }
  });
});
