import { describe, expect, it } from "vitest";
import { MCP_OAUTH_ROLES, McpOAuthService } from "../apps/engine/src/mcp-oauth.js";
import { resolveOAuthApprovalBootstrap } from "../apps/engine/src/runtime.js";

const ROLE = "market_microstructure_scanner" as const;
const SECRET = "a".repeat(64);
const REDIRECT = "https://grok.com/connectors-oauth-exchange-code/";

describe("PR26 single Grok OAuth approval secret", () => {
  it("maps one operator approval secret to every role while role authority remains resource-bound", () => {
    const bootstrap = resolveOAuthApprovalBootstrap({ GROK_MCP_OAUTH_APPROVAL_SECRET: SECRET }, true);
    const entries = bootstrap.split(",");
    expect(entries).toHaveLength(MCP_OAUTH_ROLES.length);
    for (const role of MCP_OAUTH_ROLES) expect(entries).toContain(`${role}:${SECRET}`);
  });

  it("does not fall back to the old role-mapped bootstrap secret", () => {
    expect(() => resolveOAuthApprovalBootstrap({
      GROK_MCP_OAUTH_BOOTSTRAP_TOKENS: `market_microstructure_scanner:${SECRET}`,
    }, true)).toThrow("GROK_MCP_OAUTH_APPROVAL_SECRET_REQUIRED");
  });

  it("fails closed when OAuth is enabled without a usable operator secret", () => {
    expect(() => resolveOAuthApprovalBootstrap({}, true)).toThrow("GROK_MCP_OAUTH_APPROVAL_SECRET_REQUIRED");
    expect(() => resolveOAuthApprovalBootstrap({ GROK_MCP_OAUTH_APPROVAL_SECRET: "short" }, true)).toThrow("GROK_MCP_OAUTH_APPROVAL_SECRET_TOO_SHORT");
    expect(() => resolveOAuthApprovalBootstrap({ GROK_MCP_OAUTH_APPROVAL_SECRET: `${SECRET},bad` }, true)).toThrow("GROK_MCP_OAUTH_APPROVAL_SECRET_INVALID");
    expect(resolveOAuthApprovalBootstrap({}, false)).toBe("");
  });

  it("authorizes the real Grok callback with the single secret and still rejects a wrong secret", () => {
    const bootstrapTokens = resolveOAuthApprovalBootstrap({ GROK_MCP_OAUTH_APPROVAL_SECRET: SECRET }, true);
    const service = new McpOAuthService({
      enabled: true,
      issuer: "https://engine.example",
      bootstrapTokens,
      accessTokenTtlSeconds: 3_600,
      authorizationCodeTtlSeconds: 300,
    });
    const context = { origin: "https://engine.example", endpoint: "/oauth/authorize", remoteAddress: "127.0.0.1" };
    const registration = service.register({
      client_name: "Grok",
      redirect_uris: [REDIRECT],
      grant_types: ["authorization_code", "refresh_token"],
      response_types: ["code"],
      scope: MCP_OAUTH_ROLES.map((role) => `mcp:${role}`).join(" "),
    }, { ...context, endpoint: "/oauth/register" });
    expect(registration.status).toBe(201);
    const clientId = String(registration.body.client_id);
    const resource = `https://engine.example/mcp/${ROLE}`;
    const input = {
      client_id: clientId,
      redirect_uri: REDIRECT,
      response_type: "code",
      scope: MCP_OAUTH_ROLES.map((role) => `mcp:${role}`).join(" "),
      state: "grok-state",
      code_challenge: "x".repeat(43),
      code_challenge_method: "S256",
      resource,
    };

    expect(service.authorizationPage(input, context, ROLE).status).toBe(200);
    expect(service.approve({ ...input, bootstrap_secret: "wrong-secret-value-that-is-long-enough" }, context, ROLE).status).toBe(401);
    const approved = service.approve({ ...input, bootstrap_secret: SECRET }, context, ROLE);
    expect(approved.status).toBe(302);
    expect(approved.headers?.location).toContain("https://grok.com/connectors-oauth-exchange-code/");
    expect(approved.headers?.location).toContain("state=grok-state");
  });
});
