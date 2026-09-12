import { describe, expect, it } from "vitest";
import { McpOAuthService } from "../apps/engine/src/mcp-oauth.js";

const MICRO_SCOPE = "mcp:market_microstructure_scanner";

function service() {
  return new McpOAuthService({
    enabled: true,
    issuer: "https://api.personachat.io",
    bootstrapTokens: "",
    accessTokenTtlSeconds: 3600,
    authorizationCodeTtlSeconds: 300,
  });
}

describe("Grok Bot Cursor OAuth redirect compatibility", () => {
  it("accepts the observed Cursor/Grok Bot DCR shape", () => {
    const result = service().register(
      {
        client_name: "Grok Bot MCP",
        redirect_uris: [
          "https://www.cursor.com/oauth/callback",
          "cursor://anysphere.cursor-mcp/oauth/callback",
          "http://localhost:8787/callback",
        ],
        token_endpoint_auth_method: "none",
        grant_types: ["authorization_code", "refresh_token"],
        response_types: ["code"],
        scope: MICRO_SCOPE,
      },
      {
        origin: "https://api.personachat.io",
        endpoint: "/oauth/register",
        remoteAddress: "grok-bot",
      },
    );

    expect(result.status).toBe(201);
    expect(result.body.grant_types).toEqual(["authorization_code"]);
  });

  it("rejects other custom Cursor redirect targets", () => {
    const invalidRedirects = [
      "cursor://evil.example/oauth/callback",
      "cursor://anysphere.cursor-mcp/not-oauth",
      "cursor://anysphere.cursor-mcp/oauth/callback?evil=1",
      "cursor://anysphere.cursor-mcp/oauth/callback#evil",
      "otherapp://anysphere.cursor-mcp/oauth/callback",
    ];

    for (const redirectUri of invalidRedirects) {
      const result = service().register(
        {
          redirect_uris: [redirectUri],
          token_endpoint_auth_method: "none",
          grant_types: ["authorization_code"],
          response_types: ["code"],
          scope: MICRO_SCOPE,
        },
        {
          origin: "https://api.personachat.io",
          endpoint: "/oauth/register",
          remoteAddress: redirectUri,
        },
      );

      expect(result.status).toBe(400);
      expect(result.body).toMatchObject({
        error: "invalid_redirect_uri",
      });
    }
  });
});
