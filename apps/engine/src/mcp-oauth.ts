import { createHash, randomBytes, timingSafeEqual } from "node:crypto";
import type { ExternalAgentRoleId } from "@pm/core";
import { externalAgentRoleIdSchema, externalAgentRoleDefinitions } from "@pm/core";
import { OAuthMetadataSchema, OAuthProtectedResourceMetadataSchema } from "@modelcontextprotocol/sdk/shared/auth.js";

// The pinned MCP SDK exposes the RFC metadata schemas but no Fastify OAuth
// authorization-server implementation. This adapter keeps the compatibility
// surface small and bounded while validating every published metadata document
// with the SDK's official schemas.

export const MCP_OAUTH_ROLES = externalAgentRoleDefinitions.map((item) => item.id) as readonly ExternalAgentRoleId[];
export const MCP_OAUTH_SCOPES = MCP_OAUTH_ROLES.map((role) => `mcp:${role}`) as readonly string[];

const MAX_CLIENTS = 256;
const MAX_CODES = 512;
const MAX_TOKENS = 1_024;
const MAX_RATE_KEYS = 512;
const REGISTRATION_WINDOW_MS = 60_000;
const CLIENT_REGISTRATION_TTL_MS = 24 * 60 * 60 * 1_000;
const MAX_REGISTRATIONS_PER_WINDOW = 30;
const MAX_AUTHORIZATIONS_PER_WINDOW = 60;
const MAX_TOKEN_REQUESTS_PER_WINDOW = 120;

export interface McpOAuthConfig {
  enabled: boolean;
  issuer: string;
  bootstrapTokens: string;
  accessTokenTtlSeconds: number;
  authorizationCodeTtlSeconds: number;
}

export interface OAuthSafeEvent {
  event: "protected_resource_401" | "resource_metadata_requested" | "authorization_metadata_requested" | "dynamic_client_registered" | "dynamic_client_registration_rejected" | "authorization_started" | "authorization_approved" | "token_issued" | "mcp_authenticated";
  role?: ExternalAgentRoleId;
  endpoint?: string;
  status?: string;
  reason?: string;
  clientFingerprint?: string | null;
  metadataFields?: string[];
  requestedGrantTypes?: string[];
  tokenEndpointAuthMethod?: string | null;
  redirectUris?: Array<{ scheme: string; hostname: string }>;
  requestedScopes?: string[];
  timestamp: string;
}

export interface OAuthAuthResult {
  ok: true;
  clientId: ExternalAgentRoleId;
  tokenFingerprint: string;
  scope: string;
  resource: string;
  expiresAt: number;
}

export type OAuthFailure = {
  ok: false;
  status: 400 | 401 | 403 | 404 | 429 | 503;
  reason: string;
  tokenFingerprint: string | null;
  challenge?: string;
};

export interface OAuthRequestContext {
  origin: string;
  endpoint?: string;
  remoteAddress?: string;
}

interface OAuthClient {
  clientId: string;
  redirectUris: string[];
  clientName: string | null;
  scope: string | null;
  issuedAt: number;
  lastUsedAt: number;
  expiresAt: number;
}

interface AuthorizationCode {
  clientId: string;
  redirectUri: string;
  role: ExternalAgentRoleId;
  resource: string;
  scopes: string[];
  codeChallenge: string;
  expiresAt: number;
}

interface AccessToken {
  clientId: string;
  role: ExternalAgentRoleId;
  resource: string;
  scopes: string[];
  expiresAt: number;
}

interface RateState { startedAt: number; count: number; lastUsedAt: number; }

export class McpOAuthService {
  private readonly clients = new Map<string, OAuthClient>();
  private readonly codes = new Map<string, AuthorizationCode>();
  private readonly tokens = new Map<string, AccessToken>();
  private readonly bootstrap = new Map<ExternalAgentRoleId, Buffer>();
  private readonly registrationRate = new Map<string, RateState>();
  private readonly authorizationRate = new Map<string, RateState>();
  private readonly tokenRate = new Map<string, RateState>();

  constructor(
    private readonly config: McpOAuthConfig,
    private readonly now: () => number = () => Date.now(),
    private readonly observe?: (event: OAuthSafeEvent) => void,
  ) {
    for (const entry of config.bootstrapTokens.split(",").map((item) => item.trim()).filter(Boolean)) {
      const separator = entry.indexOf(":");
      if (separator <= 0 || separator === entry.length - 1) continue;
      const role = externalAgentRoleIdSchema.safeParse(entry.slice(0, separator).trim());
      const secret = entry.slice(separator + 1).trim();
      if (role.success && secret) this.bootstrap.set(role.data, sha256(secret));
    }
  }

  get enabled(): boolean { return this.config.enabled; }

  roleFromPath(value: string): ExternalAgentRoleId | null {
    const parsed = externalAgentRoleIdSchema.safeParse(value);
    return parsed.success ? parsed.data : null;
  }

  scopeForRole(role: ExternalAgentRoleId): string { return `mcp:${role}`; }

  resourceForRole(role: ExternalAgentRoleId, requestOrigin: string): string {
    const base = this.config.issuer || requestOrigin;
    return `${base.replace(/\/$/, "")}/mcp/${role}`;
  }

  protectedResourceMetadata(role: ExternalAgentRoleId, requestOrigin: string): Record<string, unknown> {
    const issuer = this.issuerFor(requestOrigin);
    return OAuthProtectedResourceMetadataSchema.parse({
      resource: this.resourceForRole(role, requestOrigin),
      authorization_servers: [issuer],
      scopes_supported: [this.scopeForRole(role)],
      bearer_methods_supported: ["header"],
      resource_name: externalAgentRoleDefinitions.find((item) => item.id === role)?.name ?? role,
    });
  }

  authorizationServerMetadata(requestOrigin: string): Record<string, unknown> {
    const issuer = this.issuerFor(requestOrigin);
    return OAuthMetadataSchema.parse({
      issuer,
      authorization_endpoint: `${issuer}/oauth/authorize`,
      token_endpoint: `${issuer}/oauth/token`,
      registration_endpoint: `${issuer}/oauth/register`,
      response_types_supported: ["code"],
      grant_types_supported: ["authorization_code"],
      code_challenge_methods_supported: ["S256"],
      token_endpoint_auth_methods_supported: ["none"],
      scopes_supported: [...MCP_OAUTH_SCOPES],
    });
  }

  issuerFor(requestOrigin: string): string {
    return (this.config.issuer || requestOrigin).replace(/\/$/, "");
  }

  register(input: unknown, context: OAuthRequestContext): { status: number; body: Record<string, unknown> } {
    this.prune();
    if (!this.config.enabled) return this.rejectRegistration(input, context, 404, "NOT_FOUND", "oauth_disabled");
    if (!this.consumeRate(this.registrationRate, context.remoteAddress ?? "unknown", MAX_REGISTRATIONS_PER_WINDOW)) {
      return this.rejectRegistration(input, context, 429, "temporarily_unavailable", "rate_limited");
    }
    if (!input || typeof input !== "object" || Array.isArray(input)) return this.rejectRegistration(input, context, 400, "invalid_client_metadata", "invalid_client_metadata");
    const raw = input as Record<string, unknown>;
    const redirects = raw.redirect_uris;
    if (!Array.isArray(redirects) || redirects.length === 0 || redirects.length > 8 || !redirects.every((item) => typeof item === "string" && validRedirectUri(item))) {
      return this.rejectRegistration(input, context, 400, "invalid_redirect_uri", "invalid_redirect_uri");
    }
    const authMethod = raw.token_endpoint_auth_method;
    if (authMethod !== undefined && authMethod !== "none") return this.rejectRegistration(input, context, 400, "invalid_client_metadata", "unsupported_token_endpoint_auth_method");
    const grants = raw.grant_types;
    if (grants !== undefined) {
      if (!Array.isArray(grants) || grants.length === 0 || grants.some((item) => typeof item !== "string")) {
        return this.rejectRegistration(input, context, 400, "invalid_client_metadata", "invalid_grant_types");
      }
      if (!grants.includes("authorization_code") || grants.some((item) => item !== "authorization_code" && item !== "refresh_token")) {
        return this.rejectRegistration(input, context, 400, "invalid_client_metadata", "unsupported_grant_type");
      }
    }
    const responseTypes = raw.response_types;
    if (responseTypes !== undefined && (!Array.isArray(responseTypes) || responseTypes.some((item) => item !== "code"))) return this.rejectRegistration(input, context, 400, "invalid_client_metadata", "unsupported_response_type");
    const requestedScope = typeof raw.scope === "string" ? raw.scope.trim() : "";
    if (requestedScope && !scopeListValid(requestedScope)) return this.rejectRegistration(input, context, 400, "invalid_scope", "invalid_scope");
    const now = this.now();
    const clientId = randomToken(18);
    const client: OAuthClient = {
      clientId,
      redirectUris: redirects as string[],
      clientName: typeof raw.client_name === "string" ? raw.client_name.slice(0, 120) : null,
      scope: requestedScope || null,
      issuedAt: now,
      lastUsedAt: now,
      expiresAt: now + CLIENT_REGISTRATION_TTL_MS,
    };
    if (!this.ensureClientCapacity(now)) return this.rejectRegistration(input, context, 503, "temporarily_unavailable", "client_capacity_exhausted");
    this.clients.set(clientId, client);
    this.observeEvent({ event: "dynamic_client_registered", endpoint: context.endpoint, status: "ok", clientFingerprint: fingerprint(clientId), timestamp: new Date(now).toISOString() });
    return {
      status: 201,
      body: {
        client_id: client.clientId,
        client_id_issued_at: Math.floor(now / 1000),
        redirect_uris: client.redirectUris,
        token_endpoint_auth_method: "none",
        grant_types: ["authorization_code"],
        response_types: ["code"],
        ...(client.clientName ? { client_name: client.clientName } : {}),
        ...(client.scope ? { scope: client.scope } : {}),
      },
    };
  }

  authorizationPage(input: Record<string, string | undefined>, context: OAuthRequestContext, role: ExternalAgentRoleId): { status: number; body: string; headers?: Record<string, string> } {
    this.prune();
    if (!this.consumeRate(this.authorizationRate, context.remoteAddress ?? "unknown", MAX_AUTHORIZATIONS_PER_WINDOW)) return { status: 429, body: JSON.stringify({ error: "temporarily_unavailable" }), headers: { "content-type": "application/json" } };
    const validation = this.validateAuthorizationInput(input, context, role);
    if (!validation.ok) return { status: validation.status, body: JSON.stringify({ error: validation.reason }), headers: { "content-type": "application/json" } };
    validation.client.lastUsedAt = this.now();
    this.observeEvent({ event: "authorization_started", role, endpoint: context.endpoint, status: "pending", clientFingerprint: fingerprint(validation.client.clientId), timestamp: new Date(this.now()).toISOString() });
    const hidden = ["client_id", "redirect_uri", "response_type", "scope", "state", "code_challenge", "code_challenge_method", "resource"]
      .map((key) => `<input type="hidden" name="${key}" value="${escapeHtml(input[key] ?? "")}">`).join("");
    const roleName = externalAgentRoleDefinitions.find((item) => item.id === role)?.name ?? role;
    const clientName = validation.client.clientName?.trim() || "Unnamed public MCP client";
    const redirectHost = new URL(validation.redirectUri).hostname;
    const body = `<!doctype html><html><head><meta charset="utf-8"><title>Authorize Polymarket MCP connector</title></head><body><h1>Authorize Polymarket MCP connector</h1><p>Role: <strong>${escapeHtml(roleName)}</strong></p><p>Client: <strong>${escapeHtml(clientName)}</strong></p><p>Redirect host: <code>${escapeHtml(redirectHost)}</code></p><p>Resource: <code>${escapeHtml(validation.resource)}</code></p><p>No wallet or user trading credentials are involved.</p><form method="post" action="/oauth/authorize">${hidden}<label>Connection secret <input type="password" name="bootstrap_secret" autocomplete="off" required></label><button type="submit">Authorize</button></form></body></html>`;
    return { status: 200, body, headers: { "content-type": "text/html; charset=utf-8", "cache-control": "no-store" } };
  }

  approve(input: Record<string, string | undefined>, context: OAuthRequestContext, role: ExternalAgentRoleId): { status: number; body?: Record<string, unknown>; headers?: Record<string, string> } {
    this.prune();
    if (!this.consumeRate(this.authorizationRate, context.remoteAddress ?? "unknown", MAX_AUTHORIZATIONS_PER_WINDOW)) return { status: 429, body: { error: "temporarily_unavailable" } };
    const validation = this.validateAuthorizationInput(input, context, role);
    if (!validation.ok) return { status: validation.status, body: { error: validation.reason } };
    validation.client.lastUsedAt = this.now();
    const supplied = typeof input.bootstrap_secret === "string" ? sha256(input.bootstrap_secret) : null;
    const expected = this.bootstrap.get(role);
    if (!supplied || !expected || !safeEqual(supplied, expected)) return { status: 401, body: { error: "access_denied" } };
    const code = randomToken(32);
    this.codes.set(digestKey(code), {
      clientId: validation.client.clientId,
      redirectUri: validation.redirectUri,
      role,
      resource: validation.resource,
      scopes: validation.scopes,
      codeChallenge: input.code_challenge!,
      expiresAt: this.now() + this.config.authorizationCodeTtlSeconds * 1000,
    });
    const redirect = new URL(validation.redirectUri);
    redirect.searchParams.set("code", code);
    if (input.state !== undefined) redirect.searchParams.set("state", input.state);
    this.observeEvent({ event: "authorization_approved", role, endpoint: context.endpoint, status: "ok", clientFingerprint: fingerprint(validation.client.clientId), timestamp: new Date(this.now()).toISOString() });
    return { status: 302, headers: { location: redirect.toString(), "cache-control": "no-store" } };
  }

  token(input: Record<string, string | undefined>, context: OAuthRequestContext): { status: number; body: Record<string, unknown> } {
    this.prune();
    if (!this.config.enabled) return { status: 404, body: { error: "not_found" } };
    const key = input.client_id ?? context.remoteAddress ?? "unknown";
    if (!this.consumeRate(this.tokenRate, key, MAX_TOKEN_REQUESTS_PER_WINDOW)) return { status: 429, body: { error: "temporarily_unavailable" } };
    if (input.grant_type !== "authorization_code" || !input.client_id || !input.code || !input.code_verifier || !input.redirect_uri || !input.resource) return { status: 400, body: { error: "invalid_request" } };
    const client = this.clients.get(input.client_id);
    const record = this.codes.get(digestKey(input.code));
    if (!client || !record || record.expiresAt <= this.now()) return { status: 400, body: { error: "invalid_grant" } };
    if (record.clientId !== client.clientId || record.redirectUri !== input.redirect_uri || record.resource !== input.resource) return { status: 400, body: { error: "invalid_grant" } };
    if (!validCodeVerifier(input.code_verifier) || !safeEqual(sha256Base64Url(input.code_verifier), Buffer.from(record.codeChallenge))) return { status: 400, body: { error: "invalid_grant" } };
    this.codes.delete(digestKey(input.code));
    client.lastUsedAt = this.now();
    const token = randomToken(32);
    const expiresAt = this.now() + this.config.accessTokenTtlSeconds * 1000;
    this.tokens.set(digestKey(token), { clientId: client.clientId, role: record.role, resource: record.resource, scopes: record.scopes, expiresAt });
    this.observeEvent({ event: "token_issued", role: record.role, endpoint: context.endpoint, status: "ok", clientFingerprint: fingerprint(client.clientId), timestamp: new Date(this.now()).toISOString() });
    return { status: 200, body: { access_token: token, token_type: "Bearer", expires_in: Math.max(1, Math.floor((expiresAt - this.now()) / 1000)), scope: record.scopes.join(" ") } };
  }

  authenticate(header: string | undefined, role: ExternalAgentRoleId, context: OAuthRequestContext): OAuthAuthResult | OAuthFailure {
    this.prune();
    const challenge = `Bearer realm="polymarket-mcp", resource_metadata="${this.protectedMetadataUrl(role, context.origin)}", scope="${this.scopeForRole(role)}"`;
    if (!this.config.enabled) return { ok: false, status: 503, reason: "MCP_OAUTH_NOT_ENABLED", tokenFingerprint: null, challenge };
    if (!header?.startsWith("Bearer ")) {
      this.observeFailure(role, context, "missing_bearer");
      return { ok: false, status: 401, reason: "INVALID_BEARER_TOKEN", tokenFingerprint: null, challenge };
    }
    const raw = header.slice(7).trim();
    const digest = sha256(raw);
    const record = this.tokens.get(digest.toString("hex"));
    const fp = digest.toString("hex").slice(0, 12);
    if (!record || record.expiresAt <= this.now()) {
      this.observeFailure(role, context, "invalid_or_expired");
      return { ok: false, status: 401, reason: "INVALID_BEARER_TOKEN", tokenFingerprint: fp, challenge };
    }
    if (record.role !== role || record.resource !== this.resourceForRole(role, context.origin)) {
      this.observeFailure(role, context, "wrong_resource");
      return { ok: false, status: 401, reason: "INVALID_RESOURCE", tokenFingerprint: fp, challenge };
    }
    if (!record.scopes.includes(this.scopeForRole(role))) {
      this.observeFailure(role, context, "insufficient_scope");
      return { ok: false, status: 403, reason: "INSUFFICIENT_SCOPE", tokenFingerprint: fp, challenge };
    }
    this.observeEvent({ event: "mcp_authenticated", role, endpoint: context.endpoint, status: "ok", clientFingerprint: fingerprint(record.clientId), timestamp: new Date(this.now()).toISOString() });
    return { ok: true, clientId: record.role, tokenFingerprint: fp, scope: record.scopes.join(" "), resource: record.resource, expiresAt: Math.floor(record.expiresAt / 1000) };
  }

  protectedMetadataUrl(role: ExternalAgentRoleId, requestOrigin: string): string {
    return `${this.issuerFor(requestOrigin)}/.well-known/oauth-protected-resource/mcp/${role}`;
  }

  footprint(): { clients: number; codes: number; tokens: number; rateKeys: number } {
    this.prune();
    return { clients: this.clients.size, codes: this.codes.size, tokens: this.tokens.size, rateKeys: this.registrationRate.size + this.authorizationRate.size + this.tokenRate.size };
  }

  private validateAuthorizationInput(input: Record<string, string | undefined>, context: OAuthRequestContext, role: ExternalAgentRoleId): { ok: true; client: OAuthClient; redirectUri: string; resource: string; scopes: string[] } | { ok: false; status: 400 | 401; reason: string } {
    if (!input.client_id || !input.redirect_uri || input.response_type !== "code" || !input.code_challenge || input.code_challenge_method !== "S256" || !input.resource) return { ok: false, status: 400, reason: "invalid_request" };
    const client = this.clients.get(input.client_id);
    if (!client || !client.redirectUris.includes(input.redirect_uri)) return { ok: false, status: 400, reason: "invalid_client" };
    const resource = this.resourceForRole(role, context.origin);
    if (input.resource !== resource) return { ok: false, status: 400, reason: "invalid_target" };
    const scopes = (input.scope ?? this.scopeForRole(role)).split(/\s+/).filter(Boolean);
    if (!scopes.includes(this.scopeForRole(role)) || scopes.some((scope) => scope !== this.scopeForRole(role))) return { ok: false, status: 400, reason: "invalid_scope" };
    if (client.scope && client.scope.split(/\s+/).some((scope) => !scopes.includes(scope))) return { ok: false, status: 400, reason: "invalid_scope" };
    return { ok: true, client, redirectUri: input.redirect_uri, resource, scopes };
  }

  private consumeRate(store: Map<string, RateState>, key: string, limit: number): boolean {
    const now = this.now();
    const current = store.get(key);
    if (!current || now - current.startedAt >= REGISTRATION_WINDOW_MS) {
      if (store.size >= MAX_RATE_KEYS && !current) {
        const oldest = [...store.entries()].sort((a, b) => a[1].lastUsedAt - b[1].lastUsedAt)[0]?.[0];
        if (oldest) store.delete(oldest);
      }
      store.set(key, { startedAt: now, count: 1, lastUsedAt: now });
      return true;
    }
    current.lastUsedAt = now;
    if (current.count >= limit) return false;
    current.count += 1;
    return true;
  }

  private prune(): void {
    const now = this.now();
    for (const [key, item] of this.codes) if (item.expiresAt <= now) this.codes.delete(key);
    for (const [key, item] of this.tokens) if (item.expiresAt <= now) this.tokens.delete(key);
    for (const store of [this.registrationRate, this.authorizationRate, this.tokenRate]) {
      for (const [key, item] of store) if (now - item.lastUsedAt >= REGISTRATION_WINDOW_MS) store.delete(key);
    }
    this.reclaimExpiredClients(now);
    if (this.codes.size > MAX_CODES) this.evictOldest(this.codes, MAX_CODES, (item) => item.expiresAt);
    if (this.tokens.size > MAX_TOKENS) this.evictOldest(this.tokens, MAX_TOKENS, (item) => item.expiresAt);
  }

  private reclaimExpiredClients(now: number): void {
    for (const [clientId, client] of this.clients) {
      if (client.expiresAt <= now && !this.clientHasActiveGrant(clientId, now)) this.clients.delete(clientId);
    }
  }

  private ensureClientCapacity(now: number): boolean {
    this.reclaimExpiredClients(now);
    if (this.clients.size < MAX_CLIENTS) return true;
    const candidate = [...this.clients.entries()]
      .filter(([clientId]) => !this.clientHasActiveGrant(clientId, now))
      .sort(([, left], [, right]) => left.lastUsedAt - right.lastUsedAt || left.issuedAt - right.issuedAt)[0]?.[0];
    if (candidate) this.clients.delete(candidate);
    return this.clients.size < MAX_CLIENTS;
  }

  private clientHasActiveGrant(clientId: string, now: number): boolean {
    for (const code of this.codes.values()) if (code.clientId === clientId && code.expiresAt > now) return true;
    for (const token of this.tokens.values()) if (token.clientId === clientId && token.expiresAt > now) return true;
    return false;
  }

  private evictOldest<T>(store: Map<string, T>, limit: number, at: (item: T) => number): void {
    while (store.size > limit) {
      const oldest = [...store.entries()].sort((a, b) => at(a[1]) - at(b[1]))[0]?.[0];
      if (!oldest) return;
      store.delete(oldest);
    }
  }

  private observeFailure(role: ExternalAgentRoleId, context: OAuthRequestContext, status: string): void {
    this.observeEvent({ event: "protected_resource_401", role, endpoint: context.endpoint, status, clientFingerprint: null, timestamp: new Date(this.now()).toISOString() });
  }

  private observeEvent(event: OAuthSafeEvent): void { this.observe?.(event); }

  private rejectRegistration(input: unknown, context: OAuthRequestContext, status: 400 | 404 | 429 | 503, error: string, reason: string): { status: number; body: Record<string, unknown> } {
    const diagnostic = registrationDiagnostic(input);
    this.observeEvent({
      event: "dynamic_client_registration_rejected",
      endpoint: context.endpoint,
      status: "rejected",
      reason,
      ...diagnostic,
      timestamp: new Date(this.now()).toISOString(),
    });
    return { status, body: { error } };
  }
}

function sha256(value: string): Buffer { return createHash("sha256").update(value).digest(); }
function digestKey(value: string): string { return sha256(value).toString("hex"); }
function sha256Base64Url(value: string): Buffer { return Buffer.from(sha256(value).toString("base64url")); }
function randomToken(bytes: number): string { return randomBytes(bytes).toString("base64url"); }
function fingerprint(value: string): string { return digestKey(value).slice(0, 12); }
function safeEqual(a: Buffer, b: Buffer): boolean { return a.length === b.length && timingSafeEqual(a, b); }
function scopeListValid(value: string): boolean { return value.split(/\s+/).filter(Boolean).every((item) => MCP_OAUTH_SCOPES.includes(item)); }
function registrationDiagnostic(input: unknown): Pick<OAuthSafeEvent, "metadataFields" | "requestedGrantTypes" | "tokenEndpointAuthMethod" | "redirectUris" | "requestedScopes"> {
  const empty: Pick<OAuthSafeEvent, "metadataFields" | "requestedGrantTypes" | "tokenEndpointAuthMethod" | "redirectUris" | "requestedScopes"> = {
    metadataFields: [], requestedGrantTypes: [], tokenEndpointAuthMethod: null, redirectUris: [], requestedScopes: [],
  };
  if (!input || typeof input !== "object" || Array.isArray(input)) return empty;
  const raw = input as Record<string, unknown>;
  const metadataFields = Object.keys(raw).slice(0, 24).map((field) => field.slice(0, 64));
  const grantValue = raw.grant_types;
  const requestedGrantTypes = Array.isArray(grantValue)
    ? grantValue.slice(0, 8).map((item) => typeof item === "string" ? item.slice(0, 80) : `<${typeof item}>`)
    : grantValue === undefined
      ? []
      : [typeof grantValue === "string" ? grantValue.slice(0, 80) : `<${typeof grantValue}>`];
  const tokenEndpointAuthMethod = raw.token_endpoint_auth_method === undefined
    ? null
    : typeof raw.token_endpoint_auth_method === "string"
      ? raw.token_endpoint_auth_method.slice(0, 80)
      : `<${typeof raw.token_endpoint_auth_method}>`;
  const redirectUris = Array.isArray(raw.redirect_uris)
    ? raw.redirect_uris.slice(0, 8).flatMap((item) => {
      if (typeof item !== "string") return [];
      try {
        const url = new URL(item);
        return [{ scheme: url.protocol.replace(/:$/, "").slice(0, 16), hostname: url.hostname.slice(0, 255) }];
      } catch { return []; }
    })
    : [];
  const requestedScopes = typeof raw.scope === "string"
    ? raw.scope.trim().split(/\s+/).filter(Boolean).slice(0, 16).map((scope) => scope.slice(0, 80))
    : [];
  return { metadataFields, requestedGrantTypes, tokenEndpointAuthMethod, redirectUris, requestedScopes };
}
function validRedirectUri(value: string): boolean {
  try {
    const url = new URL(value);
    const loopback = ["localhost", "127.0.0.1", "[::1]", "::1"].includes(url.hostname);
    return (url.protocol === "https:" || (url.protocol === "http:" && loopback)) && !url.hash && !/[\s\u0000-\u001f]/.test(value);
  } catch { return false; }
}
function validCodeVerifier(value: string): boolean { return /^[A-Za-z0-9._~-]{43,128}$/.test(value); }
function escapeHtml(value: string): string {
  return value.replace(/[&<>\"']/g, (char) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", "\"": "&quot;", "'": "&#39;" }[char] ?? char));
}
