import type { EngineEnv, GrokActivationBridge, GrokActivationRequest, ExternalAgentRoleId, GrokSignalSubmission } from "@pm/core";
import { NoopGrokActivationBridge } from "./noop.js";
import { SlackGrokActivationBridge } from "./slack.js";
import { XaiGrokActivationBridge } from "./xai.js";
import type { GrokRoleContext, GrokSubmissionMode, RoleRunOutcome } from "./role-runner.js";

export interface GrokRuntimeBindings {
  contextProvider?: (request: GrokActivationRequest, role: ExternalAgentRoleId) => Promise<GrokRoleContext> | GrokRoleContext;
  submitSignal?: (submission: GrokSignalSubmission, clientId: ExternalAgentRoleId, mode?: GrokSubmissionMode) => Promise<{ status: string; signalId?: string }>;
  onRoleResult?: (request: GrokActivationRequest, outcome: RoleRunOutcome) => void | Promise<void>;
  now?: () => Date;
}

export function createGrokActivationBridge(env: EngineEnv, fetchImpl: typeof fetch = fetch, bindings?: GrokRuntimeBindings): GrokActivationBridge {
  if (env.GROK_ACTIVATION_PROVIDER === "xai") {
    if (!env.XAI_API_KEY) throw new Error("XAI_API_KEY_REQUIRED");
    if (!env.XAI_MODEL) throw new Error("XAI_MODEL_REQUIRED");
    return new XaiGrokActivationBridge({
      apiKey: env.XAI_API_KEY, baseUrl: env.XAI_API_BASE_URL, model: env.XAI_MODEL,
      timeoutMs: env.XAI_REQUEST_TIMEOUT_MS, maxOutputTokens: env.XAI_MAX_OUTPUT_TOKENS,
      maxRoleCallsPerMinute: env.XAI_MAX_ROLE_CALLS_PER_MINUTE,
      fetchImpl, contextProvider: bindings?.contextProvider, submitSignal: bindings?.submitSignal, onRoleResult: bindings?.onRoleResult, now: bindings?.now,
    });
  }
  if (env.GROK_ACTIVATION_PROVIDER !== "slack") return new NoopGrokActivationBridge();
  if (!env.SLACK_BOT_TOKEN || !env.SLACK_CHANNEL_ID) return new NoopGrokActivationBridge();
  return new SlackGrokActivationBridge({
    token: env.SLACK_BOT_TOKEN,
    channelId: env.SLACK_CHANNEL_ID,
    apiUrl: env.SLACK_API_URL,
    fetchImpl,
  });
}

export { NoopGrokActivationBridge } from "./noop.js";
export { SlackGrokActivationBridge } from "./slack.js";
export { XaiGrokActivationBridge } from "./xai.js";
export { XaiInferenceClient, XaiInferenceError } from "./xai-client.js";
export { GrokRoleRunner, GrokRoleRuntimeError, boundedContextForPrompt } from "./role-runner.js";
export type { GrokRoleContext, RoleRuntimeMetric } from "./role-runner.js";
