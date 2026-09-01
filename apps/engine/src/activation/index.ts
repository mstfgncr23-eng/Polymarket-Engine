import type { EngineEnv, GrokActivationBridge } from "@pm/core";
import { NoopGrokActivationBridge } from "./noop.js";
import { SlackGrokActivationBridge } from "./slack.js";

export function createGrokActivationBridge(env: EngineEnv, fetchImpl: typeof fetch = fetch): GrokActivationBridge {
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
