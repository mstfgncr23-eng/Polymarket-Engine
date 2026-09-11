import { describe, expect, it } from "vitest";
import { mcpWriteInputShapeForClient } from "../apps/engine/src/mcp.js";

const validMicroOutput = {
  confidence: 0.8,
  evidenceIds: ["event_contract_test"],
  expiresAt: "2026-09-11T15:37:24.293Z",
  reasonCodes: ["CONTRACT_TEST"],
  direction: "UP",
  spread: "0.01",
  imbalance: 0.4,
  depthChange: 0.1,
  pairCost: "0.93",
  unusualConditions: [],
};

describe("Micro MCP published write contract", () => {
  const shape = mcpWriteInputShapeForClient(
    "market_microstructure_scanner",
    "submit_signal",
  );

  it("publishes the real market microstructure output schema", () => {
    expect(shape.output.safeParse(validMicroOutput).success).toBe(true);
  });

  it("rejects an empty output instead of forcing schema discovery by trial and error", () => {
    expect(shape.output.safeParse({}).success).toBe(false);
  });

  it("rejects guessed extra output fields because the real schema is strict", () => {
    expect(shape.output.safeParse({
      ...validMicroOutput,
      guessedField: "nope",
    }).success).toBe(false);
  });

  it("pins submit_signal identity to market_microstructure_scanner", () => {
    expect(
      shape.agentId.safeParse("market_microstructure_scanner").success,
    ).toBe(true);

    expect(
      shape.agentId.safeParse("crypto_market_scanner").success,
    ).toBe(false);

    expect(
      shape.role.safeParse("market_microstructure_scanner").success,
    ).toBe(true);
  });
});
