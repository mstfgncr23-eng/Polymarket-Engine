import { describe, expect, it } from "vitest";
import { d, projectGuardianBookSignal, type MarketSnapshot } from "@pm/core";
import { snapshot } from "./fixtures.js";

// Independent arithmetic oracle for the original engine top-of-book projection.
// No engine, process, timers, DB, WebSocket, orders or shadow decisions are created.
function oracle(up: MarketSnapshot, down: MarketSnapshot, held: "UP" | "DOWN") {
  const calc = (book: MarketSnapshot) => {
    const bp = d(book.bids[0]!.price), ap = d(book.asks[0]!.price);
    const bq = d(book.bids[0]!.quantity), aq = d(book.asks[0]!.quantity);
    return { mid: bp.plus(ap).div(2), imbalance: bq.minus(aq).div(bq.plus(aq)) };
  };
  const u = calc(up), v = calc(down);
  return {
    eventId: `${up.eventId}:${down.eventId}`,
    heldPrice: (held === "UP" ? u : v).mid.toFixed(),
    bookPressure: u.imbalance.minus(v.imbalance).div(2).times(held === "UP" ? -1 : 1).toFixed(),
  };
}

const pair = (upBid = "0.40", upAsk = "0.60", downBid = "0.35", downAsk = "0.45") => {
  const up = snapshot({ marketId: "guard-book", tokenId: "token-up", outcome: "UP",
    eventId: "source-up", bids: [{ price: upBid, quantity: "100" }],
    asks: [{ price: upAsk, quantity: "300" }] });
  const down = snapshot({ marketId: "guard-book", tokenId: "token-down", outcome: "DOWN",
    eventId: "source-down", bids: [{ price: downBid, quantity: "300" }],
    asks: [{ price: downAsk, quantity: "100" }] });
  return { up, down };
};

describe("G2B pure Guardian book-signal extraction", () => {
  it("G2B-01 preserves UP-positive pressure and exact ordered evidence IDs", () => {
    const { up, down } = pair();
    expect(projectGuardianBookSignal(up, down, "UP")).toEqual({
      eventId: "source-up:source-down", heldPrice: "0.5", bookPressure: "0.5",
    });
    expect(projectGuardianBookSignal(up, down, "DOWN")).toEqual({
      eventId: "source-up:source-down", heldPrice: "0.4", bookPressure: "-0.5",
    });
  });

  it("G2B-02 agrees with the original engine arithmetic on varied valid books", () => {
    for (const [upBid, upAsk, downBid, downAsk] of [
      ["0.10", "0.20", "0.70", "0.80"],
      ["0.01", "0.02", "0.98", "0.99"],
      ["0.53", "0.53", "0.47", "0.47"],
      ["0.66", "0.75", "0.20", "0.31"],
      ["0.00001", "0.1", "0.5", "0.95"],
    ]) {
      const { up, down } = pair(upBid, upAsk, downBid, downAsk);
      for (const held of ["UP", "DOWN"] as const) {
        expect(projectGuardianBookSignal(up, down, held)).toEqual(oracle(up, down, held));
      }
    }
  });

  it("G2B-03 returns null when either book lacks an executable top level", () => {
    const { up, down } = pair();
    expect(projectGuardianBookSignal({ ...up, bids: [] }, down, "UP")).toBeNull();
    expect(projectGuardianBookSignal(up, { ...down, asks: [] }, "DOWN")).toBeNull();
  });

  it("G2B-04 rejects crossed, negative or zero-denominator book metrics", () => {
    const { up, down } = pair();
    expect(projectGuardianBookSignal({ ...up, asks: [{ price: "0.39", quantity: "100" }] }, down, "UP")).toBeNull();
    expect(projectGuardianBookSignal({ ...up, bids: [{ price: "-0.1", quantity: "100" }] }, down, "UP")).toBeNull();
    expect(projectGuardianBookSignal(up, { ...down, bids: [{ price: "0.35", quantity: "0" }],
      asks: [{ price: "0.45", quantity: "0" }] }, "DOWN")).toBeNull();
  });

  it("G2B-05 does not mutate snapshots, and is stable on repeated evaluations", () => {
    const { up, down } = pair();
    const before = JSON.stringify({ up, down });
    const first = projectGuardianBookSignal(up, down, "UP");
    for (let i = 0; i < 12; i += 1) expect(projectGuardianBookSignal(up, down, "UP")).toEqual(first);
    expect(JSON.stringify({ up, down })).toBe(before);
  });
});
