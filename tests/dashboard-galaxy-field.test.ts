import { describe, expect, it } from "vitest";
import {
  FIELD_FILAMENT_COUNT,
  FIELD_MAX_PACKETS,
  FIELD_TRAIL_POINTS,
  createPacketState,
  fieldGeometrySignature,
  paintGalaxyField,
  sampleFilamentPoint,
  stepPackets,
} from "../apps/dashboard/lib/galaxy-field.js";

describe("Galaxy deforming filament field", () => {
  it("changes camera and projected geometry across 12 seconds (not just dots on rails)", () => {
    const times = [0, 3, 6, 9, 12] as const;
    const sigs = times.map((t) => fieldGeometrySignature(t));

    const pairs: Array<[number, number]> = [
      [0, 1], [0, 2], [0, 3], [0, 4],
      [1, 2], [1, 3], [1, 4],
      [2, 3], [2, 4],
      [3, 4],
    ];
    let farPairs = 0;
    for (const [a, b] of pairs) {
      const left = sigs[a]!;
      const right = sigs[b]!;
      const dist = Math.hypot(left.rx - right.rx, left.aspect - right.aspect);
      if (dist > 0.08) farPairs += 1;
    }
    expect(farPairs).toBeGreaterThanOrEqual(3);

    const widths = new Set(sigs.map((s) => s.width.toFixed(1)));
    const heights = new Set(sigs.map((s) => s.height.toFixed(1)));
    expect(widths.size + heights.size).toBeGreaterThan(2);
    expect(sigs[0]!.rx).not.toBeCloseTo(sigs[2]!.rx, 2);
  });

  it("keeps packet ring buffers bounded while stepping for 10 seconds", () => {
    const state = createPacketState();
    expect(state.packets.length).toBe(FIELD_MAX_PACKETS);
    expect(state.count).toBeLessThanOrEqual(FIELD_MAX_PACKETS);

    let t = 0;
    const dt = 1 / 30;
    for (let step = 0; step < 300; step += 1) {
      t += dt;
      stepPackets(state, t, dt, [
        { kind: "trigger", ageSec: 0.2 },
        { kind: "fill", ageSec: 0.1 },
      ]);
      expect(state.count).toBeLessThanOrEqual(FIELD_MAX_PACKETS);
      expect(state.packets.length).toBe(FIELD_MAX_PACKETS);
      for (const packet of state.packets) {
        expect(packet.trail.length).toBe(FIELD_TRAIL_POINTS);
        expect(packet.trailLen).toBeLessThanOrEqual(FIELD_TRAIL_POINTS);
      }
    }
  });

  it("deforms filament 3D coordinates at the same theta across time", () => {
    const theta = 1.1;
    const a = sampleFilamentPoint(7, theta, 0);
    const b = sampleFilamentPoint(7, theta, 4);
    const c = sampleFilamentPoint(7, theta, 9);
    const d1 = Math.hypot(a.x - b.x, a.y - b.y, a.z - b.z);
    const d2 = Math.hypot(b.x - c.x, b.y - c.y, b.z - c.z);
    expect(d1).toBeGreaterThan(2);
    expect(d2).toBeGreaterThan(2);
    expect(FIELD_FILAMENT_COUNT).toBe(64);
  });

  it("paints reduced-motion path without requiring packets", () => {
    const canvas = {
      width: 1200,
      height: 760,
      getContext() {
        return null;
      },
    } as unknown as HTMLCanvasElement;

    // Minimal stub canvas context — enough for paintGalaxyField control flow.
    const calls: string[] = [];
    const ctx = {
      canvas,
      save() { calls.push("save"); },
      restore() { calls.push("restore"); },
      setTransform() { calls.push("setTransform"); },
      fillRect() { calls.push("fillRect"); },
      beginPath() {},
      moveTo() {},
      lineTo() {},
      closePath() {},
      stroke() {},
      fill() {},
      arc() {},
      createRadialGradient() {
        return { addColorStop() {} };
      },
      fillStyle: "",
      strokeStyle: "",
      lineWidth: 1,
      lineJoin: "round",
      lineCap: "round",
      shadowBlur: 0,
      shadowColor: "",
      globalCompositeOperation: "source-over",
    } as unknown as CanvasRenderingContext2D;

    expect(() => paintGalaxyField(ctx, 12, { reducedMotion: true })).not.toThrow();
    expect(calls).toContain("fillRect");
  });
});
