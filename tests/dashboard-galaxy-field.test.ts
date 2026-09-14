import { describe, expect, it } from "vitest";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import {
  FIELD_FILAMENT_COUNT,
  FIELD_MAX_PACKETS,
  FIELD_PRIMARY_COUNT,
  FIELD_SECONDARY_COUNT,
  FIELD_SAMPLES,
  FIELD_TRAIL_POINTS,
  FIELD_VIEW,
  backgroundVisibility,
  createPacketState,
  deviceBackingStore,
  edgeOnAmount,
  fieldCamera,
  fieldFallbackPaths,
  fieldFamilyCounts,
  fieldGeometrySignature,
  filamentFamily,
  filamentHasFarAndNearSegments,
  filamentHideHash,
  filamentTier,
  paintGalaxyField,
  sampleFilamentPoint,
  stepPackets,
} from "../apps/dashboard/lib/galaxy-field.js";

describe("Galaxy deforming filament field", () => {
  it("changes camera and projected geometry across 12 seconds (families still deform)", () => {
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

  it("organizes filaments into orbital families with presentation hierarchy", () => {
    const counts = fieldFamilyCounts();
    expect(counts.A + counts.B + counts.C + counts.D).toBe(FIELD_FILAMENT_COUNT);
    expect(counts.A).toBeGreaterThan(0);
    expect(counts.B).toBeGreaterThan(0);
    expect(counts.C).toBeGreaterThan(0);
    expect(counts.D).toBeGreaterThan(0);
    expect(FIELD_FILAMENT_COUNT).toBe(48);
    expect(FIELD_PRIMARY_COUNT).toBe(8);
    expect(FIELD_SECONDARY_COUNT).toBe(20);

    let primary = 0;
    let secondary = 0;
    let background = 0;
    for (let i = 0; i < FIELD_FILAMENT_COUNT; i += 1) {
      const tier = filamentTier(i);
      if (tier === "primary") primary += 1;
      else if (tier === "secondary") secondary += 1;
      else background += 1;
      expect(["A", "B", "C", "D"]).toContain(filamentFamily(i));
    }
    expect(primary).toBe(FIELD_PRIMARY_COUNT);
    expect(secondary).toBe(FIELD_SECONDARY_COUNT);
    expect(background).toBe(FIELD_FILAMENT_COUNT - FIELD_PRIMARY_COUNT - FIELD_SECONDARY_COUNT);
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
    expect(FIELD_SAMPLES).toBeGreaterThan(40);
  });

  it("maps logical view to device backing store consistently across DPR values", () => {
    for (const dpr of [1, 1.25, 1.5, 2]) {
      const backing = deviceBackingStore(FIELD_VIEW.width, FIELD_VIEW.height, dpr);
      expect(backing.dpr).toBe(Math.min(dpr, 2));
      expect(backing.width).toBe(Math.round(FIELD_VIEW.width * backing.dpr));
      expect(backing.height).toBe(Math.round(FIELD_VIEW.height * backing.dpr));
      expect(backing.width / backing.dpr).toBeCloseTo(FIELD_VIEW.width, 5);
      expect(backing.height / backing.dpr).toBeCloseTo(FIELD_VIEW.height, 5);
    }
    const capped = deviceBackingStore(FIELD_VIEW.width, FIELD_VIEW.height, 3);
    expect(capped.dpr).toBe(2);
  });

  it("classifies segments far and near on a single filament (not meanZ)", () => {
    let found = false;
    for (const t of [0, 3, 6, 9, 12]) {
      for (let i = 0; i < FIELD_FILAMENT_COUNT; i += 1) {
        if (filamentHasFarAndNearSegments(i, t)) {
          found = true;
          break;
        }
      }
      if (found) break;
    }
    expect(found).toBe(true);
  });

  it("exports organized Canvas-free fallback paths at t=0", () => {
    const paths = fieldFallbackPaths(0);
    expect(paths.length).toBeGreaterThan(20);
    const families = new Set(paths.map((p) => p.family));
    expect(families.has("A")).toBe(true);
    expect(families.has("B")).toBe(true);
    expect(families.has("C")).toBe(true);
    expect(families.has("D")).toBe(true);
    const tiers = new Set(paths.map((p) => p.tier));
    expect(tiers.has("primary")).toBe(true);
    expect(tiers.has("secondary")).toBe(true);
    for (const path of paths) {
      expect(path.points.length).toBeGreaterThan(8);
    }
  });

  it("deterministically reduces background density near edge-on", () => {
    const face = edgeOnAmount(fieldCamera(0));
    // Find a t where camera is more edge-on.
    let edgeT = 0;
    let edgeAmt = face;
    for (let t = 0; t <= 12; t += 0.5) {
      const amt = edgeOnAmount(fieldCamera(t));
      if (amt > edgeAmt) {
        edgeAmt = amt;
        edgeT = t;
      }
    }
    expect(edgeAmt).toBeGreaterThan(face);

    const hiddenA: number[] = [];
    const hiddenB: number[] = [];
    for (let i = 0; i < FIELD_FILAMENT_COUNT; i += 1) {
      if (filamentTier(i) !== "background") continue;
      const a = backgroundVisibility(i, edgeAmt);
      const b = backgroundVisibility(i, edgeAmt);
      expect(a).toBe(b);
      if (a < 0.05) hiddenA.push(i);
      // Same t + same rx => same hidden set (recompute via camera at edgeT).
      const again = backgroundVisibility(i, edgeOnAmount(fieldCamera(edgeT)));
      if (again < 0.05) hiddenB.push(i);
    }
    expect(hiddenA).toEqual(hiddenB);
    expect(hiddenA.length).toBeGreaterThan(0);
    // Primaries never culled by backgroundVisibility.
    for (let i = 0; i < FIELD_FILAMENT_COUNT; i += 1) {
      if (filamentTier(i) === "primary") {
        expect(backgroundVisibility(i, 1)).toBe(1);
      }
    }
    expect(filamentHideHash(3)).toBe(filamentHideHash(3));
  });

  it("paints reduced-motion path without requiring packets (frozen geometry)", () => {
    const calls: string[] = [];
    const ctx = {
      save() { calls.push("save"); },
      restore() { calls.push("restore"); },
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
      globalAlpha: 1,
      globalCompositeOperation: "source-over",
    } as unknown as CanvasRenderingContext2D;

    expect(() => paintGalaxyField(ctx, 12, { reducedMotion: true })).not.toThrow();
    expect(calls).toContain("fillRect");
    // paint must not call setTransform — DPR mapping lives in canvas resize only.
    expect(calls).not.toContain("setTransform");
  });
});

describe("GalaxyFieldCanvas RAF contract", () => {
  it("cancels requestAnimationFrame on effect cleanup", () => {
    const src = readFileSync(resolve("apps/dashboard/app/components/galaxy-field-canvas.tsx"), "utf8");
    expect(src).toMatch(/cancelAnimationFrame/);
    expect(src).toMatch(/requestAnimationFrame\(tick\)/);
    expect(src).toMatch(/return \(\) => \{/);
    expect(src).toMatch(/setTransform\(backing\.dpr/);
    expect(src).toMatch(/\/\/ setTransform ONCE on resize/);
    const fieldSrc = readFileSync(resolve("apps/dashboard/lib/galaxy-field.ts"), "utf8");
    expect(fieldSrc).not.toMatch(/\.setTransform\s*\(/);
  });
});

