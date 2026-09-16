import type { GalaxyMotionKind, GalaxySceneModel, GalaxyTransient } from "./galaxy-scene";
import { MAX_GALAXY_POSITION_SATELLITES } from "./galaxy-scene";
import { ASTRA_AUTHORITY } from "./dashboard-selectors";

export const GALAXY_VIEW = {
  width: 1200,
  height: 760,
  cx: 600,
  cy: 372,
  focal: 1580,
} as const;

export const MAX_MARKET_TICKS = 18;
export const MAX_VISIBLE_REFERENCES = 10;
export const MAX_VISIBLE_POSITIONS = MAX_GALAXY_POSITION_SATELLITES;

export type GalaxyOrbitId =
  | "halo"
  | "trigger"
  | "risk"
  | "execution"
  | "market"
  | "portfolio"
  | "reference";

export interface GalaxyOrbitDef {
  id: GalaxyOrbitId;
  radius: number;
  aspect: number;
  rotateX: number;
  rotateZ: number;
  speed: number;
  phase: number;
  stroke: string;
}

export interface GalaxyProjectedPoint {
  x: number;
  y: number;
  z: number;
  scale: number;
}

export type GalaxyVisualKind =
  | "core"
  | "trigger"
  | "risk"
  | "execution"
  | "market"
  | "portfolio"
  | "reference"
  | "position"
  | "density";

export interface GalaxyVisualSlot {
  id: string;
  kind: GalaxyVisualKind;
  orbitId: GalaxyOrbitId | "core";
  angle: number;
  label: string;
  detail: string;
  tone: string;
  size: "core" | "primary" | "secondary" | "tick";
  available?: boolean;
  marketId?: string | null;
}

export const GALAXY_ORBITS: readonly GalaxyOrbitDef[] = [
  { id: "halo", radius: 148, aspect: 0.90, rotateX: 1.08, rotateZ: -0.16, speed: 0.52, phase: 0.4, stroke: "rgba(255,188,96,.42)" },
  { id: "trigger", radius: 228, aspect: 0.76, rotateX: 0.72, rotateZ: -0.48, speed: 0.34, phase: 1.7, stroke: "rgba(236,176,86,.36)" },
  { id: "risk", radius: 298, aspect: 0.98, rotateX: 0.98, rotateZ: 0.14, speed: 0.26, phase: 3.1, stroke: "rgba(220,164,82,.32)" },
  { id: "execution", radius: 368, aspect: 0.74, rotateX: 0.58, rotateZ: -0.36, speed: 0.20, phase: 5.0, stroke: "rgba(208,152,78,.30)" },
  { id: "market", radius: 438, aspect: 0.86, rotateX: 0.88, rotateZ: 0.24, speed: 0.15, phase: 2.2, stroke: "rgba(196,144,76,.26)" },
  { id: "portfolio", radius: 500, aspect: 0.72, rotateX: 0.66, rotateZ: -0.20, speed: 0.11, phase: 4.4, stroke: "rgba(184,136,74,.22)" },
  { id: "reference", radius: 540, aspect: 0.90, rotateX: 0.98, rotateZ: 0.08, speed: 0.08, phase: 0.9, stroke: "rgba(172,128,78,.20)" },
];

const PARKED_ANGLE: Record<Exclude<GalaxyOrbitId, "halo" | "reference">, number> = {
  trigger: 3.35,
  risk: 0.18,
  execution: 4.95,
  market: 2.35,
  portfolio: 5.55,
};

export function galaxyOrbit(id: GalaxyOrbitId): GalaxyOrbitDef {
  return GALAXY_ORBITS.find((orbit) => orbit.id === id)!;
}

export function projectOrbitPoint(orbit: GalaxyOrbitDef, theta: number, parallax = 0): GalaxyProjectedPoint {
  const rotateZ = orbit.rotateZ + parallax;
  const x0 = Math.cos(theta) * orbit.radius;
  const y0 = Math.sin(theta) * orbit.radius * orbit.aspect;
  const x1 = x0 * Math.cos(rotateZ) - y0 * Math.sin(rotateZ);
  const y1 = x0 * Math.sin(rotateZ) + y0 * Math.cos(rotateZ);
  const y2 = y1 * Math.cos(orbit.rotateX);
  const z2 = y1 * Math.sin(orbit.rotateX);
  const persp = GALAXY_VIEW.focal / (GALAXY_VIEW.focal - z2);
  return {
    x: GALAXY_VIEW.cx + x1 * persp,
    y: GALAXY_VIEW.cy + y2 * persp,
    z: z2,
    scale: persp,
  };
}

export function orbitPolyline(orbit: GalaxyOrbitDef, samples = 72, parallax = 0): string {
  const points = Array.from({ length: samples }, (_, index) => projectOrbitPoint(orbit, (index / samples) * Math.PI * 2, parallax));
  return `${points.map((point, index) => `${index === 0 ? "M" : "L"} ${point.x.toFixed(2)} ${point.y.toFixed(2)}`).join(" ")} Z`;
}

function pathFromPoints(points: GalaxyProjectedPoint[]): string {
  if (points.length === 0) return "";
  return points.map((point, index) => `${index === 0 ? "M" : "L"} ${point.x.toFixed(2)} ${point.y.toFixed(2)}`).join(" ");
}

export function orbitDepthPaths(orbit: GalaxyOrbitDef, samples = 80, parallax = 0): { far: string[]; near: string[] } {
  const points = Array.from({ length: samples }, (_, index) => projectOrbitPoint(orbit, (index / samples) * Math.PI * 2, parallax));
  const far: string[] = [];
  const near: string[] = [];
  let current: GalaxyProjectedPoint[] = [];
  let farSide = points[0]!.z < 0;
  const flush = () => {
    const path = pathFromPoints(current);
    if (!path) return;
    (farSide ? far : near).push(path);
    current = [];
  };
  for (const [index, point] of points.entries()) {
    const nextFar = point.z < 0;
    if (index > 0 && nextFar !== farSide) {
      current.push(point);
      flush();
      farSide = nextFar;
    }
    current.push(point);
  }
  if (current.length) {
    if (farSide === (points[0]!.z < 0) && ((points[0]!.z < 0) === farSide)) current.push(points[0]!);
    flush();
  }
  return { far, near };
}

export function marketTickCount(outcomes: number): number {
  return Math.max(0, Math.min(MAX_MARKET_TICKS, outcomes));
}

export function visibleReferenceCount(feeds: number): number {
  return Math.max(0, Math.min(MAX_VISIBLE_REFERENCES, feeds));
}

export function visiblePositionCount(positions: number): number {
  return Math.max(0, Math.min(MAX_VISIBLE_POSITIONS, positions));
}

function clamp(value: number, min: number, max: number): number {
  return Math.min(max, Math.max(min, value));
}

export function labelAnchor(point: GalaxyProjectedPoint): GalaxyProjectedPoint {
  const dx = point.x - GALAXY_VIEW.cx;
  const dy = point.y - GALAXY_VIEW.cy;
  const length = Math.hypot(dx, dy) || 1;
  return {
    x: clamp(point.x + (dx / length) * 42, 48, GALAXY_VIEW.width - 48),
    y: clamp(point.y + (dy / length) * 28, 28, GALAXY_VIEW.height - 52),
    z: point.z,
    scale: point.scale,
  };
}

/** Title lines for the core caption. When kicker === title, draw a single title (no duplicate). */
export function coreCaptionLines(identity: { kicker: string; title: string; detail: string }): {
  titles: string[];
  detail: string;
} {
  if (identity.kicker === identity.title) {
    return { titles: [identity.title], detail: identity.detail };
  }
  return { titles: [identity.kicker, identity.title], detail: identity.detail };
}

export type SemanticLabelNode = {
  id: string;
  kind: GalaxyVisualKind | "tick";
  size: GalaxyVisualSlot["size"];
  x: number;
  y: number;
};

export type LaidOutSemanticLabel = {
  id: string;
  kind: GalaxyVisualKind | "tick";
  x: number;
  y: number;
  showLabel: boolean;
  showDetail: boolean;
};

function semanticLabelPriority(kind: GalaxyVisualKind | "tick", size: GalaxyVisualSlot["size"]): number {
  if (size === "tick" || kind === "density") return 0;
  if (kind === "trigger" || kind === "risk" || kind === "execution" || kind === "market" || kind === "portfolio") return 3;
  if (kind === "reference") return 2;
  if (kind === "position") return 1;
  return 0;
}

const LABEL_MIN_DIST = 54;
const LABEL_PAD_X = 48;
const LABEL_PAD_Y_TOP = 28;
const LABEL_PAD_Y_BOTTOM = 52;
const RADIAL_OFFSETS = [16, 28] as const;

function clampLabelXY(x: number, y: number): { x: number; y: number } {
  return {
    x: clamp(x, LABEL_PAD_X, GALAXY_VIEW.width - LABEL_PAD_X),
    y: clamp(y, LABEL_PAD_Y_TOP, GALAXY_VIEW.height - LABEL_PAD_Y_BOTTOM),
  };
}

function outwardUnit(x: number, y: number): { ux: number; uy: number } {
  const dx = x - GALAXY_VIEW.cx;
  const dy = y - GALAXY_VIEW.cy;
  const length = Math.hypot(dx, dy) || 1;
  return { ux: dx / length, uy: dy / length };
}

/**
 * Deterministic overlap resolution for satellite labels.
 * Primary kinds stay readable; lower-priority labels are offset, then lose detail, then hide.
 */
export function layoutSemanticLabels(nodes: readonly SemanticLabelNode[]): LaidOutSemanticLabel[] {
  type Working = LaidOutSemanticLabel & { priority: number; size: GalaxyVisualSlot["size"] };
  const working: Working[] = nodes.map((node) => {
    const priority = semanticLabelPriority(node.kind, node.size);
    const clamped = clampLabelXY(node.x, node.y);
    return {
      id: node.id,
      kind: node.kind,
      x: clamped.x,
      y: clamped.y,
      showLabel: priority > 0,
      showDetail: priority > 0,
      priority,
      size: node.size,
    };
  });

  // Stable order: higher priority first, then id for determinism.
  const order = working
    .map((_, index) => index)
    .sort((a, b) => {
      const pa = working[a]!.priority;
      const pb = working[b]!.priority;
      if (pb !== pa) return pb - pa;
      return working[a]!.id.localeCompare(working[b]!.id);
    });

  for (let oi = 0; oi < order.length; oi += 1) {
    const i = order[oi]!;
    const self = working[i]!;
    if (!self.showLabel) continue;

    for (let oj = 0; oj < oi; oj += 1) {
      const j = order[oj]!;
      const other = working[j]!;
      if (!other.showLabel) continue;

      const dist = Math.hypot(self.x - other.x, self.y - other.y);
      if (dist >= LABEL_MIN_DIST) continue;

      // Lower priority loses (self is later in priority-sorted order when priorities differ).
      // When equal priority, the later id loses.
      const selfLoses = self.priority < other.priority
        || (self.priority === other.priority && self.id.localeCompare(other.id) > 0);
      if (!selfLoses) continue;

      // Primary kinds: prefer offset / hide detail over full hide.
      const keepReadable = self.priority >= 3;
      const { ux, uy } = outwardUnit(self.x, self.y);
      let resolved = false;
      for (const offset of RADIAL_OFFSETS) {
        const next = clampLabelXY(self.x + ux * offset, self.y + uy * offset);
        const nextDist = Math.hypot(next.x - other.x, next.y - other.y);
        if (nextDist >= LABEL_MIN_DIST) {
          self.x = next.x;
          self.y = next.y;
          resolved = true;
          break;
        }
      }
      if (resolved) continue;

      // Still colliding: hide detail first.
      self.showDetail = false;
      // Re-check distance after offset attempts failed — try one more combined max offset.
      const maxed = clampLabelXY(self.x + ux * RADIAL_OFFSETS[1], self.y + uy * RADIAL_OFFSETS[1]);
      self.x = maxed.x;
      self.y = maxed.y;
      const after = Math.hypot(self.x - other.x, self.y - other.y);
      if (after >= LABEL_MIN_DIST) continue;

      if (!keepReadable) {
        self.showLabel = false;
        self.showDetail = false;
      }
    }
  }

  // Second pass: pairwise among still-visible to catch chains.
  for (let oi = 0; oi < order.length; oi += 1) {
    const i = order[oi]!;
    const self = working[i]!;
    if (!self.showLabel) continue;
    for (let oj = 0; oj < oi; oj += 1) {
      const j = order[oj]!;
      const other = working[j]!;
      if (!other.showLabel) continue;
      const dist = Math.hypot(self.x - other.x, self.y - other.y);
      if (dist >= LABEL_MIN_DIST) continue;
      const selfLoses = self.priority < other.priority
        || (self.priority === other.priority && self.id.localeCompare(other.id) > 0);
      if (!selfLoses) continue;
      if (self.showDetail) {
        self.showDetail = false;
      } else if (self.priority < 3) {
        self.showLabel = false;
      }
    }
  }

  return working.map(({ id, kind, x, y, showLabel, showDetail }) => ({
    id,
    kind,
    x,
    y,
    showLabel,
    showDetail,
  }));
}

export function eventOrbitId(kind: GalaxyMotionKind): GalaxyOrbitId | "core" {
  if (kind === "decision") return "core";
  if (kind === "trigger") return "trigger";
  if (kind === "risk-accept" || kind === "risk-veto") return "risk";
  if (kind === "order" || kind === "fill" || kind === "cancel" || kind === "execution-error") return "execution";
  if (kind === "settlement") return "portfolio";
  if (kind === "market") return "market";
  return "reference";
}

export function galaxyCoreIdentity(model: GalaxySceneModel): { kicker: string; title: string; detail: string } {
  const authority = model.core.authority;
  if (authority === ASTRA_AUTHORITY) {
    return {
      kicker: "ASTRA",
      title: "DECISION CORE",
      detail: model.core.available ? model.core.direction : "AWAITING",
    };
  }
  if (!model.core.available || authority === "—" || authority === "AUTHORITY UNAVAILABLE") {
    return {
      kicker: "DECISION CORE",
      title: "AWAITING",
      detail: model.core.available ? model.core.direction : "AWAITING",
    };
  }
  return {
    kicker: "DECISION CORE",
    title: "DECISION CORE",
    detail: model.core.direction,
  };
}

function satelliteMatchesSettlement(
  event: Pick<GalaxyTransient, "marketId" | "asset" | "positionId">,
  satellite: { id: string; marketId?: string | null; label?: string },
): boolean {
  if (event.positionId && satellite.id === event.positionId) return true;
  if (event.marketId && satellite.marketId && satellite.marketId === event.marketId) return true;
  if (event.asset && satellite.label === event.asset) return true;
  return false;
}

/** Correlate settlement animation to one matching satellite, else only the portfolio/ledger node. */
export function settlementAppliesToSlot(
  event: Pick<GalaxyTransient, "kind" | "marketId" | "asset" | "positionId">,
  slot: Pick<GalaxyVisualSlot, "kind" | "id" | "label" | "marketId">,
  satellites: Array<{ id: string; marketId?: string | null; label?: string }>,
): boolean {
  if (event.kind !== "settlement") return false;
  const matched = satellites.find((satellite) => satelliteMatchesSettlement(event, satellite));
  if (matched) {
    return slot.kind === "position" && (
      slot.id === matched.id
      || (Boolean(event.marketId) && slot.marketId === event.marketId)
      || (Boolean(event.positionId) && slot.id === event.positionId)
    );
  }
  return slot.kind === "portfolio";
}

export function buildGalaxyVisualSlots(model: GalaxySceneModel): GalaxyVisualSlot[] {
  const identity = galaxyCoreIdentity(model);
  const slots: GalaxyVisualSlot[] = [
    {
      id: "core",
      kind: "core",
      orbitId: "core",
      angle: 0,
      label: identity.kicker,
      detail: identity.detail,
      tone: model.core.authorityTone,
      size: "core",
      available: model.core.available,
    },
    {
      id: "trigger",
      kind: "trigger",
      orbitId: "trigger",
      angle: PARKED_ANGLE.trigger,
      label: "TRIGGER",
      detail: model.trigger.status,
      tone: model.trigger.tone,
      size: "primary",
    },
    {
      id: "risk",
      kind: "risk",
      orbitId: "risk",
      angle: PARKED_ANGLE.risk,
      label: "RISK",
      detail: model.risk.status,
      tone: model.risk.tone,
      size: "primary",
    },
    {
      id: "execution",
      kind: "execution",
      orbitId: "execution",
      angle: PARKED_ANGLE.execution,
      label: "EXECUTION",
      detail: model.execution.status,
      tone: model.execution.tone,
      size: "primary",
    },
    {
      id: "market",
      kind: "market",
      orbitId: "market",
      angle: PARKED_ANGLE.market,
      label: "MARKET",
      detail: `${model.market.outcomes} · ${model.market.qualified} Q`,
      tone: model.market.tone,
      size: "primary",
    },
    {
      id: "portfolio",
      kind: "portfolio",
      orbitId: "portfolio",
      angle: PARKED_ANGLE.portfolio,
      label: "LEDGER",
      detail: model.portfolio.status,
      tone: model.portfolio.tone,
      size: "primary",
    },
  ];

  const ticks = marketTickCount(model.market.outcomes);
  for (let index = 0; index < ticks; index += 1) {
    slots.push({
      id: `density:${index}`,
      kind: "density",
      orbitId: "market",
      angle: (index / ticks) * Math.PI * 2 + 0.18,
      label: "",
      detail: "",
      tone: index < model.market.qualified ? "healthy" : "neutral",
      size: "tick",
    });
  }

  const references = model.reference.feeds.slice(0, visibleReferenceCount(model.reference.feeds.length));
  for (const [index, feed] of references.entries()) {
    slots.push({
      id: `reference:${feed.asset}`,
      kind: "reference",
      orbitId: "reference",
      angle: (index / Math.max(references.length, 1)) * Math.PI * 2 - 0.55,
      label: feed.asset,
      detail: feed.state,
      tone: feed.tone,
      size: "secondary",
    });
  }

  const satellites = model.portfolio.satellites.slice(0, visiblePositionCount(model.portfolio.satellites.length));
  for (const [index, satellite] of satellites.entries()) {
    slots.push({
      id: satellite.id,
      kind: "position",
      orbitId: "portfolio",
      angle: PARKED_ANGLE.portfolio + 0.55 + index * 0.42,
      label: satellite.label,
      detail: "OPEN",
      tone: model.portfolio.tone,
      size: "secondary",
      marketId: satellite.marketId,
    });
  }

  return slots;
}