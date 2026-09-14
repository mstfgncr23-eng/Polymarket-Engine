export const FIELD_FILAMENT_COUNT = 48;
export const FIELD_PRIMARY_COUNT = 8;
export const FIELD_SECONDARY_COUNT = 20;
export const FIELD_BACKGROUND_COUNT = 20;
export const FIELD_SAMPLES = 56;
export const FIELD_MAX_PACKETS = 12;
export const FIELD_TRAIL_POINTS = 24;
export const FIELD_KEEP_OUT = 78;
export const FIELD_VIEW = { width: 1200, height: 760, cx: 600, cy: 372, focal: 1500 } as const;

export type FieldVec3 = { x: number; y: number; z: number };
export type FieldProjected = { x: number; y: number; z: number; scale: number };
export type FieldCamera = { rx: number; ry: number; rz: number };
export type FilamentTier = "primary" | "secondary" | "background";
export type FilamentFamily = "A" | "B" | "C" | "D";

export type FieldEventKind =
  | "decision"
  | "risk-veto"
  | "risk-accept"
  | "trigger"
  | "order"
  | "fill"
  | "cancel"
  | "execution-error"
  | "settlement"
  | "reference"
  | "market";

export type FieldPaintEvent = {
  kind: FieldEventKind;
  ageSec: number;
  severity?: number;
};

export type FieldPaintOpts = {
  reducedMotion?: boolean;
  events?: readonly FieldPaintEvent[];
  packets?: PacketState;
};

export type PacketTrailSample = { x: number; y: number; z: number; scale: number };
export type FieldPacket = {
  filament: number;
  theta: number;
  speed: number;
  trail: PacketTrailSample[];
  trailLen: number;
  trailWrite: number;
  alive: boolean;
  boost: number;
};

export type PacketState = {
  packets: FieldPacket[];
  count: number;
  spawnCursor: number;
};

export type FallbackPath = {
  family: FilamentFamily;
  tier: FilamentTier;
  points: Array<{ x: number; y: number }>;
  opacity: number;
  width: number;
};

type FilamentSeed = {
  family: FilamentFamily;
  tier: FilamentTier;
  a0: number;
  e0: number;
  di: number;
  phase: number;
  omega: number;
  Aw: number;
  Av: number;
  hueBias: number;
};

function clamp(value: number, min: number, max: number): number {
  return Math.min(max, Math.max(min, value));
}

function hash01(i: number, salt: number): number {
  const x = Math.sin(i * 127.1 + salt * 311.7) * 43758.5453123;
  return x - Math.floor(x);
}

/** Stable 0..1 hash used for edge-on background culling. */
export function filamentHideHash(i: number): number {
  return hash01(i, 91);
}

function buildSeeds(): FilamentSeed[] {
  // Family quotas ~ primary disk / inner tilt / outer shell / cross-plane accent.
  const plan: Array<{ family: FilamentFamily; count: number; baseInc: number; aMin: number; aMax: number; warp: number; vert: number }> = [
    { family: "A", count: 18, baseInc: 0.02, aMin: 150, aMax: 420, warp: 18, vert: 12 },
    { family: "B", count: 10, baseInc: 0.22, aMin: 110, aMax: 260, warp: 14, vert: 20 },
    { family: "C", count: 14, baseInc: -0.08, aMin: 320, aMax: 520, warp: 36, vert: 22 },
    { family: "D", count: 6, baseInc: 0.85, aMin: 180, aMax: 360, warp: 10, vert: 48 },
  ];

  const seeds: FilamentSeed[] = [];
  let index = 0;
  for (const family of plan) {
    for (let k = 0; k < family.count; k += 1) {
      const u = (k + 0.37) / family.count;
      const a0 = family.aMin + (family.aMax - family.aMin) * Math.pow(u, 0.72);
      // Presentation hierarchy: first 8 overall primary, next 20 secondary, rest background.
      // Prefer primaries from family A then B.
      let tier: FilamentTier = "background";
      if (index < FIELD_PRIMARY_COUNT) tier = "primary";
      else if (index < FIELD_PRIMARY_COUNT + FIELD_SECONDARY_COUNT) tier = "secondary";
      seeds.push({
        family: family.family,
        tier,
        a0,
        e0: 0.04 + 0.18 * hash01(index, 2) + (family.family === "C" ? 0.08 : 0),
        di: family.baseInc + (hash01(index, 3) - 0.5) * (family.family === "D" ? 0.25 : 0.12),
        phase: hash01(index, 4) * Math.PI * 2 + k * 0.31,
        omega: hash01(index, 5) * Math.PI * 2,
        Aw: family.warp * (0.7 + 0.5 * hash01(index, 6)),
        Av: family.vert * (0.7 + 0.5 * hash01(index, 7)),
        hueBias: 0.15 + 0.55 * hash01(index, 8),
      });
      index += 1;
    }
  }
  return seeds;
}

const SEEDS: FilamentSeed[] = buildSeeds();

export function filamentTier(i: number): FilamentTier {
  return SEEDS[i % FIELD_FILAMENT_COUNT]!.tier;
}

export function filamentFamily(i: number): FilamentFamily {
  return SEEDS[i % FIELD_FILAMENT_COUNT]!.family;
}

export function fieldCamera(t: number): FieldCamera {
  return {
    rx: 0.55 + 0.85 * Math.sin(t / 11),
    ry: 0.22 * Math.sin(t / 17),
    rz: 0.16 * Math.sin(t / 13) + 0.06 * Math.sin(t / 7),
  };
}

/** 0 = face-on / open disk, 1 = strongly edge-on (compressed projected height). */
export function edgeOnAmount(camera: FieldCamera): number {
  // rx near 0 => looking along normal-ish; larger |sin(rx)| compresses disk height.
  const compress = Math.abs(Math.sin(camera.rx));
  return clamp((compress - 0.35) / 0.55, 0, 1);
}

/**
 * Deterministic background cull set for a given edge-on amount.
 * Same (t, rx) => same hidden set. Smooth fade via returned visibility 0..1.
 */
export function backgroundVisibility(i: number, edgeOn: number): number {
  const seed = SEEDS[i % FIELD_FILAMENT_COUNT]!;
  if (seed.tier !== "background") return 1;
  const h = filamentHideHash(i);
  // Hide up to ~55% of background as we approach edge-on; fade band around threshold.
  const threshold = 1 - edgeOn * 0.55;
  if (h < threshold - 0.08) return 1;
  if (h > threshold + 0.08) return 0;
  const fade = (threshold + 0.08 - h) / 0.16;
  return clamp(fade, 0, 1);
}

export function deviceBackingStore(
  logicalWidth: number,
  logicalHeight: number,
  dpr: number,
): { width: number; height: number; dpr: number } {
  const capped = Math.min(Math.max(dpr, 0.5), 2);
  return {
    width: Math.round(logicalWidth * capped),
    height: Math.round(logicalHeight * capped),
    dpr: capped,
  };
}

function rotateZ(x: number, y: number, z: number, angle: number): FieldVec3 {
  const c = Math.cos(angle);
  const s = Math.sin(angle);
  return { x: x * c - y * s, y: x * s + y * c, z };
}

function rotateX(x: number, y: number, z: number, angle: number): FieldVec3 {
  const c = Math.cos(angle);
  const s = Math.sin(angle);
  return { x, y: y * c - z * s, z: y * s + z * c };
}

function rotateY(x: number, y: number, z: number, angle: number): FieldVec3 {
  const c = Math.cos(angle);
  const s = Math.sin(angle);
  return { x: x * c + z * s, y, z: -x * s + z * c };
}

export function sampleFilamentPoint(i: number, theta: number, t: number): FieldVec3 {
  const s = SEEDS[i % FIELD_FILAMENT_COUNT]!;
  // Slow breathing deformation — avoid high-frequency jitter.
  const a = s.a0 * (1 + 0.08 * Math.sin(t / 10.5 + s.phase));
  const e = clamp(s.e0 + 0.1 * Math.sin(t / 16 + s.phase * 1.3), 0.02, 0.42);
  const b = a * Math.sqrt(1 - e * e);
  const inc = clamp(s.di + 0.06 * Math.sin(t / 12 + s.phase), s.di - 0.18, s.di + 0.18);
  const prec = s.omega + 0.045 * t + 0.1 * Math.sin(t / 22 + s.phase);
  const warp = s.Aw * Math.sin(2 * theta + t / 8.2 + s.phase);
  const vert = s.Av * Math.sin(theta + t / 9.5 + s.phase * 0.55);
  let local: FieldVec3 = {
    x: a * Math.cos(theta + s.phase),
    y: b * Math.sin(theta + s.phase),
    z: vert + warp,
  };
  local = rotateZ(local.x, local.y, local.z, prec);
  local = rotateX(local.x, local.y, local.z, inc);

  // Soft keep-out: rarely cross the core; push radial xy outward if inside.
  const radial = Math.hypot(local.x, local.y);
  if (radial < FIELD_KEEP_OUT && radial > 1e-3) {
    const push = FIELD_KEEP_OUT / radial;
    local = { x: local.x * push, y: local.y * push, z: local.z * 0.55 };
  }
  return local;
}

export function projectFieldPoint(p: FieldVec3, camera: FieldCamera): FieldProjected {
  let r = rotateZ(p.x, p.y, p.z, camera.rz);
  r = rotateY(r.x, r.y, r.z, camera.ry);
  r = rotateX(r.x, r.y, r.z, camera.rx);
  const denom = FIELD_VIEW.focal - r.z;
  const persp = FIELD_VIEW.focal / (Math.abs(denom) < 1e-3 ? 1e-3 : denom);
  return {
    x: FIELD_VIEW.cx + r.x * persp,
    y: FIELD_VIEW.cy + r.y * persp,
    z: r.z,
    scale: persp,
  };
}

/** Place a semantic marker in the deforming volume without defining field geometry. */
export function projectMarkerInField(radius: number, angle: number, t: number): FieldProjected {
  const camera = fieldCamera(t);
  const a = radius * (1 + 0.03 * Math.sin(t / 10.5 + angle));
  const e = 0.14 + 0.06 * Math.sin(t / 16 + angle);
  const b = a * Math.sqrt(1 - e * e);
  const local: FieldVec3 = {
    x: a * Math.cos(angle),
    y: b * Math.sin(angle),
    z: 10 * Math.sin(angle + t / 9.5),
  };
  return projectFieldPoint(local, camera);
}

export function fieldGeometrySignature(t: number): {
  rx: number;
  aspect: number;
  width: number;
  height: number;
  meanZ: number;
} {
  const camera = fieldCamera(t);
  let minX = Infinity;
  let maxX = -Infinity;
  let minY = Infinity;
  let maxY = -Infinity;
  let sumZ = 0;
  let n = 0;
  for (let i = 0; i < FIELD_FILAMENT_COUNT; i += 3) {
    for (let s = 0; s < FIELD_SAMPLES; s += 3) {
      const theta = (s / FIELD_SAMPLES) * Math.PI * 2;
      const p = projectFieldPoint(sampleFilamentPoint(i, theta, t), camera);
      minX = Math.min(minX, p.x);
      maxX = Math.max(maxX, p.x);
      minY = Math.min(minY, p.y);
      maxY = Math.max(maxY, p.y);
      sumZ += p.z;
      n += 1;
    }
  }
  const width = Math.max(1, maxX - minX);
  const height = Math.max(1, maxY - minY);
  return {
    rx: camera.rx,
    aspect: width / height,
    width,
    height,
    meanZ: sumZ / Math.max(1, n),
  };
}

// Frame-local projected buffer: [filament][sample] -> x,y,z,scale (no grow).
const SAMPLES_INCL = FIELD_SAMPLES + 1;
const STRIDE = 4;
const PROJECTED = new Float32Array(FIELD_FILAMENT_COUNT * SAMPLES_INCL * STRIDE);

function bufIndex(filament: number, sample: number): number {
  return (filament * SAMPLES_INCL + sample) * STRIDE;
}

function projectAllFilaments(t: number, camera: FieldCamera): void {
  for (let i = 0; i < FIELD_FILAMENT_COUNT; i += 1) {
    for (let s = 0; s <= FIELD_SAMPLES; s += 1) {
      const theta = (s / FIELD_SAMPLES) * Math.PI * 2;
      const p = projectFieldPoint(sampleFilamentPoint(i, theta, t), camera);
      const idx = bufIndex(i, s);
      PROJECTED[idx] = p.x;
      PROJECTED[idx + 1] = p.y;
      PROJECTED[idx + 2] = p.z;
      PROJECTED[idx + 3] = p.scale;
    }
  }
}

function readProjected(filament: number, sample: number): FieldProjected {
  const idx = bufIndex(filament, sample);
  return {
    x: PROJECTED[idx]!,
    y: PROJECTED[idx + 1]!,
    z: PROJECTED[idx + 2]!,
    scale: PROJECTED[idx + 3]!,
  };
}

/**
 * True when a filament at time t has at least one far (z<0) and one near (z>=0) segment.
 * Segment classification is per consecutive sample pair — not whole-filament meanZ.
 */
export function filamentHasFarAndNearSegments(i: number, t: number): boolean {
  const camera = fieldCamera(t);
  let hasFar = false;
  let hasNear = false;
  let prev = projectFieldPoint(sampleFilamentPoint(i, 0, t), camera);
  for (let s = 1; s <= FIELD_SAMPLES; s += 1) {
    const theta = (s / FIELD_SAMPLES) * Math.PI * 2;
    const curr = projectFieldPoint(sampleFilamentPoint(i, theta, t), camera);
    const midZ = (prev.z + curr.z) * 0.5;
    if (midZ < 0) hasFar = true;
    else hasNear = true;
    prev = curr;
    if (hasFar && hasNear) return true;
  }
  return false;
}

function emptyPacket(filament: number, theta: number, speed: number): FieldPacket {
  return {
    filament,
    theta,
    speed,
    trail: Array.from({ length: FIELD_TRAIL_POINTS }, () => ({ x: 0, y: 0, z: 0, scale: 1 })),
    trailLen: 0,
    trailWrite: 0,
    alive: true,
    boost: 0,
  };
}

export function createPacketState(): PacketState {
  const packets: FieldPacket[] = Array.from({ length: FIELD_MAX_PACKETS }, (_, i) =>
    emptyPacket((i * 7 + 3) % FIELD_FILAMENT_COUNT, (i / FIELD_MAX_PACKETS) * Math.PI * 2, 0.22 + (i % 5) * 0.04),
  );
  return { packets, count: Math.min(8, FIELD_MAX_PACKETS), spawnCursor: 0 };
}

function pushTrail(packet: FieldPacket, sample: PacketTrailSample): void {
  packet.trail[packet.trailWrite] = sample;
  packet.trailWrite = (packet.trailWrite + 1) % FIELD_TRAIL_POINTS;
  packet.trailLen = Math.min(FIELD_TRAIL_POINTS, packet.trailLen + 1);
}

export function stepPackets(
  state: PacketState,
  t: number,
  dt: number,
  events: readonly FieldPaintEvent[] = [],
): PacketState {
  const camera = fieldCamera(t);
  const boostKinds = new Set(events.filter((e) => e.ageSec < 2.2).map((e) => e.kind));
  const wantBoost = boostKinds.has("trigger")
    || boostKinds.has("order")
    || boostKinds.has("fill")
    || boostKinds.has("cancel")
    || boostKinds.has("execution-error")
    || boostKinds.has("settlement");

  for (let i = 0; i < state.count; i += 1) {
    const packet = state.packets[i]!;
    packet.theta = (packet.theta + packet.speed * dt) % (Math.PI * 2);
    if (wantBoost) packet.boost = Math.min(1, packet.boost + dt * 1.8);
    else packet.boost = Math.max(0, packet.boost - dt * 0.7);
    const world = sampleFilamentPoint(packet.filament, packet.theta, t);
    const projected = projectFieldPoint(world, camera);
    pushTrail(packet, {
      x: projected.x,
      y: projected.y,
      z: projected.z,
      scale: projected.scale,
    });
  }

  if (wantBoost && state.count < FIELD_MAX_PACKETS) {
    const i = state.count;
    const filament = (state.spawnCursor * 11 + 5) % FIELD_FILAMENT_COUNT;
    state.packets[i] = emptyPacket(filament, t * 0.4 + i, 0.32 + (i % 3) * 0.06);
    state.packets[i]!.boost = 1;
    state.count += 1;
    state.spawnCursor += 1;
  }

  if (state.count > FIELD_MAX_PACKETS) state.count = FIELD_MAX_PACKETS;
  return state;
}

function tierAlphaMul(tier: FilamentTier, edgeOn: number): number {
  if (tier === "primary") return 1;
  if (tier === "secondary") return 0.55 * (1 - edgeOn * 0.35);
  return 0.22 * (1 - edgeOn * 0.55);
}

function tierWidthMul(tier: FilamentTier): number {
  if (tier === "primary") return 1.35;
  if (tier === "secondary") return 0.85;
  return 0.55;
}

function keepOutDim(worldXyDist: number): number {
  if (worldXyDist >= FIELD_KEEP_OUT * 1.15) return 1;
  if (worldXyDist <= FIELD_KEEP_OUT * 0.85) return 0.12;
  return 0.12 + 0.88 * ((worldXyDist - FIELD_KEEP_OUT * 0.85) / (FIELD_KEEP_OUT * 0.3));
}

function eventMods(events: readonly FieldPaintEvent[] | undefined) {
  const active = (events ?? []).filter((e) => e.ageSec < 2.8);
  const decision = active.find((e) => e.kind === "decision");
  const veto = active.find((e) => e.kind === "risk-veto");
  const accept = active.find((e) => e.kind === "risk-accept");
  return {
    coreBloom: decision ? clamp(1 - decision.ageSec / 2.4, 0, 1) : 0,
    redShock: veto ? clamp(1 - veto.ageSec / 2.0, 0, 1) : 0,
    greenPulse: accept ? clamp(1 - accept.ageSec / 2.0, 0, 1) : 0,
  };
}

function paintCore(
  ctx: CanvasRenderingContext2D,
  mods: { coreBloom: number; redShock: number; greenPulse: number },
  bloomScale: number,
): void {
  const { cx, cy } = FIELD_VIEW;
  const bloom = (1 + mods.coreBloom * 0.45) * bloomScale;

  const halo = ctx.createRadialGradient(cx, cy, 0, cx, cy, 78 * bloom);
  halo.addColorStop(0, "rgba(255, 168, 64, 0.22)");
  halo.addColorStop(0.4, "rgba(180, 96, 28, 0.1)");
  halo.addColorStop(1, "rgba(0, 0, 0, 0)");
  ctx.fillStyle = halo;
  ctx.beginPath();
  ctx.arc(cx, cy, 78 * bloom, 0, Math.PI * 2);
  ctx.fill();

  const amber = ctx.createRadialGradient(cx, cy, 0, cx, cy, 24 * bloom);
  amber.addColorStop(0, "rgba(255, 200, 110, 0.78)");
  amber.addColorStop(0.55, "rgba(230, 130, 42, 0.4)");
  amber.addColorStop(1, "rgba(0, 0, 0, 0)");
  ctx.fillStyle = amber;
  ctx.beginPath();
  ctx.arc(cx, cy, 24 * bloom, 0, Math.PI * 2);
  ctx.fill();

  // Hottest white reserved for the core pin.
  const hot = ctx.createRadialGradient(cx, cy, 0, cx, cy, 6.5 * bloom);
  hot.addColorStop(0, "rgba(255, 250, 235, 0.95)");
  hot.addColorStop(0.5, "rgba(255, 220, 150, 0.85)");
  hot.addColorStop(1, "rgba(0, 0, 0, 0)");
  ctx.fillStyle = hot;
  ctx.beginPath();
  ctx.arc(cx, cy, 6.5 * bloom, 0, Math.PI * 2);
  ctx.fill();

  if (mods.redShock > 0) {
    ctx.strokeStyle = `rgba(212, 106, 106, ${0.5 * mods.redShock})`;
    ctx.lineWidth = 1.8;
    ctx.beginPath();
    ctx.arc(cx, cy, 36 + (1 - mods.redShock) * 200, 0, Math.PI * 2);
    ctx.stroke();
  }
  if (mods.greenPulse > 0) {
    ctx.strokeStyle = `rgba(125, 206, 160, ${0.45 * mods.greenPulse})`;
    ctx.lineWidth = 1.4;
    ctx.beginPath();
    ctx.arc(cx, cy, 28 + (1 - mods.greenPulse) * 150, 0, Math.PI * 2);
    ctx.stroke();
  }
}

function strokeSegmentPass(
  ctx: CanvasRenderingContext2D,
  side: "far" | "near",
  edgeOn: number,
  withBloom: boolean,
): void {
  const bloomMul = withBloom ? (1 - edgeOn * 0.7) : 0;

  for (let i = 0; i < FIELD_FILAMENT_COUNT; i += 1) {
    const seed = SEEDS[i]!;
    const vis = backgroundVisibility(i, edgeOn);
    if (vis <= 0.01) continue;

    const alphaMul = tierAlphaMul(seed.tier, edgeOn) * vis;
    const widthMul = tierWidthMul(seed.tier);
    // Amber/gold — avoid white filaments.
    const r = Math.round(210 + seed.hueBias * 35);
    const g = Math.round(130 + seed.hueBias * 55);
    const b = Math.round(42 + seed.hueBias * 28);

    let drawing = false;
    for (let s = 0; s < FIELD_SAMPLES; s += 1) {
      const a = readProjected(i, s);
      const c = readProjected(i, s + 1);
      const midZ = (a.z + c.z) * 0.5;
      const isFar = midZ < 0;
      if (side === "far" && !isFar) {
        drawing = false;
        continue;
      }
      if (side === "near" && isFar) {
        drawing = false;
        continue;
      }

      const depth = clamp((midZ + 300) / 600, 0, 1);
      const alpha = (0.05 + depth * 0.32) * alphaMul;
      // Outer fade: points far from canvas center get softer.
      const dist = Math.hypot((a.x + c.x) * 0.5 - FIELD_VIEW.cx, (a.y + c.y) * 0.5 - FIELD_VIEW.cy);
      const outer = clamp(1 - (dist - 420) / 280, 0.25, 1);
      // Approximate keep-out dim from projected radius vs core.
      const coreDist = Math.hypot((a.x + c.x) * 0.5 - FIELD_VIEW.cx, (a.y + c.y) * 0.5 - FIELD_VIEW.cy);
      const nearCore = keepOutDim(coreDist * 0.55);

      const segAlpha = alpha * outer * nearCore;
      if (segAlpha < 0.01) {
        drawing = false;
        continue;
      }

      const width = (0.7 + depth * 1.15) * widthMul;
      ctx.strokeStyle = `rgba(${r}, ${g}, ${b}, ${segAlpha})`;
      ctx.lineWidth = width;
      ctx.lineJoin = "round";
      ctx.lineCap = "round";
      if (bloomMul > 0.05 && seed.tier === "primary" && s % 7 === 0) {
        ctx.shadowColor = `rgba(255, 170, 60, ${0.22 * segAlpha * bloomMul})`;
        ctx.shadowBlur = 5 * bloomMul;
      } else {
        ctx.shadowBlur = 0;
      }

      if (!drawing) {
        ctx.beginPath();
        ctx.moveTo(a.x, a.y);
        drawing = true;
      }
      ctx.lineTo(c.x, c.y);
      // Stroke short runs to allow per-segment alpha variation without one flat path.
      if (s % 4 === 3 || s === FIELD_SAMPLES - 1) {
        ctx.stroke();
        drawing = false;
      }
    }
    if (drawing) ctx.stroke();
  }
  ctx.shadowBlur = 0;
}

function readTrail(packet: FieldPacket, indexFromHead: number): PacketTrailSample | null {
  if (indexFromHead >= packet.trailLen) return null;
  const idx = (packet.trailWrite - 1 - indexFromHead + FIELD_TRAIL_POINTS * 4) % FIELD_TRAIL_POINTS;
  return packet.trail[idx] ?? null;
}

function paintPackets(ctx: CanvasRenderingContext2D, state: PacketState | undefined): void {
  if (!state) return;
  for (let i = 0; i < state.count; i += 1) {
    const packet = state.packets[i]!;
    if (!packet.alive || packet.trailLen < 2) continue;
    const boost = 1 + packet.boost * 0.55;
    for (let k = packet.trailLen - 1; k >= 1; k -= 1) {
      const a = readTrail(packet, k);
      const b = readTrail(packet, k - 1);
      if (!a || !b) continue;
      const fade = 1 - k / FIELD_TRAIL_POINTS;
      const alpha = (0.04 + 0.38 * fade) * boost;
      ctx.beginPath();
      ctx.moveTo(a.x, a.y);
      ctx.lineTo(b.x, b.y);
      ctx.strokeStyle = `rgba(255, ${Math.round(175 + 40 * fade)}, ${Math.round(70 + 35 * fade)}, ${alpha})`;
      ctx.lineWidth = (0.25 + 1.6 * fade) * boost;
      ctx.lineCap = "round";
      ctx.stroke();
    }
    const head = readTrail(packet, 0);
    if (head) {
      const r = (1.6 + packet.boost * 1.2) * Math.min(head.scale, 1.35);
      const g = ctx.createRadialGradient(head.x, head.y, 0, head.x, head.y, r * 2.4);
      // Brief white only on boosted heads; ambient stays amber.
      if (packet.boost > 0.35) {
        g.addColorStop(0, "rgba(255, 248, 230, 0.9)");
        g.addColorStop(0.4, "rgba(255, 200, 110, 0.55)");
      } else {
        g.addColorStop(0, "rgba(255, 200, 120, 0.7)");
        g.addColorStop(0.4, "rgba(220, 140, 50, 0.35)");
      }
      g.addColorStop(1, "rgba(0, 0, 0, 0)");
      ctx.fillStyle = g;
      ctx.beginPath();
      ctx.arc(head.x, head.y, r * 2.4, 0, Math.PI * 2);
      ctx.fill();
      ctx.fillStyle = packet.boost > 0.35 ? "rgba(255, 245, 220, 0.92)" : "rgba(255, 190, 100, 0.85)";
      ctx.beginPath();
      ctx.arc(head.x, head.y, r, 0, Math.PI * 2);
      ctx.fill();
    }
  }
}

/**
 * Paint into an already-transformed 2D context using LOGICAL 1200x760 coords.
 * Caller (GalaxyFieldCanvas) must apply the device-pixel transform once on resize — never inside paint.
 */
export function paintGalaxyField(
  ctx: CanvasRenderingContext2D,
  t: number,
  opts: FieldPaintOpts = {},
): void {
  const reduced = Boolean(opts.reducedMotion);
  const time = reduced ? 0 : t;
  const camera = fieldCamera(time);
  const edgeOn = edgeOnAmount(camera);
  const mods = eventMods(opts.events);
  const bloomScale = 1 - edgeOn * 0.45;

  // One geometry pass per frame into reused typed buffer.
  projectAllFilaments(time, camera);

  ctx.save();
  // Do NOT reset transform — preserve DPR mapping from canvas resize.
  ctx.globalCompositeOperation = "source-over";
  ctx.globalAlpha = 1;
  ctx.fillStyle = "#050308";
  ctx.fillRect(0, 0, FIELD_VIEW.width, FIELD_VIEW.height);

  const wash = ctx.createRadialGradient(
    FIELD_VIEW.cx,
    FIELD_VIEW.cy,
    40,
    FIELD_VIEW.cx,
    FIELD_VIEW.cy,
    520,
  );
  wash.addColorStop(0, "rgba(22, 14, 7, 0.5)");
  wash.addColorStop(0.45, "rgba(8, 5, 3, 0.3)");
  wash.addColorStop(1, "rgba(3, 2, 3, 0)");
  ctx.fillStyle = wash;
  ctx.fillRect(0, 0, FIELD_VIEW.width, FIELD_VIEW.height);

  ctx.globalCompositeOperation = "lighter";
  strokeSegmentPass(ctx, "far", edgeOn, true);
  paintCore(ctx, mods, bloomScale);
  strokeSegmentPass(ctx, "near", edgeOn, true);
  if (!reduced) paintPackets(ctx, opts.packets);

  ctx.restore();
}

/** Static SVG path data for Canvas-unavailable fallback (organized families at t). */
export function fieldFallbackPaths(t = 0): FallbackPath[] {
  const camera = fieldCamera(t);
  const edgeOn = edgeOnAmount(camera);
  const paths: FallbackPath[] = [];
  const step = 2; // slightly coarser for SVG payload
  for (let i = 0; i < FIELD_FILAMENT_COUNT; i += 1) {
    const seed = SEEDS[i]!;
    const vis = backgroundVisibility(i, edgeOn);
    if (vis <= 0.02 && seed.tier === "background") continue;
    const points: Array<{ x: number; y: number }> = [];
    for (let s = 0; s <= FIELD_SAMPLES; s += step) {
      const theta = (s / FIELD_SAMPLES) * Math.PI * 2;
      const p = projectFieldPoint(sampleFilamentPoint(i, theta, t), camera);
      points.push({ x: Number(p.x.toFixed(2)), y: Number(p.y.toFixed(2)) });
    }
    const opacity = Number((tierAlphaMul(seed.tier, edgeOn) * vis * 0.55).toFixed(3));
    const width = Number((tierWidthMul(seed.tier) * 1.1).toFixed(2));
    paths.push({ family: seed.family, tier: seed.tier, points, opacity, width });
  }
  return paths;
}

export function fieldFamilyCounts(): Record<FilamentFamily, number> {
  return SEEDS.reduce(
    (acc, s) => {
      acc[s.family] += 1;
      return acc;
    },
    { A: 0, B: 0, C: 0, D: 0 } as Record<FilamentFamily, number>,
  );
}


