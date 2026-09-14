export const FIELD_FILAMENT_COUNT = 64;
export const FIELD_SAMPLES = 72;
export const FIELD_MAX_PACKETS = 12;
export const FIELD_TRAIL_POINTS = 24;
export const FIELD_VIEW = { width: 1200, height: 760, cx: 600, cy: 372, focal: 1500 } as const;

export type FieldVec3 = { x: number; y: number; z: number };
export type FieldProjected = { x: number; y: number; z: number; scale: number };
export type FieldCamera = { rx: number; ry: number; rz: number };

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

type FilamentSeed = {
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

const SEEDS: FilamentSeed[] = Array.from({ length: FIELD_FILAMENT_COUNT }, (_, i) => {
  const t = (i + 0.37) / FIELD_FILAMENT_COUNT;
  return {
    a0: 130 + 400 * Math.pow(t, 0.68),
    e0: 0.05 + 0.32 * hash01(i, 2),
    di: (hash01(i, 3) - 0.5) * 0.7,
    phase: hash01(i, 4) * Math.PI * 2,
    omega: hash01(i, 5) * Math.PI * 2,
    Aw: 22 + 48 * hash01(i, 6),
    Av: 16 + 42 * hash01(i, 7),
    hueBias: hash01(i, 8),
  };
});

export function fieldCamera(t: number): FieldCamera {
  return {
    rx: 0.55 + 0.85 * Math.sin(t / 11),
    ry: 0.25 * Math.sin(t / 17),
    rz: 0.18 * Math.sin(t / 13) + 0.08 * Math.sin(t / 7),
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
  const a = s.a0 * (1 + 0.12 * Math.sin(t / 9.2 + s.phase));
  const e = clamp(s.e0 + 0.18 * Math.sin(t / 14.5 + s.phase * 1.7), 0.02, 0.55);
  const b = a * Math.sqrt(1 - e * e);
  // Camera owns disk↔edge-on. Filament inclination is only a small per-filament offset.
  const inc = clamp(s.di + 0.12 * Math.sin(t / 8.4 + s.phase), -0.35, 0.35);
  const prec = s.omega + 0.07 * t + 0.18 * Math.sin(t / 19 + s.phase);
  const warp = s.Aw * Math.sin(2 * theta + t / 6.5 + s.phase)
    + s.Aw * 0.35 * Math.sin(3 * theta + t / 4.1 + s.phase * 0.7);
  const vert = s.Av * Math.sin(theta + t / 7.8 + s.phase * 0.6)
    + s.Av * 0.28 * Math.sin(2.3 * theta - t / 5.5 + s.omega);
  let local: FieldVec3 = {
    x: a * Math.cos(theta + s.phase),
    y: b * Math.sin(theta + s.phase),
    z: vert + warp,
  };
  local = rotateZ(local.x, local.y, local.z, prec);
  local = rotateX(local.x, local.y, local.z, inc);
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
  const a = radius * (1 + 0.04 * Math.sin(t / 9.2 + angle));
  const e = 0.18 + 0.08 * Math.sin(t / 14.5 + angle);
  const b = a * Math.sqrt(1 - e * e);
  const local: FieldVec3 = {
    x: a * Math.cos(angle),
    y: b * Math.sin(angle),
    z: 12 * Math.sin(angle + t / 7.8),
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
  for (let i = 0; i < FIELD_FILAMENT_COUNT; i += 4) {
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
    emptyPacket((i * 7 + 3) % FIELD_FILAMENT_COUNT, (i / FIELD_MAX_PACKETS) * Math.PI * 2, 0.28 + (i % 5) * 0.05),
  );
  return { packets, count: Math.min(10, FIELD_MAX_PACKETS), spawnCursor: 0 };
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
    state.packets[i] = emptyPacket(filament, t * 0.4 + i, 0.4 + (i % 3) * 0.08);
    state.packets[i]!.boost = 1;
    state.count += 1;
    state.spawnCursor += 1;
  }

  // Soft recycle: never grow past FIELD_MAX_PACKETS; overwrite oldest slot if needed.
  if (state.count > FIELD_MAX_PACKETS) state.count = FIELD_MAX_PACKETS;
  return state;
}

function depthAlpha(z: number): number {
  const depth = clamp((z + 320) / 640, 0, 1);
  return 0.08 + depth * 0.37;
}

function depthWidth(z: number): number {
  const depth = clamp((z + 320) / 640, 0, 1);
  return 1.1 + depth * 1.5;
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
): void {
  const { cx, cy } = FIELD_VIEW;
  const bloom = 1 + mods.coreBloom * 0.55;

  const halo = ctx.createRadialGradient(cx, cy, 0, cx, cy, 90 * bloom);
  halo.addColorStop(0, "rgba(255, 176, 72, 0.28)");
  halo.addColorStop(0.35, "rgba(200, 110, 36, 0.12)");
  halo.addColorStop(1, "rgba(0, 0, 0, 0)");
  ctx.fillStyle = halo;
  ctx.beginPath();
  ctx.arc(cx, cy, 90 * bloom, 0, Math.PI * 2);
  ctx.fill();

  const amber = ctx.createRadialGradient(cx, cy, 0, cx, cy, 28 * bloom);
  amber.addColorStop(0, "rgba(255, 210, 120, 0.85)");
  amber.addColorStop(0.55, "rgba(240, 140, 48, 0.45)");
  amber.addColorStop(1, "rgba(0, 0, 0, 0)");
  ctx.fillStyle = amber;
  ctx.beginPath();
  ctx.arc(cx, cy, 28 * bloom, 0, Math.PI * 2);
  ctx.fill();

  const hot = ctx.createRadialGradient(cx, cy, 0, cx, cy, 8 * bloom);
  hot.addColorStop(0, "rgba(255, 252, 240, 1)");
  hot.addColorStop(0.55, "rgba(255, 230, 170, 0.9)");
  hot.addColorStop(1, "rgba(0, 0, 0, 0)");
  ctx.fillStyle = hot;
  ctx.beginPath();
  ctx.arc(cx, cy, 8 * bloom, 0, Math.PI * 2);
  ctx.fill();

  if (mods.redShock > 0) {
    ctx.strokeStyle = `rgba(212, 106, 106, ${0.55 * mods.redShock})`;
    ctx.lineWidth = 2;
    ctx.beginPath();
    ctx.arc(cx, cy, 40 + (1 - mods.redShock) * 220, 0, Math.PI * 2);
    ctx.stroke();
  }
  if (mods.greenPulse > 0) {
    ctx.strokeStyle = `rgba(125, 206, 160, ${0.5 * mods.greenPulse})`;
    ctx.lineWidth = 1.6;
    ctx.beginPath();
    ctx.arc(cx, cy, 32 + (1 - mods.greenPulse) * 160, 0, Math.PI * 2);
    ctx.stroke();
  }
}

function paintFilamentPass(
  ctx: CanvasRenderingContext2D,
  t: number,
  camera: FieldCamera,
  side: "behind" | "front",
  withBloom: boolean,
): void {
  for (let i = 0; i < FIELD_FILAMENT_COUNT; i += 1) {
    const seed = SEEDS[i]!;
    const points: FieldProjected[] = [];
    let meanZ = 0;
    for (let s = 0; s <= FIELD_SAMPLES; s += 1) {
      const theta = (s / FIELD_SAMPLES) * Math.PI * 2;
      const p = projectFieldPoint(sampleFilamentPoint(i, theta, t), camera);
      points.push(p);
      meanZ += p.z;
    }
    meanZ /= points.length;
    const isBehind = meanZ < 0;
    if (side === "behind" && !isBehind) continue;
    if (side === "front" && isBehind) continue;

    const alpha = depthAlpha(meanZ);
    const width = depthWidth(meanZ);
    const warm = 0.75 + seed.hueBias * 0.25;
    ctx.beginPath();
    for (let s = 0; s < points.length; s += 1) {
      const p = points[s]!;
      if (s === 0) ctx.moveTo(p.x, p.y);
      else ctx.lineTo(p.x, p.y);
    }
    ctx.closePath();
    ctx.strokeStyle = `rgba(${Math.round(255 * warm)}, ${Math.round(170 + seed.hueBias * 40)}, ${Math.round(60 + seed.hueBias * 30)}, ${alpha})`;
    ctx.lineWidth = width;
    ctx.lineJoin = "round";
    ctx.lineCap = "round";
    if (withBloom && i % 4 === 0) {
      ctx.shadowColor = `rgba(255, 180, 70, ${0.35 * alpha})`;
      ctx.shadowBlur = 8;
    } else {
      ctx.shadowBlur = 0;
    }
    ctx.stroke();
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
    const boost = 1 + packet.boost * 0.8;
    for (let k = packet.trailLen - 1; k >= 1; k -= 1) {
      const a = readTrail(packet, k);
      const b = readTrail(packet, k - 1);
      if (!a || !b) continue;
      const fade = 1 - k / FIELD_TRAIL_POINTS;
      const alpha = (0.08 + 0.55 * fade) * boost;
      ctx.beginPath();
      ctx.moveTo(a.x, a.y);
      ctx.lineTo(b.x, b.y);
      ctx.strokeStyle = `rgba(255, ${Math.round(210 + 30 * fade)}, ${Math.round(140 + 40 * fade)}, ${alpha})`;
      ctx.lineWidth = (0.4 + 2.6 * fade) * boost;
      ctx.lineCap = "round";
      ctx.stroke();
    }
    const head = readTrail(packet, 0);
    if (head) {
      const r = (2.4 + packet.boost * 1.8) * head.scale;
      const g = ctx.createRadialGradient(head.x, head.y, 0, head.x, head.y, r * 3);
      g.addColorStop(0, "rgba(255, 252, 235, 0.95)");
      g.addColorStop(0.35, "rgba(255, 210, 120, 0.65)");
      g.addColorStop(1, "rgba(0, 0, 0, 0)");
      ctx.fillStyle = g;
      ctx.beginPath();
      ctx.arc(head.x, head.y, r * 3, 0, Math.PI * 2);
      ctx.fill();
      ctx.fillStyle = "rgba(255, 248, 230, 0.95)";
      ctx.beginPath();
      ctx.arc(head.x, head.y, r, 0, Math.PI * 2);
      ctx.fill();
    }
  }
}

export function paintGalaxyField(
  ctx: CanvasRenderingContext2D,
  t: number,
  opts: FieldPaintOpts = {},
): void {
  const reduced = Boolean(opts.reducedMotion);
  const time = reduced ? 0 : t;
  const camera = fieldCamera(time);
  const mods = eventMods(opts.events);

  ctx.save();
  ctx.setTransform(1, 0, 0, 1, 0, 0);
  ctx.globalCompositeOperation = "source-over";
  ctx.fillStyle = "#050308";
  ctx.fillRect(0, 0, FIELD_VIEW.width, FIELD_VIEW.height);

  // Soft near-black wash / nebula
  const wash = ctx.createRadialGradient(
    FIELD_VIEW.cx,
    FIELD_VIEW.cy,
    40,
    FIELD_VIEW.cx,
    FIELD_VIEW.cy,
    520,
  );
  wash.addColorStop(0, "rgba(26, 16, 8, 0.55)");
  wash.addColorStop(0.45, "rgba(10, 6, 4, 0.35)");
  wash.addColorStop(1, "rgba(3, 2, 3, 0)");
  ctx.fillStyle = wash;
  ctx.fillRect(0, 0, FIELD_VIEW.width, FIELD_VIEW.height);

  ctx.globalCompositeOperation = "lighter";
  paintFilamentPass(ctx, time, camera, "behind", true);
  paintCore(ctx, mods);
  paintFilamentPass(ctx, time, camera, "front", true);
  if (!reduced) paintPackets(ctx, opts.packets);

  ctx.restore();
}
