/**
 * Proof frames for the deforming filament field.
 * Uses the same parametric model as apps/dashboard/lib/galaxy-field.ts
 * Writes SVG (and PPM/PNG when ffmpeg is available) at t=0,3,6,9,12.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { spawnSync } from "node:child_process";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../../..");
const OUT = process.argv[2]
  ? path.resolve(process.argv[2])
  : path.join(ROOT, "reports");

const W = 1200, H = 760, CX = 600, CY = 372, FOCAL = 1500;
const FILAMENTS = 64, SAMPLES = 72;

function clamp(v, a, b) { return Math.min(b, Math.max(a, v)); }
function hash01(i, salt) {
  const x = Math.sin(i * 127.1 + salt * 311.7) * 43758.5453123;
  return x - Math.floor(x);
}

const SEEDS = Array.from({ length: FILAMENTS }, (_, i) => {
  const t = (i + 0.37) / FILAMENTS;
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

function camera(t) {
  return {
    rx: 0.55 + 0.85 * Math.sin(t / 11),
    ry: 0.25 * Math.sin(t / 17),
    rz: 0.18 * Math.sin(t / 13) + 0.08 * Math.sin(t / 7),
  };
}

function rotZ(x, y, z, a) {
  const c = Math.cos(a), s = Math.sin(a);
  return { x: x * c - y * s, y: x * s + y * c, z };
}
function rotX(x, y, z, a) {
  const c = Math.cos(a), s = Math.sin(a);
  return { x, y: y * c - z * s, z: y * s + z * c };
}
function rotY(x, y, z, a) {
  const c = Math.cos(a), s = Math.sin(a);
  return { x: x * c + z * s, y, z: -x * s + z * c };
}

function sample(i, theta, t) {
  const s = SEEDS[i];
  const a = s.a0 * (1 + 0.12 * Math.sin(t / 9.2 + s.phase));
  const e = clamp(s.e0 + 0.18 * Math.sin(t / 14.5 + s.phase * 1.7), 0.02, 0.55);
  const b = a * Math.sqrt(1 - e * e);
  const inc = clamp(s.di + 0.12 * Math.sin(t / 8.4 + s.phase), -0.35, 0.35);
  const prec = s.omega + 0.07 * t + 0.18 * Math.sin(t / 19 + s.phase);
  const warp = s.Aw * Math.sin(2 * theta + t / 6.5 + s.phase)
    + s.Aw * 0.35 * Math.sin(3 * theta + t / 4.1 + s.phase * 0.7);
  const vert = s.Av * Math.sin(theta + t / 7.8 + s.phase * 0.6)
    + s.Av * 0.28 * Math.sin(2.3 * theta - t / 5.5 + s.omega);
  let p = { x: a * Math.cos(theta + s.phase), y: b * Math.sin(theta + s.phase), z: vert + warp };
  p = rotZ(p.x, p.y, p.z, prec);
  p = rotX(p.x, p.y, p.z, inc);
  return p;
}

function project(p, cam) {
  let r = rotZ(p.x, p.y, p.z, cam.rz);
  r = rotY(r.x, r.y, r.z, cam.ry);
  r = rotX(r.x, r.y, r.z, cam.rx);
  const persp = FOCAL / (FOCAL - r.z);
  return { x: CX + r.x * persp, y: CY + r.y * persp, z: r.z, s: persp };
}

function depthAlpha(z) {
  const depth = clamp((z + 320) / 640, 0, 1);
  return 0.08 + depth * 0.37;
}
function depthWidth(z) {
  const depth = clamp((z + 320) / 640, 0, 1);
  return 1.1 + depth * 1.5;
}

function renderSvg(t) {
  const cam = camera(t);
  const parts = [];
  parts.push(`<?xml version="1.0" encoding="UTF-8"?>`);
  parts.push(`<svg xmlns="http://www.w3.org/2000/svg" width="${W}" height="${H}" viewBox="0 0 ${W} ${H}">`);
  parts.push(`<rect width="100%" height="100%" fill="#050308"/>`);
  parts.push(`<defs>
    <radialGradient id="coreHalo" cx="50%" cy="49%" r="12%">
      <stop offset="0%" stop-color="rgba(255,176,72,0.28)"/>
      <stop offset="35%" stop-color="rgba(200,110,36,0.12)"/>
      <stop offset="100%" stop-color="rgba(0,0,0,0)"/>
    </radialGradient>
    <radialGradient id="coreAmber" cx="50%" cy="49%" r="4%">
      <stop offset="0%" stop-color="rgba(255,210,120,0.9)"/>
      <stop offset="55%" stop-color="rgba(240,140,48,0.45)"/>
      <stop offset="100%" stop-color="rgba(0,0,0,0)"/>
    </radialGradient>
    <radialGradient id="coreHot" cx="50%" cy="49%" r="1.2%">
      <stop offset="0%" stop-color="#fffce8"/>
      <stop offset="55%" stop-color="rgba(255,230,170,0.9)"/>
      <stop offset="100%" stop-color="rgba(0,0,0,0)"/>
    </radialGradient>
    <filter id="bloom" x="-20%" y="-20%" width="140%" height="140%">
      <feGaussianBlur stdDeviation="3.2" result="b"/>
      <feMerge><feMergeNode in="b"/><feMergeNode in="SourceGraphic"/></feMerge>
    </filter>
  </defs>`);
  parts.push(`<rect width="100%" height="100%" fill="url(#coreHalo)"/>`);

  const filaments = [];
  for (let i = 0; i < FILAMENTS; i++) {
    const pts = [];
    let meanZ = 0;
    for (let s = 0; s <= SAMPLES; s++) {
      const theta = (s / SAMPLES) * Math.PI * 2;
      const p = project(sample(i, theta, t), cam);
      pts.push(p);
      meanZ += p.z;
    }
    meanZ /= pts.length;
    filaments.push({ i, pts, meanZ, seed: SEEDS[i] });
  }
  filaments.sort((a, b) => a.meanZ - b.meanZ);

  for (const f of filaments) {
    const alpha = depthAlpha(f.meanZ);
    const width = depthWidth(f.meanZ);
    const warm = 0.75 + f.seed.hueBias * 0.25;
    const r = Math.round(255 * warm);
    const g = Math.round(170 + f.seed.hueBias * 40);
    const b = Math.round(60 + f.seed.hueBias * 30);
    const d = f.pts.map((p, idx) => `${idx === 0 ? "M" : "L"}${p.x.toFixed(1)},${p.y.toFixed(1)}`).join(" ") + " Z";
    const bloom = f.i % 4 === 0 ? ` filter="url(#bloom)"` : "";
    parts.push(`<path d="${d}" fill="none" stroke="rgba(${r},${g},${b},${alpha.toFixed(3)})" stroke-width="${width.toFixed(2)}" stroke-linejoin="round" stroke-linecap="round"${bloom}/>`);
  }

  // packets
  for (let p = 0; p < 10; p++) {
    const fi = (p * 7 + 3) % FILAMENTS;
    const trail = [];
    for (let k = 0; k < 24; k++) {
      const theta = (t * (0.35 + p * 0.03) + p * 0.7 - k * 0.045) % (Math.PI * 2);
      trail.push(project(sample(fi, theta < 0 ? theta + Math.PI * 2 : theta, t), cam));
    }
    for (let k = trail.length - 1; k >= 1; k--) {
      const fade = 1 - k / trail.length;
      const a = trail[k], b = trail[k - 1];
      parts.push(`<line x1="${a.x.toFixed(1)}" y1="${a.y.toFixed(1)}" x2="${b.x.toFixed(1)}" y2="${b.y.toFixed(1)}" stroke="rgba(255,${Math.round(210 + 30 * fade)},${Math.round(140 + 40 * fade)},${(0.08 + 0.55 * fade).toFixed(3)})" stroke-width="${(0.4 + 2.6 * fade).toFixed(2)}" stroke-linecap="round"/>`);
    }
    const head = trail[0];
    parts.push(`<circle cx="${head.x.toFixed(1)}" cy="${head.y.toFixed(1)}" r="${(2.8 * head.s).toFixed(2)}" fill="rgba(255,248,230,0.95)"/>`);
  }

  parts.push(`<circle cx="${CX}" cy="${CY}" r="90" fill="url(#coreHalo)"/>`);
  parts.push(`<circle cx="${CX}" cy="${CY}" r="28" fill="url(#coreAmber)"/>`);
  parts.push(`<circle cx="${CX}" cy="${CY}" r="8" fill="url(#coreHot)"/>`);
  parts.push(`<!-- t=${t}s rx=${cam.rx.toFixed(3)} -->`);
  parts.push(`</svg>`);
  return { svg: parts.join("\n"), cam };
}

function renderPpm(t) {
  const cam = camera(t);
  const buf = new Float32Array(W * H * 3);
  for (let i = 0; i < W * H; i++) {
    buf[i * 3] = 5; buf[i * 3 + 1] = 3; buf[i * 3 + 2] = 8;
  }
  const plot = (x, y, r, g, b, a, thick = 1) => {
    const rad = Math.max(1, Math.round(thick));
    const xi = x | 0, yi = y | 0;
    for (let dy = -rad; dy <= rad; dy++) {
      for (let dx = -rad; dx <= rad; dx++) {
        if (dx * dx + dy * dy > rad * rad) continue;
        const px = xi + dx, py = yi + dy;
        if (px < 0 || py < 0 || px >= W || py >= H) continue;
        const fall = 1 - Math.sqrt(dx * dx + dy * dy) / (rad + 0.01);
        const i = (py * W + px) * 3;
        const aa = a * fall;
        buf[i] = Math.min(255, buf[i] + r * aa);
        buf[i + 1] = Math.min(255, buf[i + 1] + g * aa);
        buf[i + 2] = Math.min(255, buf[i + 2] + b * aa);
      }
    }
  };
  const line = (x0, y0, x1, y1, r, g, b, a, thick) => {
    const steps = Math.max(1, Math.hypot(x1 - x0, y1 - y0) | 0);
    for (let s = 0; s <= steps; s++) {
      const u = s / steps;
      plot(x0 + (x1 - x0) * u, y0 + (y1 - y0) * u, r, g, b, a, thick);
    }
  };

  const segs = [];
  for (let f = 0; f < FILAMENTS; f++) {
    let prev = null;
    let meanZ = 0;
    const pts = [];
    for (let s = 0; s <= SAMPLES; s++) {
      const theta = (s / SAMPLES) * Math.PI * 2;
      const p = project(sample(f, theta, t), cam);
      pts.push(p);
      meanZ += p.z;
      if (prev) segs.push({ x0: prev.x, y0: prev.y, x1: p.x, y1: p.y, z: (prev.z + p.z) * 0.5, f });
      prev = p;
    }
  }
  segs.sort((a, b) => a.z - b.z);
  for (const seg of segs) {
    const a = depthAlpha(seg.z);
    const thick = depthWidth(seg.z);
    line(seg.x0, seg.y0, seg.x1, seg.y1, 255, 185, 75, a, thick);
    if (seg.f % 4 === 0) line(seg.x0, seg.y0, seg.x1, seg.y1, 255, 160, 50, a * 0.35, thick + 3);
  }
  // core
  for (let y = CY - 95; y <= CY + 95; y++) {
    for (let x = CX - 95; x <= CX + 95; x++) {
      const d = Math.hypot(x - CX, y - CY);
      if (d > 95) continue;
      const i = (y * W + x) * 3;
      const hot = Math.exp(-(d * d) / 40);
      const amber = Math.exp(-(d * d) / 280);
      const halo = Math.exp(-(d * d) / 2800);
      buf[i] = Math.min(255, buf[i] + 255 * hot + 180 * amber + 70 * halo);
      buf[i + 1] = Math.min(255, buf[i + 1] + 245 * hot + 120 * amber + 40 * halo);
      buf[i + 2] = Math.min(255, buf[i + 2] + 210 * hot + 40 * amber + 10 * halo);
    }
  }
  // packets
  for (let p = 0; p < 10; p++) {
    const fi = (p * 7 + 3) % FILAMENTS;
    let prev = null;
    for (let k = 0; k < 24; k++) {
      let theta = (t * (0.35 + p * 0.03) + p * 0.7 - k * 0.045) % (Math.PI * 2);
      if (theta < 0) theta += Math.PI * 2;
      const q = project(sample(fi, theta, t), cam);
      if (prev) {
        const fade = 1 - k / 24;
        line(prev.x, prev.y, q.x, q.y, 255, 230, 170, 0.15 + 0.55 * fade, 0.4 + 2.6 * fade);
      }
      prev = q;
      if (k === 0) plot(q.x, q.y, 255, 248, 230, 0.95, 3.2 * q.s);
    }
  }
  return { buf, cam };
}

fs.mkdirSync(OUT, { recursive: true });
const times = [0, 3, 6, 9, 12];
for (const t of times) {
  const { svg, cam } = renderSvg(t);
  const svgPath = path.join(OUT, `galaxy-field-t${t}.svg`);
  fs.writeFileSync(svgPath, svg);
  const { buf } = renderPpm(t);
  const ppmPath = path.join(OUT, `galaxy-field-t${t}.ppm`);
  const pngPath = path.join(OUT, `galaxy-field-t${t}.png`);
  const header = Buffer.from(`P6\n${W} ${H}\n255\n`);
  const rgb = Buffer.alloc(W * H * 3);
  for (let i = 0; i < rgb.length; i++) rgb[i] = Math.min(255, buf[i] | 0);
  fs.writeFileSync(ppmPath, Buffer.concat([header, rgb]));
  const ff = spawnSync("ffmpeg", ["-y", "-i", ppmPath, pngPath], { encoding: "utf8" });
  console.log(`t=${t}s rx=${cam.rx.toFixed(3)} -> ${svgPath}${ff.status === 0 ? ` + ${pngPath}` : " (png skipped)"}`);
}
