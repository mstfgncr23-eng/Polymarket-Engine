const fs = require("fs");
const path = require("path");
const { spawn } = require("child_process");

const ROOT = process.cwd();
const DASHBOARD = path.join(ROOT, "apps", "dashboard");
const REPORTS = path.join(ROOT, "reports");
const PORT = Number(process.env.GALAXY_PROOF_PORT || 3017);
const BASE = `http://127.0.0.1:${PORT}`;
const TIMES = [0, 3, 6, 9, 12];
fs.mkdirSync(REPORTS, { recursive: true });

function percentile(sorted, p) {
  if (!sorted.length) return 0;
  const idx = Math.min(sorted.length - 1, Math.max(0, Math.ceil((p / 100) * sorted.length) - 1));
  return sorted[idx];
}

async function waitReady(timeoutMs = 120000) {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    try {
      const res = await fetch(`${BASE}/galaxy-harness`);
      if (res.status === 200) return;
    } catch {}
    await new Promise((r) => setTimeout(r, 300));
  }
  throw new Error("server not ready");
}

function startServer() {
  const standalone = path.join(DASHBOARD, ".next", "standalone", "apps", "dashboard", "server.js");
  const staticSrc = path.join(DASHBOARD, ".next", "static");
  const staticDst = path.join(DASHBOARD, ".next", "standalone", "apps", "dashboard", ".next", "static");
  fs.cpSync(staticSrc, staticDst, { recursive: true });
  const child = spawn(process.execPath, [standalone], {
    cwd: path.dirname(standalone),
    stdio: ["ignore", "pipe", "pipe"],
    env: { ...process.env, PORT: String(PORT), HOSTNAME: "127.0.0.1" },
  });
  child.stdout.on("data", (d) => process.stdout.write(`[next] ${d}`));
  child.stderr.on("data", (d) => process.stderr.write(`[next] ${d}`));
  return child;
}

async function waitField(page) {
  for (let i = 0; i < 60; i += 1) {
    const ok = await page.evaluate(() => !!document.querySelector("canvas.galaxyFieldCanvas, svg[data-galaxy-fallback]"));
    if (ok) return;
    await page.waitForTimeout(250);
  }
  throw new Error("field never mounted");
}

async function captureFieldPng(page, out) {
  const dataUrl = await page.evaluate(() => {
    const canvas = document.querySelector("canvas.galaxyFieldCanvas");
    if (canvas) return canvas.toDataURL("image/png");
    const svg = document.querySelector("svg[data-galaxy-fallback], svg.galaxyOrbitalField, .galaxyOrbital svg");
    if (!svg) return null;
    const xml = new XMLSerializer().serializeToString(svg);
    return "data:image/svg+xml;charset=utf-8," + encodeURIComponent(xml);
  });
  if (!dataUrl) throw new Error("no field surface to capture");
  if (dataUrl.startsWith("data:image/png")) {
    fs.writeFileSync(out, Buffer.from(dataUrl.split(",")[1], "base64"));
    return;
  }
  // SVG fallback: write .svg sibling and also a PNG via viewport clip of the svg using CDP without font wait.
  const svgPath = out.replace(/\.png$/i, ".svg");
  const raw = decodeURIComponent(dataUrl.split(",")[1]);
  fs.writeFileSync(svgPath, raw);
  const box = await page.evaluate(() => {
    const el = document.querySelector("svg[data-galaxy-fallback], .galaxyOrbital");
    if (!el) return null;
    const r = el.getBoundingClientRect();
    return { x: r.x, y: r.y, width: r.width, height: r.height };
  });
  if (box && box.width > 0) {
    const buf = await page.screenshot({
      type: "png",
      clip: {
        x: Math.max(0, box.x),
        y: Math.max(0, box.y),
        width: Math.min(box.width, 1200),
        height: Math.min(box.height, 760),
      },
      timeout: 10000,
    });
    fs.writeFileSync(out, buf);
  } else {
    // last resort: keep svg only and copy bytes as placeholder note
    fs.writeFileSync(out, Buffer.from(raw));
  }
}

async function main() {
  const { chromium } = await import("playwright");
  let server = null;
  try { await waitReady(1500); } catch { server = startServer(); await waitReady(120000); }

  const browser = await chromium.launch({ headless: true });
  const results = { shots: [], fps: null, fallback: null };

  try {
    {
      const context = await browser.newContext({ viewport: { width: 1280, height: 860 }, deviceScaleFactor: 1 });
      const page = await context.newPage();
      for (const t of TIMES) {
        await page.goto(`${BASE}/galaxy-harness?t=${t}`, { waitUntil: "load", timeout: 120000 });
        await waitField(page);
        await page.waitForTimeout(400);
        const out = path.join(REPORTS, `galaxy-runtime-dpr1-t${t}.png`);
        await captureFieldPng(page, out);
        results.shots.push(out);
        console.log("shot", out);
      }

      await page.goto(`${BASE}/galaxy-harness`, { waitUntil: "load", timeout: 120000 });
      await waitField(page);
      await page.waitForTimeout(1500);
      const metrics = await page.evaluate(async () => {
        const canvas = document.querySelector("canvas.galaxyFieldCanvas");
        if (!canvas) return { paintMs: [], intervals: [] };
        const paintMs = [];
        const intervals = [];
        let lastPaint = -1;
        let lastTs = 0;
        const target = 320;
        const deadline = performance.now() + 25000;
        await new Promise((resolve) => {
          const tick = (ts) => {
            if (lastTs) intervals.push(ts - lastTs);
            lastTs = ts;
            const ms = Number(canvas.dataset.fieldFrameMs);
            if (Number.isFinite(ms) && ms !== lastPaint) {
              paintMs.push(ms);
              lastPaint = ms;
            }
            if (paintMs.length >= target || performance.now() > deadline) resolve();
            else requestAnimationFrame(tick);
          };
          requestAnimationFrame(tick);
        });
        return { paintMs, intervals };
      });
      const sorted = [...metrics.paintMs].sort((a, b) => a - b);
      const mean = metrics.paintMs.reduce((a, b) => a + b, 0) / Math.max(1, metrics.paintMs.length);
      const intervalSorted = [...metrics.intervals].sort((a, b) => a - b);
      const meanInterval = metrics.intervals.reduce((a, b) => a + b, 0) / Math.max(1, metrics.intervals.length);
      results.fps = {
        browser: "Chromium (Playwright)",
        viewport: "1280x860",
        dpr: 1,
        environment: "Windows laptop",
        samples: metrics.paintMs.length,
        p50_ms: Number(percentile(sorted, 50).toFixed(3)),
        p95_ms: Number(percentile(sorted, 95).toFixed(3)),
        p99_ms: Number(percentile(sorted, 99).toFixed(3)),
        max_ms: Number((sorted[sorted.length - 1] ?? 0).toFixed(3)),
        mean_ms: Number(mean.toFixed(3)),
        paint_budget_fps: Number((mean > 0 ? 1000 / mean : 0).toFixed(2)),
        raf_interval_mean_ms: Number(meanInterval.toFixed(3)),
        raf_interval_p95_ms: Number(percentile(intervalSorted, 95).toFixed(3)),
        achieved_display_fps: Number((meanInterval > 0 ? 1000 / meanInterval : 0).toFixed(2)),
        note: "paint_* from data-field-frame-ms; achieved_display_fps from rAF intervals (headless may throttle)",
      };
      console.log("fps", results.fps);
      await context.close();
    }

    {
      const context = await browser.newContext({ viewport: { width: 1280, height: 860 }, deviceScaleFactor: 2 });
      const page = await context.newPage();
      for (const t of TIMES) {
        await page.goto(`${BASE}/galaxy-harness?t=${t}`, { waitUntil: "load", timeout: 120000 });
        await waitField(page);
        await page.waitForTimeout(400);
        const out = path.join(REPORTS, `galaxy-runtime-dpr2-t${t}.png`);
        await captureFieldPng(page, out);
        results.shots.push(out);
        console.log("shot", out);
      }
      await context.close();
    }

    {
      const context = await browser.newContext({ viewport: { width: 1280, height: 860 }, deviceScaleFactor: 1 });
      const page = await context.newPage();
      await page.addInitScript(() => {
        const original = HTMLCanvasElement.prototype.getContext;
        HTMLCanvasElement.prototype.getContext = function (type, ...rest) {
          if (type === "2d") return null;
          return original.call(this, type, ...rest);
        };
      });
      await page.goto(`${BASE}/galaxy-harness?fallback=1`, { waitUntil: "load", timeout: 120000 });
      await waitField(page);
      await page.waitForTimeout(500);
      const has = await page.evaluate(() => !!document.querySelector("svg[data-galaxy-fallback='svg-field']"));
      if (!has) throw new Error("fallback svg missing");
      const out = path.join(REPORTS, "galaxy-runtime-fallback.png");
      await captureFieldPng(page, out);
      results.fallback = out;
      console.log("fallback", out);
      await context.close();
    }
  } finally {
    await browser.close();
    if (server) try { server.kill(); } catch {}
  }

  const summaryPath = path.join(REPORTS, "galaxy-runtime-proof.json");
  fs.writeFileSync(summaryPath, JSON.stringify(results, null, 2));
  fs.copyFileSync(__filename, path.join(DASHBOARD, "scripts", "galaxy-runtime-proof.cjs"));
  console.log(JSON.stringify(results, null, 2));
  console.log("Wrote", summaryPath);
}

main().catch((e) => { console.error(e); process.exit(1); });
