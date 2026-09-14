"use client";

import { useEffect, useRef, useState } from "react";
import {
  FIELD_VIEW,
  createPacketState,
  deviceBackingStore,
  fieldFallbackPaths,
  paintGalaxyField,
  stepPackets,
  type FieldPaintEvent,
  type PacketState,
} from "../../lib/galaxy-field";
import type { GalaxyTransient } from "../../lib/galaxy-scene";
import { GALAXY_FPS } from "../../lib/galaxy-scene";
import { GalaxyFieldSvgFallback } from "./galaxy-field-svg-fallback";

export type GalaxyFieldCanvasProps = {
  reducedMotion: boolean;
  transients: GalaxyTransient[];
  frame: number;
  durationInFrames: number;
  /** When true, force the static SVG path (tests / proof). */
  forceFallback?: boolean;
  /** When set, paint this fixed field time every frame (proof frames). */
  freezeTimeSec?: number | null;
};

function toPaintEvents(
  transients: GalaxyTransient[],
  frame: number,
  durationInFrames: number,
): FieldPaintEvent[] {
  return transients.map((event) => {
    const elapsed = (frame - event.startFrame + durationInFrames) % durationInFrames;
    return {
      kind: event.kind,
      ageSec: elapsed / GALAXY_FPS,
      severity: event.severity,
    };
  });
}

export function GalaxyFieldCanvas({
  reducedMotion,
  transients,
  frame,
  durationInFrames,
  forceFallback = false,
  freezeTimeSec = null,
}: GalaxyFieldCanvasProps) {
  const canvasRef = useRef<HTMLCanvasElement | null>(null);
  const rafRef = useRef<number>(0);
  const packetsRef = useRef<PacketState>(createPacketState());
  const startRef = useRef<number>(0);
  const lastRef = useRef<number>(0);
  const reducedRef = useRef(reducedMotion);
  const freezeRef = useRef(freezeTimeSec);
  const transientsRef = useRef(transients);
  const frameRef = useRef(frame);
  const durationRef = useRef(durationInFrames);
  const [canvasUnavailable, setCanvasUnavailable] = useState(forceFallback);

  reducedRef.current = reducedMotion;
  freezeRef.current = freezeTimeSec;
  transientsRef.current = transients;
  frameRef.current = frame;
  durationRef.current = durationInFrames;

  useEffect(() => {
    if (forceFallback) {
      setCanvasUnavailable(true);
      return;
    }

    const canvas = canvasRef.current;
    if (!canvas) return;

    const ctx = canvas.getContext("2d", { alpha: false, desynchronized: true });
    if (!ctx) {
      setCanvasUnavailable(true);
      return;
    }
    setCanvasUnavailable(false);

    const resize = () => {
      const raw = window.devicePixelRatio || 1;
      const backing = deviceBackingStore(FIELD_VIEW.width, FIELD_VIEW.height, raw);
      canvas.width = backing.width;
      canvas.height = backing.height;
      // setTransform ONCE on resize — paintGalaxyField uses logical 1200x760 coords.
      ctx.setTransform(backing.dpr, 0, 0, backing.dpr, 0, 0);
    };
    resize();

    packetsRef.current = createPacketState();
    startRef.current = performance.now();
    lastRef.current = startRef.current;

    const paintOnce = (t: number, dt: number) => {
      const started = performance.now();
      const events = toPaintEvents(
        transientsRef.current,
        frameRef.current,
        durationRef.current,
      );
      const freeze = freezeRef.current;
      const reduced = reducedRef.current;
      const time = freeze != null ? freeze : reduced ? 0 : t;
      if (!reduced && freeze == null) {
        stepPackets(packetsRef.current, time, dt, events);
      }
      if (reduced) {
        paintGalaxyField(ctx, time, { reducedMotion: true, events });
      } else if (freeze == null) {
        paintGalaxyField(ctx, time, { reducedMotion: false, events, packets: packetsRef.current });
      } else {
        paintGalaxyField(ctx, time, { reducedMotion: false, events });
      }
      const ms = performance.now() - started;
      canvas.dataset.fieldFrameMs = ms.toFixed(2);
      canvas.dataset.fieldDpr = String(Math.min(window.devicePixelRatio || 1, 2));
      canvas.dataset.fieldT = time.toFixed(3);
    };

    const tick = (now: number) => {
      if (document.hidden) {
        lastRef.current = now;
        rafRef.current = window.requestAnimationFrame(tick);
        return;
      }
      const t = (now - startRef.current) / 1000;
      const dt = Math.min(0.05, Math.max(0, (now - lastRef.current) / 1000));
      lastRef.current = now;
      paintOnce(t, dt);
      rafRef.current = window.requestAnimationFrame(tick);
    };

    paintOnce(freezeTimeSec ?? 0, 0);
    rafRef.current = window.requestAnimationFrame(tick);

    const onVis = () => {
      lastRef.current = performance.now();
    };
    const onResize = () => resize();
    document.addEventListener("visibilitychange", onVis);
    window.addEventListener("resize", onResize);

    return () => {
      document.removeEventListener("visibilitychange", onVis);
      window.removeEventListener("resize", onResize);
      if (rafRef.current) {
        window.cancelAnimationFrame(rafRef.current);
        rafRef.current = 0;
      }
    };
  }, [forceFallback, freezeTimeSec]);

  if (canvasUnavailable) {
    const t = freezeTimeSec ?? 0;
    return <GalaxyFieldSvgFallback paths={fieldFallbackPaths(t)} />;
  }

  return <canvas
    ref={canvasRef}
    className="galaxyFieldCanvas"
    width={FIELD_VIEW.width}
    height={FIELD_VIEW.height}
    data-field-frame-ms="0"
    data-field-dpr="1"
    data-field-t="0"
    aria-hidden="true"
  />;
}

