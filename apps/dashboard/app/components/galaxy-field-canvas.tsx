"use client";

import { useEffect, useRef } from "react";
import {
  FIELD_VIEW,
  createPacketState,
  paintGalaxyField,
  stepPackets,
  type FieldPaintEvent,
  type PacketState,
} from "../../lib/galaxy-field";
import type { GalaxyTransient } from "../../lib/galaxy-scene";
import { GALAXY_FPS } from "../../lib/galaxy-scene";

export type GalaxyFieldCanvasProps = {
  reducedMotion: boolean;
  transients: GalaxyTransient[];
  frame: number;
  durationInFrames: number;
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
}: GalaxyFieldCanvasProps) {
  const canvasRef = useRef<HTMLCanvasElement | null>(null);
  const rafRef = useRef<number>(0);
  const ctxRef = useRef<CanvasRenderingContext2D | null>(null);
  const packetsRef = useRef<PacketState>(createPacketState());
  const startRef = useRef<number>(0);
  const lastRef = useRef<number>(0);
  const reducedRef = useRef(reducedMotion);
  const transientsRef = useRef(transients);
  const frameRef = useRef(frame);
  const durationRef = useRef(durationInFrames);

  reducedRef.current = reducedMotion;
  transientsRef.current = transients;
  frameRef.current = frame;
  durationRef.current = durationInFrames;

  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;

    const ctx = canvas.getContext("2d", { alpha: false, desynchronized: true });
    if (!ctx) return;
    ctxRef.current = ctx;

    const resize = () => {
      const dpr = Math.min(window.devicePixelRatio || 1, 2);
      canvas.width = Math.round(FIELD_VIEW.width * dpr);
      canvas.height = Math.round(FIELD_VIEW.height * dpr);
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
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
      if (!reducedRef.current) {
        stepPackets(packetsRef.current, t, dt, events);
      }
      const paintOpts = reducedRef.current
        ? { reducedMotion: true as const, events }
        : { reducedMotion: false as const, events, packets: packetsRef.current };
      paintGalaxyField(ctx, t, paintOpts);
      const ms = performance.now() - started;
      canvas.dataset.fieldFrameMs = ms.toFixed(2);
    };

    const tick = (now: number) => {
      if (document.hidden) {
        lastRef.current = now;
        rafRef.current = window.requestAnimationFrame(tick);
        return;
      }
      const t = reducedRef.current ? 0 : (now - startRef.current) / 1000;
      const dt = Math.min(0.05, Math.max(0, (now - lastRef.current) / 1000));
      lastRef.current = now;
      paintOnce(t, dt);
      rafRef.current = window.requestAnimationFrame(tick);
    };

    paintOnce(0, 0);
    rafRef.current = window.requestAnimationFrame(tick);

    const onVis = () => {
      lastRef.current = performance.now();
    };
    document.addEventListener("visibilitychange", onVis);

    return () => {
      document.removeEventListener("visibilitychange", onVis);
      if (rafRef.current) {
        window.cancelAnimationFrame(rafRef.current);
        rafRef.current = 0;
      }
      ctxRef.current = null;
    };
  }, []);

  return <canvas
    ref={canvasRef}
    className="galaxyFieldCanvas"
    width={FIELD_VIEW.width}
    height={FIELD_VIEW.height}
    data-field-frame-ms="0"
    aria-hidden="true"
  />;
}
