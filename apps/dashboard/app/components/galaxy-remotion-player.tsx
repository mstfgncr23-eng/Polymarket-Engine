"use client";

import { useCallback, useEffect, useId, useMemo, useRef, useState } from "react";
import { Player, type ErrorFallback, type PlayerRef } from "@remotion/player";
import type { AnyZodObject } from "remotion";
import {
  advanceGalaxyMotionState,
  buildGalaxySceneModel,
  expireGalaxyMotionState,
  GALAXY_DURATION_IN_FRAMES,
  GALAXY_FPS,
  initializeGalaxyMotionState,
  MAX_GALAXY_TRANSIENTS,
} from "../../lib/galaxy-scene";
import type { DashboardSnapshot } from "../../lib/types";
import { GalaxyRemotionScene, type GalaxyRemotionSceneProps } from "./galaxy-remotion-scene";

function usePrefersReducedMotion(): boolean {
  const [reducedMotion, setReducedMotion] = useState(() => window.matchMedia("(prefers-reduced-motion: reduce)").matches);
  useEffect(() => {
    const media = window.matchMedia("(prefers-reduced-motion: reduce)");
    const update = () => setReducedMotion(media.matches);
    media.addEventListener("change", update);
    return () => media.removeEventListener("change", update);
  }, []);
  return reducedMotion;
}

function RemotionFailure({ error, onError }: { error: Error; onError: (error: Error) => void }) {
  useEffect(() => onError(error), [error, onError]);
  return <div className="remotionFailure">Switching to the static system map…</div>;
}

export function GalaxyRemotionPlayer({ snapshot, onError, onReady }: {
  snapshot: DashboardSnapshot;
  onError: (error: Error) => void;
  onReady: () => void;
}) {
  const playerRef = useRef<PlayerRef>(null);
  const instanceId = useId();
  const reducedMotion = usePrefersReducedMotion();
  const [motion, setMotion] = useState(() => initializeGalaxyMotionState(snapshot, 0, Date.now()));

  useEffect(() => {
    const frame = playerRef.current?.getCurrentFrame() ?? 0;
    setMotion((current) => advanceGalaxyMotionState(current, snapshot, frame, Date.now()));
  }, [snapshot]);

  const hasTransients = motion.transients.length > 0;
  useEffect(() => {
    if (!hasTransients) return;
    const timer = window.setInterval(() => setMotion((current) => expireGalaxyMotionState(current, Date.now())), 750);
    return () => window.clearInterval(timer);
  }, [hasTransients]);

  useEffect(() => onReady(), [onReady]);

  const model = useMemo(() => buildGalaxySceneModel(snapshot), [snapshot]);
  const inputProps = useMemo<GalaxyRemotionSceneProps>(() => ({
    model,
    transients: motion.transients,
    reducedMotion,
    durationInFrames: GALAXY_DURATION_IN_FRAMES,
  }), [model, motion.transients, reducedMotion]);
  const renderError = useCallback<ErrorFallback>(({ error }) => <RemotionFailure error={error} onError={onError} />, [onError]);

  return <div
    className="galaxyPlayerShell"
    data-galaxy-player-instance={instanceId}
    data-galaxy-transient-count={motion.transients.length}
    data-galaxy-transient-cap={MAX_GALAXY_TRANSIENTS}
  >
    <Player<AnyZodObject, GalaxyRemotionSceneProps>
      ref={playerRef}
      component={GalaxyRemotionScene}
      inputProps={inputProps}
      durationInFrames={GALAXY_DURATION_IN_FRAMES}
      compositionWidth={1200}
      compositionHeight={760}
      fps={GALAXY_FPS}
      autoPlay={!reducedMotion}
      initiallyMuted
      loop
      controls={false}
      clickToPlay={false}
      allowFullscreen={false}
      numberOfSharedAudioTags={0}
      errorFallback={renderError}
      className="galaxyPlayer"
      style={{ width: "100%", height: "100%" }}
    />
    <span className="galaxyRuntime" aria-hidden="true">PLAYER LIVE · {motion.transients.length}/{MAX_GALAXY_TRANSIENTS} SIGNALS · {reducedMotion ? "REDUCED MOTION" : "FRAME DRIVEN"}</span>
  </div>;
}
