import { AbsoluteFill, useCurrentFrame } from "remotion";
import type { GalaxySceneModel, GalaxyTransient } from "../../lib/galaxy-scene";
import { GalaxyOrbitalVisual } from "./galaxy-orbital-visual";

export type GalaxyRemotionSceneProps = {
  model: GalaxySceneModel;
  transients: GalaxyTransient[];
  reducedMotion: boolean;
  durationInFrames: number;
};

export function GalaxyRemotionScene({ model, transients, reducedMotion, durationInFrames }: GalaxyRemotionSceneProps) {
  const currentFrame = useCurrentFrame();
  const frame = reducedMotion ? 0 : currentFrame;
  return <AbsoluteFill className="remotionGalaxyScene">
    <GalaxyOrbitalVisual
      model={model}
      transients={transients}
      frame={frame}
      reducedMotion={reducedMotion}
      durationInFrames={durationInFrames}
    />
  </AbsoluteFill>;
}
