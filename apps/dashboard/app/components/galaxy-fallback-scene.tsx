import { buildGalaxySceneModel } from "../../lib/galaxy-scene";
import type { DashboardSnapshot } from "../../lib/types";
import { GalaxyOrbitalVisual } from "./galaxy-orbital-visual";

export function GalaxyFallbackScene({ snapshot }: { snapshot: DashboardSnapshot }) {
  const model = buildGalaxySceneModel(snapshot);
  return <div className="galaxyMap galaxyMap--fallback" aria-label="Fallback orbital map from market and reference data through the native decision core, risk, execution, and portfolio">
    <GalaxyOrbitalVisual
      model={model}
      transients={[]}
      frame={0}
      reducedMotion
      durationInFrames={1}
      ambient
    />
  </div>;
}
