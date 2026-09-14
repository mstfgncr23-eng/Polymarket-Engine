import { FIELD_VIEW, type FallbackPath } from "../../lib/galaxy-field";

export function GalaxyFieldSvgFallback({ paths }: { paths: FallbackPath[] }) {
  return <svg
    className="galaxyFieldCanvas galaxyFieldSvgFallback"
    viewBox={`0 0 ${FIELD_VIEW.width} ${FIELD_VIEW.height}`}
    width="100%"
    height="100%"
    aria-hidden="true"
    data-galaxy-fallback="svg-field"
  >
    <rect x="0" y="0" width={FIELD_VIEW.width} height={FIELD_VIEW.height} fill="#050308" />
    <defs>
      <radialGradient id="galaxyFallbackCore" cx="50%" cy="49%" r="8%">
        <stop offset="0%" stopColor="#fff8e6" stopOpacity="0.95" />
        <stop offset="45%" stopColor="#ffb84a" stopOpacity="0.55" />
        <stop offset="100%" stopColor="#050308" stopOpacity="0" />
      </radialGradient>
    </defs>
    {paths.map((path, index) => {
      const d = path.points.map((p, i) => `${i === 0 ? "M" : "L"}${p.x.toFixed(1)} ${p.y.toFixed(1)}`).join(" ");
      const stroke = path.tier === "primary"
        ? "rgba(232, 168, 72, 0.7)"
        : path.tier === "secondary"
          ? "rgba(200, 140, 58, 0.4)"
          : "rgba(160, 110, 48, 0.18)";
      return <path
        key={`f-${path.family}-${index}`}
        d={`${d} Z`}
        fill="none"
        stroke={stroke}
        strokeWidth={path.width}
        strokeLinecap="round"
        strokeLinejoin="round"
        opacity={path.opacity}
        data-family={path.family}
        data-tier={path.tier}
      />;
    })}
    <circle cx={FIELD_VIEW.cx} cy={FIELD_VIEW.cy} r="72" fill="url(#galaxyFallbackCore)" />
    <circle cx={FIELD_VIEW.cx} cy={FIELD_VIEW.cy} r="7" fill="rgba(255,248,230,0.95)" />
  </svg>;
}
