import type { ReactNode } from "react";
import type { Tone } from "../../lib/dashboard-selectors";
import { StatusPill } from "./primitives";

export function SystemNode({ id, eyebrow, title, status, tone, metric, detail, pulse, children }: {
  id: string;
  eyebrow: string;
  title: string;
  status: string;
  tone: Tone;
  metric?: string;
  detail?: string;
  pulse?: string | null;
  children?: ReactNode;
}) {
  return <article className={`systemNode systemNode--${tone}`} aria-labelledby={`${id}-title`}>
    {pulse && <span className="nodePulse" key={pulse} aria-hidden="true" />}
    <div className="nodeTopline"><span className="nodeEyebrow">{eyebrow}</span><StatusPill label={status} tone={tone} /></div>
    <h3 id={`${id}-title`}>{title}</h3>
    {metric && <strong className="nodeMetric">{metric}</strong>}
    {detail && <p className="nodeDetail">{detail}</p>}
    {children}
  </article>;
}
