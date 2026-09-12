import type { ReactNode } from "react";
import type { Tone } from "../../lib/dashboard-selectors";

export function Panel({ title, kicker, meta, children, className = "" }: { title: string; kicker?: string; meta?: string; children: ReactNode; className?: string }) {
  return <section className={`panel ${className}`}><div className="panelHeader"><div>{kicker && <p className="panelKicker">{kicker}</p>}<h2>{title}</h2></div>{meta && <span className="panelMeta">{meta}</span>}</div>{children}</section>;
}

export function StatusPill({ label, tone = "neutral", pulse = false }: { label: string; tone?: Tone; pulse?: boolean }) {
  return <span className={`statusPill statusPill--${tone}${pulse ? " statusPill--pulse" : ""}`}><i />{label}</span>;
}

export function Metric({ label, value, detail, tone = "neutral" }: { label: string; value: string; detail?: string; tone?: Tone }) {
  return <div className={`metric metric--${tone}`}><span>{label}</span><strong>{value}</strong>{detail && <small>{detail}</small>}</div>;
}

export function EmptyState({ title = "Awaiting data", detail }: { title?: string; detail?: string }) {
  return <div className="emptyState"><span className="emptyOrb" /><strong>{title}</strong>{detail && <p>{detail}</p>}</div>;
}

export function DataValue({ label, value, detail }: { label: string; value: string; detail?: string }) {
  return <div className="dataValue"><span>{label}</span><strong>{value}</strong>{detail && <small>{detail}</small>}</div>;
}
