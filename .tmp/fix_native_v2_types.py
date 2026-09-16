from pathlib import Path

path = Path("packages/core/src/native-v2-shadow-features.ts")
text = path.read_text()
old = '''  const sourceMs = Date.parse(snapshot.sourceTimestamp);
  const deltaSeconds = previous && sourceMs > previous.sourceMs ? (sourceMs - previous.sourceMs) / 1_000 : null;
  const spreadVelocityBpsPerSecond = deltaSeconds && deltaSeconds > 0
    ? (spreadBps - previous.features.spreadBps) / deltaSeconds
    : null;
  const midVelocityBpsPerSecond = deltaSeconds && deltaSeconds > 0 && previous.features.mid > 0
    ? (((mid / previous.features.mid) - 1) * 10_000) / deltaSeconds
    : null;'''
new = '''  const sourceMs = Date.parse(snapshot.sourceTimestamp);
  const previousFeatures = previous?.features ?? null;
  const deltaSeconds = previous && sourceMs > previous.sourceMs ? (sourceMs - previous.sourceMs) / 1_000 : null;
  const spreadVelocityBpsPerSecond = deltaSeconds && deltaSeconds > 0 && previousFeatures
    ? (spreadBps - previousFeatures.spreadBps) / deltaSeconds
    : null;
  const midVelocityBpsPerSecond = deltaSeconds && deltaSeconds > 0 && previousFeatures && previousFeatures.mid > 0
    ? (((mid / previousFeatures.mid) - 1) * 10_000) / deltaSeconds
    : null;'''
if text.count(old) != 1:
    raise SystemExit(f"expected one type-narrowing target, got {text.count(old)}")
path.write_text(text.replace(old, new, 1))
