from pathlib import Path

path = Path("apps/engine/src/engine.ts")
text = path.read_text()
old = '''        if (this.referenceUnreliable.has(asset) || this.triggerEngine.referenceWindowOverflowed(asset)) {
          this.triggerEngine.resetReferenceAfterReconnect(asset);
        this.nativeV2Shadow.resetReference(asset);
        }'''
new = '''        if (this.referenceUnreliable.has(asset) || this.triggerEngine.referenceWindowOverflowed(asset)) {
          this.triggerEngine.resetReferenceAfterReconnect(asset);
          this.nativeV2Shadow.resetReference(asset);
        }'''
if text.count(old) != 1:
    raise SystemExit(f"expected one formatting target, got {text.count(old)}")
path.write_text(text.replace(old, new, 1))
