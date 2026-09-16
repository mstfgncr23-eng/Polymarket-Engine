from pathlib import Path


def once(text: str, old: str, new: str, label: str) -> str:
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"{label}: expected one match, got {count}")
    return text.replace(old, new, 1)

store = Path("apps/engine/src/native-v2-shadow-store.ts")
text = store.read_text()
text = once(
    text,
    '''  noteObserved(): void {\n    this.observed += 1;\n  }\n''',
    '''  reset(): void {\n    this.slots.fill(undefined);\n    this.cursor = 0;\n    this.sizeValue = 0;\n    this.sequence = 0;\n    this.observed = 0;\n    this.evaluated = 0;\n    this.wouldSubmit = 0;\n    this.vetoed = 0;\n    this.preflightBlocked = 0;\n    this.droppedBusy = 0;\n    this.riskVetoes.clear();\n    this.preflightBlockers.clear();\n  }\n\n  noteObserved(): void {\n    this.observed += 1;\n  }\n''',
    "store reset",
)
store.write_text(text)

engine = Path("apps/engine/src/engine.ts")
text = engine.read_text()
text = once(
    text,
    '''    const selectedAtDecision = input.opportunity.best;\n    if (input.ranking.marketRank !== 1 || !selectedAtDecision?.actionable) return null;\n\n    this.nativeV2ShadowStore.noteObserved();\n    if (this.nativeV2ShadowExecutionBusy) {\n      this.nativeV2ShadowStore.noteDroppedBusy();\n      return null;\n    }\n\n    const now = this.nowFn();\n''',
    '''    const selectedAtDecision = input.opportunity.best;\n    if (input.ranking.marketRank !== 1 || !selectedAtDecision?.actionable) return null;\n\n    const now = this.nowFn();\n    // Phase-4 data is scoped to a real entry-capable experiment. Do not fill\n    // the bounded RAM export with idle or post-entry-close observations.\n    if (!experimentAllowsFirstLeg(this.experiment, now)) return null;\n    this.nativeV2ShadowStore.noteObserved();\n    if (this.nativeV2ShadowExecutionBusy) {\n      this.nativeV2ShadowStore.noteDroppedBusy();\n      return null;\n    }\n\n''',
    "engine running-only gate",
)
text = once(
    text,
    '''        this.crashInjector.maybeCrash("AFTER_START_VALIDATED_BEFORE_RUNTIME_ADMISSION", `experiment:${next.id}:start_validated`);\n        this.syncExperimentWindow();''',
    '''        this.nativeV2ShadowStore.reset();\n        this.crashInjector.maybeCrash("AFTER_START_VALIDATED_BEFORE_RUNTIME_ADMISSION", `experiment:${next.id}:start_validated`);\n        this.syncExperimentWindow();''',
    "engine per-experiment reset",
)
engine.write_text(text)
