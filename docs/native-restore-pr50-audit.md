# Native Restore: PR50 Market-Data Hot-Path Audit

Restore baseline: `0a148e2c7416dc5afc5700bb90c261d5d13902bc`

Source PR: #50, incremental `price_change` hot-path optimization.

## Verdict

KEEP conceptually, but do not copy the whole post-Astra `packages/market-data/src/polymarket.ts` file into the native branch.

The PR50 optimization is decision-authority independent, but the post-Astra file also contains unrelated changes from later architecture work. Extraction must therefore be patch-level and regression-driven.

## What PR50 changes

The optimization preserves the existing trusted-book semantics while reducing repeated O(n) work for incremental order-book updates.

Core changes:

- add side ownership flags (`bidsOwned`, `asksOwned`) to `BookState`
- stage each token once per source payload
- copy-on-write only the side actually mutated
- retain validation proof for unchanged levels
- freeze independent per-entry published arrays so source-order snapshot semantics remain unchanged
- skip a second full-book validation when the staged state already has a valid proof
- use published array lengths in normalization metrics

## Invariants that MUST survive extraction

1. malformed incremental data causes zero trusted partial mutation
2. stale/out-of-order timestamps are safe-dropped
3. one malformed entry in a payload invalidates/resyncs the affected token(s)
4. trusted publication remains atomic
5. unchanged sides may be shared only while they remain immutable to that staged state
6. a later mutation must copy the side before writing
7. every accepted source entry still emits its own immutable snapshot in source order
8. full-book messages still validate every level
9. insert/replace incremental levels still validate the changed level
10. deleting one already-trusted level does not force a re-walk of all unchanged levels

## Why whole-file copy is rejected

Comparing the pre-Astra baseline to current main shows `packages/market-data/src/polymarket.ts` accumulated substantially more than PR50 alone. PR50 itself is a focused optimization, but current-main whole-file replacement would silently import unrelated post-Astra behavior.

Therefore:

- do NOT copy current-main `polymarket.ts` wholesale
- do NOT make PR50 a prerequisite for proving the restored native economic chain
- first restore correctness, then extract the PR50 patch exactly with its tests

## Tests/benchmarks to carry with the optimization

- `tests/pr50-incremental-book-update.test.ts`
- relevant semantics from `tests/normalization-hot-path.bench.test.ts`
- `scripts/price-change-incremental-bench.ts`

Local benchmark numbers from the post-Astra branch are evidence for performance only, not proof of production behavior. The restored native engine must still receive a production-shaped CPU/event-loop check after deployment.

## Extraction status

- [x] source PR audited
- [x] confirmed authority-independent
- [x] confirmed whole-file post-Astra copy is unsafe
- [ ] exact `polymarket.ts` patch extracted onto native baseline
- [ ] PR50 regression tests carried
- [ ] benchmark carried
- [ ] production-shaped validation after native restore
