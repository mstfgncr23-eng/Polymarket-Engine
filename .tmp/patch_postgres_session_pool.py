from pathlib import Path

path = Path('packages/db/src/journal.ts')
text = path.read_text()
old = '''    this.client = postgres(databaseUrl, {\n      max: 10,\n      idle_timeout: 20,\n      connect_timeout: 10,\n      onclose: () => undefined,\n      onnotice: () => undefined,\n'''
new = '''    this.client = postgres(databaseUrl, {\n      max: 10,\n      // This engine is a single long-lived VPS process. Recycling every idle\n      // connection after 20s forces repeated TCP/TLS/auth handshakes through\n      // the shared Supavisor session pooler and turns transient connect latency\n      // into journal degradation. Keep the application pool warm instead.\n      idle_timeout: 0,\n      // Keep startup bounded, but use postgres.js' normal 30s connect budget\n      // rather than the previous aggressive 10s cutoff.\n      connect_timeout: 30,\n      onclose: () => undefined,\n      onnotice: () => undefined,\n'''
if text.count(old) != 1:
    raise SystemExit(f'journal constructor match count={text.count(old)}')
path.write_text(text.replace(old, new, 1))

test = Path('tests/postgres-session-pool-config.test.ts')
test.write_text('''import { describe, expect, it } from "vitest";\nimport { readFileSync } from "node:fs";\nimport { resolve } from "node:path";\n\ndescribe("postgres journal session-pool stability", () => {\n  it("keeps long-lived VPS connections warm and retains a bounded connect timeout", () => {\n    const source = readFileSync(resolve("packages/db/src/journal.ts"), "utf8");\n    expect(source).toMatch(/max:\\s*10/);\n    expect(source).toMatch(/idle_timeout:\\s*0/);\n    expect(source).toMatch(/connect_timeout:\\s*30/);\n    expect(source).not.toMatch(/idle_timeout:\\s*20/);\n    expect(source).not.toMatch(/connect_timeout:\\s*10/);\n  });\n});\n''')
