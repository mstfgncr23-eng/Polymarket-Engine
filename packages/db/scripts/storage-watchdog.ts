import postgres from "postgres";

const MiB = 1024 * 1024;

function envNumber(name: string, fallback: number): number {
  const raw = process.env[name];
  if (raw == null || raw.trim() === "") return fallback;
  const value = Number(raw);
  if (!Number.isFinite(value) || value < 0) throw new Error(`${name}_INVALID`);
  return value;
}

function envBoolean(name: string, fallback: boolean): boolean {
  const raw = process.env[name];
  if (raw == null || raw.trim() === "") return fallback;
  return raw === "1" || raw.toLowerCase() === "true";
}

type Snapshot = {
  atMs: number;
  databaseBytes: number;
  journalBytes: number;
  journalRows: number;
  journalSelectRows: number | null;
};

type Severity = "ok" | "warning" | "hard_stop";

const databaseUrl = process.env.DATABASE_URL;
if (!databaseUrl) throw new Error("DATABASE_URL_REQUIRED");

const pollMs = envNumber("STORAGE_WATCHDOG_POLL_MS", 60_000);
const growthWindowMs = envNumber("STORAGE_WATCHDOG_GROWTH_WINDOW_MS", 10 * 60_000);
const once = envBoolean("STORAGE_WATCHDOG_ONCE", false);

const dbWarnBytes = envNumber("STORAGE_WATCHDOG_DB_WARN_MB", 100) * MiB;
const dbHardBytes = envNumber("STORAGE_WATCHDOG_DB_HARD_MB", 250) * MiB;
const journalWarnBytes = envNumber("STORAGE_WATCHDOG_JOURNAL_WARN_MB", 75) * MiB;
const journalHardBytes = envNumber("STORAGE_WATCHDOG_JOURNAL_HARD_MB", 150) * MiB;
const growthWarnBytes = envNumber("STORAGE_WATCHDOG_GROWTH_WARN_MB", 10) * MiB;
const growthHardBytes = envNumber("STORAGE_WATCHDOG_GROWTH_HARD_MB", 25) * MiB;
const amplificationWarn = envNumber("STORAGE_WATCHDOG_READ_AMP_WARN", 5);
const amplificationHard = envNumber("STORAGE_WATCHDOG_READ_AMP_HARD", 20);
const minWritesForAmplification = envNumber("STORAGE_WATCHDOG_READ_AMP_MIN_WRITES", 10);

const sql = postgres(databaseUrl, {
  max: 1,
  idle_timeout: 5,
  connect_timeout: 10,
  prepare: false,
});

const history: Snapshot[] = [];
let stopping = false;

async function readJournalSelectRows(): Promise<number | null> {
  try {
    const rows = await sql<{ rows_read: string }[]>`
      select coalesce(sum(rows), 0)::bigint::text as rows_read
      from pg_stat_statements
      where query ilike '%journal_events%'
        and query ~* '^\\s*select'
    `;
    return Number(rows[0]?.rows_read ?? 0);
  } catch {
    return null;
  }
}

async function takeSnapshot(): Promise<Snapshot> {
  const db = await sql<{ database_bytes: string }[]>`
    select pg_database_size(current_database())::bigint::text as database_bytes
  `;
  const relation = await sql<{ relation_name: string | null }[]>`
    select to_regclass('public.journal_events')::text as relation_name
  `;

  let journalBytes = 0;
  let journalRows = 0;
  if (relation[0]?.relation_name) {
    const journal = await sql<{ journal_bytes: string; journal_rows: string }[]>`
      select
        pg_total_relation_size('public.journal_events'::regclass)::bigint::text as journal_bytes,
        (select count(*) from public.journal_events)::bigint::text as journal_rows
    `;
    journalBytes = Number(journal[0]?.journal_bytes ?? 0);
    journalRows = Number(journal[0]?.journal_rows ?? 0);
  }

  return {
    atMs: Date.now(),
    databaseBytes: Number(db[0]?.database_bytes ?? 0),
    journalBytes,
    journalRows,
    journalSelectRows: await readJournalSelectRows(),
  };
}

function baselineFor(current: Snapshot): Snapshot | null {
  const cutoff = current.atMs - growthWindowMs;
  let candidate: Snapshot | null = null;
  for (const item of history) {
    if (item.atMs > cutoff) break;
    candidate = item;
  }
  return candidate;
}

function classify(current: Snapshot, previous: Snapshot | null) {
  const reasons: string[] = [];
  let severity: Severity = "ok";

  const elevate = (next: Severity, reason: string) => {
    reasons.push(reason);
    if (next === "hard_stop" || (next === "warning" && severity === "ok")) severity = next;
  };

  if (current.databaseBytes >= dbHardBytes) elevate("hard_stop", "DATABASE_SIZE_HARD_LIMIT");
  else if (current.databaseBytes >= dbWarnBytes) elevate("warning", "DATABASE_SIZE_WARNING");

  if (current.journalBytes >= journalHardBytes) elevate("hard_stop", "JOURNAL_SIZE_HARD_LIMIT");
  else if (current.journalBytes >= journalWarnBytes) elevate("warning", "JOURNAL_SIZE_WARNING");

  let databaseGrowthBytes: number | null = null;
  let journalGrowthBytes: number | null = null;
  let writtenRows: number | null = null;
  let readRows: number | null = null;
  let readAmplification: number | null = null;

  if (previous) {
    databaseGrowthBytes = current.databaseBytes - previous.databaseBytes;
    journalGrowthBytes = current.journalBytes - previous.journalBytes;
    writtenRows = Math.max(0, current.journalRows - previous.journalRows);

    if (databaseGrowthBytes >= growthHardBytes || journalGrowthBytes >= growthHardBytes) {
      elevate("hard_stop", "GROWTH_RATE_HARD_LIMIT");
    } else if (databaseGrowthBytes >= growthWarnBytes || journalGrowthBytes >= growthWarnBytes) {
      elevate("warning", "GROWTH_RATE_WARNING");
    }

    if (current.journalSelectRows != null && previous.journalSelectRows != null && current.journalSelectRows >= previous.journalSelectRows) {
      readRows = current.journalSelectRows - previous.journalSelectRows;
      if (writtenRows >= minWritesForAmplification) {
        readAmplification = readRows / writtenRows;
        if (readAmplification >= amplificationHard) elevate("hard_stop", "JOURNAL_READ_AMPLIFICATION_HARD_LIMIT");
        else if (readAmplification >= amplificationWarn) elevate("warning", "JOURNAL_READ_AMPLIFICATION_WARNING");
      }
    }
  }

  return {
    severity,
    reasons,
    metrics: {
      databaseBytes: current.databaseBytes,
      databaseMiB: current.databaseBytes / MiB,
      journalBytes: current.journalBytes,
      journalMiB: current.journalBytes / MiB,
      journalRows: current.journalRows,
      journalSelectRows: current.journalSelectRows,
      growthWindowMs,
      databaseGrowthBytes,
      journalGrowthBytes,
      writtenRows,
      readRows,
      readAmplification,
    },
  };
}

async function sample(): Promise<Severity> {
  const current = await takeSnapshot();
  const previous = baselineFor(current);
  const result = classify(current, previous);
  history.push(current);
  const keepAfter = current.atMs - growthWindowMs * 2;
  while (history.length > 1 && history[0]!.atMs < keepAfter) history.shift();

  process.stdout.write(`${JSON.stringify({
    kind: "storage_watchdog",
    at: new Date(current.atMs).toISOString(),
    ...result,
  })}\n`);

  return result.severity;
}

async function shutdown(code = 0): Promise<never> {
  if (stopping) process.exit(code);
  stopping = true;
  await sql.end({ timeout: 2 });
  process.exit(code);
}

process.on("SIGINT", () => { void shutdown(130); });
process.on("SIGTERM", () => { void shutdown(143); });

try {
  for (;;) {
    const severity = await sample();
    if (severity === "hard_stop") {
      // Deliberately does not execute Docker/shell mutations itself. A canary
      // harness or operator should treat exit code 2 as an immediate stop gate.
      await shutdown(2);
    }
    if (once) await shutdown(severity === "warning" ? 1 : 0);
    await new Promise((resolve) => setTimeout(resolve, pollMs));
  }
} catch (error) {
  process.stderr.write(`${JSON.stringify({ kind: "storage_watchdog_error", message: error instanceof Error ? error.message : String(error) })}\n`);
  await shutdown(3);
}
