import type { EventJournal, JournalAccessMode } from "@pm/db";
import type { GuardianCheckpointReadSource } from "./guardian-readonly-checkpoint.js";
import type { GuardianDurableReadSource } from "./guardian-readonly-durable-feed.js";

/**
 * G2H actual EventJournal API bridge, never a second writer.
 *
 * Construct EventJournal with `access:"readonly"` and connectWithoutMigrate
 * from the future isolated G2I process. A database credential must also have
 * SELECT-only permissions; the in-process mode alone is not an ACL.
 *
 * We intentionally do NOT accept unscoped EventJournal as an exported
 * dependency with mutation methods. All reads recheck capability mode.
 */
export function guardianReadOnlyJournalSource(
  journal: Pick<EventJournal, "listVerifiedCheckpoints" | "latestDurableSequence" | "readDurableTransitionsAfter"> & {
    readonly access: JournalAccessMode;
  },
): GuardianCheckpointReadSource & GuardianDurableReadSource {
  const guard=():void=>{
    if (journal.access!=="readonly") throw new Error("GUARDIAN_SOURCE_REQUIRES_READONLY_JOURNAL");
  };
  guard();
  if (typeof journal.listVerifiedCheckpoints!=="function"
    || typeof journal.latestDurableSequence!=="function"
    || typeof journal.readDurableTransitionsAfter!=="function")
    throw new Error("GUARDIAN_SOURCE_REQUIRES_VERIFIED_CHECKPOINT_API");
  return Object.freeze({
    listVerifiedCheckpoints:async()=>{
      guard();
      return journal.listVerifiedCheckpoints!();
    },
    latestDurableSequence:async()=>{
      guard();
      return journal.latestDurableSequence();
    },
    readDurableTransitionsAfter:async(cursor:number,limit:number)=>{
      guard();
      if (!Number.isSafeInteger(cursor)||cursor<0||!Number.isSafeInteger(limit)||limit<1||limit>512)
        throw new Error("GUARDIAN_SOURCE_INVALID_READ_BOUNDS");
      return journal.readDurableTransitionsAfter(cursor,limit);
    },
  });
}
