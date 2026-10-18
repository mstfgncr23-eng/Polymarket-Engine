import { createWriteStream, mkdirSync, writeFileSync, renameSync } from "node:fs";
import { join } from "node:path";
import { createGzip, type Gzip } from "node:zlib";
import { finished } from "node:stream/promises";

/** Inside the sole Engine, using its existing normalized inputs. No subscriptions,
 * decisions, positions or orders. Bounded gzip backpressure is an explicit gap. */
export class RawEvidence {
  private stream: Gzip | null = null;
  private bucket = -1;
  private root = "";
  private experiment = "";
  private sealed = false;
  private closing: Promise<unknown>[] = [];
  private accepted = 0;
  private dropped = 0;
  private rawBytes = 0;
  private compressedBytes = 0;
  private fault: string | null = null;
  private gaps: { from: string; to: string; count: number }[] = [];
  private files: string[] = [];
  constructor(private readonly directory: string) {}
  status() { return { experimentId: this.experiment, accepted: this.accepted, dropped: this.dropped,
    rawBytes: this.rawBytes, compressedBytes: this.compressedBytes, fault: this.fault, gaps: this.gaps, files: this.files }; }
  record(experimentId: string, value: unknown): void {
    if (this.sealed && experimentId === this.experiment) return;
    const now = Date.now();
    try {
      if (experimentId !== this.experiment) {
        this.finishPart();
        this.experiment = experimentId;
        this.sealed = false;
        this.root = join(this.directory, "roi1-evidence", experimentId.replace(/[^a-zA-Z0-9_-]/g,"_"));
        mkdirSync(this.root, { recursive: true });
        this.bucket = -1; this.accepted = 0; this.dropped = 0; this.rawBytes = 0; this.compressedBytes = 0;
        this.gaps = []; this.files = []; this.fault = null;
      }
      if (this.fault || this.compressedBytes >= 2 * 1024 ** 3 || (this.stream?.writableLength ?? 0) > 8 * 1024 ** 2) {
        this.noteGap(now); return;
      }
      const bucket = Math.floor(now / 60000);
      if (!this.stream || bucket !== this.bucket) {
        this.finishPart(); this.bucket = bucket;
        const name = `${bucket}-${process.pid}.jsonl.gz`;
        const output = createWriteStream(join(this.root,name), { flags: "wx" });
        const gzip = createGzip({ level: 1 });
        this.stream = gzip; this.files.push(name);
        output.on("error",e => { this.fault = String(e); gzip.destroy(e); });
        gzip.on("error",e => { this.fault = String(e); });
        gzip.on("data", chunk => { this.compressedBytes += chunk.length; });
        gzip.pipe(output);
        const done = finished(output).catch(e => { this.fault = String(e); });
        this.closing.push(done);
        this.persistStatus();
      }
      const line = JSON.stringify({ recordedAt: new Date(now).toISOString(), ...value as object }) + "\n";
      const bytes = Buffer.byteLength(line);
      if (bytes > 4 * 1024 ** 2) { this.noteGap(now); return; }
      this.stream!.write(line); this.accepted++; this.rawBytes += bytes;
    } catch (error) { this.fault = String(error); this.noteGap(now); }
  }
  async close(): Promise<void> {
    this.sealed = true;
    this.finishPart(); await Promise.all(this.closing); this.closing = []; this.persistStatus();
  }
  private finishPart(): void { if (this.stream) { this.stream.end(); this.stream = null; } }
  private noteGap(now: number): void {
    this.dropped++;
    const at = new Date(now).toISOString(), last = this.gaps.at(-1);
    if (last && now - Date.parse(last.to) <= 1000) { last.to = at; last.count++; }
    else if (this.gaps.length < 1024) this.gaps.push({ from: at, to: at, count: 1 });
  }
  private persistStatus(): void {
    if (!this.root) return;
    try { const path = join(this.root,"capture-status.json"); writeFileSync(path+".tmp",JSON.stringify(this.status(),null,2)); renameSync(path+".tmp",path); }
    catch (error) { this.fault = String(error); }
  }
}
