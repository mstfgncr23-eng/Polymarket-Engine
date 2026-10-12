"""Current Engine scope tests for the existing forced-command verifier."""
import importlib.util, tempfile, pathlib, json, hashlib, unittest, sys
sys.dont_write_bytecode = True
path = pathlib.Path(__file__).parents[1] / "deploy/guardian/ovh-retention-verify.py"
spec = importlib.util.spec_from_file_location("ovh_verify", path)
verifier = importlib.util.module_from_spec(spec); spec.loader.exec_module(verifier)

class Verification(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = pathlib.Path(self.tmp.name)
        self.folder = self.root / "simple-engine/journal/raw_telemetry/bucket_20261005"
        self.folder.mkdir(parents=True)
        self.event = {"type":"MARKET_SNAPSHOT","occurredAt":"2026-10-05T10:30:00.000Z","recordedAt":"2026-10-05T10:30:00.000Z","commitSeq":2,"payload":{}}
        self.manifest = {"kind":"journal_bucket","retentionClass":"raw_telemetry", "eventTypes":["MARKET_SNAPSHOT"],
            "occurredFrom":"2026-10-05T10:00:00.000Z","occurredBefore":"2026-10-05T11:00:00.000Z",
            "commitSeqTo":2,"recordedAtTo":"2026-10-05T11:00:00.000Z","unattributedOnly":True}
    def tearDown(self): self.tmp.cleanup()
    def write(self, name, value):
        data = value if isinstance(value, bytes) else (json.dumps(value)+"\n").encode()
        (self.folder/name).write_bytes(data)
        return {"file":name,"bytes":len(data),"sha256":hashlib.sha256(data).hexdigest()}
    def proof(self):
        raw=self.write("events.jsonl",self.event);raw["eventCount"]=1
        self.manifest["rawEvents"]=raw;self.write("manifest.json",self.manifest)
        return self.request("journal_bucket",("manifest.json","events.jsonl"))
    def request(self,kind,names):
        files={}
        for name in names:
            b=(self.folder/name).read_bytes();files[name]={"bytes":len(b),"sha256":hashlib.sha256(b).hexdigest()}
        return {"source":"polymarket","kind":kind,"directory":str(self.folder),"files":files}
    def denied(self,request):
        with self.assertRaises(SystemExit) as error: verifier.verify(request,self.root)
        self.assertEqual(error.exception.code,78)
    def test_current_engine_manifest_count_scope_and_sha(self):
        proof=self.proof();r=verifier.verify(proof,self.root)
        self.assertEqual(r["fileCount"],2);self.assertEqual(r["source"],"polymarket")
    def test_legacy_probe_compatibility(self):
        self.folder=self.root/"probe-legacy";self.folder.mkdir()
        self.write("manifest.json",b"legacy probe\n");self.write("events.jsonl",b"probe\n")
        proof=self.request("journal_bucket",("manifest.json","events.jsonl"));proof["source"]="polymarket_g4_paper_20261012"
        self.assertEqual(verifier.verify(proof,self.root)["fileCount"],2)
    def test_corrupt_remote_bytes(self):
        proof=self.proof();(self.folder/"events.jsonl").write_bytes(b"corrupt")
        self.denied(proof)
    def test_hash_mismatch(self):
        proof=self.proof();proof["files"]["events.jsonl"]["sha256"]="0"*64;self.denied(proof)
    def test_manifest_count_mismatch(self):
        proof=self.proof();self.manifest["rawEvents"]["eventCount"]=2;self.write("manifest.json",self.manifest)
        self.denied(self.request("journal_bucket",("manifest.json","events.jsonl")))
    def test_row_cursor_outside_fence(self):
        self.event["commitSeq"]=3;self.denied(self.proof())
    def test_row_recorded_at_outside_fence(self):
        self.event["recordedAt"]="2026-10-06T10:00:00.000Z";self.denied(self.proof())
    def test_active_attributed_rows_not_in_unattributed_bucket(self):
        self.event["payload"]["experimentId"]="experiment_active";self.denied(self.proof())
    def test_engine_scope_cannot_alias_legacy_folder(self):
        proof=self.proof();proof["directory"]=str(self.root/"probe-legacy");self.denied(proof)
    def test_verified_read_file_reuses_same_channel(self):
        proof=self.proof();proof["readFile"]="events.jsonl";verifier.verify(proof,self.root)
        proof["readFile"]="../../secret";self.denied(proof)
    def recovery(self, sequences):
        self.folder=self.root/"simple-engine/recovery/bundle_0001";self.folder.mkdir(parents=True)
        cp=self.write("checkpoints.json",[{"id":"cp1","payload":{"durableCursor":2,"checkpointContentHash":"h"}},
            {"id":"cp2","payload":{"durableCursor":3,"checkpointContentHash":"h"}}])
        data="".join(json.dumps({"sequence":n,"operationKey":"op"+str(n),"payloadHash":"hash"})+"\n" for n in sequences).encode()
        rows=self.write("durable-transitions.jsonl",data);rows["eventCount"]=len(sequences)
        self.write("manifest.json",{"kind":"engine_recovery","replayFromCursor":2,"throughCursor":4,"prefixThroughCursor":1,
            "checkpoints":cp,"durableTransitions":rows})
        return self.request("engine_recovery",("manifest.json","checkpoints.json","durable-transitions.jsonl"))
    def test_recovery_cursor_tail(self): self.assertEqual(verifier.verify(self.recovery([1,3,4]),self.root)["fileCount"],3)
    def test_recovery_gap_rejected(self): self.denied(self.recovery([1,4]))

if __name__=="__main__": unittest.main()
