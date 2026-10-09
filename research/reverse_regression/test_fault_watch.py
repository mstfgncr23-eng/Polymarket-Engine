#!/usr/bin/env python3
import json
import pathlib
import tempfile
import unittest
import subprocess
import os
import sys
from fault_watch import Detector, decode, persist

BASE=1760000000000
def line(asset, millis, abort="false", kind="stale_feed"):
    return json.dumps({
        "time": millis, "feed": "reference:"+asset,
        "msg": "market feed error",
        "err": {"message":f"REFERENCE_CLOSE:kind={kind}:code=4000:wasClean=false:abort={abort}:reason=stale feed"}
    })

class StopOnFailureTest(unittest.TestCase):
    def test_one_or_six_refs_dont_stop(self):
        d=Detector()
        for i in range(6):
            self.assertIsNone(d.note(line(f"ASSET{i}",BASE+i)))
    def test_mass_ref_detects_and_stops(self):
        d=Detector()
        for i in range(7):
            r=d.note(line(f"ASSET{i}",BASE+i))
        self.assertEqual(r["reason"],"REF_MASS")
        self.assertEqual(len(r["detail"]["reference_assets"]),7)
        self.assertEqual(r["status"],"FAULT_DETECTED")
    def test_clob_coincidence(self):
        d=Detector()
        d.note(json.dumps({"time":BASE, "msg":"market feed error",
            "err":{"message":"CLOB_MARKET_WS_STALE"}}))
        for i in range(7):
            r=d.note(line(f"ASSET{i}",BASE+i+2))
        self.assertEqual(r["reason"],"REF_AND_CLOB")
        self.assertLessEqual(r["detail"]["clob_stale_delta_ms"],20)
    def test_abort_does_not_count(self):
        d=Detector()
        for i in range(9):
            self.assertIsNone(d.note(line(f"ASSET{i}",BASE+i,"true")))
    def test_db_timeout(self):
        d=Detector()
        r=d.note(json.dumps({"time":BASE,"msg":"critical journal append failed",
            "err":{"message":"write CONNECT_TIMEOUT polymarket-postgres:5432"}}))
        self.assertEqual(r["reason"],"DB_CONNECT_TIMEOUT")
    def test_docker_wrapper(self):
        obj={"time":BASE, "feed":"reference:BTC","msg":"market feed error"}
        self.assertEqual(decode(json.dumps({"log":json.dumps(obj),"time":"2026-10-09T00:00:00Z"})),obj)
    def test_fixture_catches_clob_after_reference_and_eighth_asset(self):
        fixture=pathlib.Path(__file__).parent/"fixtures/clob_after_reference.jsonl"
        with tempfile.TemporaryDirectory() as tmp:
            out=pathlib.Path(tmp)
            p=subprocess.run([sys.executable,str(pathlib.Path(__file__).parent/"fault_watch.py"),
                "--fixture",str(fixture),"--out",str(out)],
                capture_output=True,text=True,timeout=10)
            self.assertEqual(p.returncode,10,p.stdout+p.stderr)
            report=json.loads((out/"incident.json").read_text(encoding="utf8"))
            self.assertEqual(report["reason"],"REF_AND_CLOB")
            self.assertEqual(len(report["detail"]["reference_assets"]),8)
            self.assertEqual(len(report["evidence"]),9)

    def test_rejects_nonisolated_database_before_process_spawn(self):
        with tempfile.TemporaryDirectory() as tmp:
            out=pathlib.Path(tmp)/"evidence"
            env=os.environ.copy()
            env.update({
                "PM_FORENSIC_ENV":"isolated-paper",
                "NODE_ENV":"development","TRADING_MODE":"paper",
                "DATABASE_URL":"postgresql://fake:fake@not-local.example/polymarket",
            })
            proc=subprocess.run([sys.executable,str(pathlib.Path(__file__).parent/"fault_watch.py"),
                "--confirm-isolated-paper","--max-seconds","5","--out",str(out),
                "--",sys.executable,"-c","print('SHOULD_NOT_RUN')"],
                env=env,capture_output=True,text=True,timeout=10)
            self.assertNotEqual(proc.returncode,0)
            self.assertIn("LIVE_RUN_REFUSED",proc.stderr)
            self.assertFalse(out.exists())

    def test_out_path(self):
        with tempfile.TemporaryDirectory() as tmp:
            out=pathlib.Path(tmp)
            persist({"status":"INCONCLUSIVE_NO_TARGET","reason":None,"evidence":[]},out)
            self.assertTrue((out/"incident.json").exists())

if __name__=="__main__":
    unittest.main(verbosity=2)
