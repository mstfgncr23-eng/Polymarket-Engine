#!/usr/bin/env python3
import json
import pathlib
import tempfile
import unittest
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
    def test_out_path(self):
        with tempfile.TemporaryDirectory() as tmp:
            out=pathlib.Path(tmp)
            persist({"status":"INCONCLUSIVE_NO_TARGET","reason":None,"evidence":[]},out)
            self.assertTrue((out/"incident.json").exists())

if __name__=="__main__":
    unittest.main(verbosity=2)
