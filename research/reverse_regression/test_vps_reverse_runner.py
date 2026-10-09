import json
import unittest
from vps_reverse_runner import IMAGES, container_arguments, find_reference_cluster, parse_events

def log(at, asset, abort="false"):
    return json.dumps({"time":at,"feed":"reference:"+asset,
      "err":{"message":f"REFERENCE_CLOSE:kind=stale_feed:code=4000:wasClean=false:abort={abort}:reason=stale feed"}})

class ForensicStageRunnerTest(unittest.TestCase):
    def test_expected_stages(self):
        self.assertEqual(len(IMAGES),8)
        self.assertIn("current",IMAGES)
        self.assertIn("safe",IMAGES)

    def test_no_prod_db_network_mount_or_port(self):
        args=container_arguments("current","pmf-reg-test")
        joined=" ".join(args)
        for forbidden in ("polymarket-runtime","polymarket-postgres","/data/archive","--publish","--privileged","ENGINE_OPERATOR_TOKEN"):
            self.assertNotIn(forbidden,joined)
        self.assertNotIn("--cpus",args)
        self.assertIn("JOURNAL_MODE=memory",args)
        self.assertIn("TRADING_MODE=paper",args)
        self.assertIn("--cpuset-cpus",args)
        self.assertIn("--cpu-shares",args)
        self.assertIn("--memory",args)

    def test_mass_7(self):
        sample="\n".join(log(1000+i,f"A{i}") for i in range(7))
        fault=find_reference_cluster(parse_events(sample))
        self.assertEqual(len(fault["assets"]),7)
        self.assertLessEqual(fault["spread_ms"],1000)

    def test_duplicate_asset_not_mass(self):
        sample="\n".join(log(1000+i,"BTC") for i in range(9))
        self.assertIsNone(find_reference_cluster(parse_events(sample)))

    def test_abort_ignored(self):
        sample="\n".join(log(1000+i,f"A{i}",abort="true") for i in range(9))
        self.assertIsNone(find_reference_cluster(parse_events(sample)))

    def test_outside_window_not_mass(self):
        sample="\n".join(log(1000+i*2000,f"A{i}") for i in range(8))
        self.assertIsNone(find_reference_cluster(parse_events(sample)))

    def test_clob_stale(self):
        sample=json.dumps({"time":1000,"err":{"message":"CLOB_MARKET_WS_STALE"}})
        self.assertEqual(parse_events(sample)[0]["kind"],"CLOB_STALE")

if __name__=="__main__":
    unittest.main(verbosity=2)
