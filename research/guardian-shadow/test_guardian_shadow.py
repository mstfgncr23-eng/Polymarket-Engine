#!/usr/bin/env python3
"""Passive recording tests. Policy/execution coverage lives in engine tests."""
import argparse
import asyncio
import json
import tempfile
import unittest
from pathlib import Path

from guardian_shadow_24h import Jsonl, PassiveRecorder, active_tokens


class PassiveRecorderTests(unittest.IsolatedAsyncioTestCase):
    def test_selects_raw_recording_scope_only_from_engine_positions(self):
        state = {"positions": [{"position": {"marketId": "open"}}], "tokenMap": [
            {"marketId": "open", "tokenId": "up"}, {"marketId": "open", "tokenId": "down"},
            {"marketId": "closed", "tokenId": "other"}]}
        self.assertEqual(active_tokens(state), {"up", "down"})
        state["positions"] = []
        self.assertEqual(active_tokens(state), set())

    def test_records_unmodified_raw_evidence_without_numeric_policy(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "raw.jsonl"
            sink = Jsonl(path)
            frame = {"received_ns": 123, "raw": '{"asset_id":"up","price":"0.25"}'}
            sink.write(frame)
            sink.close()
            self.assertEqual(json.loads(path.read_text()), frame)

    async def test_only_read_allowlisted_engine_paths(self):
        with tempfile.TemporaryDirectory() as directory:
            recorder = PassiveRecorder(argparse.Namespace(output_dir=directory, source_git_sha="test",
                engine_url="http://engine", engine_read_token="read", duration_hours=1))
            class Session:
                def __init__(self): self.calls = []
                async def get(self, url, **kwargs): self.calls.append((url, kwargs)); return "response"
            session = Session()
            try:
                self.assertEqual(await recorder.get(session, "/api/shadow/guardian-state"), "response")
                self.assertEqual(session.calls[0][0], "http://engine/api/shadow/guardian-state")
                self.assertEqual(session.calls[0][1]["headers"], {"Authorization": "Bearer read"})
                with self.assertRaisesRegex(ValueError, "READ_ENDPOINT_NOT_ALLOWED"):
                    await recorder.get(session, "/api/operator/start")
                self.assertEqual(len(session.calls), 1)
                manifest = json.loads((Path(directory) / "manifest.json").read_text())
                self.assertFalse(manifest["counterfactual_policy"])
                self.assertEqual(manifest["policy_authority"], "Engine Guardian")
            finally:
                for sink in (recorder.raw, recorder.runtime, recorder.events, recorder.diagnostics): sink.close()

    async def test_refuses_non_paper_runtime(self):
        with tempfile.TemporaryDirectory() as directory:
            recorder = PassiveRecorder(argparse.Namespace(output_dir=directory, source_git_sha="test",
                engine_url="http://engine", engine_read_token="read", duration_hours=1))
            class Response:
                def raise_for_status(self): pass
                async def json(self): return {"paperMode": False}
                async def __aenter__(self): return self
                async def __aexit__(self, *args): pass
            class Session:
                async def get(self, *args, **kwargs): return Response()
            try:
                with self.assertRaisesRegex(RuntimeError, "PAPER_RUNTIME_REQUIRED"):
                    await asyncio.wait_for(recorder.poll(Session()), timeout=1)
            finally:
                for sink in (recorder.raw, recorder.runtime, recorder.events, recorder.diagnostics): sink.close()


if __name__ == "__main__":
    unittest.main()
