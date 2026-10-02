#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import importlib.util
import json
import sys
import tempfile
import types
import unittest
from decimal import Decimal
from pathlib import Path

# Unit tests exercise pure/local observer logic without network access.
# aiohttp is intentionally stubbed so CI does not need to install runtime deps.
sys.modules.setdefault("aiohttp", types.ModuleType("aiohttp"))

HERE = Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location("guardian_shadow_24h", HERE / "guardian_shadow_24h.py")
assert SPEC and SPEC.loader
gs = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = gs
SPEC.loader.exec_module(gs)


def position() -> dict:
    return {
        "id": "pos-1",
        "marketId": "m",
        "upQuantity": "5",
        "downQuantity": "0",
        "averageUpCost": "0.85",
        "averageDownCost": "0",
        "pairedQuantity": "0",
        "unpairedOutcome": "UP",
        "unpairedQuantity": "5",
        "unhedgedStartedAt": "2026-01-01T00:00:00.000Z",
        "combinedPairCost": "0.85",
        "totalFees": "0.05",
        "totalSlippage": "0",
        "openedAt": "2026-01-01T00:00:00.000Z",
        "updatedAt": "2026-01-01T00:00:00.000Z",
    }


def full_book(token: str, bids: dict[str, str], asks: dict[str, str]) -> object:
    book = gs.Book(token)
    book.bids = dict(bids)
    book.asks = dict(asks)
    book.source_timestamp = "1767225600000"
    book.source_ms = 1767225600000
    book.received_at = gs.utc_now()
    book.received_mono_ns = gs.time.monotonic_ns()
    book.ws_generation = 1
    book.trusted = True
    return book


class ShadowCase(unittest.IsolatedAsyncioTestCase):
    def make_shadow(self):
        temp = tempfile.TemporaryDirectory()
        args = argparse.Namespace(
            engine_url="http://127.0.0.1:8080",
            engine_read_token="read",
            source_git_sha="a" * 40,
            output_dir=temp.name,
            duration_hours=24.0,
            settlement_grace_minutes=30.0,
        )
        shadow = gs.GuardianShadow(args)
        shadow.experiment_validated = True
        shadow.bound_experiment_id = "exp"
        shadow.experiment_id = "exp"
        shadow.experiment_status = "RUNNING"
        shadow.positions = {"m": position()}
        shadow.authority = {
            "m": {
                "position": position(),
                "normalHedgeAdmissible": False,
                "normalHedgeVetoes": [],
                "hedgeMakerOwnsResidual": False,
                "riskReductionInFlight": False,
            }
        }
        schedule = {
            "rate": "0",
            "exponent": 1,
            "takerOnly": True,
            "rebateRate": "0",
            "source": "test",
            "effectiveAt": "2026-01-01T00:00:00.000Z",
        }
        shadow.market_tokens = {"m": {"UP": "u", "DOWN": "d"}}
        shadow.token_meta = {
            "u": {"marketId": "m", "tokenId": "u", "outcome": "UP", "minOrderSize": "1", "feeSchedule": schedule},
            "d": {"marketId": "m", "tokenId": "d", "outcome": "DOWN", "minOrderSize": "1", "feeSchedule": schedule},
        }
        shadow.books = {
            "u": full_book("u", {"0.70": "100"}, {"0.71": "100"}),
            "d": full_book("d", {"0.29": "100"}, {"0.35": "100"}),
        }
        return temp, shadow

    async def close_shadow(self, temp, shadow):
        if shadow.latency_tasks:
            await asyncio.gather(*list(shadow.latency_tasks), return_exceptions=True)
        for writer in (
            shadow.engine_events, shadow.position_states, shadow.raw_clob,
            shadow.observations, shadow.actions, shadow.latency, shadow.ledger,
            shadow.book_checkpoints, shadow.integrity,
        ):
            if not writer.fh.closed:
                writer.close()
        temp.cleanup()

    async def test_over_cap_full_sell_records_helper(self):
        temp, shadow = self.make_shadow()
        try:
            async def revalidate(market, key, source):
                return shadow.positions[market], shadow.authority[market]
            async def no_latency(action, ms):
                return None
            shadow.revalidate_authority = revalidate
            shadow.capture_latency = no_latency

            econ = shadow.economics(shadow.positions["m"])
            self.assertGreaterEqual(Decimal(econ["mark_loss_usd"]), gs.TRIGGER_LOSS)
            self.assertLess(Decimal(econ["lock"]["guaranteed_locked_pnl"]), -gs.PAIR_LOCK_CAP)
            self.assertIsNotNone(econ["sell"])

            await shadow.evaluate_market("m", "unit")
            await asyncio.sleep(0)
            self.assertEqual(shadow.helper_actions, 1)
            action = json.loads((Path(temp.name) / "guardian_actions.jsonl").read_text().strip())
            self.assertEqual(action["failure_reason"], "PR3_CAP_OR_DEPTH_FAIL")
            self.assertEqual(action["episode_key"], gs.episode_key(position()))
            self.assertTrue(action["economics"]["sell"]["full"])
        finally:
            await self.close_shadow(temp, shadow)

    async def test_canonical_normal_priority_blocks_helper(self):
        temp, shadow = self.make_shadow()
        try:
            shadow.authority["m"]["normalHedgeAdmissible"] = True
            async def revalidate(market, key, source):
                return shadow.positions[market], shadow.authority[market]
            shadow.revalidate_authority = revalidate
            await shadow.evaluate_market("m", "unit")
            self.assertEqual(shadow.helper_actions, 0)
            self.assertEqual(shadow.last_status[gs.episode_key(position())], "NORMAL_HEDGE_PRIORITY")
        finally:
            await self.close_shadow(temp, shadow)

    async def test_affordable_full_pr3_blocks_helper(self):
        temp, shadow = self.make_shadow()
        try:
            shadow.books["d"] = full_book("d", {"0.09": "100"}, {"0.10": "100"})
            async def should_not_revalidate(*args):
                self.fail("affordable PR3 must suppress helper before authority revalidation")
            shadow.revalidate_authority = should_not_revalidate
            econ = shadow.economics(shadow.positions["m"])
            self.assertGreaterEqual(Decimal(econ["lock"]["guaranteed_locked_pnl"]), -gs.PAIR_LOCK_CAP)
            await shadow.evaluate_market("m", "unit")
            self.assertEqual(shadow.helper_actions, 0)
            self.assertEqual(shadow.last_status[gs.episode_key(position())], "PR3_AFFORDABLE")
        finally:
            await self.close_shadow(temp, shadow)

    async def test_partial_held_depth_never_invents_full_sell(self):
        temp, shadow = self.make_shadow()
        try:
            shadow.books["u"] = full_book("u", {"0.70": "1"}, {"0.71": "100"})
            econ = shadow.economics(shadow.positions["m"])
            self.assertIsNone(econ["sell"])
            await shadow.evaluate_market("m", "unit")
            self.assertEqual(shadow.helper_actions, 0)
            self.assertEqual(shadow.last_status[gs.episode_key(position())], "HELPER_NOT_EXECUTABLE")
        finally:
            await self.close_shadow(temp, shadow)

    async def test_recorded_rescue_error_hands_to_sell_even_if_lock_affordable(self):
        temp, shadow = self.make_shadow()
        try:
            shadow.books["d"] = full_book("d", {"0.09": "100"}, {"0.10": "100"})
            start = shadow.positions["m"]["unhedgedStartedAt"]
            identity = gs.rescue_episode_identity("m", start)
            shadow.rescue_failure_episodes[identity] = {
                "event_id": "e1",
                "event_type": "PAPER_EXECUTION_ERROR",
                "at": "2026-01-01T00:00:01.000Z",
                "order_id": "o1",
                "unhedged_started_at": start,
            }
            async def revalidate(market, key, source):
                return shadow.positions[market], shadow.authority[market]
            async def no_latency(action, ms):
                return None
            shadow.revalidate_authority = revalidate
            shadow.capture_latency = no_latency

            await shadow.evaluate_market("m", "rescue_execution_error")
            await asyncio.sleep(0)
            self.assertEqual(shadow.helper_actions, 1)
            action = json.loads((Path(temp.name) / "guardian_actions.jsonl").read_text().strip())
            self.assertEqual(action["failure_reason"], "RESCUE_EXECUTION_ERROR")
        finally:
            await self.close_shadow(temp, shadow)

    async def test_rescue_failure_never_carries_into_a_new_episode(self):
        temp, shadow = self.make_shadow()
        try:
            current = shadow.positions["m"]
            current_start = current["unhedgedStartedAt"]
            shadow.rescue_failure_episodes[gs.rescue_episode_identity("m", current_start)] = {
                "event_id": "e1",
                "event_type": "PAPER_EXECUTION_ERROR",
                "at": "2026-01-01T00:00:01.000Z",
                "order_id": "o1",
                "unhedged_started_at": current_start,
            }
            self.assertTrue(shadow.rescue_error_for_episode("m", current))

            newer = dict(current)
            newer["unhedgedStartedAt"] = "2026-01-01T00:01:00.000Z"
            newer["openedAt"] = "2026-01-01T00:01:00.000Z"
            self.assertFalse(shadow.rescue_error_for_episode("m", newer))
        finally:
            await self.close_shadow(temp, shadow)

    async def test_authority_poll_replays_newest_first_risk_feed_chronologically(self):
        temp, shadow = self.make_shadow()
        try:
            start = shadow.positions["m"]["unhedgedStartedAt"]
            state = {
                "paperMode": True,
                "experiment": {
                    "id": "exp",
                    "status": "RUNNING",
                    "durationHours": 24,
                    "gitSha": "a" * 40,
                    "closePolicy": "observe_until_settlement",
                    "startedAt": "2026-01-01T00:00:00.000Z",
                    "entryClosesAt": "2026-01-02T00:00:00.000Z",
                    "unresolvedPositionCount": 1,
                },
                "riskConfiguration": {
                    "maxOrderSizeUsd": 25,
                    "minExpectedEdge": 0.015,
                    "staleFeedTimeoutMs": 15000,
                    "catastrophicPairLockMinUnhedgedMs": 10000,
                    "catastrophicPairLockTriggerLossUsd": 0.35,
                    "catastrophicPairLockMaxLockedLossUsd": 0.60,
                },
                "positions": [{
                    "position": shadow.positions["m"],
                    "normalHedgeAdmissible": False,
                    "normalHedgeVetoes": [],
                    "hedgeMakerOwnsResidual": False,
                    "riskReductionInFlight": False,
                }],
                "tokenMap": list(shadow.token_meta.values()),
                # Engine feed order is newest-first: failure appears before create.
                "recentRiskEvents": [
                    {
                        "id": "failure",
                        "at": "2026-01-01T00:00:02.000Z",
                        "type": "RISK_REDUCTION_LIFECYCLE_BLOCKED",
                        "marketId": "m",
                        "detail": {"intent": "RESCUE_SECOND_LEG", "orderId": "o1"},
                    },
                    {
                        "id": "created",
                        "at": "2026-01-01T00:00:01.000Z",
                        "type": "PAPER_ORDER_CREATED",
                        "marketId": "m",
                        "detail": {
                            "intent": "RESCUE_SECOND_LEG",
                            "orderId": "o1",
                            "deRisk": {"unhedgedStartedAt": start},
                        },
                    },
                ],
            }
            await shadow.apply_guardian_state(state, "unit_authority_poll", evaluate=False)
            self.assertTrue(shadow.rescue_error_for_episode("m", shadow.positions["m"]))
            self.assertNotIn("RESCUE_FAILURE_EPISODE_UNBOUND", shadow.incomplete_reasons)
        finally:
            await self.close_shadow(temp, shadow)

    async def test_position_activation_without_trusted_books_invalidates_evidence(self):
        temp, shadow = self.make_shadow()
        try:
            shadow.positions = {}
            shadow.position_fingerprint = ""
            shadow.books["u"].trusted = False
            await shadow.set_positions({"m": position()}, "unit_activation", evaluate=False)
            self.assertIn("POSITION_ACTIVATED_WITHOUT_TRUSTED_BOOKS", shadow.incomplete_reasons)
        finally:
            await self.close_shadow(temp, shadow)

    async def test_latency_known_requires_new_full_sell_state_inside_25ms_window(self):
        temp, shadow = self.make_shadow()
        try:
            action = {
                "action_id": "a1",
                "market_id": "m",
                "position": shadow.positions["m"],
                "decision_mono_ns": gs.time.monotonic_ns(),
            }

            async def update_after_target():
                await asyncio.sleep(0.030)
                book = shadow.books["u"]
                book._stamp("1767225600030", 1)

            updater = asyncio.create_task(update_after_target())
            await shadow.capture_latency(action, 25)
            await updater
            rows = [
                json.loads(line)
                for line in (Path(temp.name) / "latency_samples.jsonl").read_text().splitlines()
                if line.strip()
            ]
            self.assertEqual(rows[-1]["status"], "KNOWN")
            self.assertTrue(rows[-1]["book_observed_within_25ms"])

            action2 = {
                "action_id": "a2",
                "market_id": "m",
                "position": shadow.positions["m"],
                "decision_mono_ns": gs.time.monotonic_ns(),
            }
            await shadow.capture_latency(action2, 1)
            rows = [
                json.loads(line)
                for line in (Path(temp.name) / "latency_samples.jsonl").read_text().splitlines()
                if line.strip()
            ]
            self.assertEqual(rows[-1]["status"], "UNKNOWN")
            self.assertIsNone(rows[-1]["economics"])
        finally:
            await self.close_shadow(temp, shadow)


class PureMathCase(unittest.TestCase):
    def test_completion_residue_gate_requires_zero_positions_and_reserved_cash(self):
        good_state = {"positions": [], "experiment": {"unresolvedPositionCount": 0}}
        good_dashboard = {"positions": [], "balance": {"reservedCash": "0"}}
        self.assertEqual(gs.completion_residue_reasons(good_state, good_dashboard), [])

        bad_state = {"positions": [{"marketId": "m"}], "experiment": {"unresolvedPositionCount": 1}}
        bad_dashboard = {"positions": [{"marketId": "m"}], "balance": {"reservedCash": "1.25"}}
        reasons = set(gs.completion_residue_reasons(bad_state, bad_dashboard))
        self.assertIn("FINAL_UNPAIRED_POSITIONS_REMAIN", reasons)
        self.assertIn("FINAL_UNRESOLVED_POSITIONS_REMAIN", reasons)
        self.assertIn("FINAL_OPEN_POSITIONS_REMAIN", reasons)
        self.assertIn("FINAL_RESERVED_CASH_REMAIN", reasons)

    def test_walk_skips_zero_price_and_enforces_notional_cap(self):
        book = full_book("x", {}, {"0": "100", "0.50": "100"})
        result = gs.walk(book, Decimal("10"), "BUY", Decimal("25"))
        self.assertTrue(result["full"])
        self.assertEqual(result["filled_qty"], "10.00000000")
        capped = gs.walk(book, Decimal("100"), "BUY", Decimal("25"))
        self.assertFalse(capped["full"])
        self.assertEqual(capped["notional"], "25.00000000")

    def test_fee_matches_taker_formula_and_rounding(self):
        schedule = {"rate": "0.25", "exponent": 2}
        value = gs.fee(Decimal("5"), Decimal("0.40"), schedule)
        expected = (Decimal("5") * Decimal("0.25") * (Decimal("0.40") * Decimal("0.60")) ** 2).quantize(
            Decimal("0.00001"), rounding=gs.ROUND_HALF_UP
        )
        self.assertEqual(value, expected)


if __name__ == "__main__":
    unittest.main()
