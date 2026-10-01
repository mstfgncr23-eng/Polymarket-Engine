#!/usr/bin/env python3
"""Read-only forward observer for the locked two-stage Guardian PAPER policy."""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import signal
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP, getcontext
from pathlib import Path
from typing import Any

import aiohttp

getcontext().prec = 40
TRIGGER_LOSS = Decimal("0.35")
PAIR_LOCK_CAP = Decimal("0.60")
STALE_BOOK_SECONDS = 15.0
LATENCY_MS = (25, 50, 100, 150, 250, 500)
CLOB_WS = "wss://ws-subscriptions-clob.polymarket.com/ws/market"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def D(value: Any) -> Decimal:
    return Decimal(str(value))


def dec(value: Decimal, places: int = 8) -> str:
    q = Decimal(1).scaleb(-places)
    return format(value.quantize(q, rounding=ROUND_HALF_UP), "f")


def compact_json(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"), sort_keys=True, ensure_ascii=False)


class Jsonl:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.fh = self.path.open("a", encoding="utf-8", buffering=1)

    def write(self, value: dict[str, Any]) -> None:
        self.fh.write(compact_json(value) + "\n")

    def close(self) -> None:
        self.fh.flush()
        os.fsync(self.fh.fileno())
        self.fh.close()


@dataclass
class Book:
    token_id: str
    bids: dict[str, str] = field(default_factory=dict)
    asks: dict[str, str] = field(default_factory=dict)
    source_timestamp: str | None = None
    received_at: str | None = None
    received_mono_ns: int = 0
    ws_generation: int = 0
    trusted: bool = False

    def set_full(self, raw: dict[str, Any], generation: int) -> None:
        self.bids = {
            str(x.get("price")): str(x.get("size", x.get("quantity", "0")))
            for x in raw.get("bids", [])
            if x.get("price") is not None
        }
        self.asks = {
            str(x.get("price")): str(x.get("size", x.get("quantity", "0")))
            for x in raw.get("asks", [])
            if x.get("price") is not None
        }
        self._stamp(raw.get("timestamp"), generation)
        self.trusted = True

    def change(self, side: str, price: str, size: str, source_ts: Any, generation: int) -> None:
        levels = self.bids if side == "BUY" else self.asks
        if D(size) <= 0:
            levels.pop(price, None)
        else:
            levels[price] = size
        self._stamp(source_ts, generation)

    def _stamp(self, source_ts: Any, generation: int) -> None:
        self.source_timestamp = str(source_ts) if source_ts is not None else None
        self.received_at = utc_now()
        self.received_mono_ns = time.monotonic_ns()
        self.ws_generation = generation

    def age_ms(self) -> float:
        if self.received_mono_ns <= 0:
            return float("inf")
        return (time.monotonic_ns() - self.received_mono_ns) / 1_000_000.0

    def levels(self, side: str) -> list[tuple[Decimal, Decimal]]:
        src = self.bids if side == "SELL" else self.asks
        return sorted(
            ((D(p), D(q)) for p, q in src.items() if D(q) > 0),
            key=lambda x: x[0],
            reverse=(side == "SELL"),
        )


def walk(book: Book, qty: Decimal, side: str) -> dict[str, Any]:
    remaining = qty
    notional = Decimal(0)
    worst: Decimal | None = None
    levels_used = 0
    for price, available in book.levels(side):
        take = min(remaining, available)
        if take <= 0:
            continue
        notional += take * price
        remaining -= take
        worst = price
        levels_used += 1
        if remaining <= 0:
            break
    filled = qty - remaining
    return {
        "requested_qty": dec(qty),
        "filled_qty": dec(filled),
        "notional": dec(notional),
        "vwap": dec(notional / filled) if filled > 0 else "0",
        "worst_price": dec(worst) if worst is not None else None,
        "full": remaining == 0,
        "levels_used": levels_used,
    }


def fee(qty: Decimal, price: Decimal, schedule: dict[str, Any] | None) -> Decimal | None:
    if not schedule:
        return None
    rate = D(schedule.get("rate", "0"))
    exponent = int(schedule.get("exponent", 1))
    value = qty * rate * ((price * (Decimal(1) - price)) ** exponent)
    if value < Decimal("0.000005"):
        return Decimal(0)
    return value.quantize(Decimal("0.00001"), rounding=ROUND_HALF_UP)


def episode_key(position: dict[str, Any]) -> str:
    return "|".join([
        str(position.get("marketId")),
        str(position.get("unpairedOutcome")),
        str(position.get("unhedgedStartedAt") or position.get("openedAt")),
    ])


class GuardianShadow:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.base = args.engine_url.rstrip("/")
        self.headers = {"Authorization": f"Bearer {args.engine_read_token}"}
        self.out = Path(args.output_dir)
        self.out.mkdir(parents=True, exist_ok=True)

        self.engine_events = Jsonl(self.out / "engine_events.jsonl")
        self.position_states = Jsonl(self.out / "position_states.jsonl")
        self.raw_clob = Jsonl(self.out / "clob_active_markets.jsonl")
        self.observations = Jsonl(self.out / "guardian_observations.jsonl")
        self.actions = Jsonl(self.out / "guardian_actions.jsonl")
        self.latency = Jsonl(self.out / "latency_samples.jsonl")
        self.ledger = Jsonl(self.out / "ledger_samples.jsonl")

        self.session: aiohttp.ClientSession | None = None
        self.stop = asyncio.Event()
        self.refresh = asyncio.Event()
        self.tokens_changed = asyncio.Event()

        self.token_meta: dict[str, dict[str, Any]] = {}
        self.market_tokens: dict[str, dict[str, str]] = {}
        self.books: dict[str, Book] = {}
        self.positions: dict[str, dict[str, Any]] = {}
        self.position_fingerprint = ""
        self.shadow_closed: set[str] = set()
        self.last_status: dict[str, str] = {}
        self.rescue_error_at: dict[str, int] = {}
        self.ws_generation = 0
        self.raw_events = 0
        self.helper_actions = 0
        self.min_helper_pnl: Decimal | None = None
        self.experiment_id: str | None = None
        self.experiment_status: str | None = None
        self.experiment_started_mono: float | None = None
        self.entry_closed_mono: float | None = None
        self.start_wall = utc_now()
        self.last_ledger_write = 0.0

    async def get_json(self, path: str) -> dict[str, Any]:
        assert self.session
        async with self.session.get(self.base + path, headers=self.headers, timeout=10) as resp:
            if resp.status != 200:
                raise RuntimeError(f"GET {path} -> {resp.status}: {await resp.text()}")
            return await resp.json()

    def atomic_json(self, name: str, value: Any) -> None:
        path = self.out / name
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(json.dumps(value, indent=2, sort_keys=True), encoding="utf-8")
        os.replace(tmp, path)

    async def preflight(self) -> None:
        health = await self.get_json("/health")
        ready = await self.get_json("/ready")
        state = await self.get_json("/api/shadow/guardian-state")
        if health.get("mode") != "paper" or state.get("paperMode") is not True:
            raise RuntimeError("PAPER_SAFETY_GATE_FAILED")
        if ready.get("status") != "ready":
            raise RuntimeError(f"ENGINE_NOT_READY:{ready}")
        self.atomic_json("manifest.json", {
            "schema": 1,
            "kind": "two-stage-guardian-shadow-24h",
            "started_at": self.start_wall,
            "engine_url": self.base,
            "clob_ws": CLOB_WS,
            "trigger_loss_usd": str(TRIGGER_LOSS),
            "pair_lock_cap_usd": str(PAIR_LOCK_CAP),
            "latency_ms": list(LATENCY_MS),
            "stale_book_seconds": STALE_BOOK_SECONDS,
            "duration_hours": self.args.duration_hours,
            "settlement_grace_minutes": self.args.settlement_grace_minutes,
            "paper_safety": True,
            "health": health,
            "ready": ready,
        })
        await self.apply_guardian_state(state, "preflight")

    async def apply_guardian_state(self, state: dict[str, Any], source: str) -> None:
        self.experiment_id = state.get("experimentId")
        self.experiment_status = state.get("experimentStatus")
        self.note_experiment_status()

        old_tokens = set(self.token_meta)
        meta: dict[str, dict[str, Any]] = {}
        mapping: dict[str, dict[str, str]] = {}
        for item in state.get("tokenMap", []):
            token = str(item.get("tokenId") or "")
            market = str(item.get("marketId") or "")
            outcome = str(item.get("outcome") or "")
            if token and market and outcome in ("UP", "DOWN"):
                meta[token] = item
                mapping.setdefault(market, {})[outcome] = token
        self.token_meta = meta
        self.market_tokens = mapping
        if set(meta) != old_tokens:
            self.tokens_changed.set()

        positions: dict[str, dict[str, Any]] = {}
        for item in state.get("positions", []):
            p = item.get("position") or {}
            market = str(p.get("marketId") or "")
            if market and D(p.get("unpairedQuantity", "0")) > 0 and p.get("unpairedOutcome") in ("UP", "DOWN"):
                positions[market] = p
        await self.set_positions(positions, source)

    def note_experiment_status(self) -> None:
        now = time.monotonic()
        if self.experiment_status == "RUNNING" and self.experiment_started_mono is None:
            self.experiment_started_mono = now
        if self.experiment_status == "ENTRY_CLOSED" and self.entry_closed_mono is None:
            self.entry_closed_mono = now

    async def set_positions(self, positions: dict[str, dict[str, Any]], source: str) -> None:
        fingerprint = hashlib.sha256(compact_json(positions).encode()).hexdigest()
        if fingerprint != self.position_fingerprint:
            self.position_fingerprint = fingerprint
            self.position_states.write({
                "at": utc_now(),
                "monotonic_ns": time.monotonic_ns(),
                "source": source,
                "positions": positions,
                "experiment_id": self.experiment_id,
                "experiment_status": self.experiment_status,
            })
        self.positions = positions
        for market in positions:
            await self.evaluate_market(market, "position_state")

    async def state_loop(self) -> None:
        while not self.stop.is_set():
            try:
                state = await self.get_json("/api/shadow/guardian-state")
                await self.apply_guardian_state(state, "poll")
                now = time.monotonic()
                if now - self.last_ledger_write >= 5:
                    dash = await self.get_json("/api/dashboard")
                    self.ledger.write({
                        "at": utc_now(),
                        "monotonic_ns": time.monotonic_ns(),
                        "experiment_id": (dash.get("engine") or {}).get("experimentId"),
                        "experiment_status": (dash.get("engine") or {}).get("experimentStatus"),
                        "balance": dash.get("balance"),
                    })
                    self.last_ledger_write = now
            except Exception as exc:
                self.observations.write({"at": utc_now(), "type": "STATE_POLL_ERROR", "error": repr(exc)})
            try:
                await asyncio.wait_for(self.refresh.wait(), timeout=1.0)
                self.refresh.clear()
            except asyncio.TimeoutError:
                pass

    async def sse_loop(self) -> None:
        assert self.session
        while not self.stop.is_set():
            try:
                async with self.session.get(self.base + "/events", headers=self.headers, timeout=None) as resp:
                    if resp.status != 200:
                        raise RuntimeError(f"SSE_STATUS_{resp.status}")
                    event_name: str | None = None
                    data_lines: list[str] = []
                    async for raw in resp.content:
                        line = raw.decode("utf-8", errors="replace").rstrip("\r\n")
                        if line == "":
                            if data_lines:
                                await self.handle_sse(event_name or "message", json.loads("\n".join(data_lines)))
                            event_name, data_lines = None, []
                            continue
                        if line.startswith(":"):
                            continue
                        if line.startswith("event:"):
                            event_name = line[6:].strip()
                        elif line.startswith("data:"):
                            data_lines.append(line[5:].lstrip())
                        if self.stop.is_set():
                            break
            except Exception as exc:
                self.observations.write({"at": utc_now(), "type": "SSE_ERROR", "error": repr(exc)})
                await asyncio.sleep(1)

    async def handle_sse(self, event_name: str, data: dict[str, Any]) -> None:
        if event_name == "snapshot":
            engine = data.get("engine") or {}
            self.experiment_id = engine.get("experimentId")
            self.experiment_status = engine.get("experimentStatus")
            self.note_experiment_status()
            positions = {
                str(p.get("marketId")): p
                for p in data.get("positions", [])
                if D(p.get("unpairedQuantity", "0")) > 0 and p.get("unpairedOutcome") in ("UP", "DOWN")
            }
            await self.set_positions(positions, "engine_sse")
            return

        if event_name != "decision":
            return
        self.engine_events.write({
            "received_at": utc_now(),
            "received_mono_ns": time.monotonic_ns(),
            "event": data,
        })
        typ = str(data.get("type") or "")
        market = str(data.get("marketId") or "")
        detail = data.get("detail") or {}
        if typ == "PAPER_EXECUTION_ERROR" and detail.get("intent") == "RESCUE_SECOND_LEG" and market:
            self.rescue_error_at[market] = time.monotonic_ns()
            await self.evaluate_market(market, "rescue_execution_error")
        if typ in {
            "PAPER_FILL", "PAPER_ORDER_CREATED", "PAPER_EXECUTION_ERROR",
            "POSITION_UPDATED", "HEDGE_COMPLETE", "RISK_ACCEPT", "RISK_VETO",
        }:
            self.refresh.set()

    async def ws_loop(self) -> None:
        assert self.session
        while not self.stop.is_set():
            tokens = tuple(sorted(self.token_meta))
            if not tokens:
                await asyncio.sleep(0.5)
                continue
            self.tokens_changed.clear()
            self.ws_generation += 1
            generation = self.ws_generation
            try:
                async with self.session.ws_connect(CLOB_WS, autoping=True, heartbeat=20) as ws:
                    await ws.send_json({
                        "assets_ids": list(tokens),
                        "custom_feature_enabled": True,
                        "type": "market",
                    })
                    self.observations.write({
                        "at": utc_now(), "type": "CLOB_CONNECTED",
                        "ws_generation": generation, "token_count": len(tokens),
                    })
                    while not self.stop.is_set() and not self.tokens_changed.is_set():
                        try:
                            msg = await asyncio.wait_for(ws.receive(), timeout=10)
                        except asyncio.TimeoutError:
                            await ws.send_str("PING")
                            continue
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            if msg.data == "PONG":
                                continue
                            await self.handle_clob_payload(msg.data, generation)
                        elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                            break
            except Exception as exc:
                self.observations.write({
                    "at": utc_now(), "type": "CLOB_ERROR",
                    "ws_generation": generation, "error": repr(exc),
                })
                await asyncio.sleep(1)

    async def handle_clob_payload(self, text: str, generation: int) -> None:
        recv_at, recv_ns = utc_now(), time.monotonic_ns()
        try:
            decoded = json.loads(text)
        except json.JSONDecodeError:
            self.observations.write({"at": recv_at, "type": "CLOB_MALFORMED"})
            return
        events = decoded if isinstance(decoded, list) else [decoded]
        touched: set[str] = set()
        for raw in events:
            if not isinstance(raw, dict):
                continue
            typ = raw.get("event_type")
            if typ == "book":
                token = str(raw.get("asset_id") or "")
                if token not in self.token_meta:
                    continue
                self.books.setdefault(token, Book(token)).set_full(raw, generation)
                market = str(self.token_meta[token].get("marketId"))
                touched.add(market)
                if market in self.positions:
                    self.write_raw(raw, recv_at, recv_ns, generation)
            elif typ == "price_change":
                raw_markets: set[str] = set()
                for ch in raw.get("price_changes", []):
                    if not isinstance(ch, dict):
                        continue
                    token = str(ch.get("asset_id") or "")
                    if token not in self.token_meta:
                        continue
                    book = self.books.get(token)
                    if not book or not book.trusted:
                        continue
                    side = str(ch.get("side") or "").upper()
                    if side not in ("BUY", "SELL"):
                        continue
                    book.change(side, str(ch.get("price")), str(ch.get("size", "0")), raw.get("timestamp"), generation)
                    market = str(self.token_meta[token].get("marketId"))
                    touched.add(market)
                    raw_markets.add(market)
                if any(m in self.positions for m in raw_markets):
                    self.write_raw(raw, recv_at, recv_ns, generation)
            elif typ == "last_trade_price":
                token = str(raw.get("asset_id") or "")
                meta = self.token_meta.get(token)
                if meta and str(meta.get("marketId")) in self.positions:
                    self.write_raw(raw, recv_at, recv_ns, generation)

        for market in touched:
            if market in self.positions:
                await self.evaluate_market(market, "clob_update")

    def write_raw(self, raw: dict[str, Any], recv_at: str, recv_ns: int, generation: int) -> None:
        self.raw_events += 1
        self.raw_clob.write({
            "received_at": recv_at,
            "received_mono_ns": recv_ns,
            "ws_generation": generation,
            "raw": raw,
        })

    def fresh_book(self, token: str | None) -> Book | None:
        if not token:
            return None
        book = self.books.get(token)
        if not book or not book.trusted or book.age_ms() > STALE_BOOK_SECONDS * 1000:
            return None
        return book

    def economics(self, position: dict[str, Any]) -> dict[str, Any]:
        market = str(position["marketId"])
        held_outcome = str(position["unpairedOutcome"])
        opp_outcome = "DOWN" if held_outcome == "UP" else "UP"
        mapping = self.market_tokens.get(market, {})
        held_token = mapping.get(held_outcome)
        opp_token = mapping.get(opp_outcome)
        held = self.fresh_book(held_token)
        opp = self.fresh_book(opp_token)
        qty = D(position["unpairedQuantity"])
        result: dict[str, Any] = {
            "market_id": market,
            "held_outcome": held_outcome,
            "quantity": dec(qty),
            "held_token": held_token,
            "opposite_token": opp_token,
            "mark_loss_usd": None,
            "sell": None,
            "lock": None,
        }
        if not held or not held.bids:
            return result

        bids = [(D(p), D(q)) for p, q in held.bids.items() if D(q) > 0]
        if not bids:
            return result
        best_bid = max(p for p, _ in bids)
        avg_cost = D(position["averageUpCost"] if held_outcome == "UP" else position["averageDownCost"])
        result["mark_loss_usd"] = dec(qty * max(avg_cost - best_bid, Decimal(0)))

        held_meta = self.token_meta.get(held_token or "", {})
        sell_walk = walk(held, qty, "SELL")
        sell_fee = fee(qty, D(sell_walk["vwap"]), held_meta.get("feeSchedule")) if sell_walk["full"] else None
        min_sell = held_meta.get("minOrderSize")
        if sell_walk["full"] and sell_fee is not None and min_sell is not None and qty >= D(min_sell):
            up, down = D(position["upQuantity"]), D(position["downQuantity"])
            paired = min(up, down)
            inventory_cost = up * D(position["averageUpCost"]) + down * D(position["averageDownCost"])
            pnl = paired + D(sell_walk["notional"]) - inventory_cost - D(position["totalFees"]) - sell_fee
            result["sell"] = {
                **sell_walk,
                "fee_usd": dec(sell_fee, 5),
                "guaranteed_terminal_pnl": dec(pnl),
                "book_received_at": held.received_at,
                "book_age_ms": held.age_ms(),
                "ws_generation": held.ws_generation,
            }

        if opp:
            opp_meta = self.token_meta.get(opp_token or "", {})
            lock_walk = walk(opp, qty, "BUY")
            lock_fee = fee(qty, D(lock_walk["vwap"]), opp_meta.get("feeSchedule")) if lock_walk["full"] else None
            min_buy = opp_meta.get("minOrderSize")
            if lock_walk["full"] and lock_fee is not None and min_buy is not None and qty >= D(min_buy):
                up, down = D(position["upQuantity"]), D(position["downQuantity"])
                final_paired = max(up, down)
                inventory_cost = up * D(position["averageUpCost"]) + down * D(position["averageDownCost"])
                locked = final_paired - inventory_cost - D(lock_walk["notional"]) - D(position["totalFees"]) - lock_fee
                result["lock"] = {
                    **lock_walk,
                    "fee_usd": dec(lock_fee, 5),
                    "guaranteed_locked_pnl": dec(locked),
                    "book_received_at": opp.received_at,
                    "book_age_ms": opp.age_ms(),
                    "ws_generation": opp.ws_generation,
                }
        return result

    async def evaluate_market(self, market: str, trigger_source: str) -> None:
        position = self.positions.get(market)
        if not position:
            return
        key = episode_key(position)
        if key in self.shadow_closed:
            return
        econ = self.economics(position)
        mark = econ.get("mark_loss_usd")
        if mark is None or D(mark) < TRIGGER_LOSS:
            self.record_status(key, "BELOW_TRIGGER", market, econ, trigger_source)
            return

        failure_reason = "RESCUE_EXECUTION_ERROR" if market in self.rescue_error_at else None
        lock_info = econ.get("lock")
        if failure_reason is None:
            if lock_info is not None and D(lock_info["guaranteed_locked_pnl"]) >= -PAIR_LOCK_CAP:
                self.record_status(key, "PR3_AFFORDABLE", market, econ, trigger_source)
                return
            failure_reason = "PR3_CAP_OR_DEPTH_FAIL"

        sell_info = econ.get("sell")
        if sell_info is None:
            self.record_status(key, "HELPER_NOT_EXECUTABLE", market, econ, trigger_source, failure_reason)
            return

        decision_ns = time.monotonic_ns()
        action_id = "gsh_" + hashlib.sha256(f"{key}|{decision_ns}".encode()).hexdigest()[:20]
        action = {
            "action_id": action_id,
            "decision_at": utc_now(),
            "decision_mono_ns": decision_ns,
            "experiment_id": self.experiment_id,
            "episode_key": key,
            "market_id": market,
            "trigger_source": trigger_source,
            "failure_reason": failure_reason,
            "policy": "TWO_STAGE_GUARDIAN_V1_LOCKED",
            "position": position,
            "economics": econ,
        }
        self.actions.write(action)
        self.shadow_closed.add(key)
        self.helper_actions += 1
        pnl = D(sell_info["guaranteed_terminal_pnl"])
        self.min_helper_pnl = pnl if self.min_helper_pnl is None else min(self.min_helper_pnl, pnl)
        self.last_status[key] = "SHADOW_HELPER_SELL"
        self.observations.write({
            "at": utc_now(),
            "type": "SHADOW_HELPER_SELL",
            "action_id": action_id,
            "market_id": market,
            "pnl": str(pnl),
            "failure_reason": failure_reason,
        })
        for ms in LATENCY_MS:
            asyncio.create_task(self.capture_latency(action, ms))

    def record_status(
        self,
        key: str,
        status: str,
        market: str,
        econ: dict[str, Any],
        trigger_source: str,
        failure_reason: str | None = None,
    ) -> None:
        if self.last_status.get(key) == status:
            return
        self.last_status[key] = status
        self.observations.write({
            "at": utc_now(),
            "monotonic_ns": time.monotonic_ns(),
            "type": "GUARDIAN_STATE",
            "episode_key": key,
            "market_id": market,
            "status": status,
            "failure_reason": failure_reason,
            "trigger_source": trigger_source,
            "economics": econ,
        })

    async def capture_latency(self, action: dict[str, Any], latency_ms: int) -> None:
        target_ns = int(action["decision_mono_ns"]) + latency_ms * 1_000_000
        wait = (target_ns - time.monotonic_ns()) / 1_000_000_000
        if wait > 0:
            await asyncio.sleep(wait)
        actual_ns = time.monotonic_ns()
        self.latency.write({
            "action_id": action["action_id"],
            "market_id": action["market_id"],
            "target_latency_ms": latency_ms,
            "captured_at": utc_now(),
            "captured_mono_ns": actual_ns,
            "scheduler_error_ms": (actual_ns - target_ns) / 1_000_000.0,
            "economics": self.economics(action["position"]),
        })

    async def summary_loop(self) -> None:
        while not self.stop.is_set():
            elapsed = None if self.experiment_started_mono is None else time.monotonic() - self.experiment_started_mono
            summary = {
                "updated_at": utc_now(),
                "experiment_id": self.experiment_id,
                "experiment_status": self.experiment_status,
                "experiment_elapsed_seconds": elapsed,
                "active_positions": len(self.positions),
                "tracked_tokens": len(self.token_meta),
                "trusted_direct_books": sum(1 for b in self.books.values() if b.trusted),
                "raw_active_market_events": self.raw_events,
                "helper_actions": self.helper_actions,
                "worst_helper_guaranteed_pnl": str(self.min_helper_pnl) if self.min_helper_pnl is not None else None,
                "paper_only": True,
            }
            self.atomic_json("live_summary.json", summary)
            print(compact_json(summary), flush=True)
            await asyncio.sleep(30)

    async def stop_condition_loop(self) -> None:
        no_start_deadline = time.monotonic() + 600
        while not self.stop.is_set():
            now = time.monotonic()
            if self.experiment_started_mono is None:
                if now > no_start_deadline:
                    raise RuntimeError("EXPERIMENT_DID_NOT_START_WITHIN_10_MINUTES")
            else:
                elapsed = now - self.experiment_started_mono
                duration = self.args.duration_hours * 3600
                if self.experiment_status == "COMPLETED" and elapsed >= duration * 0.95:
                    self.stop.set()
                    return
                if elapsed >= duration and self.experiment_status in ("ENTRY_CLOSED", "COMPLETED"):
                    if self.experiment_status == "COMPLETED":
                        self.stop.set()
                        return
                    if self.entry_closed_mono is None:
                        self.entry_closed_mono = now
                    if now - self.entry_closed_mono >= self.args.settlement_grace_minutes * 60:
                        self.stop.set()
                        return
            await asyncio.sleep(5)

    async def finish(self) -> None:
        try:
            self.atomic_json("final_dashboard.json", await self.get_json("/api/dashboard"))
        except Exception as exc:
            self.atomic_json("final_dashboard_error.json", {"at": utc_now(), "error": repr(exc)})
        self.atomic_json("run_complete.json", {
            "completed_at": utc_now(),
            "experiment_id": self.experiment_id,
            "experiment_status": self.experiment_status,
            "helper_actions": self.helper_actions,
            "worst_helper_guaranteed_pnl": str(self.min_helper_pnl) if self.min_helper_pnl is not None else None,
            "raw_active_market_events": self.raw_events,
            "paper_only": True,
        })
        for writer in (
            self.engine_events,
            self.position_states,
            self.raw_clob,
            self.observations,
            self.actions,
            self.latency,
            self.ledger,
        ):
            writer.close()

    async def run(self) -> None:
        timeout = aiohttp.ClientTimeout(total=None, sock_connect=10, sock_read=None)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            self.session = session
            await self.preflight()
            tasks = [
                asyncio.create_task(self.state_loop()),
                asyncio.create_task(self.sse_loop()),
                asyncio.create_task(self.ws_loop()),
                asyncio.create_task(self.summary_loop()),
                asyncio.create_task(self.stop_condition_loop()),
            ]
            try:
                while not self.stop.is_set():
                    for task in tasks:
                        if task.done() and task.exception():
                            raise task.exception()
                    await asyncio.sleep(0.25)
            finally:
                self.stop.set()
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
                await self.finish()


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--engine-url", default=os.getenv("ENGINE_BASE_URL", "http://127.0.0.1:8080"))
    p.add_argument("--engine-read-token", default=os.getenv("ENGINE_READ_TOKEN"))
    p.add_argument("--output-dir", required=True)
    p.add_argument("--duration-hours", type=float, default=24.0)
    p.add_argument("--settlement-grace-minutes", type=float, default=30.0)
    args = p.parse_args()
    if not args.engine_read_token:
        p.error("ENGINE_READ_TOKEN is required")
    return args


async def amain() -> None:
    shadow = GuardianShadow(parse_args())
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, shadow.stop.set)
        except NotImplementedError:
            pass
    await shadow.run()


if __name__ == "__main__":
    asyncio.run(amain())
