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
NORMAL_MIN_LOCKED_EDGE = Decimal("0.015")
MAX_ORDER_SIZE_USD = Decimal("25")
STALE_BOOK_SECONDS = 15.0
LATENCY_MS = (25, 50, 100, 150, 250, 500)
CLOB_WS = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
CONTRACT_PATH = Path(__file__).with_name("TEST_CONTRACT.md")
EXPECTED_CONTRACT_SHA256 = "545fd28c6452d2b5af6ed69c74b1cb4aff1423562311e91f88b44650a83aa9a9"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def D(value: Any) -> Decimal:
    return Decimal(str(value))


def dec(value: Decimal, places: int = 8) -> str:
    q = Decimal(1).scaleb(-places)
    return format(value.quantize(q, rounding=ROUND_HALF_UP), "f")


def compact_json(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"), sort_keys=True, ensure_ascii=False)


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def parse_source_ms(value: Any) -> int | None:
    if value is None:
        return None
    try:
        number = int(str(value))
        return number if number > 0 else None
    except ValueError:
        try:
            return int(datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp() * 1000)
        except ValueError:
            return None


class Jsonl:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.fh = self.path.open("a", encoding="utf-8", buffering=1)

    def write(self, value: dict[str, Any]) -> None:
        self.fh.write(compact_json(value) + "\n")

    def sync(self) -> None:
        self.fh.flush()
        os.fsync(self.fh.fileno())

    def close(self) -> None:
        self.sync()
        self.fh.close()


@dataclass
class Book:
    token_id: str
    bids: dict[str, str] = field(default_factory=dict)
    asks: dict[str, str] = field(default_factory=dict)
    source_timestamp: str | None = None
    source_ms: int | None = None
    received_at: str | None = None
    received_mono_ns: int = 0
    ws_generation: int = 0
    trusted: bool = False

    def set_full(self, raw: dict[str, Any], generation: int) -> bool:
        incoming_ms = parse_source_ms(raw.get("timestamp"))
        if incoming_ms is None:
            self.trusted = False
            raise ValueError("CLOB_SOURCE_TIMESTAMP_MISSING")
        for side_name in ("bids", "asks"):
            for level in raw.get(side_name, []):
                price = D(level.get("price"))
                size = D(level.get("size", level.get("quantity", "0")))
                if price <= 0 or price > 1 or size < 0:
                    self.trusted = False
                    raise ValueError(f"CLOB_INVALID_LEVEL:{side_name}:{price}:{size}")
        if (
            generation == self.ws_generation
            and incoming_ms is not None
            and self.source_ms is not None
            and incoming_ms < self.source_ms
        ):
            return False
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
        return True

    def change(self, side: str, price: str, size: str, source_ts: Any, generation: int) -> bool:
        incoming_ms = parse_source_ms(source_ts)
        if incoming_ms is None:
            self.trusted = False
            raise ValueError("CLOB_SOURCE_TIMESTAMP_MISSING")
        parsed_price = D(price)
        parsed_size = D(size)
        if parsed_price <= 0 or parsed_price > 1 or parsed_size < 0:
            self.trusted = False
            raise ValueError(f"CLOB_INVALID_CHANGE:{side}:{parsed_price}:{parsed_size}")
        if (
            generation == self.ws_generation
            and incoming_ms is not None
            and self.source_ms is not None
            and incoming_ms < self.source_ms
        ):
            return False
        levels = self.bids if side == "BUY" else self.asks
        if D(size) <= 0:
            levels.pop(price, None)
        else:
            levels[price] = size
        self._stamp(source_ts, generation)
        return True

    def _stamp(self, source_ts: Any, generation: int) -> None:
        self.source_timestamp = str(source_ts) if source_ts is not None else None
        self.source_ms = parse_source_ms(source_ts)
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


def walk(book: Book, qty: Decimal, side: str, max_notional: Decimal = MAX_ORDER_SIZE_USD) -> dict[str, Any]:
    remaining = qty
    notional = Decimal(0)
    worst: Decimal | None = None
    levels_used = 0
    for price, available in book.levels(side):
        remaining_notional = max_notional - notional
        if remaining_notional <= 0:
            break
        if price <= 0:
            continue
        take = min(remaining, available, remaining_notional / price)
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
    if qty < 0 or price < 0 or price > 1:
        raise ValueError(f"INVALID_FEE_INPUT:{qty}:{price}")
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
        self.book_checkpoints = Jsonl(self.out / "book_checkpoints.jsonl")
        self.integrity = Jsonl(self.out / "integrity_events.jsonl")

        self.session: aiohttp.ClientSession | None = None
        self.stop = asyncio.Event()
        self.refresh = asyncio.Event()
        self.tokens_changed = asyncio.Event()

        self.token_meta: dict[str, dict[str, Any]] = {}
        self.market_tokens: dict[str, dict[str, str]] = {}
        self.books: dict[str, Book] = {}
        self.positions: dict[str, dict[str, Any]] = {}
        self.authority: dict[str, dict[str, Any]] = {}
        self.position_fingerprint = ""
        self.shadow_closed: set[str] = set()
        self.last_status: dict[str, str] = {}
        self.rescue_error_at: dict[str, str] = {}
        self.seen_engine_event_ids: set[str] = set()
        self.latency_tasks: set[asyncio.Task[Any]] = set()
        self.latency_watch_until_ns: dict[str, int] = {}
        self.incomplete_reasons: set[str] = set()
        self.bound_experiment_id: str | None = None
        self.experiment_validated = False
        self.stop_reason: str | None = None
        self.ws_generation = 0
        self.raw_events = 0
        self.helper_actions = 0
        self.latency_samples_written = 0
        self.latency_unknown_samples = 0
        self.min_helper_pnl: Decimal | None = None
        self.experiment_id: str | None = None
        self.experiment_status: str | None = None
        self.experiment_started_at: str | None = None
        self.entry_closes_at: str | None = None
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

    def note_incomplete(self, reason: str, detail: dict[str, Any] | None = None, critical: bool = True) -> None:
        self.integrity.write({
            "at": utc_now(),
            "monotonic_ns": time.monotonic_ns(),
            "reason": reason,
            "critical": critical,
            "detail": detail or {},
            "active_markets": sorted(self.positions),
        })
        if critical:
            self.incomplete_reasons.add(reason)

    def sync_writers(self) -> None:
        for writer in (
            self.engine_events, self.position_states, self.raw_clob,
            self.observations, self.actions, self.latency, self.ledger,
            self.book_checkpoints, self.integrity,
        ):
            writer.sync()

    def book_evidence(self, book: Book | None) -> dict[str, Any] | None:
        if book is None or not book.trusted:
            return None
        return {
            "token_id": book.token_id,
            "source_timestamp": book.source_timestamp,
            "source_ms": book.source_ms,
            "received_at": book.received_at,
            "received_mono_ns": book.received_mono_ns,
            "ws_generation": book.ws_generation,
            "age_ms": book.age_ms(),
            "bids": [[dec(p), dec(q)] for p, q in book.levels("SELL")],
            "asks": [[dec(p), dec(q)] for p, q in book.levels("BUY")],
        }

    def write_book_checkpoint(self, market: str, reason: str) -> None:
        position = self.positions.get(market)
        if not position:
            return
        held_outcome = str(position["unpairedOutcome"])
        opposite_outcome = "DOWN" if held_outcome == "UP" else "UP"
        mapping = self.market_tokens.get(market, {})
        held_token = mapping.get(held_outcome)
        opposite_token = mapping.get(opposite_outcome)
        self.book_checkpoints.write({
            "at": utc_now(),
            "monotonic_ns": time.monotonic_ns(),
            "reason": reason,
            "market_id": market,
            "episode_key": episode_key(position),
            "position": position,
            "held_meta": self.token_meta.get(held_token or ""),
            "opposite_meta": self.token_meta.get(opposite_token or ""),
            "held_book": self.book_evidence(self.books.get(held_token or "")),
            "opposite_book": self.book_evidence(self.books.get(opposite_token or "")),
        })

    async def preflight(self) -> None:
        if abs(self.args.duration_hours - 24.0) > 1e-12:
            raise RuntimeError(f"LOCKED_DURATION_MUST_BE_24H:{self.args.duration_hours}")
        if (
            len(self.args.source_git_sha) != 40
            or any(ch not in "0123456789abcdef" for ch in self.args.source_git_sha.lower())
        ):
            raise RuntimeError(f"INVALID_SOURCE_GIT_SHA:{self.args.source_git_sha}")
        if not CONTRACT_PATH.exists():
            raise RuntimeError("TEST_CONTRACT_MISSING")
        contract_hash = sha256_file(CONTRACT_PATH)
        if contract_hash != EXPECTED_CONTRACT_SHA256:
            raise RuntimeError(
                f"TEST_CONTRACT_HASH_MISMATCH:expected={EXPECTED_CONTRACT_SHA256}:observed={contract_hash}"
            )

        health = await self.get_json("/health")
        ready = await self.get_json("/ready")
        state = await self.get_json("/api/shadow/guardian-state")
        if health.get("mode") != "paper" or state.get("paperMode") is not True:
            raise RuntimeError("PAPER_SAFETY_GATE_FAILED")
        if ready.get("status") != "ready":
            raise RuntimeError(f"ENGINE_NOT_READY:{ready}")

        risk = state.get("riskConfiguration") or {}
        expected = {
            "maxOrderSizeUsd": MAX_ORDER_SIZE_USD,
            "minExpectedEdge": NORMAL_MIN_LOCKED_EDGE,
            "staleFeedTimeoutMs": Decimal(str(int(STALE_BOOK_SECONDS * 1000))),
            "catastrophicPairLockMinUnhedgedMs": Decimal("10000"),
            "catastrophicPairLockTriggerLossUsd": TRIGGER_LOSS,
            "catastrophicPairLockMaxLockedLossUsd": PAIR_LOCK_CAP,
        }
        for key, value in expected.items():
            observed = risk.get(key)
            if observed is None or D(observed) != value:
                raise RuntimeError(f"RISK_CONFIG_MISMATCH:{key}:expected={value}:observed={observed}")

        experiment = state.get("experiment")
        status = (experiment or {}).get("status")
        if status in ("RUNNING", "ENTRY_CLOSED"):
            raise RuntimeError(f"ACTIVE_EXPERIMENT_AT_PREFLIGHT:{status}")
        if state.get("positions"):
            raise RuntimeError("OPEN_POSITION_AT_PREFLIGHT")

        (self.out / "TEST_CONTRACT.md").write_bytes(CONTRACT_PATH.read_bytes())
        self.atomic_json("manifest.json", {
            "schema": 2,
            "kind": "two-stage-guardian-shadow-24h",
            "started_at": self.start_wall,
            "engine_url": self.base,
            "clob_ws": CLOB_WS,
            "observer_sha256": sha256_file(Path(__file__)),
            "contract_sha256": contract_hash,
            "source_git_sha": self.args.source_git_sha,
            "trigger_loss_usd": str(TRIGGER_LOSS),
            "pair_lock_cap_usd": str(PAIR_LOCK_CAP),
            "normal_min_locked_edge": str(NORMAL_MIN_LOCKED_EDGE),
            "max_order_size_usd": str(MAX_ORDER_SIZE_USD),
            "latency_ms": list(LATENCY_MS),
            "stale_book_seconds": STALE_BOOK_SECONDS,
            "duration_hours": self.args.duration_hours,
            "settlement_grace_minutes": self.args.settlement_grace_minutes,
            "risk_configuration": risk,
            "paper_safety": True,
            "health": health,
            "ready": ready,
        })
        await self.apply_guardian_state(state, "preflight", evaluate=False)

    async def apply_guardian_state(self, state: dict[str, Any], source: str, evaluate: bool = True) -> None:
        experiment = state.get("experiment") or {}
        self.experiment_id = experiment.get("id")
        self.experiment_status = experiment.get("status")
        self.experiment_started_at = experiment.get("startedAt")
        self.entry_closes_at = experiment.get("entryClosesAt")

        active_or_bound_terminal = self.experiment_status in ("RUNNING", "ENTRY_CLOSED") or (
            self.experiment_status == "COMPLETED" and self.bound_experiment_id is not None and self.experiment_id == self.bound_experiment_id
        )
        if active_or_bound_terminal:
            duration = experiment.get("durationHours")
            git_sha = str(experiment.get("gitSha") or "")
            if duration is None or D(duration) != Decimal("24"):
                self.note_incomplete(
                    "EXPERIMENT_DURATION_MISMATCH",
                    {"expected": "24", "observed": duration, "experiment_id": self.experiment_id},
                )
                self.stop_reason = "EXPERIMENT_DURATION_MISMATCH"
                self.stop.set()
            if git_sha != self.args.source_git_sha:
                self.note_incomplete(
                    "EXPERIMENT_GIT_SHA_MISMATCH",
                    {"expected": self.args.source_git_sha, "observed": git_sha, "experiment_id": self.experiment_id},
                )
                self.stop_reason = "EXPERIMENT_GIT_SHA_MISMATCH"
                self.stop.set()

            if self.experiment_started_at and self.entry_closes_at:
                try:
                    started = datetime.fromisoformat(str(self.experiment_started_at).replace("Z", "+00:00"))
                    closes = datetime.fromisoformat(str(self.entry_closes_at).replace("Z", "+00:00"))
                    duration_seconds = (closes - started).total_seconds()
                    if abs(duration_seconds - 24 * 3600) > 1:
                        self.note_incomplete(
                            "EXPERIMENT_WINDOW_MISMATCH",
                            {"duration_seconds": duration_seconds, "experiment_id": self.experiment_id},
                        )
                        self.stop_reason = "EXPERIMENT_WINDOW_MISMATCH"
                        self.stop.set()
                except ValueError:
                    self.note_incomplete(
                        "EXPERIMENT_TIMESTAMP_INVALID",
                        {"startedAt": self.experiment_started_at, "entryClosesAt": self.entry_closes_at},
                    )
                    self.stop_reason = "EXPERIMENT_TIMESTAMP_INVALID"
                    self.stop.set()

        self.note_experiment_status()
        if active_or_bound_terminal and not self.incomplete_reasons:
            self.experiment_validated = True

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
            self.integrity.write({
                "at": utc_now(), "monotonic_ns": time.monotonic_ns(),
                "reason": "TOKEN_MAP_CHANGED", "critical": False,
                "detail": {"old_count": len(old_tokens), "new_count": len(meta)},
                "active_markets": sorted(self.positions),
            })

        for event in state.get("recentRiskEvents", []):
            if isinstance(event, dict):
                self.ingest_engine_event(event, source="authority_poll")

        positions: dict[str, dict[str, Any]] = {}
        authority: dict[str, dict[str, Any]] = {}
        for item in state.get("positions", []):
            p = item.get("position") or {}
            market = str(p.get("marketId") or "")
            if market and D(p.get("unpairedQuantity", "0")) > 0 and p.get("unpairedOutcome") in ("UP", "DOWN"):
                positions[market] = p
                authority[market] = item
        self.authority = authority
        await self.set_positions(positions, source, evaluate=evaluate)

    def note_experiment_status(self) -> None:
        now = time.monotonic()
        if self.experiment_status == "RUNNING":
            if self.bound_experiment_id is None:
                self.bound_experiment_id = self.experiment_id
                self.experiment_started_mono = now
            elif self.experiment_id != self.bound_experiment_id:
                self.note_incomplete(
                    "EXPERIMENT_ID_CHANGED",
                    {"bound": self.bound_experiment_id, "observed": self.experiment_id},
                )
        if self.experiment_status == "ENTRY_CLOSED" and self.entry_closed_mono is None:
            self.entry_closed_mono = now

    async def set_positions(self, positions: dict[str, dict[str, Any]], source: str, evaluate: bool = True) -> None:
        previous_markets = set(self.positions)
        fingerprint = hashlib.sha256(compact_json(positions).encode()).hexdigest()
        if fingerprint != self.position_fingerprint:
            self.position_fingerprint = fingerprint
            self.position_states.write({
                "at": utc_now(),
                "monotonic_ns": time.monotonic_ns(),
                "source": source,
                "positions": positions,
                "authority": {m: self.authority.get(m) for m in positions},
                "experiment_id": self.experiment_id,
                "experiment_status": self.experiment_status,
            })
        self.positions = positions
        for market in set(positions) - previous_markets:
            self.write_book_checkpoint(market, "POSITION_ACTIVATED")
        if evaluate:
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
                self.note_incomplete("STATE_POLL_ERROR", {"error": repr(exc)}, critical=bool(self.positions))
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
                self.note_incomplete("ENGINE_SSE_GAP", {"error": repr(exc)}, critical=bool(self.positions))
                await asyncio.sleep(1)

    def ingest_engine_event(self, data: dict[str, Any], source: str) -> str | None:
        event_id = str(data.get("id") or "")
        if not event_id:
            event_id = hashlib.sha256(compact_json(data).encode()).hexdigest()
        if event_id in self.seen_engine_event_ids:
            return None
        self.seen_engine_event_ids.add(event_id)
        self.engine_events.write({
            "received_at": utc_now(),
            "received_mono_ns": time.monotonic_ns(),
            "source": source,
            "event": data,
        })
        typ = str(data.get("type") or "")
        market = str(data.get("marketId") or "")
        detail = data.get("detail") or {}
        rescue_failure = (
            typ == "PAPER_EXECUTION_ERROR"
            or typ == "RISK_REDUCTION_LIFECYCLE_BLOCKED"
        )
        if rescue_failure and detail.get("intent") == "RESCUE_SECOND_LEG" and market:
            self.rescue_error_at[market] = str(data.get("at") or utc_now())
            return market
        return None

    async def handle_sse(self, event_name: str, data: dict[str, Any]) -> None:
        if event_name == "snapshot":
            engine = data.get("engine") or {}
            self.experiment_id = engine.get("experimentId")
            self.experiment_status = engine.get("experimentStatus")
            # SSE carries only bounded dashboard state. Exact experiment
            # manifest/SHA validation and authority binding happen only through
            # /api/shadow/guardian-state.
            positions = {
                str(p.get("marketId")): p
                for p in data.get("positions", [])
                if D(p.get("unpairedQuantity", "0")) > 0 and p.get("unpairedOutcome") in ("UP", "DOWN")
            }
            await self.set_positions(positions, "engine_sse")
            return

        if event_name != "decision":
            return
        rescue_market = self.ingest_engine_event(data, source="engine_sse")
        if rescue_market:
            await self.evaluate_market(rescue_market, "rescue_execution_error")
        typ = str(data.get("type") or "")
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
            # A reconnect begins a new trust epoch. Incrementals are ignored
            # until each token receives a fresh full book in this generation.
            for token in tokens:
                if token in self.books:
                    self.books[token].trusted = False
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
                watched = bool(self.positions) or any(
                    until >= time.monotonic_ns() for until in self.latency_watch_until_ns.values()
                )
                self.note_incomplete(
                    "CLOB_GAP",
                    {"error": repr(exc), "ws_generation": generation},
                    critical=watched,
                )
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
                accepted = self.books.setdefault(token, Book(token)).set_full(raw, generation)
                if not accepted:
                    self.observations.write({"at": recv_at, "type": "CLOB_STALE_DROP", "token_id": token})
                    continue
                market = str(self.token_meta[token].get("marketId"))
                touched.add(market)
                if self.market_needs_raw(market, recv_ns):
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
                    accepted = book.change(side, str(ch.get("price")), str(ch.get("size", "0")), raw.get("timestamp"), generation)
                    if not accepted:
                        self.observations.write({"at": recv_at, "type": "CLOB_STALE_DROP", "token_id": token})
                        continue
                    market = str(self.token_meta[token].get("marketId"))
                    touched.add(market)
                    raw_markets.add(market)
                if any(self.market_needs_raw(m, recv_ns) for m in raw_markets):
                    self.write_raw(raw, recv_at, recv_ns, generation)
            elif typ == "last_trade_price":
                token = str(raw.get("asset_id") or "")
                meta = self.token_meta.get(token)
                if meta and self.market_needs_raw(str(meta.get("marketId")), recv_ns):
                    self.write_raw(raw, recv_at, recv_ns, generation)

        for market in touched:
            if market in self.positions:
                await self.evaluate_market(market, "clob_update")

    def market_needs_raw(self, market: str, now_ns: int | None = None) -> bool:
        current_ns = now_ns if now_ns is not None else time.monotonic_ns()
        until = self.latency_watch_until_ns.get(market, 0)
        if until and until < current_ns:
            self.latency_watch_until_ns.pop(market, None)
            until = 0
        return market in self.positions or until >= current_ns

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

    def full_sell_economics(
        self,
        position: dict[str, Any],
        held_token: str,
        held: Book,
        held_meta: dict[str, Any],
    ) -> dict[str, Any] | None:
        qty = D(position["unpairedQuantity"])
        sell_walk = walk(held, qty, "SELL")
        if not sell_walk["full"]:
            return None
        min_sell = held_meta.get("minOrderSize")
        if min_sell is None or qty < D(min_sell):
            return None
        sell_fee = fee(qty, D(sell_walk["vwap"]), held_meta.get("feeSchedule"))
        if sell_fee is None:
            return None
        up, down = D(position["upQuantity"]), D(position["downQuantity"])
        paired = min(up, down)
        inventory_cost = up * D(position["averageUpCost"]) + down * D(position["averageDownCost"])
        pnl = paired + D(sell_walk["notional"]) - inventory_cost - D(position["totalFees"]) - sell_fee
        return {
            **sell_walk,
            "fee_usd": dec(sell_fee, 5),
            "guaranteed_terminal_pnl": dec(pnl),
            "token_id": held_token,
            "book_source_timestamp": held.source_timestamp,
            "book_received_at": held.received_at,
            "book_received_mono_ns": held.received_mono_ns,
            "book_age_ms": held.age_ms(),
            "ws_generation": held.ws_generation,
        }

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
        if held_token:
            result["sell"] = self.full_sell_economics(position, held_token, held, held_meta)

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

    def rescue_error_for_episode(self, market: str, position: dict[str, Any]) -> bool:
        error_at = self.rescue_error_at.get(market)
        episode_started = position.get("unhedgedStartedAt") or position.get("openedAt")
        if not error_at or not episode_started:
            return False
        try:
            error_dt = datetime.fromisoformat(error_at.replace("Z", "+00:00"))
            start_dt = datetime.fromisoformat(str(episode_started).replace("Z", "+00:00"))
            return error_dt >= start_dt
        except ValueError:
            self.note_incomplete(
                "INVALID_EPISODE_TIMESTAMP",
                {"market_id": market, "error_at": error_at, "episode_started": episode_started},
            )
            return False

    async def revalidate_authority(
        self,
        market: str,
        expected_episode_key: str,
        trigger_source: str,
    ) -> tuple[dict[str, Any], dict[str, Any]] | None:
        try:
            state = await self.get_json("/api/shadow/guardian-state")
            await self.apply_guardian_state(state, "decision_revalidation", evaluate=False)
        except Exception as exc:
            self.note_incomplete(
                "AUTHORITY_REVALIDATION_ERROR",
                {"market_id": market, "trigger_source": trigger_source, "error": repr(exc)},
            )
            self.observations.write({
                "at": utc_now(), "type": "AUTHORITY_REVALIDATION_ERROR",
                "market_id": market, "trigger_source": trigger_source, "error": repr(exc),
            })
            return None

        position = self.positions.get(market)
        authority = self.authority.get(market)
        if position is None or authority is None:
            self.observations.write({
                "at": utc_now(), "type": "AUTHORITY_POSITION_CHANGED",
                "market_id": market, "expected_episode_key": expected_episode_key,
                "observed_episode_key": None,
            })
            return None
        current_key = episode_key(position)
        if current_key != expected_episode_key:
            self.observations.write({
                "at": utc_now(), "type": "AUTHORITY_POSITION_CHANGED",
                "market_id": market, "expected_episode_key": expected_episode_key,
                "observed_episode_key": current_key,
            })
            return None
        return position, authority

    async def evaluate_market(self, market: str, trigger_source: str) -> None:
        if not self.experiment_validated or self.bound_experiment_id is None:
            return
        if self.experiment_id != self.bound_experiment_id or self.experiment_status not in ("RUNNING", "ENTRY_CLOSED"):
            return
        position = self.positions.get(market)
        if not position:
            return
        key = episode_key(position)
        if key in self.shadow_closed:
            return

        # Cheap direct-market screen. No counterfactual action is committed from
        # this state alone; any candidate helper action is revalidated against
        # canonical engine authority immediately before the final synchronous
        # direct-book fence below.
        econ = self.economics(position)
        mark = econ.get("mark_loss_usd")
        if mark is None or D(mark) < TRIGGER_LOSS:
            self.record_status(key, "BELOW_TRIGGER", market, econ, trigger_source)
            return

        exec_failed = self.rescue_error_for_episode(market, position)
        lock_info = econ.get("lock")
        if not exec_failed and lock_info is not None and D(lock_info["guaranteed_locked_pnl"]) >= -PAIR_LOCK_CAP:
            self.record_status(key, "PR3_AFFORDABLE", market, econ, trigger_source)
            return

        initial_failure = "RESCUE_EXECUTION_ERROR" if exec_failed else "PR3_CAP_OR_DEPTH_FAIL"
        if econ.get("sell") is None:
            self.record_status(key, "HELPER_NOT_EXECUTABLE", market, econ, trigger_source, initial_failure)
            return

        # Atomic authority fence: before a shadow SELL can exist, re-read the
        # current durable/in-memory engine authority for the SAME unpaired
        # episode. This is the canonical Stage-0 ownership check, not an
        # approximation from the public book.
        revalidated = await self.revalidate_authority(market, key, trigger_source)
        if revalidated is None:
            return
        current_position, authority = revalidated

        if authority.get("normalHedgeAdmissible") is True:
            self.record_status(
                key, "NORMAL_HEDGE_PRIORITY", market, self.economics(current_position),
                trigger_source, "CANONICAL_NORMAL_HEDGE_ADMISSIBLE",
            )
            return
        if authority.get("hedgeMakerOwnsResidual") is True:
            self.record_status(
                key, "HEDGE_MAKER_PRIORITY", market, self.economics(current_position),
                trigger_source, "HEDGE_MAKER_OWNS_RESIDUAL",
            )
            return

        current_exec_failed = self.rescue_error_for_episode(market, current_position)
        if authority.get("riskReductionInFlight") is True and not current_exec_failed:
            self.record_status(
                key, "RISK_REDUCTION_IN_FLIGHT", market, self.economics(current_position),
                trigger_source, "RISK_REDUCTION_IN_FLIGHT",
            )
            return

        # Final synchronous economic fence. There is no await from this point
        # through action persistence, so the public book used for mark/lock/SELL
        # is one event-loop state.
        current_econ = self.economics(current_position)
        current_mark = current_econ.get("mark_loss_usd")
        if current_mark is None or D(current_mark) < TRIGGER_LOSS:
            self.record_status(key, "BELOW_TRIGGER_AFTER_REVALIDATION", market, current_econ, trigger_source)
            return

        current_lock = current_econ.get("lock")
        if not current_exec_failed and current_lock is not None and D(current_lock["guaranteed_locked_pnl"]) >= -PAIR_LOCK_CAP:
            self.record_status(key, "PR3_AFFORDABLE_AFTER_REVALIDATION", market, current_econ, trigger_source)
            return

        failure_reason = "RESCUE_EXECUTION_ERROR" if current_exec_failed else "PR3_CAP_OR_DEPTH_FAIL"
        sell_info = current_econ.get("sell")
        if sell_info is None:
            self.record_status(key, "HELPER_NOT_EXECUTABLE_AFTER_REVALIDATION", market, current_econ, trigger_source, failure_reason)
            return

        decision_ns = time.monotonic_ns()
        action_id = "gsh_" + hashlib.sha256(f"{key}|{decision_ns}".encode()).hexdigest()[:20]
        held_token = current_econ.get("held_token")
        opposite_token = current_econ.get("opposite_token")
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
            "evidence_class": "FORWARD_CONTEMPORANEOUS_SHADOW",
            "position": current_position,
            "canonical_authority": authority,
            "economics": current_econ,
            "held_meta": self.token_meta.get(str(held_token or "")),
            "opposite_meta": self.token_meta.get(str(opposite_token or "")),
            "held_book": self.book_evidence(self.books.get(str(held_token or ""))),
            "opposite_book": self.book_evidence(self.books.get(str(opposite_token or ""))),
        }

        # Keep raw direct-CLOB evidence through the latest latency target even
        # if the real PAPER engine neutralizes the position immediately after
        # this shadow decision.
        self.latency_watch_until_ns[market] = max(
            self.latency_watch_until_ns.get(market, 0),
            decision_ns + (max(LATENCY_MS) + 25) * 1_000_000,
        )
        self.write_book_checkpoint(market, "HELPER_DECISION")
        self.actions.write(action)
        self.actions.sync()
        self.book_checkpoints.sync()
        self.raw_clob.sync()

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
            task = asyncio.create_task(self.capture_latency(action, ms))
            self.latency_tasks.add(task)
            task.add_done_callback(self.latency_tasks.discard)

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
        market = str(action["market_id"])
        position = action["position"]
        held_token = str(((action.get("economics") or {}).get("held_token")) or "")
        held_meta = action.get("held_meta") or {}
        deadline_ns = target_ns + 25_000_000

        try:
            wait = (target_ns - time.monotonic_ns()) / 1_000_000_000
            if wait > 0:
                await asyncio.sleep(wait)

            observed_book: Book | None = None
            while time.monotonic_ns() <= deadline_ns:
                book = self.books.get(held_token or "")
                if (
                    book is not None
                    and book.trusted
                    and book.received_mono_ns >= target_ns
                    and book.received_mono_ns <= deadline_ns
                ):
                    observed_book = book
                    break
                await asyncio.sleep(0.001)

            actual_ns = time.monotonic_ns()
            delayed_sell = (
                self.full_sell_economics(position, held_token, observed_book, held_meta)
                if observed_book is not None and held_token
                else None
            )
            econ = {
                "market_id": market,
                "held_token": held_token,
                "quantity": str(position.get("unpairedQuantity")),
                "sell": delayed_sell,
            } if observed_book is not None else None
            sell_executable = delayed_sell is not None
            status = "KNOWN" if observed_book is not None and sell_executable else "UNKNOWN"
            self.latency.write({
                "action_id": action["action_id"],
                "market_id": market,
                "target_latency_ms": latency_ms,
                "target_mono_ns": target_ns,
                "deadline_mono_ns": deadline_ns,
                "captured_at": utc_now(),
                "captured_mono_ns": actual_ns,
                "scheduler_error_ms": (actual_ns - target_ns) / 1_000_000.0,
                "observed_book_mono_ns": observed_book.received_mono_ns if observed_book else None,
                "book_observed_within_25ms": observed_book is not None,
                "full_sell_executable": sell_executable,
                "status": status,
                "held_book": self.book_evidence(observed_book),
                "held_meta": self.token_meta.get(held_token or ""),
                "economics": econ if status == "KNOWN" else None,
            })
            self.latency_samples_written += 1
            if status == "UNKNOWN":
                self.latency_unknown_samples += 1
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self.latency.write({
                "action_id": action["action_id"],
                "market_id": market,
                "target_latency_ms": latency_ms,
                "target_mono_ns": target_ns,
                "deadline_mono_ns": deadline_ns,
                "captured_at": utc_now(),
                "captured_mono_ns": time.monotonic_ns(),
                "status": "UNKNOWN",
                "error": repr(exc),
                "economics": None,
            })
            self.latency_samples_written += 1
            self.latency_unknown_samples += 1
            # Latency failure is represented as UNKNOWN by contract. It is not
            # converted into a fill or meteor, but remains explicit evidence.
        finally:
            self.latency.sync()

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

    def operator_stop(self, reason: str) -> None:
        if self.stop.is_set():
            return
        self.stop_reason = reason
        self.note_incomplete("OPERATOR_STOP", {"reason": reason})
        self.stop.set()

    async def stop_condition_loop(self) -> None:
        no_start_deadline = time.monotonic() + 600
        while not self.stop.is_set():
            if self.bound_experiment_id is None:
                if time.monotonic() > no_start_deadline:
                    self.stop_reason = "EXPERIMENT_DID_NOT_START"
                    self.note_incomplete("EXPERIMENT_DID_NOT_START_WITHIN_10_MINUTES")
                    self.stop.set()
                    return
                await asyncio.sleep(1)
                continue

            if self.experiment_id != self.bound_experiment_id:
                self.stop_reason = "EXPERIMENT_ID_CHANGED"
                self.note_incomplete(
                    "EXPERIMENT_ID_CHANGED",
                    {"bound": self.bound_experiment_id, "observed": self.experiment_id},
                )
                self.stop.set()
                return

            if self.experiment_status == "COMPLETED":
                self.stop_reason = "EXPERIMENT_COMPLETED"
                self.stop.set()
                return
            if self.experiment_status in ("FAILED", "ABORTED_START"):
                self.stop_reason = f"EXPERIMENT_{self.experiment_status}"
                self.note_incomplete(
                    "EXPERIMENT_TERMINAL_FAILURE",
                    {"experiment_id": self.experiment_id, "status": self.experiment_status},
                )
                self.stop.set()
                return

            if self.entry_closes_at:
                try:
                    closes = datetime.fromisoformat(str(self.entry_closes_at).replace("Z", "+00:00"))
                    deadline = closes.timestamp() + self.args.settlement_grace_minutes * 60
                    if time.time() > deadline and self.experiment_status != "COMPLETED":
                        self.stop_reason = "EXPERIMENT_NOT_COMPLETED_WITHIN_GRACE"
                        self.note_incomplete(
                            "EXPERIMENT_NOT_COMPLETED_WITHIN_GRACE",
                            {
                                "experiment_id": self.experiment_id,
                                "status": self.experiment_status,
                                "entry_closes_at": self.entry_closes_at,
                                "grace_minutes": self.args.settlement_grace_minutes,
                            },
                        )
                        self.stop.set()
                        return
                except ValueError:
                    self.stop_reason = "EXPERIMENT_TIMESTAMP_INVALID"
                    self.note_incomplete(
                        "EXPERIMENT_TIMESTAMP_INVALID",
                        {"entry_closes_at": self.entry_closes_at},
                    )
                    self.stop.set()
                    return
            await asyncio.sleep(1)

    async def finish(self) -> None:
        # The 500ms probe plus the contract's +25ms observation window must be
        # allowed to settle before any completion decision is sealed.
        pending = list(self.latency_tasks)
        if pending:
            try:
                await asyncio.wait_for(asyncio.gather(*pending, return_exceptions=True), timeout=2.0)
            except asyncio.TimeoutError:
                self.note_incomplete(
                    "LATENCY_TASK_TIMEOUT",
                    {"pending": sum(1 for task in pending if not task.done())},
                )
                for task in pending:
                    if not task.done():
                        task.cancel()
                await asyncio.gather(*pending, return_exceptions=True)

        expected_latency = self.helper_actions * len(LATENCY_MS)
        if self.latency_samples_written != expected_latency:
            self.note_incomplete(
                "LATENCY_SAMPLE_COUNT_MISMATCH",
                {
                    "helper_actions": self.helper_actions,
                    "expected": expected_latency,
                    "observed": self.latency_samples_written,
                },
            )

        # Seal one final canonical authority read. Failure here invalidates a
        # COMPLETE claim because experiment identity/status could not be proven.
        try:
            final_state = await self.get_json("/api/shadow/guardian-state")
            await self.apply_guardian_state(final_state, "final_state", evaluate=False)
            self.atomic_json("final_guardian_state.json", final_state)
        except Exception as exc:
            self.note_incomplete("FINAL_GUARDIAN_STATE_UNAVAILABLE", {"error": repr(exc)})

        try:
            self.atomic_json("final_dashboard.json", await self.get_json("/api/dashboard"))
        except Exception as exc:
            self.atomic_json("final_dashboard_error.json", {"at": utc_now(), "error": repr(exc)})
            self.note_incomplete("FINAL_DASHBOARD_UNAVAILABLE", {"error": repr(exc)})

        complete = (
            self.bound_experiment_id is not None
            and self.experiment_id == self.bound_experiment_id
            and self.experiment_status == "COMPLETED"
            and self.stop_reason == "EXPERIMENT_COMPLETED"
            and not self.incomplete_reasons
        )
        run_status = "COMPLETE" if complete else "TEST_INCOMPLETE"
        self.atomic_json("run_complete.json", {
            "status": run_status,
            "completed_at": utc_now(),
            "stop_reason": self.stop_reason,
            "source_git_sha": self.args.source_git_sha,
            "experiment_id": self.experiment_id,
            "bound_experiment_id": self.bound_experiment_id,
            "experiment_status": self.experiment_status,
            "experiment_started_at": self.experiment_started_at,
            "entry_closes_at": self.entry_closes_at,
            "helper_actions": self.helper_actions,
            "latency_samples_expected": expected_latency,
            "latency_samples_written": self.latency_samples_written,
            "latency_unknown_samples": self.latency_unknown_samples,
            "incomplete_reasons": sorted(self.incomplete_reasons),
            "worst_helper_guaranteed_pnl": str(self.min_helper_pnl) if self.min_helper_pnl is not None else None,
            "raw_active_market_events": self.raw_events,
            "paper_only": True,
        })

        self.sync_writers()
        for writer in (
            self.engine_events, self.position_states, self.raw_clob,
            self.observations, self.actions, self.latency, self.ledger,
            self.book_checkpoints, self.integrity,
        ):
            writer.close()

        if not complete:
            (self.out / "TEST_INCOMPLETE.txt").write_text(
                "TEST INCOMPLETE\n" + "\n".join(sorted(self.incomplete_reasons)) + "\n",
                encoding="utf-8",
            )

        # Hash the sealed evidence set after all append-only files are closed.
        evidence_files = sorted(
            p for p in self.out.iterdir()
            if p.is_file() and p.name not in {"SHA256SUMS.txt"}
        )
        sums = "".join(f"{sha256_file(path)}  {path.name}\n" for path in evidence_files)
        (self.out / "SHA256SUMS.txt").write_text(sums, encoding="utf-8")

    async def run(self) -> None:
        timeout = aiohttp.ClientTimeout(total=None, sock_connect=10, sock_read=None)
        fatal: Exception | None = None
        tasks: list[asyncio.Task[Any]] = []
        async with aiohttp.ClientSession(timeout=timeout) as session:
            self.session = session
            try:
                await self.preflight()
                tasks = [
                    asyncio.create_task(self.state_loop(), name="state_loop"),
                    asyncio.create_task(self.sse_loop(), name="sse_loop"),
                    asyncio.create_task(self.ws_loop(), name="clob_loop"),
                    asyncio.create_task(self.summary_loop(), name="summary_loop"),
                    asyncio.create_task(self.stop_condition_loop(), name="stop_condition_loop"),
                ]
                while not self.stop.is_set():
                    for task in tasks:
                        if task.done():
                            exc = task.exception()
                            if exc is not None:
                                raise RuntimeError(f"BACKGROUND_TASK_FAILED:{task.get_name()}:{exc!r}") from exc
                    await asyncio.sleep(0.25)
            except Exception as exc:
                fatal = exc
                self.stop_reason = self.stop_reason or "FATAL_EXCEPTION"
                self.note_incomplete("FATAL_EXCEPTION", {"error": repr(exc)})
                self.stop.set()
            finally:
                self.stop.set()
                for task in tasks:
                    if not task.done():
                        task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
                await self.finish()

        if fatal is not None:
            raise fatal


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--engine-url", default=os.getenv("ENGINE_BASE_URL", "http://127.0.0.1:8080"))
    p.add_argument("--engine-read-token", default=os.getenv("ENGINE_READ_TOKEN"))
    p.add_argument("--source-git-sha", required=True)
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
            loop.add_signal_handler(sig, lambda s=sig: shadow.operator_stop(s.name))
        except NotImplementedError:
            pass
    await shadow.run()


if __name__ == "__main__":
    asyncio.run(amain())
