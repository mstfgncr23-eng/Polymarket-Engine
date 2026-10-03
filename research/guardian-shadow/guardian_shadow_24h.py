#!/usr/bin/env python3
"""Passive PAPER runtime recorder. Engine Guardian is the only policy authority.

This file records unmodified runtime responses/events and public CLOB frames.
It does not evaluate eligibility, walk execution books, or model any fills/PnL.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import time
from pathlib import Path
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    import aiohttp

CLOB_WS = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
READ_PATHS = ("/api/dashboard", "/api/shadow/guardian-state", "/events")


class Jsonl:
    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self.fh = path.open("a", encoding="utf-8", buffering=1)

    def write(self, value: dict[str, Any]) -> None:
        self.fh.write(json.dumps(value, separators=(",", ":"), ensure_ascii=False) + "\n")

    def close(self) -> None:
        self.fh.close()


def active_tokens(state: dict[str, Any]) -> set[str]:
    """Select recording scope from engine positions; no business decision."""
    markets = {row["position"]["marketId"] for row in state.get("positions", [])}
    return {row["tokenId"] for row in state.get("tokenMap", []) if row["marketId"] in markets}


class PassiveRecorder:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.out = Path(args.output_dir)
        self.out.mkdir(parents=True, exist_ok=True)
        self.tokens: set[str] = set()
        self.stop = asyncio.Event()
        self.raw = Jsonl(self.out / "clob_active_markets.jsonl")
        self.runtime = Jsonl(self.out / "runtime_samples.jsonl")
        self.events = Jsonl(self.out / "engine_events.jsonl")
        self.diagnostics = Jsonl(self.out / "runtime_diagnostics.jsonl")
        (self.out / "manifest.json").write_text(json.dumps({
            "recorder": "PASSIVE_ENGINE_GUARDIAN_RECORDER_V1",
            "source_git_sha": args.source_git_sha,
            "policy_authority": "Engine Guardian", "counterfactual_policy": False,
        }), encoding="utf-8")

    async def get(self, session: aiohttp.ClientSession, path: str):
        if path not in READ_PATHS:
            raise ValueError("READ_ENDPOINT_NOT_ALLOWED")
        return await session.get(self.args.engine_url.rstrip("/") + path,
                                 headers={"Authorization": f"Bearer {self.args.engine_read_token}"})

    async def poll(self, session: aiohttp.ClientSession) -> None:
        while not self.stop.is_set():
            for path in READ_PATHS[:2]:
                started = time.monotonic_ns()
                try:
                    async with await self.get(session, path) as response:
                        response.raise_for_status()
                        data = await response.json()
                    if path.endswith("guardian-state"):
                        if data.get("paperMode") is not True:
                            raise RuntimeError("PAPER_RUNTIME_REQUIRED")
                        self.tokens = active_tokens(data)
                    self.runtime.write({"received_ns": time.monotonic_ns(), "endpoint": path,
                                        "request_latency_ns": time.monotonic_ns() - started, "response": data})
                except RuntimeError:
                    raise
                except Exception as exc:
                    self.diagnostics.write({"endpoint": path, "error": repr(exc), "received_ns": time.monotonic_ns()})
            await asyncio.sleep(1)

    async def sse(self, session: aiohttp.ClientSession) -> None:
        while not self.stop.is_set():
            try:
                async with await self.get(session, "/events") as response:
                    response.raise_for_status()
                    async for line in response.content:
                        self.events.write({"received_ns": time.monotonic_ns(), "raw": line.decode("utf-8")})
            except Exception as exc:
                self.diagnostics.write({"stream": "SSE", "error": repr(exc)})
                await asyncio.sleep(1)

    async def clob(self, session: aiohttp.ClientSession) -> None:
        import aiohttp
        while not self.stop.is_set():
            scope = set(self.tokens)
            if not scope:
                await asyncio.sleep(1)
                continue
            try:
                async with session.ws_connect(CLOB_WS, heartbeat=10) as ws:
                    await ws.send_json({"assets_ids": sorted(scope), "type": "market"})
                    while scope == self.tokens and not self.stop.is_set():
                        try:
                            msg = await ws.receive(timeout=1)
                        except asyncio.TimeoutError:
                            continue
                        if msg.type == aiohttp.WSMsgType.TEXT and scope == self.tokens:
                            self.raw.write({"received_ns": time.monotonic_ns(), "subscribed_tokens": sorted(scope), "raw": msg.data})
                        elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                            break
            except Exception as exc:
                self.diagnostics.write({"stream": "CLOB", "error": repr(exc)})
                await asyncio.sleep(1)

    async def run(self) -> None:
        import aiohttp
        tasks = []
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=None, sock_connect=10),
                                             read_bufsize=2 * 1024 * 1024) as session:
                tasks = [asyncio.create_task(f(session)) for f in (self.poll, self.sse, self.clob)]
                deadline = time.monotonic() + self.args.duration_hours * 3600
                while time.monotonic() < deadline:
                    for task in tasks:
                        if task.done():
                            task.result()
                            raise RuntimeError("RECORDER_TASK_STOPPED")
                    await asyncio.sleep(0.25)
        finally:
            self.stop.set()
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            for sink in (self.raw, self.runtime, self.events, self.diagnostics):
                sink.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--engine-url", default=os.getenv("ENGINE_BASE_URL", "http://127.0.0.1:8080"))
    parser.add_argument("--engine-read-token", default=os.getenv("ENGINE_READ_TOKEN"))
    parser.add_argument("--source-git-sha", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--duration-hours", type=float, default=24)
    args = parser.parse_args()
    if not args.engine_read_token or args.duration_hours <= 0:
        parser.error("A read token and positive duration are required")
    return args


if __name__ == "__main__":
    asyncio.run(PassiveRecorder(parse_args()).run())
