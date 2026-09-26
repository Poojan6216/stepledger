"""7.7 GC retention shorter than the namespace's workflow retention: `stepledger gc` must refuse."""

from __future__ import annotations

import asyncio
import sys

from bench.adversarial.common import ROOT, AttackResult


async def _gc(days: str) -> tuple[int, str]:
    proc = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "stepledger.cli",
        "gc",
        "--retention-days",
        days,
        cwd=ROOT,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
    )
    out, _ = await proc.communicate()
    return proc.returncode or 0, out.decode()


async def run() -> AttackResult:
    res = AttackResult("7.7", "GC retention shorter than the namespace's", "gc refuses to run")
    short_code, short_out = await _gc("0.5")  # 12 h < the dev server's 24 h
    ok_code, _ = await _gc("1")  # equal: allowed (dry run)
    res.measured = {
        "retention_0_5_days_exit_code": short_code,
        "refused_message": short_out.strip().splitlines()[-1][:200],
        "retention_1_day_exit_code": ok_code,
    }
    res.rate = f"refused {int(short_code == 2)}/1; equal retention allowed {int(ok_code == 0)}/1"
    res.holds = short_code == 2 and ok_code == 0
    return res
