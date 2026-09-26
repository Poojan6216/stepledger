"""The write path: one fenced ledger row per node Activity attempt, inside that attempt.

For an Activity carrying a `stepledger-seq` header, the interceptor runs the node, serializes its
`ActivityOutput` with the worker's own payload converter, and in one Postgres transaction:
upserts the row under the attempt's fence, applies the commits and abandons the header carries,
and appends an audit row. A write that is fenced out raises instead of returning success.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Literal

import psycopg
import psycopg_pool
from temporalio import activity
from temporalio.exceptions import ApplicationError
from temporalio.worker import ActivityInboundInterceptor, ExecuteActivityInput

from stepledger import headers
from stepledger._compat import ActivityInput, ActivityOutput, task_path_str
from stepledger._hooks import fault
from stepledger.canonical import Serialized, serialize
from stepledger.errors import FencedOut, FencedOutFinal
from stepledger.keys import Fence, LedgerKey
from stepledger.ledger.context import NodeContext, _current
from stepledger.ledger.store import Kind, LedgerStore, NodeWrite
from stepledger.llm.meter import CostMeter

log = logging.getLogger("stepledger")

DB_ERRORS = (psycopg.Error, psycopg_pool.PoolTimeout, OSError)


@dataclass(frozen=True)
class WriteOptions:
    store_outputs: Literal["full", "hash_only"] = "full"
    on_ledger_error: Literal["fail", "warn"] = "fail"
    snapshot_first_input: bool = True


def _kind(out: Any) -> Kind:
    if isinstance(out, ActivityOutput):
        if out.langgraph_interrupts is not None:
            return "INTERRUPT"
        if out.langgraph_command is not None:
            return "COMMAND"
    return "UPDATE"


def _node_meta(inp: Any) -> dict[str, Any]:
    if not isinstance(inp, ActivityInput):
        return {}
    config = inp.langgraph_config or {}
    meta = dict(config.get("metadata") or {})
    meta["__task_id"] = (config.get("configurable") or {}).get("__pregel_task_id")
    return meta


def _path(meta: dict[str, Any]) -> str | None:
    path = meta.get("langgraph_path")
    return task_path_str(tuple(path)) if path is not None else None


class LedgerActivityInbound(ActivityInboundInterceptor):
    def __init__(
        self,
        next: ActivityInboundInterceptor,
        store: LedgerStore,
        meter: CostMeter,
        options: WriteOptions,
    ) -> None:
        super().__init__(next)
        self._store = store
        self._meter = meter
        self._opts = options

    async def execute_activity(self, input: ExecuteActivityInput) -> Any:
        try:
            seq, commits, abandons = headers.decode(input.headers)
        except ValueError as e:
            # WORKFLOW_ONLY_CODEC encodes headers on the workflow side but never decodes them on
            # the activity side; retrying would never help.
            raise ApplicationError(
                f"stepledger: cannot read its headers ({e}); use HeaderCodecBehavior.CODEC or"
                " NO_CODEC on the client",
                type="StepledgerHeaderError",
                non_retryable=True,
            ) from e
        if seq is None:
            return await self.next.execute_activity(input)

        info = activity.info()
        key = LedgerKey(
            info.workflow_namespace or info.namespace,
            info.workflow_id or "",
            info.workflow_run_id or "",
            seq,
        )
        fence = Fence(info.current_attempt_scheduled_time, info.attempt)
        inp = input.args[0] if input.args else None
        meta = _node_meta(inp)

        started = datetime.now(UTC)
        node_ctx = NodeContext(key=key, fence=fence, store=self._store)
        token = _current.set(node_ctx)
        try:
            with self._meter.scope() as usage:
                out = await self.next.execute_activity(input)
        finally:
            _current.reset(token)
        finished = datetime.now(UTC)

        conv = activity.payload_converter()
        ser = serialize(out, conv)
        write = self._node_write(info, meta, inp, ser, _kind(out), usage, started, finished, seq)

        node_name = meta.get("langgraph_node")
        # F6 (chaos only): a zombie hangs here past its timeout, then writes late.
        await fault("F6", seq=seq, attempt=fence.attempt, node=node_name)
        try:
            t0 = time.perf_counter()
            async with self._store.tx() as tx:
                await tx.lock_seqs(key.run, [seq, *commits, *abandons])
                r = await tx.fenced_upsert(key, fence, write)
                await tx.commit(key.run, commits)
                await tx.abandon(key.run, abandons)
                await fault("F4", seq=seq, attempt=fence.attempt, node=node_name)
                await tx.audit_attempt(
                    key,
                    fence,
                    "WROTE" if r.wrote else "FENCED_OUT",
                    output_hash=ser.hash,
                    tokens_in=write.tokens_in,
                    tokens_out=write.tokens_out,
                    cost_usd=write.cost_usd,
                    write_ms=(time.perf_counter() - t0) * 1000,
                    note="run sealed" if r.run_sealed else None,
                    worker_time=finished,
                )
        except DB_ERRORS as e:
            if self._opts.on_ledger_error == "fail":
                raise
            log.warning(
                "stepledger: ledger write failed for %s; continuing (warn mode): %s", key, e
            )
            await self._degrade_later(key, fence, ser.hash, str(e))
            return out

        if not r.wrote:
            if r.row_status == "PROVISIONAL" and not r.run_sealed:
                raise FencedOut(key, fence)
            raise FencedOutFinal(key, fence, "SEALED" if r.run_sealed else r.row_status)
        await fault("F3", seq=seq, attempt=fence.attempt, node=node_name)
        return out

    def _node_write(
        self,
        info: activity.Info,
        meta: dict[str, Any],
        inp: Any,
        ser: Serialized,
        kind: Kind,
        usage: Any,
        started: datetime,
        finished: datetime,
        seq: int,
    ) -> NodeWrite:
        full = self._opts.store_outputs == "full"
        conv = activity.payload_converter()
        input_hash = input_bytes = snapshot = None
        if isinstance(inp, ActivityInput) and inp.args:
            # input_hash covers exactly what the node received (its state, or the subset its
            # input schema selects), so materialize can check its fold against every row.
            node_input = serialize(inp.args[0] if len(inp.args) == 1 else list(inp.args), conv)
            input_hash, input_bytes = node_input.hash, len(node_input.data)
            if self._opts.snapshot_first_input and seq == 0 and full and node_input.is_json:
                snapshot = node_input.plain
        activity_type = info.activity_type
        return NodeWrite(
            activity_id=info.activity_id,
            activity_type=activity_type,
            graph=activity_type.rsplit(".", 1)[0] if "." in activity_type else None,
            node=meta.get("langgraph_node"),
            lg_step=meta.get("langgraph_step"),
            lg_path=_path(meta),
            lg_task_id=meta.get("__task_id"),
            checkpoint_ns=meta.get("langgraph_checkpoint_ns"),
            kind=kind,
            output_json=ser.plain if (full and ser.is_json) else None,
            output_bytes=ser.data if (full and not ser.is_json) else None,
            output_encoding=ser.encoding,
            output_hash=ser.hash,
            input_snapshot=snapshot,
            input_hash=input_hash,
            input_bytes=input_bytes,
            tokens_in=usage.tokens_in if usage.calls else None,
            tokens_out=usage.tokens_out if usage.calls else None,
            cost_usd=usage.cost if usage.calls else None,
            started_at=started,
            finished_at=finished,
        )

    async def _degrade_later(
        self, key: LedgerKey, fence: Fence, output_hash: str, why: str
    ) -> None:
        """Warn mode: best effort to flag the run degraded and audit the lost write as DB_ERROR.
        During an outage this fails too; the seal then notices the missing rows."""
        try:
            async with self._store.tx() as tx:
                await tx.mark_degraded(key.run)
                await tx.audit_attempt(
                    key, fence, "DB_ERROR", output_hash=output_hash, note=why[:200]
                )
        except DB_ERRORS:
            pass
