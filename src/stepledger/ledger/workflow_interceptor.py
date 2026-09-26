"""Workflow side of the ledger. Deterministic: its only state is counters and id sets derived from
workflow events. No I/O, no clock, no randomness.

Outbound: every tracked node Activity gets `stepledger-seq` (a per-run counter) plus the commit
and abandon ids that no successful carrier has confirmed yet. Ids stay pending until an Activity
that carried them completes successfully; a failed carrier's ids ride on the next one.

Inbound: when the workflow really ends (classified the way the SDK does it), the
`stepledger.seal` Activity commits the rest and records the run status and final-state hash.
"""

from __future__ import annotations

import asyncio
import dataclasses
from collections.abc import Collection
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any

import temporalio.exceptions
from temporalio import workflow
from temporalio.worker import (
    ExecuteWorkflowInput,
    StartActivityInput,
    WorkflowInboundInterceptor,
    WorkflowOutboundInterceptor,
)
from temporalio.workflow import ActivityCancellationType, ActivityHandle, ContinueAsNewError

from stepledger import headers
from stepledger._compat import langgraph_used_in_this_run
from stepledger.canonical import serialize

SEAL_ACTIVITY = "stepledger.seal"
SEAL_PATCH = "stepledger-seal-v1"


@dataclass
class SealInput:
    status: str
    commits: list[int]
    abandons: list[int]
    node_count: int
    accepted_count: int
    final_state_hash: str | None
    workflow_type: str | None = None


@dataclass
class RunState:
    """Per-run ledger state. Deterministic by construction."""

    next_seq: int = 0
    accepted: int = 0
    pending_commits: set[int] = field(default_factory=set)
    pending_abandons: set[int] = field(default_factory=set)

    def assign(self) -> int:
        seq = self.next_seq
        self.next_seq += 1
        return seq

    def unconfirmed(self) -> tuple[list[int], list[int]]:
        return sorted(self.pending_commits), sorted(self.pending_abandons)

    def observe(
        self, seq: int, handle: ActivityHandle[Any], carried: tuple[list[int], list[int]]
    ) -> None:
        # cancelled() first: exception() raises CancelledError on a cancelled task.
        ok = not handle.cancelled() and handle.exception() is None
        if ok:
            # The carrier's transaction committed, so the ids it carried are confirmed.
            self.pending_commits.difference_update(carried[0])
            self.pending_abandons.difference_update(carried[1])
            self.pending_commits.add(seq)  # its result reached workflow code: accepted
            self.accepted += 1
        else:
            # Failed, or cancelled: a completion racing a cancel request is never delivered.
            self.pending_abandons.add(seq)


class LedgerOutbound(WorkflowOutboundInterceptor):
    def __init__(
        self, next: WorkflowOutboundInterceptor, state: RunState, tracked: Collection[str]
    ) -> None:
        super().__init__(next)
        self._state = state
        self._tracked = tracked

    def start_activity(self, input: StartActivityInput) -> ActivityHandle[Any]:
        if input.activity not in self._tracked:
            return self.next.start_activity(input)
        seq = self._state.assign()
        carried = self._state.unconfirmed()
        input = dataclasses.replace(
            input, headers=headers.with_headers(input.headers, seq, *carried)
        )
        handle = self.next.start_activity(input)
        handle.add_done_callback(lambda h: self._state.observe(seq, h, carried))
        return handle


def classify(e: BaseException) -> str | None:
    """How the SDK will end the run for this exception, or None for a workflow *task* failure
    (retried from history; the run is not over, so it must not be sealed).
    Mirrors worker/_workflow_instance.py:2748-2797 with public API only."""
    if isinstance(e, ContinueAsNewError):
        return "CONTINUED_AS_NEW"
    if workflow.cancellation_reason() is not None and (
        isinstance(e, asyncio.CancelledError) or temporalio.exceptions.is_cancelled_exception(e)
    ):
        return "CANCELLED"
    if isinstance(e, asyncio.CancelledError):
        return "FAILED"  # the SDK converts it to a CancelledError failure
    if workflow.is_failure_exception(e):
        return "FAILED"
    return None


class LedgerInbound(WorkflowInboundInterceptor):
    tracked: Collection[str] = frozenset()
    seal_enabled: bool = True
    seal_timeout: timedelta = timedelta(seconds=30)

    def init(self, outbound: WorkflowOutboundInterceptor) -> None:
        self._state = RunState()
        super().init(LedgerOutbound(outbound, self._state, self.tracked))

    async def execute_workflow(self, input: ExecuteWorkflowInput) -> Any:
        try:
            result = await self.next.execute_workflow(input)
        except BaseException as e:
            status = classify(e)
            if status is None:
                raise
            await self._seal(status, None, cancelled=status == "CANCELLED")
            raise
        final_hash = serialize(result, workflow.payload_converter()).hash
        await self._seal("COMPLETED", final_hash, cancelled=False)
        return result

    async def _seal(self, status: str, final_hash: str | None, *, cancelled: bool) -> None:
        if not self.seal_enabled:
            return
        if self._state.next_seq == 0 and not langgraph_used_in_this_run():
            return  # not a LangGraph run (a fully cached LangGraph run still seals)
        if not workflow.patched(SEAL_PATCH):
            return  # a run that started before the plugin: replay it unchanged
        commits, abandons = self._state.unconfirmed()
        seal_input = SealInput(
            status=status,
            commits=commits,
            abandons=abandons,
            node_count=self._state.next_seq,
            accepted_count=self._state.accepted,
            final_state_hash=final_hash,
            workflow_type=workflow.info().workflow_type,
        )
        if cancelled:
            # The run is being cancelled; the seal must still run. ABANDON means the workflow
            # does not wait for a cancel acknowledgement, and shield keeps a second cancel from
            # interrupting the await.
            handle = workflow.start_activity(
                SEAL_ACTIVITY,
                seal_input,
                start_to_close_timeout=self.seal_timeout,
                cancellation_type=ActivityCancellationType.ABANDON,
            )
            await asyncio.shield(handle)
        else:
            await workflow.execute_activity(
                SEAL_ACTIVITY, seal_input, start_to_close_timeout=self.seal_timeout
            )
