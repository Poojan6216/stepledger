"""The `stepledger.seal` Activity: closes a run in the ledger when the workflow really ends."""

from __future__ import annotations

from dataclasses import asdict
from typing import Any

from temporalio import activity

from stepledger._hooks import fault
from stepledger.keys import RunKey
from stepledger.ledger.store import LedgerStore
from stepledger.ledger.workflow_interceptor import SEAL_ACTIVITY, SealInput


class SealActivity:
    def __init__(self, store: LedgerStore) -> None:
        self._store = store

    @activity.defn(name=SEAL_ACTIVITY)
    async def seal(self, inp: SealInput) -> dict[str, Any]:
        info = activity.info()
        run = RunKey(
            info.workflow_namespace or info.namespace,
            info.workflow_id or "",
            info.workflow_run_id or "",
        )
        await fault("F5", attempt=info.attempt)
        async with self._store.tx() as tx:
            result = await tx.seal_run(
                run,
                status=inp.status,
                node_count=inp.node_count,
                accepted_count=inp.accepted_count,
                commits=inp.commits,
                abandons=inp.abandons,
                final_state_hash=inp.final_state_hash,
                workflow_type=inp.workflow_type,
            )
        return asdict(result)
