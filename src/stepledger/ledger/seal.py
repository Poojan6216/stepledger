"""The `stepledger.seal` Activity: closes a run in the ledger when the workflow really ends."""

from __future__ import annotations

import logging
from dataclasses import asdict
from typing import Any

from temporalio import activity

from stepledger._hooks import fault
from stepledger.keys import RunKey
from stepledger.ledger.activity_interceptor import DB_ERRORS
from stepledger.ledger.store import LedgerStore
from stepledger.ledger.workflow_interceptor import SEAL_ACTIVITY, SealInput

log = logging.getLogger("stepledger")


class SealActivity:
    def __init__(self, store: LedgerStore, on_ledger_error: str = "fail") -> None:
        self._store = store
        self._on_ledger_error = on_ledger_error

    @activity.defn(name=SEAL_ACTIVITY)
    async def seal(self, inp: SealInput) -> dict[str, Any]:
        info = activity.info()
        run = RunKey(
            info.workflow_namespace or info.namespace,
            info.workflow_id or "",
            info.workflow_run_id or "",
        )
        await fault("F5", attempt=info.attempt)
        try:
            return await self._seal(run, inp)
        except DB_ERRORS as e:
            if self._on_ledger_error == "fail":
                raise  # Temporal retries the seal until the database answers
            # warn mode: the workflow may finish; the run stays unsealed for `reconcile`
            log.warning(
                "stepledger: seal failed for %s; leaving the run open (warn mode): %s", run, e
            )
            return {"sealed": False, "error": str(e)[:200]}

    async def _seal(self, run: RunKey, inp: SealInput) -> dict[str, Any]:
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
