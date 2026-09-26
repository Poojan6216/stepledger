"""StepledgerPlugin: one Temporal plugin, added next to LangGraphPlugin."""

from __future__ import annotations

import asyncio
import dataclasses
from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
from datetime import timedelta
from pathlib import Path
from typing import Any, Literal

import temporalio.worker
from temporalio.converter import DataConverter
from temporalio.plugin import SimplePlugin
from temporalio.worker import (
    ActivityInboundInterceptor,
    WorkflowInboundInterceptor,
    WorkflowInterceptorClassInput,
)

from stepledger._compat import plugin_activity_names
from stepledger.config import load_settings
from stepledger.ledger.activity_interceptor import LedgerActivityInbound, WriteOptions
from stepledger.ledger.seal import SealActivity
from stepledger.ledger.store import LedgerStore
from stepledger.ledger.workflow_interceptor import LedgerInbound
from stepledger.llm.meter import CostMeter


class StepledgerInterceptor(temporalio.worker.Interceptor):
    """Worker interceptor: the activity write path plus the deterministic workflow side."""

    def __init__(
        self,
        *,
        tracked: frozenset[str],
        store: LedgerStore,
        meter: CostMeter,
        options: WriteOptions,
        seal: bool,
        seal_timeout: timedelta,
    ) -> None:
        self.tracked = tracked
        self._store = store
        self._meter = meter
        self._options = options
        self._inbound = type(
            "StepledgerWorkflowInbound",
            (LedgerInbound,),
            {"tracked": tracked, "seal_enabled": seal, "seal_timeout": seal_timeout},
        )

    def intercept_activity(self, next: ActivityInboundInterceptor) -> ActivityInboundInterceptor:
        return LedgerActivityInbound(next, self._store, self._meter, self._options)

    def workflow_interceptor_class(
        self, input: WorkflowInterceptorClassInput
    ) -> type[WorkflowInboundInterceptor] | None:
        return self._inbound


class StepledgerPlugin(SimplePlugin):
    """Every LangGraph node Activity, recorded once per execution in your Postgres.

    Pass it to `Client.connect(plugins=[...])`; workers built from that client inherit it (the
    SDK prepends client plugins to each worker's plugins), and pass `LangGraphPlugin` to the
    worker as usual::

        lg = LangGraphPlugin(graphs={"investigate": build_graph()}, ...)
        sl = StepledgerPlugin(dsn=os.environ["STEPLEDGER_DSN"], langgraph=lg)
        client = await Client.connect("localhost:7233", plugins=[sl])
        worker = Worker(client, task_queue="agents", workflows=[...], plugins=[lg])
    """

    def __init__(
        self,
        dsn: str,
        *,
        langgraph: Any,
        external_storage: bool = True,
        payload_size_threshold: int = 64 * 1024,
        store_outputs: Literal["full", "hash_only"] = "full",
        on_ledger_error: Literal["fail", "warn"] = "fail",
        snapshot_first_input: bool = True,
        seal: bool = True,
        seal_timeout: timedelta = timedelta(seconds=30),
        dedupe: bool = True,
        prices: Mapping[str, Any] | None = None,
        chunk_sizes: tuple[int, int, int] | None = None,
    ) -> None:
        if prices is None:
            prices = {k: v.model_dump() for k, v in load_settings().prices.items()}
        self.options = {
            "external_storage": external_storage,
            "payload_size_threshold": payload_size_threshold,
            "store_outputs": store_outputs,
            "on_ledger_error": on_ledger_error,
            "snapshot_first_input": snapshot_first_input,
            "seal": seal,
            "dedupe": dedupe,
            "chunk_sizes": chunk_sizes,
        }
        self.store = LedgerStore(dsn)
        self.dsn = dsn
        self.tracked = plugin_activity_names(langgraph)
        self.interceptor = StepledgerInterceptor(
            tracked=self.tracked,
            store=self.store,
            meter=CostMeter(prices),
            options=WriteOptions(
                store_outputs=store_outputs,
                on_ledger_error=on_ledger_error,
                snapshot_first_input=snapshot_first_input,
            ),
            seal=seal,
            seal_timeout=seal_timeout,
        )
        self._workers = 0
        self._closing: asyncio.Future[None] | None = None
        self.storage_driver: Any = None
        converter: Any = None
        if external_storage:
            from stepledger.storage import make_external_storage

            self.storage_driver, ext = make_external_storage(
                dsn,
                dedupe=dedupe,
                payload_size_threshold=payload_size_threshold,
                chunk_sizes=chunk_sizes,
            )

            def converter(existing: DataConverter | None) -> DataConverter:
                base = existing or DataConverter.default
                if base.external_storage is not None:
                    raise ValueError(
                        "the client's data converter already has External Storage configured;"
                        " Stepledger will not replace it (in-flight histories would become"
                        " unreadable). Pass external_storage=False to StepledgerPlugin to keep"
                        " the existing driver, or remove it to use Stepledger's."
                    )
                return dataclasses.replace(base, external_storage=ext)

        super().__init__(
            "stepledger.StepledgerPlugin",
            data_converter=converter,
            interceptors=[self.interceptor],
            activities=[SealActivity(self.store, on_ledger_error).seal],
            run_context=self._run_context,
        )

    @classmethod
    def from_config(
        cls,
        *,
        langgraph: Any,
        path: str | Path | None = None,
        seal: bool = True,
        seal_timeout: timedelta = timedelta(seconds=30),
    ) -> StepledgerPlugin:
        """Build the plugin from `stepledger.yaml` (or `path`, or `$STEPLEDGER_CONFIG`) with
        environment variables (`STEPLEDGER_DSN`, ...) overriding the file."""
        cfg = load_settings(path)
        return cls(
            cfg.dsn,
            langgraph=langgraph,
            external_storage=cfg.storage.enabled,
            payload_size_threshold=cfg.storage.payload_size_threshold,
            store_outputs=cfg.ledger.store_outputs,
            on_ledger_error=cfg.ledger.on_ledger_error,
            snapshot_first_input=cfg.ledger.snapshot_first_input,
            seal=seal,
            seal_timeout=seal_timeout,
            dedupe=cfg.storage.dedupe,
            prices={k: v.model_dump() for k, v in cfg.prices.items()},
            chunk_sizes=(cfg.storage.chunk.min, cfg.storage.chunk.avg, cfg.storage.chunk.max),
        )

    @asynccontextmanager
    async def _run_context(self) -> AsyncIterator[None]:
        """Open the ledger pool while any worker built from this plugin runs."""
        self._workers += 1
        try:
            yield
        finally:
            self._workers -= 1
            if self._workers == 0:
                closing = asyncio.ensure_future(self._close())
                try:
                    await asyncio.shield(closing)
                except asyncio.CancelledError:
                    # Worker.__aexit__ cancels its run task as soon as shutdown completes
                    # (worker/_worker.py:969), and the SDK turns any exception escaping the run
                    # into a cancel of the caller's task. Let the close finish in the background.
                    self._closing = closing

    async def _close(self) -> None:
        await self.store.close()
        if self.storage_driver is not None:
            await self.storage_driver.close()
