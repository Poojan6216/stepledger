"""Errors Stepledger raises inside Activities. All are Temporal ApplicationErrors."""

from __future__ import annotations

from temporalio.exceptions import ApplicationError

from stepledger.keys import Fence, LedgerKey


class FencedOut(ApplicationError):
    """R1: a newer attempt already wrote this PROVISIONAL row. Retryable: if this attempt is
    stale Temporal discards the failure; if the fence was wrong, the retry heals it."""

    def __init__(self, key: LedgerKey, fence: Fence) -> None:
        super().__init__(
            f"stepledger: attempt {fence.attempt} of seq {key.seq} fenced out by a newer attempt",
            type="StepledgerFencedOut",
            non_retryable=False,
        )


class FencedOutFinal(ApplicationError):
    """R2: the row is COMMITTED or ABANDONED (or the run is sealed); only a stale attempt can get
    here, so a retry could never succeed. Non-retryable."""

    def __init__(self, key: LedgerKey, fence: Fence, status: str | None) -> None:
        super().__init__(
            f"stepledger: attempt {fence.attempt} of seq {key.seq} reached a final row ({status})",
            type="StepledgerFencedOutFinal",
            non_retryable=True,
        )


class EffectDivergence(ApplicationError):
    """once(): the same effect key was used with a different request. Non-retryable."""

    def __init__(self, key: str, name: str) -> None:
        super().__init__(
            f"stepledger: effect {name!r} (key {key[:12]}) retried with a different request",
            type="StepledgerEffectDivergence",
            non_retryable=True,
        )


class UnknownEffectOutcome(ApplicationError):
    """once(): an earlier attempt started the effect and its outcome is unknown. Non-retryable;
    resolve it with `stepledger resolve <key> --outcome done|not-done`."""

    def __init__(self, key: str, name: str) -> None:
        super().__init__(
            f"stepledger: effect {name!r} (key {key}) has an unknown outcome;"
            f" run `stepledger resolve {key} --outcome done|not-done`",
            type="StepledgerUnknownEffectOutcome",
            non_retryable=True,
        )


class NotInTrackedNode(RuntimeError):
    """once() / the journal need the ledger context of a tracked node Activity."""
