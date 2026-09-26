"""Shared fixtures. Integration tests need the dev environment: `scripts/dev.sh up`."""

from __future__ import annotations

from collections.abc import Generator

import psycopg
import pytest

from stepledger.config import resolve_dsn
from stepledger.ledger.store import init_db


@pytest.hookimpl(wrapper=True)
def pytest_runtest_makereport(
    item: pytest.Item, call: pytest.CallInfo[None]
) -> Generator[None, pytest.TestReport, pytest.TestReport]:
    """A test marked never_skip that gets skipped is reported as a failure."""
    report = yield
    if report.skipped and item.get_closest_marker("never_skip") is not None:
        report.outcome = "failed"
        report.longrepr = f"never_skip test was skipped: {report.longrepr}"
    return report


@pytest.fixture(scope="session")
def dsn() -> str:
    d = resolve_dsn()
    try:
        with psycopg.connect(d, connect_timeout=3):
            pass
    except psycopg.OperationalError as e:
        pytest.skip(f"Postgres not reachable at {d}: run scripts/dev.sh up ({e})")
    init_db(d)
    return d
