"""stepledger.yaml reaches the plugin: every ledger/storage/prices setting in the file, and the
environment overriding the file."""

from __future__ import annotations

from pathlib import Path

import pytest
from temporalio.contrib.langgraph import LangGraphPlugin

from stepledger import StepledgerPlugin

YAML = """
schema_version: 1
dsn: postgresql://cfg@db/ledger
ledger:
  store_outputs: hash_only
  on_ledger_error: warn
  snapshot_first_input: false
storage:
  enabled: true
  payload_size_threshold: 16384
  dedupe: false
  chunk: {min: 2048, avg: 8192, max: 32768}
prices:
  my-model: {input: 0.5, output: 2.5}
"""


def test_from_config_reads_every_section(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("STEPLEDGER_DSN", raising=False)
    path = tmp_path / "stepledger.yaml"
    path.write_text(YAML)
    sl = StepledgerPlugin.from_config(langgraph=LangGraphPlugin(graphs={}), path=path)
    assert sl.dsn == "postgresql://cfg@db/ledger"
    assert sl.options["store_outputs"] == "hash_only"
    assert sl.options["on_ledger_error"] == "warn"
    assert sl.options["snapshot_first_input"] is False
    assert sl.options["payload_size_threshold"] == 16384
    assert sl.options["dedupe"] is False
    assert sl.storage_driver is not None and sl.storage_driver.dedupe is False
    assert sl.storage_driver.chunk_sizes == (2048, 8192, 32768)
    assert sl.interceptor._options.store_outputs == "hash_only"
    assert sl.interceptor._options.on_ledger_error == "warn"
    assert sl.interceptor._meter.prices == {"my-model": {"input": 0.5, "output": 2.5}}


def test_environment_overrides_the_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = tmp_path / "stepledger.yaml"
    path.write_text(YAML)
    monkeypatch.setenv("STEPLEDGER_DSN", "postgresql://env@db/ledger")
    sl = StepledgerPlugin.from_config(langgraph=LangGraphPlugin(graphs={}), path=path)
    assert sl.dsn == "postgresql://env@db/ledger"


def test_explicit_constructor_keeps_the_documented_defaults(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("STEPLEDGER_CONFIG", "/nonexistent/stepledger.yaml")
    sl = StepledgerPlugin("postgresql://x@y/z", langgraph=LangGraphPlugin(graphs={}))
    assert sl.options["on_ledger_error"] == "fail" and sl.options["store_outputs"] == "full"
    assert sl.options["payload_size_threshold"] == 64 * 1024 and sl.options["dedupe"] is True
    assert sl.storage_driver is not None and sl.storage_driver.chunk_sizes == (4096, 16384, 65536)


def test_refuses_to_replace_an_existing_external_storage() -> None:
    import dataclasses

    from temporalio.converter import DataConverter, ExternalStorage

    from stepledger.storage import DedupStorageDriver, FilesystemChunkBackend

    theirs = ExternalStorage(
        drivers=[DedupStorageDriver(FilesystemChunkBackend("/tmp/theirs"), name="theirs")],
        payload_size_threshold=1024,
    )
    existing = dataclasses.replace(DataConverter.default, external_storage=theirs)
    sl = StepledgerPlugin("postgresql://x@y/z", langgraph=LangGraphPlugin(graphs={}))
    assert callable(sl.data_converter)
    with pytest.raises(ValueError, match="already has External Storage"):
        sl.data_converter(existing)
    assert sl.data_converter(None).external_storage is not None  # a bare client is fine
