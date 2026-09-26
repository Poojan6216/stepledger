"""Hard Rule 12: no unqualified absolute claims in the docs."""

from __future__ import annotations

from pathlib import Path

import pytest

from tests._honesty import ROOT, doc_files, scan_vocabulary


def test_docs_have_no_unqualified_claims() -> None:
    assert scan_vocabulary(doc_files(ROOT)) == []


@pytest.mark.parametrize(
    "sentence",
    [
        "Stepledger is guaranteed to work.",
        "Writes are exactly-once.",
        "The plugin is zero-overhead.",
        "It eliminates duplicates.",
        "Coverage is 100%.",
        "The ledger is bulletproof.",
    ],
)
def test_planted_claim_fails(tmp_path: Path, sentence: str) -> None:
    readme = tmp_path / "README.md"
    readme.write_text((ROOT / "README.md").read_text() + "\n\n" + sentence + "\n")
    assert scan_vocabulary([readme])


@pytest.mark.parametrize(
    "sentence",
    [
        "One row per node Activity execution, exactly-once per Activity execution under the fence.",
        "It is not exactly-once for external effects.",
        "No history payload exceeds the threshold, which eliminates the cliff when External "
        "Storage is on.",
    ],
)
def test_qualified_claim_passes(tmp_path: Path, sentence: str) -> None:
    readme = tmp_path / "README.md"
    readme.write_text(sentence + "\n")
    assert scan_vocabulary([readme]) == []
