"""Hard Rule 10: no prompt, messages= or model call under src/stepledger/ outside llm/."""

from __future__ import annotations

import re
import shutil
from pathlib import Path

import pytest

from tests._honesty import ROOT, control_path_files, scan_control_path

SRC = ROOT / "src" / "stepledger"


def test_control_path_has_no_llm() -> None:
    assert scan_control_path(SRC) == []


def test_testing_package_is_isolated() -> None:
    """The fake LLM lives in stepledger.testing; nothing on the control path may import it."""
    importer = re.compile(r"^\s*(from|import)\s+stepledger\.testing\b", re.MULTILINE)
    offenders = [
        str(p.relative_to(SRC)) for p in control_path_files(SRC) if importer.search(p.read_text())
    ]
    assert offenders == []


@pytest.mark.parametrize(
    "plant",
    [
        "x = client.create(messages=[{'role': 'user'}])\n",
        "PROMPT = 'decide'\n",
        "import anthropic\n",
    ],
)
def test_scanner_catches_a_planted_violation(tmp_path: Path, plant: str) -> None:
    copy = tmp_path / "stepledger"
    shutil.copytree(SRC, copy, ignore=shutil.ignore_patterns("__pycache__"))
    store = copy / "ledger" / "store.py"
    store.write_text(store.read_text() + plant)
    hits = scan_control_path(copy)
    assert hits and all(h.startswith("ledger/store.py") for h in hits)
