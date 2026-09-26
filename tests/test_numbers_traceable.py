"""Hard Rule 12: every number in README.md comes from a committed results JSON."""

from __future__ import annotations

import json
from pathlib import Path

from bench.check_numbers import readme_numbers, untraceable_numbers
from tests._honesty import ROOT

RESULTS = ROOT / "bench" / "results"
ALLOW = ROOT / "bench" / "number_allowlist.txt"


def test_readme_numbers_are_traceable() -> None:
    assert untraceable_numbers(ROOT / "README.md", RESULTS, ALLOW) == []


def test_planted_number_fails(tmp_path: Path) -> None:
    readme = tmp_path / "README.md"
    readme.write_text("History reaches 27.3 MB at 30 nodes.\n")
    (tmp_path / "results").mkdir()
    assert untraceable_numbers(readme, tmp_path / "results", ALLOW) == ["27.3", "30"]


def test_measured_number_passes(tmp_path: Path) -> None:
    readme = tmp_path / "README.md"
    readme.write_text("History reaches 27.3 MB at 30 nodes (issue #1894).\n")
    results = tmp_path / "results"
    results.mkdir()
    (results / "growth.json").write_text(json.dumps({"runs": [{"nodes": 30, "mb": 27.31}]}))
    assert untraceable_numbers(readme, results, ALLOW) == []


def test_code_and_links_are_not_claims() -> None:
    text = "Run `sleep 30` or see [x](https://a/b/42).\n```\nfoo = 99\n```\nv<sup>2</sup>"
    assert readme_numbers(text) == []
