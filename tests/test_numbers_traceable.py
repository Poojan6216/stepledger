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


def test_summary_derivations_match_the_raw_results() -> None:
    """bench/results/summary.json is written by bench/report.py; a derivation bug there would
    self-certify, so the two headline ratios are recomputed here from the raw files."""
    summary = json.loads((RESULTS / "summary.json").read_text())["summary"]
    growth = json.loads((RESULTS / "growth.json").read_text())["rows"]

    def one(cfg: str, kb: int, nodes: int) -> dict[str, float]:
        return next(
            r
            for r in growth
            if r["config"] == cfg and r["kb_per_node"] == kb and r["nodes"] == nodes
        )

    ratio = (
        one("B3", 100, 80)["store_whole_blob_bytes"]
        / one("B4", 100, 80)["store_unique_chunk_bytes"]
    )
    assert summary["store_ratio_whole_blob_over_dedup"]["100KiB_80nodes"] == round(ratio, 1)
    cost = {m["mode"]: m for m in json.loads((RESULTS / "cost.json").read_text())["modes"]}
    cut = round((1 - cost["journal"]["wasted_usd"] / cost["no-journal"]["wasted_usd"]) * 100)
    assert summary["retry_waste_cut_percent"] == cut
