"""Fail if a number in README.md appears in no committed results JSON (Hard Rule 12).

    uv run python bench/check_numbers.py            # exit 1 and list offenders on failure

Numbers in code blocks, inline code, link targets and HTML are not claims and are skipped.
Non-measurement numbers (issue ids, versions, dates) go in bench/number_allowlist.txt.
"""

from __future__ import annotations

import json
import re
import sys
from collections.abc import Iterator
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

_FENCE = re.compile(r"^```.*?^```", re.MULTILINE | re.DOTALL)

_INLINE_CODE = re.compile(r"`[^`\n]*`")
_LINK_TARGET = re.compile(r"\]\([^)]*\)")
_HTML = re.compile(r"<[^>]+>")
_NUMBER = re.compile(r"(?<![\w.#/-])(\d{1,3}(?:,\d{3})+|\d+)(\.\d+)?(?![\w.])")


def readme_numbers(text: str) -> list[str]:
    """Numbers stated in prose and tables; code, links and markup are not claims."""
    text = _FENCE.sub("", text)
    text = _INLINE_CODE.sub("", text)
    text = _LINK_TARGET.sub("]", text)
    text = _HTML.sub("", text)
    return [m.group(1) + (m.group(2) or "") for m in _NUMBER.finditer(text)]


def _json_numbers(value: object) -> Iterator[float]:
    if isinstance(value, bool):
        return
    if isinstance(value, int | float):
        yield float(value)
    elif isinstance(value, dict):
        for k, v in value.items():
            yield from _json_numbers(v)
            if re.fullmatch(r"-?\d+(\.\d+)?", k):
                yield float(k)
    elif isinstance(value, list):
        for v in value:
            yield from _json_numbers(v)


def results_numbers(results_dir: Path) -> set[float]:
    found: set[float] = set()
    for path in sorted(results_dir.glob("*.json")):
        found.update(_json_numbers(json.loads(path.read_text(encoding="utf-8"))))
    return found


def allowlisted(allowlist: Path) -> set[str]:
    if not allowlist.is_file():
        return set()
    lines = (ln.split("#", 1)[0].strip() for ln in allowlist.read_text().splitlines())
    return {ln for ln in lines if ln}


def untraceable_numbers(readme: Path, results_dir: Path, allowlist: Path) -> list[str]:
    if not readme.is_file():
        return []
    known = results_numbers(results_dir)
    allowed = allowlisted(allowlist)
    bad = []
    for token in readme_numbers(readme.read_text(encoding="utf-8")):
        if token in allowed:
            continue
        value = float(token.replace(",", ""))
        decimals = len(token.split(".", 1)[1]) if "." in token else 0
        if not any(round(k, decimals) == value for k in known):
            bad.append(token)
    return bad


def main() -> int:
    bad = untraceable_numbers(
        ROOT / "README.md", ROOT / "bench" / "results", ROOT / "bench" / "number_allowlist.txt"
    )
    if bad:
        print("README numbers with no source in bench/results/*.json:", ", ".join(bad))
        return 1
    print("check_numbers: every README number traces to bench/results")
    return 0


if __name__ == "__main__":
    sys.exit(main())
