"""Scanners behind the honesty tests. Kept importable so each test can also prove it fires."""

from __future__ import annotations

import re
from collections.abc import Iterable, Iterator
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

# --- Hard Rule 10: no LLM in the control path -------------------------------------------------

# src/stepledger/llm/ is the journal wrapper (forwards and caches only). src/stepledger/testing/
# is the fault-injection harness with the fake LLM; test_testing_package_is_isolated proves no
# control-path module imports it.
CONTROL_PATH_EXEMPT = ("llm", "testing")

LLM_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("messages= kwarg", re.compile(r"\bmessages\s*=(?!=)")),
    ("prompt", re.compile(r"\bprompts?\b", re.IGNORECASE)),
    ("LLM SDK import", re.compile(r"^\s*(from|import)\s+(anthropic|openai)\b", re.MULTILINE)),
    ("chat model class", re.compile(r"\b(BaseChatModel|ChatAnthropic|ChatOpenAI)\b")),
    ("model call", re.compile(r"\.(messages|completions)\.create\(|\.a?invoke\(")),
)


def control_path_files(src: Path) -> Iterator[Path]:
    for path in sorted(src.rglob("*.py")):
        rel = path.relative_to(src)
        if rel.parts and rel.parts[0] in CONTROL_PATH_EXEMPT:
            continue
        yield path


def scan_control_path(src: Path) -> list[str]:
    hits = []
    for path in control_path_files(src):
        text = path.read_text(encoding="utf-8")
        for label, pattern in LLM_PATTERNS:
            for m in pattern.finditer(text):
                line = text.count("\n", 0, m.start()) + 1
                hits.append(f"{path.relative_to(src)}:{line}: {label}: {m.group(0).strip()!r}")
    return hits


# --- Hard Rule 12: vocabulary ----------------------------------------------------------------

BANNED = re.compile(
    r"exactly[- ]once|guarantee(?:d|s)?|zero[- ]overhead|eliminates?|100\s?%|bulletproof",
    re.IGNORECASE,
)
# A banned term passes only if its sentence names the condition it holds under, or negates it.
QUALIFIER = re.compile(
    r"\b(per|under|when|if|unless|only|within|except|provided|assuming|given|not|never|no|"
    r"does(?:n't| not)|is(?:n't| not)|without|for (?:each|every))\b|\(",
    re.IGNORECASE,
)
_FENCE = re.compile(r"^```.*?^```", re.MULTILINE | re.DOTALL)
_SENTENCE = re.compile(r"(?<=[.!?])\s+|\n\s*\n|\n\s*[-*|]")


def doc_files(root: Path) -> list[Path]:
    files = [root / "README.md", root / "RESULTS.md", *sorted((root / "docs").rglob("*.md"))]
    return [f for f in files if f.is_file()]


def scan_vocabulary(files: Iterable[Path]) -> list[str]:
    hits = []
    for path in files:
        text = path.read_text(encoding="utf-8")
        for sentence in _SENTENCE.split(text):
            for m in BANNED.finditer(sentence):
                rest = sentence[: m.start()] + sentence[m.end() :]
                if not QUALIFIER.search(rest):
                    hits.append(f"{path.name}: unqualified {m.group(0)!r} in: {sentence.strip()!r}")
    return hits
