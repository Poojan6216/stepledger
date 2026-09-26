"""4.1: chunking round-trips, and boundaries before an append point do not move."""

from __future__ import annotations

import base64
import json
import random

from hypothesis import given, settings
from hypothesis import strategies as st

from stepledger.storage.chunking import MAX_SIZE, MIN_SIZE, chunk

N = settings(max_examples=300, deadline=None)


@N
@given(st.binary(max_size=400_000))
def test_round_trip_random(data: bytes) -> None:
    parts = chunk(data)
    assert b"".join(parts) == data
    assert all(len(p) <= MAX_SIZE for p in parts)
    assert all(len(p) >= MIN_SIZE for p in parts[:-1])


def _state(n: int, seed: int) -> bytes:
    rnd = random.Random(seed)
    msgs = [
        {"role": "tool", "name": f"t{i}", "content": base64.b64encode(rnd.randbytes(6000)).decode()}
        for i in range(n)
    ]
    return json.dumps({"messages": msgs, "target": "acct"}).encode()


@N
@given(st.integers(1, 40), st.integers(0, 10**6))
def test_round_trip_structured(n: int, seed: int) -> None:
    data = _state(n, seed)
    assert b"".join(chunk(data)) == data


@N
@given(st.binary(min_size=1, max_size=300_000), st.binary(min_size=1, max_size=100_000))
def test_boundaries_before_an_append_are_stable(prefix: bytes, suffix: bytes) -> None:
    """Appending bytes can only change the chunk the append lands in; every earlier chunk is
    identical, which is what makes successive node inputs dedupe."""
    before = chunk(prefix)
    after = chunk(prefix + suffix)
    stable = before[:-1]
    assert after[: len(stable)] == stable


def test_successive_states_share_most_chunks() -> None:
    a, b = set(chunk(_state(30, 7))), set(chunk(_state(31, 7)))
    shared = sum(len(c) for c in a & b)
    assert shared / sum(len(c) for c in b) > 0.9
