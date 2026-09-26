"""2.1: Fence ordering, header round-trip, empty lists never sent."""

from __future__ import annotations

from datetime import UTC, datetime

from hypothesis import given, settings
from hypothesis import strategies as st

from stepledger import headers
from stepledger.canonical import canonical_json, chash
from stepledger.keys import Fence, LedgerKey

N = settings(max_examples=2000, deadline=None)

times = st.datetimes(
    min_value=datetime(2020, 1, 1), max_value=datetime(2040, 1, 1), timezones=st.just(UTC)
)
attempts = st.integers(min_value=1, max_value=10_000)
ids = st.lists(st.integers(min_value=0, max_value=100_000), max_size=50)


@N
@given(times, attempts, times, attempts)
def test_fence_order_is_tuple_order(t1: datetime, a1: int, t2: datetime, a2: int) -> None:
    f1, f2 = Fence(t1, a1), Fence(t2, a2)
    assert (f1 < f2) == ((t1, a1) < (t2, a2))
    assert (f1 == f2) == ((t1, a1) == (t2, a2))
    assert (f1 <= f2) == ((t1, a1) <= (t2, a2))


@N
@given(st.integers(min_value=0, max_value=2**31 - 1), ids, ids)
def test_header_round_trip(seq: int, commits: list[int], abandons: list[int]) -> None:
    h = headers.with_headers({}, seq, commits, abandons)
    got_seq, got_c, got_a = headers.decode(h)
    assert got_seq == seq
    assert got_c == sorted(set(commits))
    assert got_a == sorted(set(abandons))


@N
@given(st.integers(min_value=0, max_value=1000), ids, ids)
def test_empty_lists_are_never_sent(seq: int, commits: list[int], abandons: list[int]) -> None:
    h = headers.with_headers({}, seq, commits, abandons)
    assert (headers.COMMITS in h) == bool(commits)
    assert (headers.ABANDONS in h) == bool(abandons)
    for p in h.values():
        assert p.data not in (b"[]", b"")


def test_untracked_headers_decode_to_nothing() -> None:
    assert headers.decode({}) == (None, [], [])


def test_foreign_headers_survive() -> None:
    other = headers.encode_int(5)
    h = headers.with_headers({"x-trace": other}, 1, [], [])
    assert h["x-trace"] is other


json_values = st.recursive(
    st.none() | st.booleans() | st.integers() | st.text(),
    lambda inner: st.lists(inner, max_size=5) | st.dictionaries(st.text(), inner, max_size=5),
    max_leaves=20,
)


@N
@given(json_values)
def test_chash_is_key_order_independent(value: object) -> None:
    import json

    shuffled = json.loads(json.dumps(value))
    assert chash(value) == chash(shuffled)
    assert canonical_json(value) == canonical_json(shuffled)


@N
@given(st.text(), st.text(), st.text(), st.integers(0, 10**6), st.text(), st.integers(0, 100))
def test_effect_key_is_stable_and_separates_fields(
    ns: str, wf: str, run: str, seq: int, name: str, idx: int
) -> None:
    k = LedgerKey(ns, wf, run, seq)
    assert k.effect_key(name, idx) == LedgerKey(ns, wf, run, seq).effect_key(name, idx)
    assert k.effect_key(name, idx) != k.effect_key(name, idx + 1)
