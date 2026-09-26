"""Stepledger's Activity headers.

    stepledger-seq       this node execution's per-run sequence number
    stepledger-commits   seqs the workflow saw succeed that no successful carrier has confirmed
    stepledger-abandons  seqs the workflow saw fail or cancel, likewise unconfirmed

Values are small JSON payloads. Empty lists are never sent.
"""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence

from temporalio.api.common.v1 import Payload

SEQ = "stepledger-seq"
COMMITS = "stepledger-commits"
ABANDONS = "stepledger-abandons"
ALL = (SEQ, COMMITS, ABANDONS)

_JSON = {"encoding": b"json/plain"}


def encode_int(v: int) -> Payload:
    if isinstance(v, bool) or not isinstance(v, int) or v < 0:
        raise ValueError(f"seq must be a non-negative int, got {v!r}")
    return Payload(metadata=_JSON, data=str(v).encode())


def encode_ids(v: Sequence[int]) -> Payload:
    ids = sorted(set(v))
    if not ids:
        raise ValueError("empty id lists are never sent")
    return Payload(metadata=_JSON, data=json.dumps(ids, separators=(",", ":")).encode())


def with_headers(
    headers: Mapping[str, Payload], seq: int, commits: Sequence[int], abandons: Sequence[int]
) -> dict[str, Payload]:
    out = {k: v for k, v in headers.items() if k not in ALL}
    out[SEQ] = encode_int(seq)
    if commits:
        out[COMMITS] = encode_ids(commits)
    if abandons:
        out[ABANDONS] = encode_ids(abandons)
    return out


def _load(p: Payload) -> object:
    enc = p.metadata.get("encoding", b"")
    if enc != b"json/plain":
        raise ValueError(f"stepledger header has encoding {enc!r}; decode it with the codec first")
    return json.loads(p.data)


def decode(headers: Mapping[str, Payload]) -> tuple[int | None, list[int], list[int]]:
    seq_p = headers.get(SEQ)
    seq = None
    if seq_p is not None:
        v = _load(seq_p)
        if not isinstance(v, int) or isinstance(v, bool):
            raise ValueError(f"bad {SEQ} header: {v!r}")
        seq = v
    lists: list[list[int]] = []
    for name in (COMMITS, ABANDONS):
        p = headers.get(name)
        ids = _load(p) if p is not None else []
        if not isinstance(ids, list) or not all(isinstance(i, int) for i in ids):
            raise ValueError(f"bad {name} header: {ids!r}")
        lists.append(ids)
    return seq, lists[0], lists[1]
