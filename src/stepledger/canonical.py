"""Canonical JSON and content hashes, used only for hashing and equality.

Values are first turned into plain JSON by the worker's own payload converter, so a hash here
equals the hash of what Temporal recorded for the same value.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any

from temporalio.api.common.v1 import Payload
from temporalio.converter import PayloadConverter

JSON_PLAIN = b"json/plain"


def canonical_json(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def chash(obj: Any) -> str:
    """sha256 hex of the canonical JSON of a plain-JSON value."""
    return hashlib.sha256(canonical_json(obj).encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class Serialized:
    """A value as the payload converter serialized it."""

    encoding: str
    data: bytes
    plain: Any  # the decoded JSON value when encoding is json/plain, else None
    is_json: bool

    @property
    def hash(self) -> str:
        if self.is_json:
            return chash(self.plain)
        return hashlib.sha256(self.data).hexdigest()


def serialize(value: Any, converter: PayloadConverter) -> Serialized:
    return from_payload(converter.to_payload(value))


def from_payload(payload: Payload) -> Serialized:
    encoding = payload.metadata.get("encoding", b"").decode()
    if encoding == JSON_PLAIN.decode():
        return Serialized(encoding, payload.data, json.loads(payload.data), True)
    return Serialized(encoding, payload.data, None, False)
