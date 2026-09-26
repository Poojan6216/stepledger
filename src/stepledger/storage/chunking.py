"""Content-defined chunking (FastCDC, Xia et al., USENIX ATC 2016) with the locked parameters.

Chunk boundaries follow the content, not fixed offsets, so successive node inputs, which share
almost all their bytes, split into mostly the same chunks. Chunk id = SHA-256 of the chunk.
"""

from __future__ import annotations

import hashlib

import fastcdc

MIN_SIZE = 4 * 1024
AVG_SIZE = 16 * 1024
MAX_SIZE = 64 * 1024


def chunk(
    data: bytes, *, min_size: int = MIN_SIZE, avg_size: int = AVG_SIZE, max_size: int = MAX_SIZE
) -> list[bytes]:
    if not data:
        return [b""]
    return [
        bytes(c.data)
        for c in fastcdc.fastcdc(
            data, min_size=min_size, avg_size=avg_size, max_size=max_size, fat=True
        )
    ]


def chunk_id(data: bytes) -> bytes:
    return hashlib.sha256(data).digest()
