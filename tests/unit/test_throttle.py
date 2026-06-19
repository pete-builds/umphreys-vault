"""Token-bucket throttle tests."""

from __future__ import annotations

import asyncio
import time

import pytest

from umphreys_vault.throttle import TokenBucket


def test_rejects_non_positive_rps() -> None:
    with pytest.raises(ValueError):
        TokenBucket(rps=0)


@pytest.mark.asyncio
async def test_first_acquires_are_immediate() -> None:
    bucket = TokenBucket(rps=5)
    start = time.monotonic()
    for _ in range(5):
        await bucket.acquire()
    # Burst capacity == ceil(rps) == 5, so 5 acquires don't block meaningfully.
    assert time.monotonic() - start < 0.2


@pytest.mark.asyncio
async def test_throttle_paces_excess_requests() -> None:
    bucket = TokenBucket(rps=20, burst=1)
    start = time.monotonic()
    await bucket.acquire()  # immediate
    await bucket.acquire()  # waits ~1/20s
    elapsed = time.monotonic() - start
    assert elapsed >= 0.03


@pytest.mark.asyncio
async def test_concurrent_acquires_are_safe() -> None:
    bucket = TokenBucket(rps=50)
    await asyncio.gather(*(bucket.acquire() for _ in range(10)))
    assert bucket.tokens_available >= 0
