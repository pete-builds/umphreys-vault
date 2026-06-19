"""Per-instance async token bucket.

Same shape as phish-vault's throttle so behavior is consistent across the
two services. Bucket holds at most ceil(rps) tokens; each acquire spends
exactly one token, refilling at ``rps`` tokens per second.
"""

from __future__ import annotations

import asyncio
import math
import time


class TokenBucket:
    """Simple async token bucket. Safe under asyncio concurrency."""

    def __init__(self, rps: float, burst: int | None = None) -> None:
        if rps <= 0:
            raise ValueError("rps must be > 0")
        self.rps = rps
        self.capacity = burst if burst is not None else max(1, math.ceil(rps))
        self._tokens: float = float(self.capacity)
        self._last = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        """Block until one token is available, then spend it."""
        while True:
            async with self._lock:
                now = time.monotonic()
                elapsed = now - self._last
                self._last = now
                self._tokens = min(self.capacity, self._tokens + elapsed * self.rps)
                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return
                missing = 1.0 - self._tokens
                wait_s = missing / self.rps
            await asyncio.sleep(wait_s)

    @property
    def tokens_available(self) -> float:
        """Approximate tokens available right now (no refill since last acquire)."""
        return self._tokens
