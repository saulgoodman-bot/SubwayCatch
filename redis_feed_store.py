#!/usr/bin/env python3
"""
Redis-backed store for raw MTA GTFS-RT protobuf payloads.

Write path: feed_aggregator.py calls store_feeds_batch() every 60 s.
Read path:  bot.py calls get_feeds() per user request.

Keys
----
gtfs:feed:<md5_12>   -> raw protobuf bytes (TTL = FEED_TTL_SECONDS)
gtfs:last_updated    -> Unix float timestamp of last successful poll
gtfs:stats           -> hash of per-cycle diagnostics
"""

from __future__ import annotations

import hashlib
import logging
import time
from typing import Dict, List, Optional

import redis.asyncio as aioredis

logger = logging.getLogger("subway_bot.redis_store")

_FEED_KEY_PREFIX = "gtfs:feed:"
_LAST_UPDATED_KEY = "gtfs:last_updated"
_STATS_KEY = "gtfs:stats"

# 2x the poll interval so a single missed cycle never evicts live data.
FEED_TTL_SECONDS = 300
# Data older than this is considered unhealthy (3 missed polls + slack).
STALENESS_THRESHOLD_SECONDS = 210


def _feed_key(feed_url: str) -> str:
    """Stable, compact Redis key derived from a feed URL."""
    # 12 hex chars = 2^48 namespace; effectively collision-free for this small feed set.
    digest = hashlib.md5(feed_url.encode(), usedforsecurity=False).hexdigest()[:12]
    return f"{_FEED_KEY_PREFIX}{digest}"


class RedisFeedStore:
    """
    Async Redis client that wraps GTFS-RT binary storage and retrieval.

    All multi-key operations use pipelines to minimise round-trip latency.
    The store is connection-pool-backed and safe for concurrent coroutines.
    """

    def __init__(self, redis_url: str = "redis://localhost:6379") -> None:
        self._redis_url = redis_url
        self._client: Optional[aioredis.Redis] = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def connect(self) -> None:
        """Open the connection pool and verify reachability."""
        self._client = aioredis.from_url(
            self._redis_url,
            decode_responses=False,   # raw bytes; protobuf is not text
            socket_timeout=5,
            socket_connect_timeout=5,
            retry_on_timeout=True,
            max_connections=10,
        )
        await self._client.ping()
        logger.info("Redis connected")

    async def close(self) -> None:
        """Drain the connection pool gracefully."""
        if self._client:
            await self._client.aclose()
            logger.info("Redis connection pool closed")

    # ------------------------------------------------------------------
    # Write path (called by feed_aggregator only)
    # ------------------------------------------------------------------

    async def store_feeds_batch(
        self,
        feeds: Dict[str, bytes],
        poll_ts: Optional[float] = None,
    ) -> None:
        """
        Pipeline-write all fetched feed payloads + metadata in one round-trip.

        Parameters
        ----------
        feeds:    {feed_url: protobuf_bytes}  — successful fetches only
        poll_ts:  Unix timestamp for this poll cycle (defaults to now)
        """
        if not feeds:
            logger.warning("store_feeds_batch called with empty payload dict; skipping")
            return

        ts = poll_ts if poll_ts is not None else time.time()
        # Intentionally non-transactional: batch round-trips without MULTI/EXEC overhead.
        pipe = self._client.pipeline(transaction=False)

        for feed_url, data in feeds.items():
            pipe.setex(_feed_key(feed_url), FEED_TTL_SECONDS, data)

        pipe.set(_LAST_UPDATED_KEY, str(ts))
        pipe.hset(
            _STATS_KEY,
            mapping={
                "feeds_stored": str(len(feeds)),
                "last_poll_ts": str(ts),
                "total_bytes": str(sum(len(b) for b in feeds.values())),
            },
        )
        pipe.expire(_STATS_KEY, 3600)

        await pipe.execute()
        total_kb = sum(len(b) for b in feeds.values()) / 1024
        logger.info(
            "Stored %d feeds (%.1f KB) in Redis ts=%.3f",
            len(feeds),
            total_kb,
            ts,
        )

    # ------------------------------------------------------------------
    # Read path (called by bot handlers)
    # ------------------------------------------------------------------

    async def get_feeds(self, feed_urls: List[str]) -> Dict[str, bytes]:
        """
        Retrieve one or more feed payloads from Redis in a single pipeline.

        Returns a dict keyed by feed URL; absent or expired keys are omitted.
        A partial result (some feeds missing) is normal during aggregator warm-up.
        """
        if not feed_urls:
            return {}

        # Intentionally non-transactional: batch round-trips without MULTI/EXEC overhead.
        pipe = self._client.pipeline(transaction=False)
        for url in feed_urls:
            pipe.get(_feed_key(url))

        results = await pipe.execute()

        out: Dict[str, bytes] = {}
        for url, data in zip(feed_urls, results):
            if data is not None:
                out[url] = data
            else:
                logger.debug("Cache miss for feed_url=%s (TTL expired or not yet stored)", url)

        logger.debug("get_feeds requested=%d found=%d", len(feed_urls), len(out))
        return out

    async def get_last_updated(self) -> Optional[float]:
        """Return the Unix timestamp of the last successful aggregator poll, or None."""
        val = await self._client.get(_LAST_UPDATED_KEY)
        try:
            return float(val) if val else None
        except (ValueError, TypeError):
            return None

    # ------------------------------------------------------------------
    # Health / diagnostics
    # ------------------------------------------------------------------

    async def is_healthy(self) -> bool:
        """
        Return True when Redis is reachable AND feed data is fresh.

        Used by the bot's startup check and any monitoring hooks.
        """
        try:
            pipe = self._client.pipeline(transaction=False)
            pipe.ping()
            pipe.get(_LAST_UPDATED_KEY)
            _, last_val = await pipe.execute()
            if not last_val:
                return False
            return (time.time() - float(last_val)) < STALENESS_THRESHOLD_SECONDS
        except Exception:
            logger.exception("Redis health check failed")
            return False

    async def get_data_age_seconds(self) -> Optional[float]:
        """Return how many seconds old the most recent feed data is, or None."""
        last = await self.get_last_updated()
        return (time.time() - last) if last is not None else None
