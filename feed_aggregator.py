# feed_aggregator.py
import asyncio
import aiohttp
import os
import logging
import time
from dotenv import load_dotenv
from redis_feed_store import RedisFeedStore

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
logger = logging.getLogger("feed_aggregator")

# All unique MTA GTFS-RT feed URLs required to cover the 22+ lines
ALL_FEED_URLS = [
    "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs",
    "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-ace",
    "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-bdfm",
    "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-g",
    "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-l",
    "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-jz",
    "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-nqrw",
    "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-si",
]

async def fetch_feed(session: aiohttp.ClientSession, url: str, api_key: str):
    try:
        async with session.get(url, headers={"x-api-key": api_key}) as response:
            response.raise_for_status()
            return url, await response.read()
    except Exception as e:
        logger.error(f"Failed to fetch {url}: {e}")
        return url, None

async def poll_all_feeds(
    session: aiohttp.ClientSession,
    store: RedisFeedStore,
    api_key: str,
) -> None:
    """Fetch all feeds concurrently and persist successful payloads to Redis."""
    start = time.time()
    tasks = [fetch_feed(session, url, api_key) for url in ALL_FEED_URLS]
    results = await asyncio.gather(*tasks)
    valid_feeds = {url: data for url, data in results if data is not None}
    if valid_feeds:
        await store.store_feeds_batch(valid_feeds)
    elapsed = time.time() - start
    logger.info("Poll cycle complete in %.2fs", elapsed)


async def main():
    load_dotenv()
    api_key = os.getenv("MTA_API_KEY")
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379")
    
    if not api_key:
        logger.error("MTA_API_KEY is required.")
        return

    store = RedisFeedStore(redis_url)
    await store.connect()
    timeout = aiohttp.ClientTimeout(total=15)
    
    logger.info("Starting NYC Subway Data Aggregator...")
    
    async with aiohttp.ClientSession(timeout=timeout) as session:
        while True:
            start = time.time()
            await poll_all_feeds(session, store, api_key)
            elapsed = time.time() - start
            sleep_time = max(0, 60.0 - elapsed)
            
            logger.info(f"Poll cycle complete in {elapsed:.2f}s. Sleeping for {sleep_time:.2f}s")
            await asyncio.sleep(sleep_time)

if __name__ == "__main__":
    asyncio.run(main())
