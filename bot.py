#!/usr/bin/env python3
"""Production-ready Telegram bot for NYC MTA real-time subway arrivals."""

import logging
import os
import threading
import time
import asyncio
from dataclasses import dataclass
from collections import OrderedDict
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Dict, List, Optional, Set, Tuple

import aiohttp
import pytz
from dotenv import load_dotenv
from google.transit import gtfs_realtime_pb2
from telegram import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    ReplyKeyboardMarkup,
    Update,
)
from telegram.error import Conflict, NetworkError, TimedOut
from html import escape
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)
from thefuzz import fuzz, process
from redis_feed_store import RedisFeedStore

from station_metadata import (
    BOROUGH_STATION_ORDER,
    IMPORTANT_STATIONS,
    STATION_ALIAS_TO_STOP_PREFIXES,
    STATION_METADATA,
    get_complex_for_alias,
    get_direction_labels,
    get_lines_for_alias,
    get_station_info,
)

# Configure logger format for cloud/runtime observability.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("subway_bot")

_POLLING_CONFLICT = threading.Event()
NYC_TZ = pytz.timezone("America/New_York")
_MAX_TRACKED_CHATS = 2_000
# Allow arrivals up to 1 minute past to account for MTA feed latency (~30s updates).
# These are clamped to 0m in display_minutes to avoid showing negative values.
_FEED_LATENCY_TOLERANCE_MINUTES = -1
_DEFAULT_RETRY_DELAY_SECONDS = 10
_MAX_RETRY_DELAY_SECONDS = 120
_MIN_REQUEST_INTERVAL_SECONDS = 2.0
_HANDLER_TIMEOUT_SECONDS = 20

# Official MTA GTFS-RT feeds by line.
TRAIN_FEEDS: Dict[str, str] = {
    "A": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-ace",
    "C": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-ace",
    "E": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-ace",
    "B": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-bdfm",
    "D": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-bdfm",
    "F": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-bdfm",
    "M": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-bdfm",
    "G": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-g",
    "L": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-l",
    "J": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-jz",
    "Z": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-jz",
    "N": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-nqrw",
    "Q": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-nqrw",
    "R": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-nqrw",
    "W": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-nqrw",
    "1": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs",
    "2": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs",
    "3": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs",
    "4": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs",
    "5": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs",
    "6": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs",
    "7": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs",
    "S": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs",
    "GS": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs",
    "FS": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs",
    "H": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs",
    "SI": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-si",
    "5X": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs",
    "6X": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs",
    "7X": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs",
}
_ROUTE_ID_ALIASES: Dict[str, str] = {"GS": "S", "FS": "S", "H": "S"}

def _build_code_to_name() -> Dict[str, str]:
    """Build station alias -> human name map from the full alias set."""
    flat_important: Dict[str, str] = {
        code: name for group in IMPORTANT_STATIONS.values() for code, name in group.items()
    }
    result: Dict[str, str] = {}
    for alias, prefixes in STATION_ALIAS_TO_STOP_PREFIXES.items():
        if alias in flat_important:
            result[alias] = flat_important[alias]
            continue
        if not prefixes:
            continue
        info = STATION_METADATA.get(prefixes[0])
        if info and info["name"]:
            result[alias] = info["name"]
    return result


STATION_CODE_TO_NAME: Dict[str, str] = _build_code_to_name()


class _LRUDict(OrderedDict):
    """OrderedDict that evicts the oldest entry once the cap is reached."""

    def __init__(self, maxsize: int) -> None:
        super().__init__()
        self._maxsize = maxsize

    def __setitem__(self, key: int, value: Tuple[str, str]) -> None:
        super().__setitem__(key, value)
        self.move_to_end(key)
        if len(self) > self._maxsize:
            self.popitem(last=False)


LAST_REQUEST_BY_CHAT: _LRUDict = _LRUDict(_MAX_TRACKED_CHATS)

VALID_TRAINS_TEXT = "A,B,C,D,E,F,FS,G,GS,H,J,L,N,Q,R,S,SI,W,Z,1,2,3,4,5,5X,6,6X,7,7X"

@dataclass
class FeedCacheEntry:
    fetched_at: float
    data: Dict[str, bytes]


class FeedBatchCache:
    """Async cache for raw feed payloads with request coalescing."""

    def __init__(self, ttl_seconds: int) -> None:
        self._ttl_seconds = ttl_seconds
        self._entries: Dict[Tuple[str, ...], FeedCacheEntry] = {}
        self._in_flight: Dict[Tuple[str, ...], asyncio.Task[Dict[str, bytes]]] = {}
        self._lock = asyncio.Lock()

    async def get_or_fetch(self, session: aiohttp.ClientSession, feed_urls: List[str], api_key: str) -> Dict[str, bytes]:
        key = tuple(sorted(feed_urls))
        now = time.monotonic()
        async with self._lock:
            cached = self._entries.get(key)
            if cached and now - cached.fetched_at <= self._ttl_seconds:
                logger.info("Feed cache hit key=%s feeds=%s", key, len(key))
                return dict(cached.data)

            task = self._in_flight.get(key)
            if task is None:
                logger.info("Feed cache miss key=%s", key)
                task = asyncio.create_task(self._fetch_batch(session, list(key), api_key))
                self._in_flight[key] = task
            else:
                logger.info("Feed cache join in-flight key=%s", key)

        try:
            data = await task
        except Exception:  # noqa: BLE001 - avoid surfacing shared task exceptions to handlers
            data = {}
            logger.exception("Feed fetch task failed for key=%s", key)
        finally:
            current_time = time.monotonic()
            async with self._lock:
                self._in_flight.pop(key, None)
                if data:
                    self._entries[key] = FeedCacheEntry(fetched_at=current_time, data=dict(data))
                    self._entries = {
                        existing_key: entry
                        for existing_key, entry in self._entries.items()
                        if current_time - entry.fetched_at <= self._ttl_seconds
                    }
        return dict(data)

    async def _fetch_batch(self, session: aiohttp.ClientSession, feed_urls: List[str], api_key: str) -> Dict[str, bytes]:
        raw_results = await asyncio.gather(
            *[_fetch_feed(session, feed_url, api_key) for feed_url in feed_urls],
            return_exceptions=True,
        )
        payloads: Dict[str, bytes] = {}
        for item in raw_results:
            if isinstance(item, BaseException):
                logger.error("Unexpected exception during feed gather: %s", item, exc_info=item)
                continue
            feed_url, content, per_feed_fetch_ms = item
            logger.info("mta_metrics feed_url=%s feed_fetch_ms=%.2f", feed_url, per_feed_fetch_ms)
            if content is not None:
                payloads[feed_url] = content
        return payloads

async def _on_startup(app: Application) -> None:
    """Initialize per-application async resources."""
    timeout = aiohttp.ClientTimeout(total=15)
    app.bot_data["session"] = aiohttp.ClientSession(timeout=timeout)
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379")
    store = RedisFeedStore(redis_url)
    await store.connect()
    app.bot_data["redis_store"] = store

    from feed_aggregator import ALL_FEED_URLS, poll_all_feeds

    timeout = aiohttp.ClientTimeout(total=15)
    connector = aiohttp.TCPConnector(limit=len(ALL_FEED_URLS), ttl_dns_cache=300)
    warm_session = aiohttp.ClientSession(timeout=timeout, connector=connector)
    try:
        api_key = os.getenv("MTA_API_KEY")
        if api_key:
            await poll_all_feeds(warm_session, store, api_key)
            logger.info("Initial feed poll complete — Redis is warm")
        else:
            logger.warning("MTA_API_KEY is missing; startup warm poll skipped")
    finally:
        await warm_session.close()

    asyncio.create_task(_run_background_aggregator(app))
    logger.info("startup complete: aiohttp + redis + background aggregator")


async def _run_background_aggregator(app: Application) -> None:
    """Poll all MTA feeds every 60s and write results to Redis."""
    from feed_aggregator import ALL_FEED_URLS, poll_all_feeds

    api_key = os.getenv("MTA_API_KEY")
    redis_store: RedisFeedStore = app.bot_data["redis_store"]

    timeout = aiohttp.ClientTimeout(total=15)
    connector = aiohttp.TCPConnector(limit=len(ALL_FEED_URLS), ttl_dns_cache=300)
    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        while True:
            try:
                if api_key:
                    await poll_all_feeds(session, redis_store, api_key)
            except Exception:
                logger.exception("Background aggregator poll failed")
            await asyncio.sleep(60)


async def _on_shutdown(app: Application) -> None:
    """Close per-application async resources."""
    session: Optional[aiohttp.ClientSession] = app.bot_data.get("session")
    redis_store: Optional[RedisFeedStore] = app.bot_data.get("redis_store")
    if session and not session.closed:
        await session.close()
    if redis_store:
        await redis_store.close()
    logger.info("aiohttp session closed")


def direction_from_stop_id(stop_id: str, gtfs_base_id: str = "") -> str:
    """Infer user-facing direction label from stop suffix + station metadata."""
    if stop_id.endswith("N"):
        suffix = "north"
    elif stop_id.endswith("S"):
        suffix = "south"
    else:
        return ""
    if gtfs_base_id:
        labels = get_direction_labels(gtfs_base_id)
        if labels and labels.get(suffix):
            return str(labels[suffix]).title()
    return "Uptown" if suffix == "north" else "Downtown"


class HealthHandler(BaseHTTPRequestHandler):
    """Simple health endpoint for platforms that require an open HTTP port."""

    def do_GET(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.end_headers()
        self.wfile.write(b"ok")

    def log_message(self, format: str, *args) -> None:
        # Silence default HTTP request logs; main logger already provides runtime visibility.
        del format, args


def start_health_server() -> None:
    """Start a tiny HTTP server when PORT is provided (e.g., Render web services)."""
    port_value = os.getenv("PORT")
    if not port_value:
        logger.info("PORT not set; skipping health server startup.")
        return

    try:
        port = int(port_value)
    except ValueError:
        logger.warning("Invalid PORT value %s; skipping health server startup.", port_value)
        return

    def run_server() -> None:
        try:
            server = ThreadingHTTPServer(("0.0.0.0", port), HealthHandler)
            logger.info("Health server listening on 0.0.0.0:%s", port)
            server.serve_forever()
        except OSError as exc:
            logger.error(
                "Health server failed to bind to port %s: %s. Render health checks may fail.",
                port,
                exc,
            )

    thread = threading.Thread(target=run_server, name="health-server", daemon=True)
    thread.start()


async def _fetch_feed(
    session: aiohttp.ClientSession,
    feed_url: str,
    api_key: str,
) -> Tuple[str, Optional[bytes], float]:
    """Fetch one GTFS feed and return URL, content, and fetch latency in milliseconds."""
    fetch_started = time.perf_counter()
    try:
        async with session.get(feed_url, headers={"x-api-key": api_key}) as response:
            if response.status == 429:
                retry_after = response.headers.get("Retry-After", "unknown")
                logger.error(
                    "MTA rate limit hit feed=%s status=429 retry_after=%s",
                    feed_url,
                    retry_after,
                )
                return feed_url, None, (time.perf_counter() - fetch_started) * 1000
            response.raise_for_status()
            content = await response.read()
            return feed_url, content, (time.perf_counter() - fetch_started) * 1000
    except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
        logger.warning("MTA feed request failed feed=%s: %s", feed_url, exc)
        return feed_url, None, (time.perf_counter() - fetch_started) * 1000
    except Exception as exc:  # noqa: BLE001 - avoid bubbling to gather cancellation
        logger.exception("Unexpected error fetching feed=%s", feed_url, exc_info=exc)
        return feed_url, None, (time.perf_counter() - fetch_started) * 1000


async def fetch_mta_updates(
    _session: aiohttp.ClientSession,
    redis_store: RedisFeedStore,
    station_code: str,
    train_filter: str = "",
) -> Dict[str, object]:
    """Fetch upcoming arrivals for a station, optionally filtered to one train line."""
    station_code = station_code.upper().strip()
    train_filter = train_filter.upper().strip()

    if station_code not in STATION_ALIAS_TO_STOP_PREFIXES:
        return {
            "ok": False,
            "error": "Invalid station code. Use /stationid to see supported codes.",
        }

    if train_filter and train_filter not in TRAIN_FEEDS:
        return {
            "ok": False,
            "error": f"Invalid train line '{train_filter}'. Valid trains: {VALID_TRAINS_TEXT}",
        }

    stop_id_prefixes = STATION_ALIAS_TO_STOP_PREFIXES[station_code]
    gtfs_base_ids: Set[str] = set(stop_id_prefixes)
    if train_filter:
        feeds_for_request = {TRAIN_FEEDS[train_filter]}
    else:
        serving_routes: Set[str] = set()
        for prefix in stop_id_prefixes:
            station_info = get_station_info(prefix)
            if station_info:
                serving_routes.update(station_info["routes"])
        feeds_for_request = {TRAIN_FEEDS[route] for route in serving_routes if route in TRAIN_FEEDS}
        if not feeds_for_request:
            logger.error(
                "No feeds found for station_code=%s stop_prefixes=%s; check route entries in stations.json",
                station_code,
                stop_id_prefixes,
            )
            return {
                "ok": False,
                "error": "Station data is incomplete. Please report this to the bot administrator.",
            }
    feed_urls: List[str] = sorted(feeds_for_request)

    logger.info("Fetching MTA feeds for station_code=%s train_filter=%s", station_code, train_filter or "ALL")

    # Data flow: line -> feed URL -> protobuf bytes -> parsed trip/alert entities.
    lines_in_scope = sorted({train_filter} if train_filter else {line for line, feed in TRAIN_FEEDS.items() if feed in feeds_for_request})
    logger.info(
        "MTA data path station=%s lines=%s feeds=%s",
        station_code,
        ",".join(lines_in_scope) if lines_in_scope else "ALL",
        len(feed_urls),
    )

    now = datetime.now(tz=NYC_TZ)
    arrivals: List[Tuple[int, str, str, str]] = []
    alert_set: Set[str] = set()
    north_label = "Uptown"
    south_label = "Downtown"
    fetch_batch_started = time.perf_counter()
    raw_payloads = await redis_store.get_feeds(feed_urls)
    fetch_batch_ms = (time.perf_counter() - fetch_batch_started) * 1000

    parse_started = time.perf_counter()
    for feed_url in feed_urls:
        content = raw_payloads.get(feed_url)
        if not content:
            continue
        feed = gtfs_realtime_pb2.FeedMessage()
        try:
            feed.ParseFromString(content)
        except Exception as exc:
            logger.exception("Failed to parse protobuf feed for feed=%s: %s", feed_url, exc)
            continue

        for entity in feed.entity:
            if not entity.HasField("trip_update"):
                continue

            trip_update = entity.trip_update
            train = trip_update.trip.route_id.upper().strip() if trip_update.trip.route_id else ""
            if train not in TRAIN_FEEDS:
                continue
            canonical_train = _ROUTE_ID_ALIASES.get(train, train)
            if train_filter and canonical_train != train_filter and train != train_filter:
                continue

            for stu in trip_update.stop_time_update:
                stop_id = stu.stop_id.upper() if stu.stop_id else ""
                if len(stop_id) < 2:
                    continue
                stop_base = stop_id[:-1] if stop_id and stop_id[-1] in {"N", "S"} else stop_id
                if stop_base not in gtfs_base_ids:
                    continue

                direction_key = "north" if stop_id.endswith("N") else "south" if stop_id.endswith("S") else ""
                if not direction_key:
                    continue
                direction = direction_from_stop_id(stop_id, stop_base)
                if not direction:
                    continue
                if direction_key == "north":
                    north_label = direction
                else:
                    south_label = direction

                if not stu.HasField("arrival") or stu.arrival.time <= 0:
                    continue

                arrival_dt = datetime.fromtimestamp(stu.arrival.time, tz=NYC_TZ)
                minutes = int((arrival_dt - now).total_seconds() // 60)
                if minutes < _FEED_LATENCY_TOLERANCE_MINUTES:
                    continue
                display_minutes = max(0, minutes)

                arrivals.append((display_minutes, direction_key, canonical_train, arrival_dt.strftime("%I:%M %p")))

        for entity in feed.entity:
            if not entity.HasField("alert"):
                continue
            if entity.alert.header_text.translation:
                text = entity.alert.header_text.translation[0].text.strip()
                if text:
                    alert_set.add(text)
    parse_ms = (time.perf_counter() - parse_started) * 1000
    logger.info(
        "mta_metrics station_code=%s train_filter=%s feed_fetch_ms=%.2f parse_ms=%.2f",
        station_code,
        train_filter or "ALL",
        fetch_batch_ms,
        parse_ms,
    )

    if not arrivals:
        return {
            "ok": False,
            "error": "No upcoming arrivals found for this request right now.",
        }

    station_name = STATION_CODE_TO_NAME.get(station_code, station_code)

    arrivals.sort(key=lambda row: (row[2], row[0]))

    uptown_by_train: Dict[str, List[Tuple[int, str]]] = {}
    downtown_by_train: Dict[str, List[Tuple[int, str]]] = {}

    for minutes, direction_key, train, local_time in arrivals:
        if direction_key == "north":
            uptown_by_train.setdefault(train, [])
            if len(uptown_by_train[train]) < 2:
                uptown_by_train[train].append((minutes, local_time))
        elif direction_key == "south":
            downtown_by_train.setdefault(train, [])
            if len(downtown_by_train[train]) < 2:
                downtown_by_train[train].append((minutes, local_time))

    return {
        "ok": True,
        "station_name": station_name,
        "station_code": station_code,
        "train_filter": train_filter,
        "as_of_local_time": now.strftime("%I:%M %p"),
        "north_label": north_label,
        "south_label": south_label,
        "uptown_by_train": dict(sorted(uptown_by_train.items())),
        "downtown_by_train": dict(sorted(downtown_by_train.items())),
        "alerts": sorted(alert_set)[:3],
    }


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Send welcome and quick-start guidance."""
    del context
    if not update.effective_message:
        return
    message = (
        "<b>NYC Subway Arrival Bot</b>\n\n"
        f"{_commands_help_text()}\n\n"
        "<b>Render free-tier note</b>\n"
        "If the bot was idle, the first request can take up to 60 seconds due to a cold start while Render wakes the server.\n\n"
        "<b>Examples</b>\n"
        "/next 34SHS\n"
        "/next D 34SHS"
    )
    await update.effective_message.reply_text(message, parse_mode="HTML")


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Explain command usage and station-code model."""
    del context
    if not update.effective_message:
        return
    message = (
        "<b>How this bot works</b>\n\n"
        f"{_commands_help_text()}\n\n"
        "Train examples: A, D, Q, 2, 7\n"
        "Station codes are short aliases such as 34SHS, TS42S, GC42S.\n"
        "Use /stationid to browse all supported station codes."
    )
    await update.effective_message.reply_text(message, parse_mode="HTML")


def _commands_help_text() -> str:
    """Shared command list for /start and /help."""
    return (
        "<b>Commands</b>\n"
        "• /start - welcome message\n"
        "• /help - usage guide\n"
        "• /stationid - supported station codes\n"
        "• /refresh - rerun your last /next query\n"
        "• /next &lt;station_code&gt; - all trains at station, both directions\n"
        "• /next &lt;train&gt; &lt;station_code&gt; - one train at station, both directions"
    )


def _borough_keyboard() -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    current_row: List[InlineKeyboardButton] = []
    for borough in IMPORTANT_STATIONS:
        current_row.append(InlineKeyboardButton(borough, callback_data=f"stationid:{borough}"))
        if len(current_row) == 2:
            rows.append(current_row)
            current_row = []
    if current_row:
        rows.append(current_row)
    return InlineKeyboardMarkup(rows)


async def stationid_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show borough menu for station aliases."""
    del context
    if not update.effective_message:
        return
    await update.effective_message.reply_text(
        "Choose a borough to browse station aliases:",
        reply_markup=_borough_keyboard(),
    )


async def stationid_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle borough menu callbacks for /stationid with sorted and annotated output."""
    del context
    query = update.callback_query
    if not query or not query.data:
        return
    await query.answer()
    borough = query.data.split(":", maxsplit=1)[-1]
    stations = IMPORTANT_STATIONS.get(borough)
    if not stations:
        await query.edit_message_text("Borough menu expired. Please run /stationid again.")
        return

    geo_order: List[str] = BOROUGH_STATION_ORDER.get(borough, [])
    rank: Dict[str, int] = {alias: i for i, alias in enumerate(geo_order)}
    fallback_start = len(geo_order)
    sorted_codes: List[str] = sorted(stations.keys(), key=lambda code: (rank.get(code, fallback_start), code))

    header = f"<b>{escape(borough)} stations — downtown to uptown</b>\n"
    body_parts: List[str] = []
    for code in sorted_codes:
        name = stations[code]
        lines_here = get_lines_for_alias(code)
        is_complex_hub = get_complex_for_alias(code) is not None

        annotation = ""
        if is_complex_hub:
            annotation = f" <i>({escape(_format_lines(lines_here))} — transfer hub)</i>"
        elif len(lines_here) >= _LINE_ANNOTATION_THRESHOLD:
            annotation = f" <i>({escape(_format_lines(lines_here))})</i>"

        body_parts.append(f"• <b>{escape(code)}</b> → {escape(name)}{annotation}")

    full_body = header + "\n".join(body_parts)
    if len(full_body) > _TG_SAFE_LIMIT:
        kept: List[str] = []
        running = len(header) + len(_TG_TRUNCATION_NOTE)
        for part in body_parts:
            if running + len(part) + 1 > _TG_SAFE_LIMIT:
                break
            kept.append(part)
            running += len(part) + 1
        full_body = header + "\n".join(kept) + _TG_TRUNCATION_NOTE

    await query.edit_message_text(
        full_body,
        parse_mode="HTML",
        reply_markup=_borough_keyboard(),
    )


def refresh_reply_markup() -> ReplyKeyboardMarkup:
    """Return a compact one-tap keyboard with /refresh."""
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton("/refresh")]],
        resize_keyboard=True,
        one_time_keyboard=False,
    )


_MAX_LINE_DISPLAY = 8
_LINE_ANNOTATION_THRESHOLD = 2
_TG_MSG_LIMIT = 4096
_TG_TRUNCATION_NOTE = "\n\n<i>List truncated — use /next &lt;code&gt; to query any station.</i>"
_TG_SAFE_LIMIT = _TG_MSG_LIMIT - len(_TG_TRUNCATION_NOTE)
_TERMINAL_LABELS = {"Last Stop", "Outbound"}


def _format_lines(lines: List[str], cap: int = _MAX_LINE_DISPLAY) -> str:
    shown = lines[:cap]
    suffix = "..." if len(lines) > cap else ""
    return "/".join(shown) + suffix


def build_arrival_message(result: Dict[str, object], train_scope: str = "") -> str:
    """Build a Telegram HTML message for arrivals output."""
    station_code = str(result.get("station_code", ""))
    title = "Station Arrivals" if not train_scope else f"{train_scope} Train Arrival"
    msg_lines: List[str] = [
        f"<b>{escape(title)}</b>",
        "",
        f"<b>Station:</b> {escape(str(result['station_name']))}",
        f"<b>As of:</b> {escape(str(result.get('as_of_local_time', 'N/A')))}",
    ]

    lines_here: List[str] = get_lines_for_alias(station_code)
    lines_here_set = set(lines_here)
    if len(lines_here) > 1:
        msg_lines.append(f"<b>Lines at this station:</b> {escape(_format_lines(lines_here))}")

    complex_info = get_complex_for_alias(station_code)
    if complex_info:
        complex_lines: List[str] = complex_info.get("lines", [])
        transfer_extras = [line for line in complex_lines if line not in lines_here_set]
        if transfer_extras:
            msg_lines.append(
                f"<b>Complex transfers:</b> {escape(_format_lines(transfer_extras))} "
                f"<i>(free, via underground passage)</i>"
            )
        msg_lines.append(f"<i>{escape(complex_info['note'])}</i>")

    msg_lines.append("")
    north_label = escape(str(result.get("north_label", "Uptown")))
    south_label = escape(str(result.get("south_label", "Downtown")))
    north_label_raw = str(result.get("north_label", "Uptown"))
    south_label_raw = str(result.get("south_label", "Downtown"))
    show_north = bool(result["uptown_by_train"]) or north_label_raw not in _TERMINAL_LABELS
    show_south = bool(result["downtown_by_train"]) or south_label_raw not in _TERMINAL_LABELS

    if show_north:
        msg_lines.append(f"<b>{north_label} (next 2 per train)</b>")
        if result["uptown_by_train"]:
            for train, arrivals in result["uptown_by_train"].items():
                formatted = ", ".join(
                    f"{int(minutes)}m ({escape(local_time)})" for minutes, local_time in arrivals
                )
                msg_lines.append(f"• <b>{escape(train)}</b>: {formatted}")
        else:
            msg_lines.append("• No upcoming trains")
        msg_lines.append("")

    if show_south:
        msg_lines.append(f"<b>{south_label} (next 2 per train)</b>")
        if result["downtown_by_train"]:
            for train, arrivals in result["downtown_by_train"].items():
                formatted = ", ".join(
                    f"{int(minutes)}m ({escape(local_time)})" for minutes, local_time in arrivals
                )
                msg_lines.append(f"• <b>{escape(train)}</b>: {formatted}")
        else:
            msg_lines.append("• No upcoming trains")
        msg_lines.append("")

    msg_lines.append("")
    msg_lines.append("<b>Service Status</b>")
    if result["alerts"]:
        for alert in result["alerts"]:
            msg_lines.append(f"• {escape(alert)}")
    else:
        msg_lines.append("• No delays reported")

    return "\n".join(msg_lines)


def build_station_suggestions(input_alias: str, max_results: int = 5) -> List[str]:
    """Return closest known station aliases using fuzzy matching."""
    cleaned = input_alias.upper().strip()
    if len(cleaned) < 2:
        return []
    choices = list(STATION_ALIAS_TO_STOP_PREFIXES.keys())
    if not choices:
        return []
    threshold = 60 if len(cleaned) <= 4 else 75
    ranked = process.extract(cleaned, choices, limit=max_results, scorer=fuzz.partial_ratio)
    return [alias for alias, score in ranked if score >= threshold]


async def refresh_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Repeat the most recent /next query for this chat."""
    if not update.effective_chat:
        return

    chat_id = update.effective_chat.id
    last_request = LAST_REQUEST_BY_CHAT.get(chat_id)
    if not last_request:
        if update.effective_message:
            await update.effective_message.reply_text(
                "No previous /next request found. Run /next <station_code> first."
            )
        return

    train_filter, station_code = last_request
    logger.info("User requested /refresh train=%s station_code=%s", train_filter or "ALL", station_code)
    session: Optional[aiohttp.ClientSession] = context.application.bot_data.get("session")
    redis_store: Optional[RedisFeedStore] = context.application.bot_data.get("redis_store")
    if not session or not redis_store:
        if update.effective_message:
            await update.effective_message.reply_text("Bot is still starting up. Please try again.")
        return

    result = await fetch_mta_updates(session, redis_store, station_code, train_filter)
    if not result["ok"]:
        if update.effective_message:
            await update.effective_message.reply_text(str(result["error"]))
        return

    message = build_arrival_message(result, train_filter)
    if update.effective_message:
        await update.effective_message.reply_text(message, parse_mode="HTML", reply_markup=refresh_reply_markup())


async def next_train(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /next <station_code> or /next <train> <station_code> requests."""
    message = update.effective_message
    if not message:
        return

    try:
        async with asyncio.timeout(_HANDLER_TIMEOUT_SECONDS):
            if not context.args:
                await message.reply_text(
                    "Missing parameters. Usage:\n"
                    "/next <station_code>\n"
                    "/next <train> <station_code>\n"
                    "Examples: /next 34SHS  or  /next D 34SHS"
                )
                return

            train_filter = ""
            station_code = ""

            if len(context.args) == 1:
                station_code = context.args[0]
            elif len(context.args) == 2:
                train_filter = context.args[0]
                station_code = context.args[1]
            else:
                await message.reply_text(
                    "Invalid parameters. Usage:\n"
                    "/next <station_code>\n"
                    "/next <train> <station_code>"
                )
                return

            chat_id = update.effective_chat.id if update.effective_chat else None
            if chat_id:
                now = time.monotonic()
                last = context.application.bot_data.setdefault("last_request_time", {}).get(chat_id, 0.0)
                if now - last < _MIN_REQUEST_INTERVAL_SECONDS:
                    return
                context.application.bot_data["last_request_time"][chat_id] = now

            logger.info("User requested /next train=%s station_code=%s", train_filter or "ALL", station_code)

            normalized_station = station_code.upper().strip()
            if normalized_station not in STATION_ALIAS_TO_STOP_PREFIXES:
                suggestions = build_station_suggestions(normalized_station)
                suggestion_text = f" Try: {', '.join(suggestions)}." if suggestions else ""
                await message.reply_text(
                    f"Invalid station code '{normalized_station}'. Use /stationid to browse aliases.{suggestion_text}"
                )
                return

            normalized_train_filter = train_filter.upper().strip()
            if normalized_train_filter in _ROUTE_ID_ALIASES:
                canonical = _ROUTE_ID_ALIASES[normalized_train_filter]
                await message.reply_text(
                    f"Note: {normalized_train_filter} is treated as the {canonical} shuttle train.\n"
                    "To look up the Grand St station use: /next GS"
                )

            if update.effective_chat:
                LAST_REQUEST_BY_CHAT[update.effective_chat.id] = (normalized_train_filter, normalized_station)

            session: Optional[aiohttp.ClientSession] = context.application.bot_data.get("session")
            redis_store: Optional[RedisFeedStore] = context.application.bot_data.get("redis_store")
            if not session or not redis_store:
                await message.reply_text("Bot is still starting up. Please try again.")
                return

            result = await fetch_mta_updates(session, redis_store, station_code, normalized_train_filter)
            if not result["ok"]:
                await message.reply_text(str(result["error"]))
                return

            train_scope = str(result.get("train_filter", "")).upper()
            response_text = build_arrival_message(result, train_scope)
            await message.reply_text(response_text, parse_mode="HTML", reply_markup=refresh_reply_markup())
    except TimeoutError:
        await message.reply_text("Request timed out. Please try again.")


async def handle_plain_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Route plain-text messages as implicit /next queries."""
    message = update.effective_message
    if not message or not message.text:
        return

    text = message.text.strip()
    parts = text.split()
    if len(parts) > 2 or not parts:
        await message.reply_text(
            "Send a station code or 'TRAIN STATION_CODE'. Example: 34SHS or N 34SHS.\n"
            "Use /stationid to browse codes."
        )
        return

    context.args = parts
    await next_train(update, context)



async def handle_application_error(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle uncaught handler/runtime errors from python-telegram-bot."""
    if isinstance(context.error, Conflict):
        # Intentionally use a threading.Event from async handler code to signal synchronous main().
        _POLLING_CONFLICT.set()
        logger.error(
            "Telegram polling conflict detected (409). "
            "Only one active bot instance can poll this token. Stopping current instance."
        )
        await context.application.stop()
        return

    logger.exception("Unhandled exception while processing update", exc_info=context.error)

    if isinstance(update, Update) and update.effective_message:
        try:
            await update.effective_message.reply_text(
                "Sorry, something went wrong while processing your request. Please try again."
            )
        except Exception:
            logger.exception("Failed to send Telegram error reply to user.")

def main() -> None:
    """Initialize and run the Telegram bot."""
    load_dotenv()
    token = os.getenv("TELEGRAM_BOT_TOKEN")

    if not token:
        raise RuntimeError("TELEGRAM_BOT_TOKEN is not set.")

    webhook_base_url = os.getenv("TELEGRAM_WEBHOOK_BASE_URL", "").strip().rstrip("/")
    if not webhook_base_url:
        webhook_base_url = os.getenv("RENDER_EXTERNAL_URL", "").strip().rstrip("/")
    if webhook_base_url and not os.getenv("PORT"):
        logger.warning(
            "TELEGRAM_WEBHOOK_BASE_URL is set but PORT is not; defaulting to port 10000. "
            "Set PORT to match your platform's assigned port."
        )
    webhook_path = os.getenv("TELEGRAM_WEBHOOK_PATH", token)
    drop_pending_updates = os.getenv("DROP_PENDING_UPDATES", "false").strip().lower() == "true"

    if not webhook_base_url:
        start_health_server()

    retry_delay_seconds = int(os.getenv("BOT_STARTUP_RETRY_DELAY_SECONDS", str(_DEFAULT_RETRY_DELAY_SECONDS)))
    max_retries = int(os.getenv("BOT_STARTUP_MAX_RETRIES", "0"))
    # BOT_STARTUP_MAX_RETRIES=0 means retry forever.
    current_retry_delay = retry_delay_seconds

    attempt = 0

    while True:
        attempt += 1
        _POLLING_CONFLICT.clear()
        logger.info("Starting NYC Subway Arrival Bot (attempt %s)", attempt)
        run_started = time.monotonic()

        app = (
            Application.builder()
            .token(token)
            .post_init(_on_startup)
            .post_shutdown(_on_shutdown)
            .build()
        )
        app.add_handler(CommandHandler("start", start_command))
        app.add_handler(CommandHandler("help", help_command))
        app.add_handler(CommandHandler("stationid", stationid_command))
        app.add_handler(CallbackQueryHandler(stationid_callback, pattern=r"^stationid:"))
        app.add_handler(CommandHandler("next", next_train))
        app.add_handler(CommandHandler("refresh", refresh_command))
        app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_plain_text))
        app.add_error_handler(handle_application_error)

        try:
            if webhook_base_url:
                port = int(os.getenv("PORT", "10000"))
                webhook_url = f"{webhook_base_url}/{webhook_path}"
                logger.info("Starting webhook mode on port=%s webhook_url=%s", port, webhook_url)
                try:
                    app.run_webhook(
                        listen="0.0.0.0",
                        port=port,
                        url_path=webhook_path,
                        webhook_url=webhook_url,
                        drop_pending_updates=drop_pending_updates,
                    )
                except RuntimeError as exc:
                    if "python-telegram-bot[webhooks]" not in str(exc):
                        raise
                    logger.error(
                        "Webhook dependencies are missing and fallback to polling is disabled for safety. "
                        "Install with: pip install 'python-telegram-bot[webhooks]'."
                    )
                    raise
            else:
                app.run_polling(drop_pending_updates=drop_pending_updates)
            run_duration_seconds = time.monotonic() - run_started
            if run_duration_seconds > 60:
                current_retry_delay = retry_delay_seconds
            if _POLLING_CONFLICT.is_set():
                logger.warning(
                    "Polling stopped due to Telegram conflict; retrying in %s seconds.",
                    retry_delay_seconds,
                )
                time.sleep(retry_delay_seconds)
                continue
            logger.info("Bot polling stopped gracefully.")
            break
        except Conflict:
            logger.exception(
                "Telegram conflict during startup/polling. Retrying in %s seconds.",
                retry_delay_seconds,
            )
            time.sleep(retry_delay_seconds)
        except (TimedOut, NetworkError):
            logger.exception(
                "Telegram API was temporarily unreachable during startup/polling. "
                "Retrying in %s seconds.",
                current_retry_delay,
            )
            if max_retries > 0 and attempt >= max_retries:
                logger.error("Reached BOT_STARTUP_MAX_RETRIES=%s. Exiting.", max_retries)
                raise
            time.sleep(current_retry_delay)
            current_retry_delay = min(current_retry_delay * 2, _MAX_RETRY_DELAY_SECONDS)


if __name__ == "__main__":
    main()
