# NYC Subway Arrival Telegram Bot

A Telegram bot that returns real-time NYC subway arrivals using MTA GTFS-Realtime feeds.

## Features
- `/start` welcome message
- `/help` usage instructions
- `/stationid` station-code directory (grouped by borough)
- `/refresh` re-runs your most recent `/next` query in the same chat
- `/next <station_code>` for all trains at a station (both directions, next 2 arrivals per train)
- `/next <train> <station_code>` for one train at a station (both directions, next 2 arrivals)
- Graceful errors for invalid train lines, station codes, missing params, API issues, and timeouts
- Static station metadata is loaded once at startup from `data/stations.json` for O(1) station lookups.
- GTFS feed downloads are asynchronous (`aiohttp` + `asyncio.gather`) with latency instrumentation for feed fetch and protobuf parsing.
- Real-time GTFS payloads are cached in Redis and shared across all users through an in-process background aggregator loop.
- Station directions are rendered using metadata labels (e.g., Queens/Manhattan, Canarsie/Manhattan) instead of generic uptown/downtown when available.
- Shuttle GTFS route IDs are normalized so `GS`, `FS`, and `H` feed trips are treated as `S` for user-facing filtering.

## Requirements
- Python 3.11+
- Telegram bot token
- MTA API key

## Installation
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Environment Variables
Set these in your shell or `.env` file:

```bash
export TELEGRAM_BOT_TOKEN="your_token"
export MTA_API_KEY="your_mta_api_key"
export REDIS_URL="redis://localhost:6379"
# Optional for auto-wake webhook mode on Render web services
export TELEGRAM_WEBHOOK_BASE_URL="https://your-service.onrender.com"
# Optional: custom path segment (defaults to bot token)
export TELEGRAM_WEBHOOK_PATH="telegram-webhook"
# Recommended with a custom webhook path: Telegram authenticates requests with this secret
export TELEGRAM_WEBHOOK_SECRET="replace-with-a-random-secret"
# Optional: keep queued updates on startup (default false)
export DROP_PENDING_UPDATES="false"
```

## Run Locally
```bash
python3 bot.py
```

## Deploy to Cloud
The bot supports polling and webhooks. Last-query and rate-limit state is kept in process memory and resets on restart. Redis stores shared feed payloads.

1. Provision a Python service/container.
2. Install dependencies with `pip install -r requirements.txt`.
3. Set `TELEGRAM_BOT_TOKEN` and `MTA_API_KEY` in environment settings.
4. Set `REDIS_URL` (Upstash Redis URL recommended on free tiers).
5. Start command: `python3 bot.py`.

### Startup reliability on cloud platforms
If Telegram is temporarily unreachable during deploy/startup, the bot now retries startup automatically instead of crashing.

Optional env vars:

- `BOT_STARTUP_RETRY_DELAY_SECONDS` (default: `10`)
- `BOT_STARTUP_MAX_RETRIES` (default: `0`, meaning infinite retries)

## Example Usage
- `/next 34SHS`   (Herald Square - all trains)
- `/next D 34SHS` (D train at Herald Square)
- `/next GC42S`   (Grand Central - all trains)
- `/refresh`
- `/stationid`

### Render deployment note
If you deploy this bot as a **Render Web Service**, Render requires the process to bind an HTTP port.
The bot now starts a lightweight health server automatically when `PORT` is set, so `python3 bot.py` works on Render web services while polling Telegram updates.

If you prefer not to expose an HTTP port at all, deploy as a **Background Worker** instead.

### Redis + in-process aggregator architecture
- One service runs both:
  - Telegram bot loop
  - Background feed aggregator (polls MTA every 60s)
- On startup, the bot awaits an asynchronous warm poll so Redis is populated before user commands are served.
- User commands read protobuf bytes from Redis instead of calling MTA per request.


### Telegram polling conflict troubleshooting
If logs show `Conflict: terminated by other getUpdates request`, more than one bot instance is polling the same token.
Ensure only one active bot process/web service/worker is running for that Telegram bot token.
The bot now detects polling conflicts (HTTP 409), stops polling cleanly, and retries automatically.
If you recently changed command semantics, ensure only the latest deploy is running to avoid stale code paths.


### Webhook mode for auto-wake on /start
For Render services that sleep on inactivity, polling mode cannot receive `/start` while asleep.
Set `TELEGRAM_WEBHOOK_BASE_URL` (and optionally `TELEGRAM_WEBHOOK_PATH`) to run in webhook mode.
If `TELEGRAM_WEBHOOK_BASE_URL` is not set, the bot also auto-detects Render's `RENDER_EXTERNAL_URL`.
In webhook mode, Telegram delivers updates to your Render URL, which wakes the service automatically when a user sends `/start` or any command.
Webhook mode requires PTB webhook extras (already included in `requirements.txt` via `python-telegram-bot[webhooks]`).

By default, startup does not drop pending updates anymore, so commands sent while the service was restarting are still processed. Set `DROP_PENDING_UPDATES=true` only if you explicitly want to discard backlog.


## Architecture and development

- `bot.py`: application entry point, Telegram handlers, arrival parsing, HTTP health endpoint, startup/retry lifecycle.
- `feed_aggregator.py`: concurrent feed downloads; runs inside the bot or independently with `python3 feed_aggregator.py`.
- `redis_feed_store.py`: binary payload cache with a 300-second TTL and poll diagnostics.
- `station_metadata.py`: startup loading and indexes for `data/stations.json` and `data/station_config.json`.
- `render.yaml`: Render web-service deployment. Webhooks are selected automatically when `RENDER_EXTERNAL_URL` is present. In polling mode, `PORT` enables a separate health server.

Start a local Redis server before running the bot. Copy `.env.example` to `.env` and fill in the settings. The health endpoint checks process liveness only; it does not verify feed freshness or Redis reachability.

Run offline regression tests and syntax checks after installing dependencies:

```bash
python3 -m unittest discover -s tests -v
python3 -m compileall -q bot.py feed_aggregator.py redis_feed_store.py station_metadata.py tests
```

There is no separate build step, formatter, linter, or type-checker configured. CI runs these checks on Python 3.11 and 3.14. Tests use synthetic protobuf feeds and mock Redis/Telegram rather than production credentials.
