import asyncio
import time
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import bot
import feed_aggregator
import station_metadata as metadata
from google.transit import gtfs_realtime_pb2 as gtfs
from redis_feed_store import RedisFeedStore
from telegram.error import Conflict, NetworkError


def payload(route='D', stop='D17N', *, departure=False, canceled=False, skipped=False):
    feed = gtfs.FeedMessage()
    feed.header.gtfs_realtime_version = '2.0'
    feed.header.timestamp = int(time.time())
    trip = feed.entity.add(id='trip').trip_update
    trip.trip.route_id = route
    trip.trip.trip_id = 'trip'
    if canceled:
        trip.trip.schedule_relationship = gtfs.TripDescriptor.CANCELED
    stu = trip.stop_time_update.add(stop_id=stop)
    if skipped:
        stu.schedule_relationship = gtfs.TripUpdate.StopTimeUpdate.SKIPPED
    event = stu.departure if departure else stu.arrival
    event.time = int(time.time()) + 180
    return feed.SerializeToString()


class ArrivalTests(unittest.IsolatedAsyncioTestCase):
    async def query(self, **kwargs):
        store = SimpleNamespace(get_feeds=AsyncMock(return_value={bot.TRAIN_FEEDS['D']: payload(**kwargs)}))
        return await bot.fetch_mta_updates(store, '34SHS', 'D')

    async def test_normal_arrival(self):
        result = await self.query()
        self.assertTrue(result['ok'])
        self.assertIn('D', result['uptown_by_train'])

    async def test_departure_only_prediction(self):
        self.assertTrue((await self.query(departure=True))['ok'])

    async def test_canceled_trip_and_skipped_stop(self):
        self.assertFalse((await self.query(canceled=True))['ok'])
        self.assertFalse((await self.query(skipped=True))['ok'])

    async def test_missing_feed_never_returns_partial_arrivals(self):
        store = SimpleNamespace(get_feeds=AsyncMock(return_value={bot.TRAIN_FEEDS['D']: payload()}))
        result = await bot.fetch_mta_updates(store, '34SHS')
        self.assertFalse(result['ok'])
        self.assertIn('incomplete', result['error'])

    async def test_shuttle_filter_reads_both_feed_groups(self):
        for route in ('S', 'FS', 'H'):
            store = SimpleNamespace(get_feeds=AsyncMock(return_value={}))
            await bot.fetch_mta_updates(store, '34SHS', route)
            urls = store.get_feeds.call_args.args[0]
            self.assertIn(bot.TRAIN_FEEDS['A'], urls)

    async def test_redis_failure_is_friendly(self):
        store = SimpleNamespace(get_feeds=AsyncMock(side_effect=ConnectionError('offline')))
        self.assertFalse((await bot.fetch_mta_updates(store, '34SHS', 'D'))['ok'])


class LifecycleTests(unittest.IsolatedAsyncioTestCase):
    async def test_conflict_signals_runner_without_awaiting_stop(self):
        app = SimpleNamespace(stop_running=Mock(), stop=AsyncMock())
        bot._POLLING_CONFLICT.clear()
        await bot.handle_application_error(None, SimpleNamespace(error=Conflict('test'), application=app))
        app.stop_running.assert_called_once()
        app.stop.assert_not_awaited()
        self.assertTrue(bot._POLLING_CONFLICT.is_set())
        bot._POLLING_CONFLICT.clear()

    async def test_failed_redis_connect_closes_pool(self):
        client = SimpleNamespace(ping=AsyncMock(side_effect=ConnectionError()), aclose=AsyncMock())
        store = RedisFeedStore()
        with patch('redis_feed_store.aioredis.from_url', return_value=client):
            with self.assertRaises(ConnectionError):
                await store.connect()
        client.aclose.assert_awaited_once()
        self.assertIsNone(store._client)

    async def test_refresh_uses_rate_limited_handler(self):
        bot.LAST_REQUEST_BY_CHAT[123] = ('D', '34SHS')
        update = SimpleNamespace(effective_chat=SimpleNamespace(id=123))
        context = SimpleNamespace(application=object())
        with patch('bot.next_train', new_callable=AsyncMock) as handler:
            await bot.refresh_command(update, context)
        self.assertEqual(handler.call_args.args[1].args, ['D', '34SHS'])
        bot.LAST_REQUEST_BY_CHAT.pop(123)


class FeedValidationTests(unittest.IsolatedAsyncioTestCase):
    async def test_bad_download_does_not_replace_good_cache(self):
        valid = payload()
        for data in (b'', b'not protobuf', valid):
            response = SimpleNamespace(raise_for_status=Mock(), read=AsyncMock(return_value=data))
            manager = AsyncMock()
            manager.__aenter__.return_value = response
            session = SimpleNamespace(get=Mock(return_value=manager))
            url, result = await feed_aggregator.fetch_feed(session, 'https://example.test/feed', 'test')
            self.assertEqual(url, 'https://example.test/feed')
            self.assertEqual(result, data if data == valid else None)

    async def test_failed_request_preserves_previous_refresh(self):
        chat_id = 456
        bot.LAST_REQUEST_BY_CHAT[chat_id] = ('D', '34SHS')
        update = SimpleNamespace(effective_chat=SimpleNamespace(id=chat_id),
                                 effective_message=SimpleNamespace(reply_text=AsyncMock()))
        context = SimpleNamespace(args=['INVALID', '34SHS'],
                                  application=SimpleNamespace(bot_data={'redis_store': object()}))
        await bot.next_train(update, context)
        self.assertEqual(bot.LAST_REQUEST_BY_CHAT.pop(chat_id), ('D', '34SHS'))
        bot._LAST_REQUEST_TIME.pop(chat_id, None)


class RepositoryTests(unittest.TestCase):
    def test_metadata_references(self):
        for alias, prefixes in metadata.STATION_ALIAS_TO_STOP_PREFIXES.items():
            self.assertTrue(prefixes, alias)
            for prefix in prefixes:
                self.assertIn(prefix, metadata.STATION_METADATA, alias)
        for alias, cid in metadata.ALIAS_TO_COMPLEX.items():
            self.assertIn(alias, metadata.STATION_ALIAS_TO_STOP_PREFIXES)
            self.assertIn(cid, metadata.STATION_COMPLEXES)

    def test_all_routes_have_polled_feed(self):
        for info in metadata.STATION_METADATA.values():
            for route in info['routes']:
                self.assertIn(route, bot.TRAIN_FEEDS)
        self.assertEqual(set(bot.TRAIN_FEEDS.values()), set(feed_aggregator.ALL_FEED_URLS))

    def test_retry_creates_fresh_loop(self):
        app = Mock()
        loops = []
        def run(**kwargs):
            loop = asyncio.get_event_loop()
            loops.append(loop)
            loop.close()
            if len(loops) == 1:
                raise NetworkError('temporary')
        app.run_polling.side_effect = run
        builder = Mock()
        builder.token.return_value.post_init.return_value.post_shutdown.return_value.build.return_value = app
        with patch.dict('os.environ', {'TELEGRAM_BOT_TOKEN': 'test', 'BOT_STARTUP_MAX_RETRIES': '2',
                                     'TELEGRAM_WEBHOOK_BASE_URL': '', 'RENDER_EXTERNAL_URL': ''}), \
                patch('bot.Application.builder', return_value=builder), \
                patch('bot.start_health_server'), patch('bot.time.sleep'):
            bot.main()
        self.assertEqual(len(loops), 2)
        self.assertIsNot(loops[0], loops[1])
        asyncio.set_event_loop(None)


if __name__ == '__main__':
    unittest.main()
