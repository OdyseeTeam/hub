import asyncio
import os
import tempfile
import unittest
from contextlib import suppress
from types import SimpleNamespace
from unittest import mock

from prometheus_client import generate_latest

from hub.common import subscription_bucket_for_count
from hub.db.interface import BasePrefixDB
from hub.elastic_sync.service import ElasticSyncService
from hub.herald.jsonrpc import JSONRPCConnection, JSONRPCv2
from hub.herald.search import ResultCacheItem, SearchIndex
from hub.herald.session import LBRYElectrumX, SessionManager
from hub.metrics import (
    classify_fd_target,
    get_process_metrics,
    parse_cgroup_memory_stat,
    parse_smaps_rollup,
    read_cgroup_memory_metrics,
    read_process_fd_counts,
    resolve_cgroup_memory_path,
)


class Flag:
    def __init__(self, value):
        self.value = value

    def is_set(self):
        return self.value

    def set(self):
        self.value = True


class ProcessVisibilityMetricTests(unittest.TestCase):

    def test_parse_smaps_rollup_normalizes_fields_and_units(self):
        metrics = parse_smaps_rollup(
            """
Rss:                123 kB
Pss:                 45 kB
Pss_Anon:            40 kB
Pss_File:             5 kB
Anonymous:           39 kB
Private_Dirty:       38 kB
Swap:                 3 kB
SwapPss:              2 kB
Unrelated:         9999 kB
"""
        )

        self.assertEqual(123 * 1024, metrics["rss"])
        self.assertEqual(45 * 1024, metrics["pss"])
        self.assertEqual(40 * 1024, metrics["pss_anon"])
        self.assertEqual(5 * 1024, metrics["pss_file"])
        self.assertEqual(39 * 1024, metrics["anonymous"])
        self.assertEqual(38 * 1024, metrics["private_dirty"])
        self.assertEqual(3 * 1024, metrics["swap"])
        self.assertEqual(2 * 1024, metrics["swap_pss"])
        self.assertNotIn("unrelated", metrics)

    def test_classify_fd_target_handles_deleted_sst_files(self):
        self.assertEqual("socket", classify_fd_target("socket:[123]"))
        self.assertEqual("sst", classify_fd_target("/database/000123.sst"))
        self.assertEqual("sst", classify_fd_target("/database/000123.sst (deleted)"))
        self.assertEqual("other", classify_fd_target("/database/MANIFEST-000001"))

    def test_read_process_fd_counts_classifies_symlink_targets(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            os.symlink("socket:[123]", os.path.join(temp_dir, "0"))
            os.symlink("/database/000123.sst (deleted)", os.path.join(temp_dir, "1"))
            os.symlink("/tmp/file", os.path.join(temp_dir, "2"))

            counts = read_process_fd_counts(temp_dir)

        self.assertEqual(3, counts["total"])
        self.assertEqual(1, counts["socket"])
        self.assertEqual(1, counts["sst"])
        self.assertEqual(1, counts["other"])

    def test_parse_cgroup_memory_stat_uses_bytes_without_unit_conversion(self):
        metrics = parse_cgroup_memory_stat(
            """
anon 123
file 456
kernel 789
pagetables 321
sock 654
slab 987
swapcached 111
inactive_anon 222
active_anon 333
inactive_file 444
active_file 555
unevictable 999
"""
        )

        self.assertEqual(123, metrics["anon"])
        self.assertEqual(456, metrics["file"])
        self.assertEqual(789, metrics["kernel"])
        self.assertEqual(321, metrics["pagetables"])
        self.assertEqual(654, metrics["sock"])
        self.assertEqual(987, metrics["slab"])
        self.assertEqual(111, metrics["swapcached"])
        self.assertEqual(222, metrics["inactive_anon"])
        self.assertEqual(333, metrics["active_anon"])
        self.assertEqual(444, metrics["inactive_file"])
        self.assertEqual(555, metrics["active_file"])
        self.assertNotIn("unevictable", metrics)

    def test_read_cgroup_memory_metrics_reads_resolved_files(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            with open(os.path.join(temp_dir, "memory.stat"), "w") as stat_file:
                stat_file.write("anon 123\nfile 456\n")
            with open(os.path.join(temp_dir, "memory.current"), "w") as current_file:
                current_file.write("1000\n")
            with open(os.path.join(temp_dir, "memory.swap.current"), "w") as swap_file:
                swap_file.write("2000\n")

            metrics = read_cgroup_memory_metrics(temp_dir)

        self.assertEqual(123, metrics["anon"])
        self.assertEqual(456, metrics["file"])
        self.assertEqual(1000, metrics["current"])
        self.assertEqual(2000, metrics["swap_current"])

    def test_resolve_cgroup_memory_path_uses_current_process_cgroup(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            cgroup_dir = os.path.join(
                temp_dir, "user.slice", "app.slice", "hub.scope"
            )
            os.makedirs(cgroup_dir)
            with open(os.path.join(cgroup_dir, "memory.stat"), "w") as stat_file:
                stat_file.write("anon 1\n")
            proc_cgroup = os.path.join(temp_dir, "cgroup")
            with open(proc_cgroup, "w") as cgroup_file:
                cgroup_file.write("0::/user.slice/app.slice/hub.scope\n")

            path = resolve_cgroup_memory_path(proc_cgroup, temp_dir)

        self.assertEqual(cgroup_dir, path)

    def test_process_metric_task_exports_cgroup_when_smaps_fails(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            with mock.patch("hub.metrics.read_process_smaps_rollup", side_effect=OSError):
                with mock.patch(
                    "hub.metrics.read_cgroup_memory_metrics",
                    return_value={"anon": 123, "current": 456},
                ):
                    with mock.patch(
                        "hub.metrics.read_process_fd_counts",
                        return_value={"socket": 0, "sst": 0, "other": 0, "total": 0},
                    ):
                        task = get_process_metrics(delay=60)
                        loop.run_until_complete(asyncio.sleep(0))
                        task.cancel()
                        with suppress(asyncio.CancelledError):
                            loop.run_until_complete(task)

            output = generate_latest().decode()
        finally:
            loop.close()
            asyncio.set_event_loop(None)

        self.assertIn('scribe_cgroup_memory_bytes{field="anon"} 123.0', output)
        self.assertIn('scribe_cgroup_memory_bytes{field="current"} 456.0', output)

    def test_process_metric_task_can_be_cancelled(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            task = get_process_metrics(delay=60)
            loop.run_until_complete(asyncio.sleep(0))
            self.assertFalse(task.done())
            task.cancel()
            with suppress(asyncio.CancelledError):
                loop.run_until_complete(task)
            self.assertTrue(task.cancelled())
        finally:
            loop.close()
            asyncio.set_event_loop(None)


class RocksDBVisibilityMetricTests(unittest.TestCase):

    def test_snapshot_rocksdb_metrics_handles_missing_properties(self):
        class FakeDB:
            def get_property(self, name, column_family):
                values = {
                    b"rocksdb.estimate-table-readers-mem": b"123",
                    b"rocksdb.estimate-num-keys": b"456",
                }
                if name == b"rocksdb.block-cache-usage":
                    raise RuntimeError("unsupported")
                return values.get(name)

        prefix_db = object.__new__(BasePrefixDB)
        column_family = object()
        prefix_db._db = FakeDB()
        prefix_db.column_families = {b"B": column_family}
        prefix_db._column_family_labels = {b"B": "tx"}
        prefix_db._block_cache_sizes = {b"B": 789}

        prefix_db.snapshot_rocksdb_metrics()

        output = generate_latest().decode()
        self.assertIn(
            'scribe_db_rocksdb_property_bytes{column_family="tx",property="estimate-table-readers-mem"} 123.0',
            output,
        )
        self.assertIn(
            'scribe_db_rocksdb_property_count{column_family="tx",property="estimate-num-keys"} 456.0',
            output,
        )
        self.assertIn(
            'scribe_db_rocksdb_configured_block_cache_bytes{column_family="tx"} 789.0',
            output,
        )


    def test_snapshot_rocksdb_metrics_removes_stale_missing_properties(self):
        class FakeDB:
            enabled = True

            def get_property(self, name, column_family):
                if name == b"rocksdb.block-cache-usage" and self.enabled:
                    return b"999"
                return None

        prefix_db = object.__new__(BasePrefixDB)
        column_family = object()
        fake_db = FakeDB()
        prefix_db._db = fake_db
        prefix_db.column_families = {b"B": column_family}
        prefix_db._column_family_labels = {b"B": "tx_clear"}
        prefix_db._block_cache_sizes = {b"B": 789}

        prefix_db.snapshot_rocksdb_metrics()
        output = generate_latest().decode()
        self.assertIn(
            'scribe_db_rocksdb_property_bytes{column_family="tx_clear",property="block-cache-usage"} 999.0',
            output,
        )

        fake_db.enabled = False
        prefix_db.snapshot_rocksdb_metrics()
        output = generate_latest().decode()
        self.assertNotIn(
            'scribe_db_rocksdb_property_bytes{column_family="tx_clear",property="block-cache-usage"}',
            output,
        )


class ResponseVisibilityMetricTests(unittest.TestCase):

    def test_jsonrpc_response_observer_records_each_batch_item_method(self):
        connection = JSONRPCConnection(JSONRPCv2)
        observed = []
        connection.response_observer = lambda method, size: observed.append(
            (method, size)
        )

        requests = connection.receive_message(
            b'[{"jsonrpc":"2.0","method":"blockchain.claimtrie.search","id":1},'
            b'{"jsonrpc":"2.0","method":"blockchain.transaction.get_batch","id":2}]'
        )

        self.assertIsNone(requests[1].send_result("second"))
        self.assertIsNotNone(requests[0].send_result("first"))
        self.assertEqual(
            ["blockchain.transaction.get_batch", "blockchain.claimtrie.search"],
            [method for method, _ in observed],
        )
        self.assertTrue(all(size > 0 for _, size in observed))


class ElasticSyncVisibilityMetricTests(unittest.TestCase):

    def test_bulk_items_are_recorded_as_stream_progresses(self):
        service = object.__new__(ElasticSyncService)

        service.record_bulk_item("reindex", True)
        service.record_bulk_item("reindex", False)

        output = generate_latest().decode()
        self.assertIn(
            'scribe_elastic_sync_bulk_items_total{operation="reindex"} 2.0',
            output,
        )
        self.assertIn(
            'scribe_elastic_sync_bulk_failures_total{operation="reindex"} 1.0',
            output,
        )

    def test_startup_updates_elastic_sync_gauges_before_reindex(self):
        events = []

        class FakeGauge:
            def set(self, value):
                events.append(("block_count", value))

        async def mark(name):
            events.append(name)

        service = object.__new__(ElasticSyncService)
        service.block_bulk_sync_on_writer_catchup = lambda: mark("block_wait")
        service.read_es_height = lambda: mark("read_height")
        service.start_index = lambda: mark("start_index")
        service.start_prometheus = lambda: mark("start_prometheus")
        service.start_cancellable = lambda run: mark("start_cancellable")
        service.reindex = lambda force=False: mark(f"reindex:{force}")
        service.catch_up = lambda: mark("catch_up")
        service.update_metrics = lambda: events.append("update_metrics")
        service.run_es_notifier = object()
        service.refresh_blocks_forever = object()
        service._force_reindex = True
        service.last_state = SimpleNamespace(height=7)
        service.block_count_metric = FakeGauge()

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            async def run_start_tasks():
                for start_task in service._iter_start_tasks():
                    await start_task

            loop.run_until_complete(run_start_tasks())
        finally:
            loop.close()
            asyncio.set_event_loop(None)

        self.assertLess(
            events.index("start_prometheus"),
            events.index("update_metrics"),
        )
        self.assertLess(
            events.index("update_metrics"),
            events.index("reindex:True"),
        )

    def test_reindex_updates_metrics_after_final_height_write(self):
        events = []

        class FakePrefixDB:
            class claim_to_txo:
                @staticmethod
                def estimate_num_keys():
                    return 0

        class FakeDB:
            prefix_db = FakePrefixDB()
            db_height = 7
            db_tip = b"1" * 32

        class FakeIndices:
            async def create(self, *args, **kwargs):
                return {"acknowledged": False}

            async def refresh(self, *args, **kwargs):
                return None

        service = object.__new__(ElasticSyncService)
        service.lock = asyncio.Lock()
        service.log = mock.Mock()
        service.db = FakeDB()
        service.sync_client = SimpleNamespace(indices=FakeIndices())
        service.index = "claims"
        service.mempool_index = "claims_mempool"
        service.env = SimpleNamespace(coin=SimpleNamespace(GENESIS_HASH="00" * 32))
        service._mempool_claim_hashes = set()
        service._mempool_claim_docs = {}
        service._mempool_tx_timestamps = {}
        service._last_mempool_tx_hashes = set()
        service._last_mempool_height = -1
        service.delete_index = mock.AsyncMock()
        service._sync_all_claims = mock.AsyncMock()
        service.write_es_height = lambda height, block_hash: events.append(
            ("write", height)
        )
        service.update_metrics = lambda: events.append("update_metrics")
        service.notify_es_notification_listeners = lambda height, block_hash: events.append(
            ("notify", height)
        )

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(service._reindex())
        finally:
            loop.close()
            asyncio.set_event_loop(None)

        self.assertEqual(
            [("write", 0), "update_metrics", ("write", 7), "update_metrics", ("notify", 7)],
            events,
        )


class SearchVisibilityMetricTests(unittest.TestCase):

    def test_cached_search_observes_returned_claims_on_cache_hit(self):
        search_index = object.__new__(SearchIndex)
        kwargs = {"q": "cached"}
        cache_item = ResultCacheItem()
        cache_item.result = "cached-result"
        cache_item.result_count = 3
        search_index.search_cache = {str(kwargs): cache_item}

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            result = loop.run_until_complete(search_index.cached_search(kwargs))
        finally:
            loop.close()
            asyncio.set_event_loop(None)

        self.assertEqual("cached-result", result)
        output = generate_latest().decode()
        self.assertIn("scribe_hub_search_returned_claims_count 1.0", output)


class SessionVisibilityMetricTests(unittest.TestCase):

    def test_subscription_bucket_for_count_boundaries(self):
        self.assertEqual("0", subscription_bucket_for_count(0))
        self.assertEqual("1_10", subscription_bucket_for_count(1))
        self.assertEqual("1_10", subscription_bucket_for_count(10))
        self.assertEqual("11_100", subscription_bucket_for_count(11))
        self.assertEqual("11_100", subscription_bucket_for_count(100))
        self.assertEqual("101_1000", subscription_bucket_for_count(101))
        self.assertEqual("101_1000", subscription_bucket_for_count(1000))
        self.assertEqual("1001_10000", subscription_bucket_for_count(1001))
        self.assertEqual("1001_10000", subscription_bucket_for_count(10000))
        self.assertEqual("10001_plus", subscription_bucket_for_count(10001))

    def test_snapshot_session_metrics_records_session_shape(self):
        class FakeSession:
            def __init__(self, subscriptions, pending, can_send, closing, group):
                self._subscriptions = subscriptions
                self._pending = pending
                self._can_send = Flag(can_send)
                self._closing = closing
                self.group = group

            def sub_count(self):
                return self._subscriptions

            def count_pending_items(self):
                return self._pending

            def is_closing(self):
                return self._closing

        manager = object.__new__(SessionManager)
        group = object()
        manager.sessions = {
            1: FakeSession(0, 2, True, False, group),
            2: FakeSession(42, 5, False, True, group),
        }

        manager.snapshot_session_metrics()

        output = generate_latest().decode()
        self.assertIn("scribe_hub_session_subscriptions_count 42.0", output)
        self.assertIn("scribe_hub_session_subscriptions_max 42.0", output)
        self.assertIn("scribe_hub_session_pending_items_count 7.0", output)
        self.assertIn("scribe_hub_session_pending_items_max 5.0", output)
        self.assertIn("scribe_hub_session_paused_count 1.0", output)
        self.assertIn("scribe_hub_session_closing_count 1.0", output)
        self.assertIn("scribe_hub_session_groups_count 1.0", output)

    def test_connection_lost_emits_disconnect_histograms(self):
        class FakeConnection:
            def raise_pending_requests(self, exc):
                self.exc = exc

        class FakeTaskGroup:
            def cancel(self):
                self.cancelled = True

        class FakeSessionManager:
            session_count_metric = SessionManager.session_count_metric

            def remove_session(self, session):
                self.removed = session

        session = object.__new__(LBRYElectrumX)
        session.disconnect_reason = "send_timeout"
        session.send_size = 1234
        session.send_count = 3
        session.recv_size = 567
        session.recv_count = 2
        session.connection = FakeConnection()
        session._address = ("127.0.0.1", 50001)
        session.transport = object()
        session._task_group = FakeTaskGroup()
        session._pm_task = None
        session._can_send = Flag(True)
        session.session_manager = FakeSessionManager()
        session.client_version = "visibility-test"
        session.logger = mock.Mock()

        session.connection_lost(None)

        output = generate_latest().decode()
        self.assertIn(
            'scribe_hub_session_disconnects_total{reason="send_timeout"} 1.0',
            output,
        )
        self.assertIn("scribe_hub_session_lifetime_sent_bytes_count 1.0", output)
        self.assertIn("scribe_hub_session_lifetime_received_bytes_count 1.0", output)


if __name__ == "__main__":
    unittest.main()
