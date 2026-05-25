import unittest

from prometheus_client import CollectorRegistry, generate_latest

from hub.common import (
    LFUCache,
    LFUCacheWithMetrics,
    LRUCache,
    LRUCacheWithMetrics,
    LargestValueCache,
    register_cache_metrics,
)


def value_size(value):
    return len(value)


class CacheByteAccountingTests(unittest.TestCase):

    def assert_no_size_bookkeeping(self, cache):
        cache["a"] = "aaa"
        cache["b"] = "b"

        self.assertIsNone(cache._value_sizes)
        self.assertNotIn("estimated_bytes", cache.stats())

    def test_lru_cache_without_estimator_skips_size_bookkeeping(self):
        self.assert_no_size_bookkeeping(LRUCache(2))

    def test_lru_cache_with_metrics_without_estimator_skips_size_bookkeeping(self):
        self.assert_no_size_bookkeeping(LRUCacheWithMetrics(2))

    def test_lfu_cache_without_estimator_skips_size_bookkeeping(self):
        self.assert_no_size_bookkeeping(LFUCache(2))

    def test_lfu_cache_with_metrics_without_estimator_skips_size_bookkeeping(self):
        self.assert_no_size_bookkeeping(LFUCacheWithMetrics(2))


class LargestValueCacheTests(unittest.TestCase):

    def test_same_length_update_keeps_correct_key(self):
        cache = LargestValueCache(3)
        cache["first"] = [1]
        cache["second"] = [2]

        cache["second"] = [3]

        self.assertIn("first", cache)
        self.assertIn("second", cache)
        self.assertEqual([1], cache.pop("first"))
        self.assertEqual([3], cache.pop("second"))

    def test_extend_reindexes_for_eviction(self):
        cache = LargestValueCache(2)
        cache["small"] = [1]
        cache["large"] = [1, 2, 3]

        cache.extend_value("small", [2, 3, 4, 5])
        cache["new"] = [1, 2, 3, 4]

        self.assertIn("small", cache)
        self.assertIn("new", cache)
        self.assertNotIn("large", cache)

    def test_stats_scan_values(self):
        cache = LargestValueCache(3, value_size=value_size)
        cache["a"] = [1]
        cache["b"] = [1, 2, 3]
        cache.extend_value("a", [2])

        stats = cache.stats()

        self.assertEqual(2, stats["entries"])
        self.assertEqual(3, stats["capacity"])
        self.assertEqual(5, stats["value_items"])
        self.assertEqual(3, stats["largest_value_items"])
        self.assertEqual(5, stats["estimated_bytes"])


class LRUCacheStatsTests(unittest.TestCase):

    def assert_lru_stats(self, cache):
        cache["a"] = "aaa"
        cache["b"] = "b"
        self.assertEqual(4, cache.stats()["estimated_bytes"])

        cache["c"] = "cc"
        self.assertNotIn("a", cache)
        self.assertEqual(3, cache.stats()["estimated_bytes"])

        cache["b"] = "bbbb"
        self.assertEqual(6, cache.stats()["estimated_bytes"])

        self.assertEqual("cc", cache.pop("c"))
        self.assertEqual(4, cache.stats()["estimated_bytes"])

        del cache["b"]
        self.assertEqual(0, cache.stats()["estimated_bytes"])

        cache["d"] = "dddd"
        cache.clear()
        self.assertEqual({"entries": 0, "capacity": 2, "estimated_bytes": 0}, cache.stats())

    def test_lru_cache_stats(self):
        self.assert_lru_stats(LRUCache(2, value_size=value_size))

    def test_lru_cache_with_metrics_stats(self):
        self.assert_lru_stats(LRUCacheWithMetrics(2, value_size=value_size))


class LFUCacheStatsTests(unittest.TestCase):

    def assert_lfu_stats(self, cache):
        cache["a"] = "aaa"
        cache["b"] = "b"
        self.assertEqual(4, cache.stats()["estimated_bytes"])

        self.assertEqual("aaa", cache["a"])
        cache["c"] = "cc"
        self.assertIn("a", cache)
        self.assertIn("c", cache)
        self.assertNotIn("b", cache)
        self.assertEqual(5, cache.stats()["estimated_bytes"])

        cache["a"] = "aaaa"
        self.assertEqual(6, cache.stats()["estimated_bytes"])

        cache.pop("a")
        self.assertEqual(2, cache.stats()["estimated_bytes"])

        del cache["c"]
        self.assertEqual(0, cache.stats()["estimated_bytes"])

        cache["d"] = "dddd"
        cache.clear()
        self.assertEqual({"entries": 0, "capacity": 2, "estimated_bytes": 0}, cache.stats())

    def test_lfu_cache_stats(self):
        self.assert_lfu_stats(LFUCache(2, value_size=value_size))

    def test_lfu_cache_with_metrics_stats(self):
        self.assert_lfu_stats(LFUCacheWithMetrics(2, value_size=value_size))


class CacheMetricHelperTests(unittest.TestCase):

    def test_repeated_registration_uses_live_cache(self):
        registry = CollectorRegistry()
        first = LRUCache(10)
        first["a"] = "a"
        register_cache_metrics("cache", first, "test", registry=registry)

        second = LRUCache(10)
        second["a"] = "a"
        second["b"] = "b"
        register_cache_metrics("cache", second, "test", registry=registry)

        output = generate_latest(registry).decode()

        self.assertIn('test_cache_entries{cache="cache"} 2.0', output)
        self.assertIn('test_cache_capacity{cache="cache"} 10.0', output)
        self.assertNotIn("test_cache_estimated_bytes", output)

    def test_repeated_registration_uses_live_cache_for_estimated_bytes(self):
        registry = CollectorRegistry()
        first = LRUCache(10, value_size=value_size)
        first["a"] = "a"
        register_cache_metrics("cache", first, "test", registry=registry)

        second = LRUCache(10, value_size=value_size)
        second["a"] = "aa"
        second["b"] = "bbb"
        register_cache_metrics("cache", second, "test", registry=registry)

        output = generate_latest(registry).decode()

        self.assertIn('test_cache_estimated_bytes{cache="cache"} 5.0', output)

    def test_repeated_registration_removes_stale_estimated_bytes(self):
        registry = CollectorRegistry()
        first = LRUCache(10, value_size=value_size)
        first["a"] = "aaa"
        register_cache_metrics("cache", first, "test", registry=registry)

        second = LRUCache(10)
        second["a"] = "a"
        second["b"] = "b"
        register_cache_metrics("cache", second, "test", registry=registry)

        output = generate_latest(registry).decode()

        self.assertIn('test_cache_entries{cache="cache"} 2.0', output)
        self.assertNotIn('test_cache_estimated_bytes{cache="cache"}', output)


if __name__ == "__main__":
    unittest.main()
