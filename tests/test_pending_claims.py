import sys
import types
import unittest
from unittest.mock import MagicMock, AsyncMock, patch, PropertyMock
from collections import defaultdict
from dataclasses import dataclass

# Stub rocksdb before any hub imports
_rocksdb = types.ModuleType("rocksdb")
_rocksdb_errors = types.ModuleType("rocksdb.errors")
_rocksdb.errors = _rocksdb_errors
sys.modules.setdefault("rocksdb", _rocksdb)
sys.modules.setdefault("rocksdb.errors", _rocksdb_errors)

# Stub elasticsearch
_es = types.ModuleType("elasticsearch")
_es.AsyncElasticsearch = type("AsyncElasticsearch", (), {})
_es.NotFoundError = type("NotFoundError", (Exception,), {})
_es.ConnectionError = type("ConnectionError", (Exception,), {})
_es.ConnectionTimeout = type("ConnectionTimeout", (Exception,), {})
_es_helpers = types.ModuleType("elasticsearch.helpers")
sys.modules.setdefault("elasticsearch", _es)
sys.modules.setdefault("elasticsearch.helpers", _es_helpers)

from hub.db.pending import PendingClaimIndex, PendingClaimRecord
from hub.db.common import ResolveResult, ExpandedResolveResult
from hub.common import StagedClaimtrieItem
from hub.schema.url import PathSegment, normalize_name


@dataclass
class FakeClaimTakeoverValue:
    claim_hash: bytes
    height: int


@dataclass
class FakeClaimToTXOValue:
    tx_num: int
    position: int
    root_tx_num: int
    root_position: int
    amount: int
    name: str
    normalized_name: str
    channel_signature_is_valid: bool = False


class FakePrefixDB:
    def __init__(self):
        self._takeovers = {}
        self._channel_claims = {}

    @property
    def claim_takeover(self):
        db = self

        class Accessor:
            def get(self, name):
                return db._takeovers.get(name)

        return Accessor()

    @property
    def channel_to_claim(self):
        db = self

        class Accessor:
            def iterate(self, prefix=None, include_key=False):
                return iter(db._channel_claims.get(prefix, []))

        return Accessor()


class FakeDB:
    def __init__(self):
        self.coin = MagicMock()
        self.coin.get_expiration_height = lambda h: h + 262974
        self.db_tx_count = 1000
        self.db_height = 500
        self.tx_counts = list(range(1, 1001))
        self.prefix_db = FakePrefixDB()
        self._claim_txos = {}
        self._claims_for_name = defaultdict(list)
        self._resolve_in_channel = {}
        self._fs_claims = {}

    def get_claim_txo(self, claim_hash):
        return self._claim_txos.get(claim_hash)

    def get_claims_for_name(self, name):
        return self._claims_for_name.get(name, [])

    def get_channel_for_claim(self, *args):
        return None

    def get_repost(self, *args):
        return None

    def get_controlling_claim(self, name):
        v = self.prefix_db._takeovers.get(name)
        return v

    def get_support_amount(self, *args):
        return 0

    def get_effective_amount(self, claim_hash):
        txo = self._claim_txos.get(claim_hash)
        return txo.amount if txo else 0

    def get_reposted_count(self, *args):
        return 0

    def get_claims_in_channel_count(self, *args):
        return 0

    def get_tx_hash(self, tx_num):
        return b"\x00" * 32

    def get_raw_tx(self, *args):
        return None

    def _fs_get_claim_by_hash(self, claim_hash):
        return self._fs_claims.get(claim_hash)

    def _resolve_claim_in_channel(self, channel_hash, normalized_name):
        return self._resolve_in_channel.get((channel_hash, normalized_name))


def _make_segment(name, claim_id=None, amount_order=None):
    return PathSegment(name, claim_id, amount_order)


def _make_claim(name, claim_hash, tx_num, position=0, amount=1000,
                signing_hash=None, reposted_claim_hash=None):
    try:
        normalized = normalize_name(name)
    except UnicodeDecodeError:
        normalized = name
    return StagedClaimtrieItem(
        name, normalized, claim_hash, amount,
        0, tx_num, position, tx_num, position,
        signing_hash is not None, signing_hash, reposted_claim_hash,
    )


def _make_pending_record(claim, tx_hash, tx=None):
    return PendingClaimRecord(claim, tx_hash, tx or MagicMock())


def _build_index(db, pending_claims=None):
    idx = object.__new__(PendingClaimIndex)
    idx.db = db
    idx.coin = db.coin
    idx.raw_mempool = {}
    idx.tx_by_hash = {}
    idx.tx_num_by_hash = {}
    idx.tx_hash_by_num = {}
    idx.claims_by_hash = {}
    idx.pending_claims_by_name = defaultdict(set)
    idx.pending_claims_by_channel = defaultdict(set)
    idx.pending_channel_keys = {}
    idx.support_delta = defaultdict(int)
    idx.channel_count_delta = defaultdict(int)
    idx.reposted_delta = defaultdict(int)
    idx.removed_claim_hashes = set()
    idx.touched_names = set()
    idx.effective_amounts = {}
    idx.support_amounts = {}
    idx.controlling_claims = {}
    idx.parsed_tx_cache = {}
    if pending_claims:
        for claim_hash, record in pending_claims.items():
            idx.claims_by_hash[claim_hash] = record
            idx.pending_claims_by_name[record.claim.normalized_name].add(claim_hash)
            idx.effective_amounts[claim_hash] = record.claim.amount
            idx.support_amounts[claim_hash] = 0
        for name in idx.pending_claims_by_name:
            best = max(
                idx.pending_claims_by_name[name],
                key=lambda h: idx.effective_amounts.get(h, 0),
            )
            idx.controlling_claims[name] = best
    return idx


CLAIM_A = b"\x01" * 20
CLAIM_B = b"\x02" * 20
CLAIM_C = b"\x03" * 20
CHANNEL_A = b"\x0a" * 20
CHANNEL_B = b"\x0b" * 20
TX_A = b"\xa1" * 32
TX_B = b"\xa2" * 32


class TestShouldReturnPending(unittest.TestCase):

    def test_new_claim_no_confirmed_controlling(self):
        db = FakeDB()
        idx = _build_index(db)
        segment = _make_segment("test")
        self.assertTrue(idx._should_return_pending(CLAIM_A, segment))

    def test_new_claim_confirmed_controlling_exists(self):
        db = FakeDB()
        db.prefix_db._takeovers["test"] = FakeClaimTakeoverValue(CLAIM_B, 100)
        idx = _build_index(db)
        segment = _make_segment("test")
        self.assertFalse(idx._should_return_pending(CLAIM_A, segment))

    def test_update_to_confirmed_claim(self):
        db = FakeDB()
        db._claim_txos[CLAIM_A] = FakeClaimToTXOValue(
            10, 0, 10, 0, 500, "test", "test"
        )
        db.prefix_db._takeovers["test"] = FakeClaimTakeoverValue(CLAIM_B, 100)
        idx = _build_index(db)
        segment = _make_segment("test")
        self.assertTrue(idx._should_return_pending(CLAIM_A, segment))

    def test_explicit_claim_id_always_returns(self):
        db = FakeDB()
        db.prefix_db._takeovers["test"] = FakeClaimTakeoverValue(CLAIM_B, 100)
        idx = _build_index(db)
        segment = _make_segment("test", claim_id="0101")
        self.assertTrue(idx._should_return_pending(CLAIM_A, segment))


class TestShouldReturnPendingInChannel(unittest.TestCase):

    def test_new_stream_no_confirmed_in_channel(self):
        db = FakeDB()
        claim = _make_claim("test", CLAIM_A, 1001)
        record = _make_pending_record(claim, TX_A)
        idx = _build_index(db, {CLAIM_A: record})
        self.assertTrue(
            idx._should_return_pending_in_channel(CLAIM_A, CHANNEL_A)
        )

    def test_new_stream_confirmed_exists_in_channel(self):
        db = FakeDB()
        db._resolve_in_channel[(CHANNEL_A, "test")] = CLAIM_B
        claim = _make_claim("test", CLAIM_A, 1001)
        record = _make_pending_record(claim, TX_A)
        idx = _build_index(db, {CLAIM_A: record})
        self.assertFalse(
            idx._should_return_pending_in_channel(CLAIM_A, CHANNEL_A)
        )

    def test_update_in_channel(self):
        db = FakeDB()
        db._claim_txos[CLAIM_A] = FakeClaimToTXOValue(
            10, 0, 10, 0, 500, "test", "test"
        )
        db._resolve_in_channel[(CHANNEL_A, "test")] = CLAIM_B
        claim = _make_claim("test", CLAIM_A, 1001)
        record = _make_pending_record(claim, TX_A)
        idx = _build_index(db, {CLAIM_A: record})
        self.assertTrue(
            idx._should_return_pending_in_channel(CLAIM_A, CHANNEL_A)
        )

    def test_explicit_claim_id_always_returns(self):
        db = FakeDB()
        db._resolve_in_channel[(CHANNEL_A, "test")] = CLAIM_B
        claim = _make_claim("test", CLAIM_A, 1001)
        record = _make_pending_record(claim, TX_A)
        idx = _build_index(db, {CLAIM_A: record})
        segment = _make_segment("test", claim_id="0101")
        self.assertTrue(
            idx._should_return_pending_in_channel(CLAIM_A, CHANNEL_A, segment)
        )


class TestConfirmedClaimsVisibleWhenSpentInMempool(unittest.TestCase):

    def _make_confirmed_resolve_result(self, claim_hash):
        return ResolveResult(
            "test", "test", claim_hash, 10, 0, b"\x00" * 32, 100, 500,
            short_url="test#0101010101", is_controlling=True,
            canonical_url="test#0101010101", creation_height=50,
            activation_height=50, expiration_height=100000,
            effective_amount=500, support_amount=0, last_takeover_height=50,
            claims_in_channel=0, channel_hash=None,
            reposted_claim_hash=None, reposted=0, signature_valid=None,
            reposted_tx_hash=None, reposted_tx_position=None,
            reposted_height=None, channel_tx_hash=None,
            channel_tx_position=None, channel_height=None,
        )

    def test_get_claim_result_returns_confirmed_when_in_removed(self):
        db = FakeDB()
        confirmed = self._make_confirmed_resolve_result(CLAIM_A)
        db._fs_claims[CLAIM_A] = confirmed
        idx = _build_index(db)
        idx.removed_claim_hashes.add(CLAIM_A)
        result = idx._get_claim_result(CLAIM_A)
        self.assertIsNotNone(result)
        self.assertEqual(result.claim_hash, CLAIM_A)

    def test_select_claim_hash_includes_confirmed_from_removed(self):
        db = FakeDB()
        db._claims_for_name["test"] = [CLAIM_A]
        db._claim_txos[CLAIM_A] = FakeClaimToTXOValue(
            10, 0, 10, 0, 500, "test", "test"
        )
        idx = _build_index(db)
        idx.removed_claim_hashes.add(CLAIM_A)
        segment = _make_segment("test")
        selected, is_pending = idx._select_claim_hash(
            segment, include_confirmed=True
        )
        self.assertEqual(selected, CLAIM_A)
        self.assertFalse(is_pending)

    def test_is_valid_signature_reaches_db_fallback_when_channel_removed(self):
        db = FakeDB()
        idx = _build_index(db)
        idx.removed_claim_hashes.add(CHANNEL_A)

        db._claim_txos[CHANNEL_A] = FakeClaimToTXOValue(
            5, 0, 5, 0, 100, "@chan", "@chan"
        )

        fake_txo = MagicMock()
        fake_first_input = MagicMock()
        fake_channel_tx = MagicMock()
        fake_channel_tx.outputs = [MagicMock()]
        fake_channel_tx.outputs[0].metadata.channel.public_key_bytes = b"pubkey"

        db.coin.transaction = lambda raw: fake_channel_tx
        db.coin.verify_signed_metadata = MagicMock(return_value=True)

        raw_tx = b"\xff" * 100
        original_get_raw_tx = db.get_raw_tx
        db.get_raw_tx = lambda tx_hash: raw_tx

        result = idx._is_valid_signature(CHANNEL_A, fake_txo, fake_first_input)
        db.coin.verify_signed_metadata.assert_called_once()


class TestResolveIntegration(unittest.TestCase):

    def test_bare_stream_defers_to_confirmed_on_takeover(self):
        db = FakeDB()
        db.prefix_db._takeovers["test"] = FakeClaimTakeoverValue(CLAIM_B, 100)
        db._claim_txos[CLAIM_B] = FakeClaimToTXOValue(
            10, 0, 10, 0, 50, "test", "test"
        )
        db._claims_for_name["test"] = [CLAIM_B]

        claim = _make_claim("test", CLAIM_A, 1001, amount=10000)
        record = _make_pending_record(claim, TX_A)
        idx = _build_index(db, {CLAIM_A: record})
        idx.effective_amounts[CLAIM_A] = 10000

        result = idx.resolve("lbry://test")
        self.assertIsNone(result)

    def test_bare_stream_returns_pending_when_new(self):
        db = FakeDB()
        claim = _make_claim("test", CLAIM_A, 1001, amount=10000)
        record = _make_pending_record(claim, TX_A)
        idx = _build_index(db, {CLAIM_A: record})
        idx.effective_amounts[CLAIM_A] = 10000

        result = idx.resolve("lbry://test")
        self.assertIsNotNone(result)
        self.assertIsInstance(result, ExpandedResolveResult)

    def test_bare_channel_defers_when_confirmed_exists(self):
        db = FakeDB()
        db.prefix_db._takeovers["@channel"] = FakeClaimTakeoverValue(CHANNEL_B, 100)
        db._claim_txos[CHANNEL_B] = FakeClaimToTXOValue(
            10, 0, 10, 0, 50, "@channel", "@channel"
        )
        db._claims_for_name["@channel"] = [CHANNEL_B]

        claim = _make_claim("@channel", CHANNEL_A, 1001, amount=10000)
        record = _make_pending_record(claim, TX_A)
        idx = _build_index(db, {CHANNEL_A: record})
        idx.effective_amounts[CHANNEL_A] = 10000

        result = idx.resolve("lbry://@channel")
        self.assertIsNone(result)

    def test_channel_stream_defers_on_pending_channel_takeover(self):
        db = FakeDB()
        db.prefix_db._takeovers["@channel"] = FakeClaimTakeoverValue(CHANNEL_B, 100)
        db._claim_txos[CHANNEL_B] = FakeClaimToTXOValue(
            10, 0, 10, 0, 50, "@channel", "@channel"
        )
        db._claims_for_name["@channel"] = [CHANNEL_B]

        channel_claim = _make_claim("@channel", CHANNEL_A, 1001, amount=10000)
        channel_record = _make_pending_record(channel_claim, TX_A)

        stream_claim = _make_claim("video", CLAIM_A, 1002, amount=100,
                                   signing_hash=CHANNEL_A)
        stream_record = _make_pending_record(stream_claim, TX_B)

        idx = _build_index(db, {
            CHANNEL_A: channel_record,
            CLAIM_A: stream_record,
        })
        idx.effective_amounts[CHANNEL_A] = 10000
        idx.effective_amounts[CLAIM_A] = 100
        idx.pending_claims_by_channel[(CHANNEL_A, "video")].add(CLAIM_A)

        result = idx.resolve("lbry://@channel/video")
        self.assertIsNone(result)


class TestCachedSearchMerge(unittest.IsolatedAsyncioTestCase):

    def _make_resolve_result(self, claim_hash, name="test", amount=100):
        return ResolveResult(
            name, name, claim_hash, 10, 0, b"\x00" * 32, 100, amount,
            short_url=f"{name}#{claim_hash.hex()[:10]}",
            is_controlling=True,
            canonical_url=f"{name}#{claim_hash.hex()[:10]}",
            creation_height=50, activation_height=50,
            expiration_height=100000, effective_amount=amount,
            support_amount=0, last_takeover_height=50,
            claims_in_channel=0, channel_hash=None,
            reposted_claim_hash=None, reposted=0,
            signature_valid=None, reposted_tx_hash=None,
            reposted_tx_position=None, reposted_height=None,
            channel_tx_hash=None, channel_tx_position=None,
            channel_height=None,
        )

    def _setup_search_index(self, confirmed_rows, pending_rows, pending_extra=None):
        from hub.herald.search import SearchIndex

        idx = object.__new__(SearchIndex)
        idx.search_cache = {}
        idx.claim_cache = {}
        idx.hub_db = MagicMock()
        idx.search_timeout = 3.0
        idx.timeout_counter = None

        es_rows = []
        for row in confirmed_rows:
            es_rows.append({
                "claim_id": row.claim_hash.hex(),
                "claim_hash": row.claim_hash[::-1],
                "claim_name": row.name,
                "normalized_name": row.normalized_name,
                "tx_hash": row.tx_hash,
                "tx_num": row.tx_num,
                "tx_nout": row.position,
                "height": row.height,
                "amount": row.amount,
                "short_url": row.short_url,
                "is_controlling": row.is_controlling,
                "canonical_url": row.canonical_url,
                "creation_height": row.creation_height,
                "activation_height": row.activation_height,
                "expiration_height": row.expiration_height,
                "effective_amount": row.effective_amount,
                "support_amount": row.support_amount,
                "last_take_over_height": row.last_takeover_height,
                "claims_in_channel": row.claims_in_channel,
                "channel_hash": None,
                "reposted_claim_hash": None,
                "reposted": 0,
                "signature_valid": None,
                "censor_type": 0,
                "censoring_channel_id": None,
                "reposted_claim_id": None,
                "channel_id": None,
            })

        mock_pending = MagicMock()
        mock_pending.search = MagicMock(
            return_value=(pending_rows, pending_extra or [])
        )
        idx.hub_db.pending_claims = mock_pending

        return idx, es_rows, len(confirmed_rows)

    async def test_new_pending_prepended_total_incremented(self):
        confirmed = [
            self._make_resolve_result(CLAIM_A, "a"),
            self._make_resolve_result(CLAIM_B, "b"),
            self._make_resolve_result(CLAIM_C, "c"),
        ]
        pending_new = self._make_resolve_result(b"\x04" * 20, "new", 999)

        idx, es_rows, confirmed_total = self._setup_search_index(
            confirmed, [pending_new]
        )

        captured = {}

        async def mock_search(**kwargs):
            return es_rows, 0, confirmed_total

        def mock_make_resolve(es_result):
            claim_hash = bytes.fromhex(es_result["claim_id"])
            for row in confirmed:
                if row.claim_hash == claim_hash:
                    return row
            return None

        async def mock_get_referenced(rows):
            return []

        def mock_to_base64(response, extra, offset, total, censored):
            captured["response"] = response
            captured["total"] = total
            return "base64result"

        idx.search = mock_search
        idx._make_resolve_result = mock_make_resolve
        idx._get_referenced_rows = mock_get_referenced

        with patch("hub.herald.search.Outputs") as MockOutputs:
            MockOutputs.to_base64 = mock_to_base64
            await idx.cached_search({"limit": 10})

        self.assertEqual(len(captured["response"]), 4)
        self.assertEqual(captured["response"][0].claim_hash, b"\x04" * 20)
        self.assertEqual(captured["response"][1].claim_hash, CLAIM_A)
        self.assertEqual(captured["total"], confirmed_total + 1)

    async def test_pending_update_replaces_in_place_total_unchanged(self):
        confirmed = [
            self._make_resolve_result(CLAIM_A, "a"),
            self._make_resolve_result(CLAIM_B, "b"),
            self._make_resolve_result(CLAIM_C, "c"),
        ]
        updated_b = self._make_resolve_result(CLAIM_B, "b_updated", 9999)

        idx, es_rows, confirmed_total = self._setup_search_index(
            confirmed, [updated_b]
        )

        captured = {}

        async def mock_search(**kwargs):
            return es_rows, 0, confirmed_total

        def mock_make_resolve(es_result):
            claim_hash = bytes.fromhex(es_result["claim_id"])
            for row in confirmed:
                if row.claim_hash == claim_hash:
                    return row
            return None

        async def mock_get_referenced(rows):
            return []

        def mock_to_base64(response, extra, offset, total, censored):
            captured["response"] = response
            captured["total"] = total
            return "base64result"

        idx.search = mock_search
        idx._make_resolve_result = mock_make_resolve
        idx._get_referenced_rows = mock_get_referenced

        with patch("hub.herald.search.Outputs") as MockOutputs:
            MockOutputs.to_base64 = mock_to_base64
            await idx.cached_search({"limit": 10})

        self.assertEqual(len(captured["response"]), 3)
        self.assertEqual(captured["response"][0].claim_hash, CLAIM_A)
        self.assertEqual(captured["response"][1].claim_hash, CLAIM_B)
        self.assertEqual(captured["response"][1].name, "b_updated")
        self.assertEqual(captured["total"], confirmed_total)

    async def test_skip_merge_limit_claims_per_channel(self):
        confirmed = [self._make_resolve_result(CLAIM_A, "a")]
        pending = self._make_resolve_result(CLAIM_B, "b")
        idx, es_rows, _ = self._setup_search_index(confirmed, [pending])

        captured = {}

        async def mock_search(**kwargs):
            return es_rows, 0, 1

        def mock_make_resolve(es_result):
            return confirmed[0]

        async def mock_get_referenced(rows):
            return []

        def mock_to_base64(response, extra, offset, total, censored):
            captured["response"] = response
            return "base64result"

        idx.search = mock_search
        idx._make_resolve_result = mock_make_resolve
        idx._get_referenced_rows = mock_get_referenced

        with patch("hub.herald.search.Outputs") as MockOutputs:
            MockOutputs.to_base64 = mock_to_base64
            await idx.cached_search({"limit": 10, "limit_claims_per_channel": 3})

        idx.hub_db.pending_claims.search.assert_not_called()
        self.assertEqual(len(captured["response"]), 1)

    async def test_skip_merge_remove_duplicates(self):
        confirmed = [self._make_resolve_result(CLAIM_A, "a")]
        pending = self._make_resolve_result(CLAIM_B, "b")
        idx, es_rows, _ = self._setup_search_index(confirmed, [pending])

        captured = {}

        async def mock_search(**kwargs):
            return es_rows, 0, 1

        def mock_make_resolve(es_result):
            return confirmed[0]

        async def mock_get_referenced(rows):
            return []

        def mock_to_base64(response, extra, offset, total, censored):
            captured["response"] = response
            return "base64result"

        idx.search = mock_search
        idx._make_resolve_result = mock_make_resolve
        idx._get_referenced_rows = mock_get_referenced

        with patch("hub.herald.search.Outputs") as MockOutputs:
            MockOutputs.to_base64 = mock_to_base64
            await idx.cached_search({"limit": 10, "remove_duplicates": True})

        idx.hub_db.pending_claims.search.assert_not_called()

    async def test_skip_merge_offset(self):
        confirmed = [self._make_resolve_result(CLAIM_A, "a")]
        pending = self._make_resolve_result(CLAIM_B, "b")
        idx, es_rows, _ = self._setup_search_index(confirmed, [pending])

        async def mock_search(**kwargs):
            return es_rows, 10, 1

        def mock_make_resolve(es_result):
            return confirmed[0]

        async def mock_get_referenced(rows):
            return []

        def mock_to_base64(response, extra, offset, total, censored):
            return "base64result"

        idx.search = mock_search
        idx._make_resolve_result = mock_make_resolve
        idx._get_referenced_rows = mock_get_referenced

        with patch("hub.herald.search.Outputs") as MockOutputs:
            MockOutputs.to_base64 = mock_to_base64
            await idx.cached_search({"limit": 10, "offset": 10})

        idx.hub_db.pending_claims.search.assert_not_called()


if __name__ == "__main__":
    unittest.main()
