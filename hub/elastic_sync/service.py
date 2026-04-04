import asyncio
import errno
import json
import os
import typing
from collections import defaultdict
from time import time

from elasticsearch import AsyncElasticsearch, ConnectionError, NotFoundError
from elasticsearch.helpers import async_streaming_bulk

from hub.common import (
    ALL_FIELDS,
    INDEX_DEFAULT_SETTINGS,
    IndexVersionMismatch,
    expand_query,
)
from hub.db.common import DB_PREFIXES, ResolveResult, TrendingNotification
from hub.db.pending import PendingClaimIndex
from hub.db.revertable import RevertableOp
from hub.elastic_sync.db import ElasticSyncDB
from hub.elastic_sync.fast_ar_trending import FAST_AR_TRENDING_SCRIPT
from hub.notifier_protocol import ElasticNotifierProtocol
from hub.schema.result import Censor
from hub.service import BlockchainReaderService

if typing.TYPE_CHECKING:
    from hub.elastic_sync.env import ElasticEnv


class ElasticSyncService(BlockchainReaderService):
    VERSION = 1

    def __init__(self, env: "ElasticEnv"):
        super().__init__(
            env,
            "lbry-elastic-writer",
            thread_workers=1,
            thread_prefix="lbry-elastic-writer",
        )
        self.env = env
        # self._refresh_interval = 0.1
        self._task = None
        self.index = self.env.es_index_prefix + "claims"
        self.mempool_index = self.env.es_index_prefix + "claims_mempool"
        self._elastic_host = env.elastic_host
        self._elastic_port = env.elastic_port
        self.sync_timeout = 1800
        self.sync_client = None
        self._es_info_path = os.path.join(env.db_dir, "es_info")
        self._last_wrote_height = 0
        self._last_wrote_block_hash = None

        self._touched_claims = set()
        self._deleted_claims = set()

        self._removed_during_undo = set()

        self._trending = defaultdict(list)
        self._advanced = True
        self.synchronized = asyncio.Event()
        self._listeners: typing.List[ElasticNotifierProtocol] = []
        self._force_reindex = False
        self._pending_claims: typing.Optional[PendingClaimIndex] = getattr(
            self, "_pending_claims", None
        )
        self._mempool_claim_hashes = set()
        self._mempool_claim_docs: typing.Dict[bytes, dict] = {}
        self._mempool_tx_timestamps: typing.Dict[bytes, int] = {}
        self._last_mempool_tx_hashes: typing.Set[bytes] = set()
        self._last_mempool_height = -1

    def open_db(self):
        env = self.env
        self.db = ElasticSyncDB(
            env.coin,
            env.db_dir,
            self.secondary_name,
            -1,
            env.reorg_limit,
            env.cache_all_tx_hashes,
            blocking_channel_ids=env.blocking_channel_ids,
            filtering_channel_ids=env.filtering_channel_ids,
            executor=self._executor,
            index_address_status=env.index_address_status,
        )
        self._pending_claims = PendingClaimIndex(self.db)

    async def run_es_notifier(self, synchronized: asyncio.Event):
        started = False
        while not started:
            try:
                server = await asyncio.get_event_loop().create_server(
                    lambda: ElasticNotifierProtocol(self._listeners),
                    self.env.elastic_notifier_host,
                    self.env.elastic_notifier_port,
                )
                started = True
            except Exception as e:
                if not isinstance(e, asyncio.CancelledError):
                    self.log.error(
                        f"ES notifier server failed to listen on "
                        f"{self.env.elastic_notifier_host}:"
                        f"{self.env.elastic_notifier_port:d} : {e!r}"
                    )
                if isinstance(e, OSError) and e.errno is errno.EADDRINUSE:
                    await asyncio.sleep(3)
                    continue
                raise
        self.log.info(
            "ES notifier server listening on TCP %s:%i",
            self.env.elastic_notifier_host,
            self.env.elastic_notifier_port,
        )
        synchronized.set()
        async with server:
            await server.serve_forever()

    def notify_es_notification_listeners(self, height: int, block_hash: bytes):
        for p in self._listeners:
            p.send_height(height, block_hash)
            self.log.info("notify listener %i", height)

    def _read_es_height(self):
        info = {}
        if os.path.exists(self._es_info_path):
            with open(self._es_info_path, "r") as f:
                try:
                    info.update(json.loads(f.read()))
                except json.decoder.JSONDecodeError:
                    self.log.warning("failed to parse es sync status file")
        self._last_wrote_height = int(info.get("height", 0))
        self._last_wrote_block_hash = info.get("block_hash", None)

    async def read_es_height(self):
        await asyncio.get_event_loop().run_in_executor(
            self._executor, self._read_es_height
        )

    def write_es_height(self, height: int, block_hash: str):
        with open(self._es_info_path, "w") as f:
            f.write(json.dumps({"height": height, "block_hash": block_hash}, indent=2))
        self._last_wrote_height = height
        self._last_wrote_block_hash = block_hash

    async def get_index_version(self) -> int:
        try:
            template = await self.sync_client.indices.get_template(self.index)
            return template[self.index]["version"]
        except NotFoundError:
            return 0

    async def set_index_version(self, version):
        await self.sync_client.indices.put_template(
            self.index,
            body={"version": version, "index_patterns": ["ignored"]},
            ignore=400,
        )

    async def start_index(self) -> bool:
        if self.sync_client:
            return False
        hosts = [{"host": self._elastic_host, "port": self._elastic_port}]
        self.sync_client = AsyncElasticsearch(hosts, timeout=self.sync_timeout)
        while True:
            try:
                await self.sync_client.cluster.health(wait_for_status="yellow")
                self.log.info("ES is ready to connect to")
                break
            except ConnectionError:
                self.log.warning("Failed to connect to Elasticsearch. Waiting for it!")
                await asyncio.sleep(1)

        index_version = await self.get_index_version()

        res = await self.sync_client.indices.create(
            self.index, INDEX_DEFAULT_SETTINGS, ignore=400
        )
        acked = res.get("acknowledged", False)
        await self.sync_client.indices.delete(
            self.mempool_index, ignore_unavailable=True
        )
        await self.sync_client.indices.create(
            self.mempool_index, INDEX_DEFAULT_SETTINGS, ignore=400
        )

        if acked:
            await self.set_index_version(self.VERSION)
            return True
        elif index_version != self.VERSION:
            self.log.error(
                "es search index has an incompatible version: %s vs %s",
                index_version,
                self.VERSION,
            )
            raise IndexVersionMismatch(index_version, self.VERSION)
        else:
            await self.sync_client.indices.refresh(self.index)
            await self.sync_client.indices.refresh(self.mempool_index)
            return False

    async def stop_index(self, delete=False):
        if self.sync_client:
            if delete:
                await self.delete_index()
            await self.sync_client.close()
        self.sync_client = None

    async def delete_index(self):
        if self.sync_client:
            await self.sync_client.indices.delete(
                self.mempool_index, ignore_unavailable=True
            )
            return await self.sync_client.indices.delete(
                self.index, ignore_unavailable=True
            )

    def update_filter_query(self, censor_type, blockdict, channels=False):
        blockdict = {
            blocked.hex(): blocker.hex() for blocked, blocker in blockdict.items()
        }
        if channels:
            update = expand_query(
                channel_id__in=list(blockdict.keys()), censor_type=f"<{censor_type}"
            )
        else:
            update = expand_query(
                claim_id__in=list(blockdict.keys()), censor_type=f"<{censor_type}"
            )
        key = "channel_id" if channels else "claim_id"
        update["script"] = {
            "source": f"ctx._source.censor_type={censor_type}; "
            f"ctx._source.censoring_channel_id=params[ctx._source.{key}];",
            "lang": "painless",
            "params": blockdict,
        }
        return update

    async def apply_filters(
        self, blocked_streams, blocked_channels, filtered_streams, filtered_channels
    ):
        only_channels = lambda x: {k: chan for k, (chan, repost) in x.items()}

        async def batched_update_filter(
            items: typing.Dict[bytes, bytes], channel: bool, censor_type: int
        ):
            batches = [{}]
            for k, v in items.items():
                if len(batches[-1]) == 2000:
                    batches.append({})
                batches[-1][k] = v
            for batch in batches:
                if batch:
                    await self.sync_client.update_by_query(
                        self.index,
                        body=self.update_filter_query(
                            censor_type, only_channels(batch)
                        ),
                        slices=4,
                    )
                    if channel:
                        await self.sync_client.update_by_query(
                            self.index,
                            body=self.update_filter_query(
                                censor_type, only_channels(batch), True
                            ),
                            slices=4,
                        )
                    await self.sync_client.indices.refresh(self.index)

        if filtered_streams:
            await batched_update_filter(filtered_streams, False, Censor.SEARCH)
        if filtered_channels:
            await batched_update_filter(filtered_channels, True, Censor.SEARCH)
        if blocked_streams:
            await batched_update_filter(blocked_streams, False, Censor.RESOLVE)
        if blocked_channels:
            await batched_update_filter(blocked_channels, True, Censor.RESOLVE)

    @staticmethod
    def _upsert_claim_query(index, claim):
        return {
            "doc": {key: value for key, value in claim.items() if key in ALL_FIELDS},
            "_id": claim["claim_id"],
            "_index": index,
            "_op_type": "update",
            "doc_as_upsert": True,
        }

    @staticmethod
    def _delete_claim_query(index, claim_hash: bytes):
        return {"_index": index, "_op_type": "delete", "_id": claim_hash.hex()}

    @staticmethod
    def _update_trending_query(index, claim_hash, notifications):
        return {
            "_id": claim_hash.hex(),
            "_index": index,
            "_op_type": "update",
            "script": {
                "lang": "painless",
                "source": FAST_AR_TRENDING_SCRIPT,
                "params": {
                    "src": {
                        "changes": [
                            {
                                "height": notification.height,
                                "prev_amount": notification.prev_amount / 1e8,
                                "new_amount": notification.new_amount / 1e8,
                            }
                            for notification in notifications
                        ]
                    }
                },
            },
        }

    async def _claim_producer(self):
        for deleted in self._deleted_claims:
            yield self._delete_claim_query(self.index, deleted)

        touched_claims = list(self._touched_claims)

        for idx in range(0, len(touched_claims), 1000):
            batch = touched_claims[idx : idx + 1000]
            claims = {}
            total_extras = {}
            async for claim_hash, claim, extras in self.db._prepare_resolve_results(
                batch, include_extra=False, apply_blocking=False, apply_filtering=False
            ):
                if not claim:
                    self.log.warning("cannot sync claim %s", (claim_hash or b"").hex())
                    continue
                claims[claim_hash] = claim
                total_extras[claim_hash] = claim
                total_extras.update(extras)
            async for claim in self.db.prepare_claim_metadata_batch(
                claims, total_extras
            ):
                if claim:
                    yield self._upsert_claim_query(self.index, claim)

        for claim_hash, notifications in self._trending.items():
            yield self._update_trending_query(self.index, claim_hash, notifications)

    def _refresh_pending_claims(self):
        raw_mempool = {}
        prefix_db = self.db.prefix_db
        lower, upper = (
            prefix_db.mempool_tx.MIN_TX_HASH,
            prefix_db.mempool_tx.MAX_TX_HASH,
        )
        for k, v in prefix_db.mempool_tx.iterate(start=(lower,), stop=(upper,)):
            raw_mempool[k.tx_hash] = v.raw_tx
        current_tx_hashes = set(raw_mempool)
        if (
            current_tx_hashes == self._last_mempool_tx_hashes
            and self._last_mempool_height == self.db.db_height
        ):
            return False
        stale_tx_hashes = set(self._mempool_tx_timestamps).difference(current_tx_hashes)
        for tx_hash in stale_tx_hashes:
            self._mempool_tx_timestamps.pop(tx_hash, None)
        now = int(time())
        for tx_hash in current_tx_hashes:
            self._mempool_tx_timestamps.setdefault(tx_hash, now)
        self._pending_claims.rebuild(raw_mempool)
        self._last_mempool_tx_hashes = current_tx_hashes
        self._last_mempool_height = self.db.db_height
        return True

    async def _mempool_claim_producer(
        self,
        deleted_claims: typing.Set[bytes],
        updated_claims: typing.Dict[bytes, dict],
    ):
        for claim_hash in deleted_claims:
            yield self._delete_claim_query(self.mempool_index, claim_hash)
        for claim in updated_claims.values():
            yield self._upsert_claim_query(self.mempool_index, claim)

    async def refresh_mempool_index(self):
        changed = await asyncio.get_event_loop().run_in_executor(
            self._executor, self._refresh_pending_claims
        )
        if not changed:
            return
        await self.sync_client.indices.create(
            self.mempool_index, INDEX_DEFAULT_SETTINGS, ignore=400
        )
        previous_claim_hashes = self._mempool_claim_hashes
        current_claim_hashes = set(self._pending_claims.claims_by_hash)
        deleted_claims = previous_claim_hashes.difference(current_claim_hashes)
        if not deleted_claims and not current_claim_hashes:
            self._mempool_claim_hashes = current_claim_hashes
            self._mempool_claim_docs.clear()
            return
        current_docs = {}
        async for claim in self.db.prepare_pending_claim_metadata_batch(
            self._pending_claims,
            pending_tx_timestamps=self._mempool_tx_timestamps,
        ):
            if claim:
                current_docs[bytes.fromhex(claim["claim_id"])] = claim
        # tx_num is a virtual sequential number that shifts whenever a new
        # mempool tx sorts between existing ones — exclude it from the delta
        # comparison so we don't rewrite every doc on each new tx arrival.
        _VOLATILE_FIELDS = {"tx_num"}

        def _doc_changed(claim_hash, new_doc):
            old_doc = self._mempool_claim_docs.get(claim_hash)
            if old_doc is None:
                return True
            return any(
                new_doc.get(k) != old_doc.get(k)
                for k in set(new_doc) | set(old_doc)
                if k not in _VOLATILE_FIELDS
            )

        updated_claims = {
            claim_hash: claim
            for claim_hash, claim in current_docs.items()
            if _doc_changed(claim_hash, claim)
        }
        self._mempool_claim_hashes = current_claim_hashes
        if not deleted_claims and not updated_claims:
            self._mempool_claim_docs = current_docs
            return
        cnt = 0
        success = 0
        async for ok, item in async_streaming_bulk(
            self.sync_client,
            self._mempool_claim_producer(deleted_claims, updated_claims),
            raise_on_error=False,
        ):
            cnt += 1
            if not ok:
                self.log.warning("mempool indexing failed for an item: %s", item)
            else:
                success += 1
        self._mempool_claim_docs = current_docs
        try:
            await self.sync_client.indices.refresh(self.mempool_index)
        except NotFoundError:
            await self.sync_client.indices.create(
                self.mempool_index, INDEX_DEFAULT_SETTINGS, ignore=400
            )
            await self.sync_client.indices.refresh(self.mempool_index)
        self.log.info(
            "Indexed mempool overlay claims. %i/%i successful, %i pending claims",
            success,
            cnt,
            len(current_claim_hashes),
        )

    def advance(self, height: int):
        super().advance(height)
        touched_or_deleted = self.db.prefix_db.touched_or_deleted.get(height)
        for k, v in self.db.prefix_db.trending_notification.iterate((height,)):
            self._trending[k.claim_hash].append(
                TrendingNotification(k.height, v.previous_amount, v.new_amount)
            )
        if touched_or_deleted:
            readded_after_reorg = self._removed_during_undo.intersection(
                touched_or_deleted.touched_claims
            )
            self._deleted_claims.difference_update(readded_after_reorg)
            self._touched_claims.update(touched_or_deleted.touched_claims)
            self._deleted_claims.update(touched_or_deleted.deleted_claims)
            self._touched_claims.difference_update(self._deleted_claims)
            for to_del in touched_or_deleted.deleted_claims:
                if to_del in self._trending:
                    self._trending.pop(to_del)
        self._advanced = True

    def unwind(self):
        self.db.block_timestamp_cache.clear()
        reverted_block_hash = self.db.block_hashes[-1]
        super().unwind()
        packed = self.db.prefix_db.undo.get(len(self.db.tx_counts), reverted_block_hash)
        touched_or_deleted = None
        claims_to_delete = []
        # find and apply the touched_or_deleted items in the undos for the reverted blocks
        assert packed, f"missing undo information for block {len(self.db.tx_counts)}"
        while packed:
            op, packed = RevertableOp.unpack(packed)
            if op.is_delete and op.key.startswith(DB_PREFIXES.touched_or_deleted.value):
                assert touched_or_deleted is None, "only should have one match"
                touched_or_deleted = self.db.prefix_db.touched_or_deleted.unpack_value(
                    op.value
                )
            elif op.is_delete and op.key.startswith(DB_PREFIXES.claim_to_txo.value):
                v = self.db.prefix_db.claim_to_txo.unpack_value(op.value)
                if v.root_tx_num == v.tx_num and v.root_tx_num > self.db.tx_counts[-1]:
                    claims_to_delete.append(
                        self.db.prefix_db.claim_to_txo.unpack_key(op.key).claim_hash
                    )
        if touched_or_deleted:
            self._touched_claims.update(
                set(touched_or_deleted.deleted_claims).union(
                    touched_or_deleted.touched_claims.difference(set(claims_to_delete))
                )
            )
            self._deleted_claims.update(claims_to_delete)
            self._removed_during_undo.update(claims_to_delete)
        self._advanced = True
        self.log.warning(
            "delete %i claim and upsert %i from reorg",
            len(self._deleted_claims),
            len(self._touched_claims),
        )

    async def poll_for_changes(self):
        await super().poll_for_changes()
        cnt = 0
        success = 0
        if self._advanced:
            if self._touched_claims or self._deleted_claims or self._trending:
                async for ok, item in async_streaming_bulk(
                    self.sync_client, self._claim_producer(), raise_on_error=False
                ):
                    cnt += 1
                    if not ok:
                        self.log.warning("indexing failed for an item: %s", item)
                    else:
                        success += 1
                await self.sync_client.indices.refresh(self.index)
                await self.apply_filters(
                    self.db.blocked_streams,
                    self.db.blocked_channels,
                    self.db.filtered_streams,
                    self.db.filtered_channels,
                )
            self.write_es_height(self.db.db_height, self.db.db_tip[::-1].hex())
            self.log.info(
                "Indexing block %i done. %i/%i successful",
                self._last_wrote_height,
                success,
                cnt,
            )
            self._touched_claims.clear()
            self._deleted_claims.clear()
            self._removed_during_undo.clear()
            self._trending.clear()
            self._advanced = False
            self.synchronized.set()
            self.notify_es_notification_listeners(
                self._last_wrote_height, self.db.db_tip
            )
        await self.refresh_mempool_index()

    @property
    def last_synced_height(self) -> int:
        return self._last_wrote_height

    async def catch_up(self):
        last_state = self.db.prefix_db.db_state.get()
        db_height = last_state.height
        if (
            last_state
            and self._last_wrote_height
            and db_height > self._last_wrote_height
        ):
            self.log.warning(
                "syncing ES from block %i to rocksdb height of %i (%i blocks to sync)",
                self._last_wrote_height,
                last_state.height,
                last_state.height - self._last_wrote_height,
            )
            for _ in range(self._last_wrote_height + 1, last_state.height + 1):
                super().unwind()
            for height in range(self._last_wrote_height + 1, last_state.height + 1):
                self.advance(height)
        else:
            return
        success = 0
        cnt = 0
        if self._touched_claims or self._deleted_claims or self._trending:
            async for ok, item in async_streaming_bulk(
                self.sync_client, self._claim_producer(), raise_on_error=False
            ):
                cnt += 1
                if not ok:
                    self.log.warning("indexing failed for an item: %s", item)
                else:
                    success += 1
            await self.sync_client.indices.refresh(self.index)
            await self.apply_filters(
                self.db.blocked_streams,
                self.db.blocked_channels,
                self.db.filtered_streams,
                self.db.filtered_channels,
            )
        self.write_es_height(db_height, last_state.tip[::-1].hex())
        self._touched_claims.clear()
        self._deleted_claims.clear()
        self._removed_during_undo.clear()
        self._trending.clear()
        self._advanced = False
        self.notify_es_notification_listeners(self._last_wrote_height, last_state.tip)
        self.log.info(
            "Indexing block %i done. %i/%i successful",
            self._last_wrote_height,
            success,
            cnt,
        )

    async def reindex(self, force=False):
        if force or self._last_wrote_height == 0 and self.db.db_height > 0:
            if self._last_wrote_height == 0:
                self.log.info(
                    "running initial ES indexing of rocksdb at block height %i",
                    self.db.db_height,
                )
            else:
                self.log.info(
                    "reindex (last wrote: %i, db height: %i)",
                    self._last_wrote_height,
                    self.db.db_height,
                )
            await self._reindex()

    async def block_bulk_sync_on_writer_catchup(self):
        def _check_if_catching_up():
            self.db.prefix_db.try_catch_up_with_primary()
            state = self.db.prefix_db.db_state.get()
            return state.catching_up

        loop = asyncio.get_event_loop()

        catching_up = True
        while catching_up:
            catching_up = await loop.run_in_executor(
                self._executor, _check_if_catching_up
            )
            if catching_up:
                await asyncio.sleep(1)
            else:
                return

    def _iter_start_tasks(self):
        yield self.block_bulk_sync_on_writer_catchup()
        yield self.read_es_height()
        yield self.start_index()
        yield self.start_cancellable(self.run_es_notifier)
        yield self.reindex(force=self._force_reindex)
        yield self.catch_up()
        self.block_count_metric.set(self.last_state.height)
        yield self.start_prometheus()
        yield self.start_cancellable(self.refresh_blocks_forever)

    def _iter_stop_tasks(self):
        yield self._stop_cancellable_tasks()
        yield self.stop_index()

    def run(self, reindex=False):
        self._force_reindex = reindex
        return super().run()

    async def start(self, reindex=False):
        self._force_reindex = reindex
        try:
            return await super().start()
        finally:
            self._force_reindex = False

    async def _reindex(self):
        async with self.lock:
            self.log.info(
                "reindexing %i claims (estimate)",
                self.db.prefix_db.claim_to_txo.estimate_num_keys(),
            )
            await self.delete_index()
            res = await self.sync_client.indices.create(
                self.index, INDEX_DEFAULT_SETTINGS, ignore=400
            )
            acked = res.get("acknowledged", False)
            if acked:
                await self.set_index_version(self.VERSION)
            await self.sync_client.indices.create(
                self.mempool_index, INDEX_DEFAULT_SETTINGS, ignore=400
            )
            await self.sync_client.indices.refresh(self.index)
            await self.sync_client.indices.refresh(self.mempool_index)
            self.write_es_height(0, self.env.coin.GENESIS_HASH)
            await self._sync_all_claims()
            self._mempool_claim_hashes.clear()
            self._mempool_claim_docs.clear()
            self._mempool_tx_timestamps.clear()
            self._last_mempool_tx_hashes = set()
            self._last_mempool_height = -1
            await self.sync_client.indices.refresh(self.index)
            self.write_es_height(self.db.db_height, self.db.db_tip[::-1].hex())
            self.notify_es_notification_listeners(self.db.db_height, self.db.db_tip)
            self.log.info("finished reindexing")

    async def _sync_all_claims(self, batch_size=100000):
        async def all_claims_producer():
            current_height = self.db.db_height
            async for claim in self.db.all_claims_producer(batch_size=batch_size):
                yield self._upsert_claim_query(self.index, claim)

            self.log.info("applying trending")

            for batch_height in range(0, current_height, 10000):
                notifications = defaultdict(list)
                for k, v in self.db.prefix_db.trending_notification.iterate(
                    start=(batch_height,), stop=(batch_height + 10000,)
                ):
                    notifications[k.claim_hash].append(
                        TrendingNotification(k.height, v.previous_amount, v.new_amount)
                    )

                async for (k,), v in self.db.prefix_db.claim_to_txo.multi_get_async_gen(
                    self._executor, [(claim_hash,) for claim_hash in notifications]
                ):
                    if not v:
                        notifications.pop(k)

                for claim_hash, trending in notifications.items():
                    yield self._update_trending_query(self.index, claim_hash, trending)
            self._trending.clear()

        cnt = 0
        success = 0
        producer = all_claims_producer()

        finished = False
        try:
            async for ok, item in async_streaming_bulk(
                self.sync_client, producer, raise_on_error=False
            ):
                cnt += 1
                if not ok:
                    self.log.warning("indexing failed for an item: %s", item)
                else:
                    success += 1
                if cnt % batch_size == 0:
                    self.log.info(f"indexed {success}/{cnt} claims")
            finished = True
            await self.sync_client.indices.refresh(self.index)
            self.log.info("indexed %i/%i claims", success, cnt)
        finally:
            if not finished:
                await producer.aclose()
            self.shutdown_event.set()
