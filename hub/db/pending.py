import typing
from bisect import bisect_right
from collections import defaultdict
from dataclasses import dataclass
from struct import pack

from hub.common import StagedClaimtrieItem, hash160
from hub.db.common import ExpandedResolveResult, ResolveResult
from hub.schema.url import URL, PathSegment, normalize_name
from hub.scribe.transaction import Tx, TxOutput
from hub.scribe.transaction.deserializer import Deserializer

if typing.TYPE_CHECKING:
    from hub.db import SecondaryDB


@dataclass
class PendingClaimRecord:
    claim: StagedClaimtrieItem
    tx_hash: bytes
    tx: Tx


class PendingClaimIndex:
    def __init__(self, db: "SecondaryDB"):
        self.db = db
        self.coin = db.coin
        self.clear()

    def clear(self):
        self.raw_mempool: typing.Dict[bytes, bytes] = {}
        self.tx_by_hash: typing.Dict[bytes, Tx] = {}
        self.tx_num_by_hash: typing.Dict[bytes, int] = {}
        self.tx_hash_by_num: typing.Dict[int, bytes] = {}
        self.claims_by_hash: typing.Dict[bytes, PendingClaimRecord] = {}
        self.pending_claims_by_name: typing.DefaultDict[str, typing.Set[bytes]] = (
            defaultdict(set)
        )
        self.pending_claims_by_channel: typing.DefaultDict[
            typing.Tuple[bytes, str], typing.Set[bytes]
        ] = defaultdict(set)
        self.pending_channel_keys: typing.Dict[bytes, bytes] = {}
        self.support_delta: typing.DefaultDict[bytes, int] = defaultdict(int)
        self.channel_count_delta: typing.DefaultDict[bytes, int] = defaultdict(int)
        self.reposted_delta: typing.DefaultDict[bytes, int] = defaultdict(int)
        self.removed_claim_hashes: typing.Set[bytes] = set()
        self.touched_names: typing.Set[str] = set()
        self.effective_amounts: typing.Dict[bytes, int] = {}
        self.support_amounts: typing.Dict[bytes, int] = {}
        self.controlling_claims: typing.Dict[str, bytes] = {}
        self.parsed_tx_cache: typing.Dict[bytes, Tx] = {}

    def rebuild(self, raw_mempool: typing.Dict[bytes, bytes]):
        self.clear()
        if not raw_mempool:
            return
        self.raw_mempool = dict(raw_mempool)
        for tx_hash, raw_tx in raw_mempool.items():
            self.tx_by_hash[tx_hash] = Deserializer(raw_tx).read_tx()

        remaining = set(self.tx_by_hash)
        next_tx_num = self.db.db_tx_count + 1
        while remaining:
            ready = sorted(
                tx_hash
                for tx_hash in remaining
                if all(
                    txi.prev_hash not in remaining
                    for txi in self.tx_by_hash[tx_hash].inputs
                    if not txi.is_generation()
                )
            )
            if not ready:
                ready = [sorted(remaining)[0]]
            for tx_hash in ready:
                self.tx_num_by_hash[tx_hash] = next_tx_num
                self.tx_hash_by_num[next_tx_num] = tx_hash
                next_tx_num += 1
                self._apply_transaction(tx_hash, self.tx_by_hash[tx_hash])
                remaining.remove(tx_hash)
        self._finalize()

    def has_pending_claims(self) -> bool:
        return bool(self.claims_by_hash)

    def get_tx_hash(self, tx_num: int) -> typing.Optional[bytes]:
        return self.tx_hash_by_num.get(tx_num)

    def get_tx_num(self, tx_hash: bytes) -> typing.Optional[int]:
        return self.tx_num_by_hash.get(tx_hash)

    def resolve(self, url: str) -> typing.Optional[ExpandedResolveResult]:
        try:
            parsed = URL.parse(url)
        except ValueError:
            return None

        if parsed.has_stream_in_channel:
            channel_hash, _ = self._select_claim_hash(
                parsed.channel, channel_only=True, include_confirmed=True
            )
            if not channel_hash:
                return None
            stream_hash, stream_is_pending = self._select_claim_hash(
                parsed.stream, channel_hash=channel_hash, include_confirmed=True
            )
            if not stream_hash or not stream_is_pending:
                return None
            return self._expand_claim(stream_hash, channel_hash=channel_hash)

        if parsed.has_channel:
            claim_hash, is_pending = self._select_claim_hash(
                parsed.channel, channel_only=True, include_confirmed=True
            )
            if not claim_hash or not is_pending:
                return None
            return self._expand_claim(claim_hash)

        if parsed.has_stream:
            claim_hash, is_pending = self._select_claim_hash(
                parsed.stream, include_confirmed=True
            )
            if not claim_hash or not is_pending:
                return None
            return self._expand_claim(claim_hash)

        return None

    def search(
        self, kwargs: dict
    ) -> typing.Tuple[typing.List[ResolveResult], typing.List[ResolveResult]]:
        if kwargs.get("offset"):
            return [], []

        supported = {
            "name",
            "claim_id",
            "claim_ids",
            "txid",
            "nout",
            "channel_id",
            "channel_ids",
            "is_controlling",
            "claim_type",
            "has_channel_signature",
            "valid_channel_signature",
            "invalid_channel_signature",
            "limit",
            "offset",
            "order_by",
            "remove_duplicates",
            "no_totals",
        }
        if any(key not in supported for key in kwargs):
            return [], []

        candidate_hashes: typing.Optional[typing.Set[bytes]] = None

        def intersect(matches: typing.Iterable[bytes]):
            nonlocal candidate_hashes
            matches = set(matches)
            if candidate_hashes is None:
                candidate_hashes = matches
            else:
                candidate_hashes.intersection_update(matches)

        if kwargs.get("name"):
            try:
                intersect(self.pending_claims_by_name[normalize_name(kwargs["name"])])
            except UnicodeDecodeError:
                intersect(self.pending_claims_by_name[kwargs["name"]])
        if kwargs.get("claim_id"):
            claim_prefix = kwargs["claim_id"]
            intersect(
                claim_hash
                for claim_hash in self.claims_by_hash
                if claim_hash.hex().startswith(claim_prefix)
            )
        if kwargs.get("claim_ids"):
            wanted = set(kwargs["claim_ids"])
            intersect(
                claim_hash
                for claim_hash in self.claims_by_hash
                if claim_hash.hex() in wanted
            )
        if kwargs.get("txid"):
            tx_hash = bytes.fromhex(kwargs["txid"])[::-1]
            tx_num = self.tx_num_by_hash.get(tx_hash)
            if tx_num is not None and kwargs.get("nout") is not None:
                claim_hash = self._claim_hash_for_txo(
                    tx_hash,
                    int(kwargs["nout"]),
                    self.tx_by_hash[tx_hash].outputs[int(kwargs["nout"])],
                )
                if claim_hash in self.claims_by_hash:
                    intersect([claim_hash])
                else:
                    intersect([])
        if kwargs.get("channel_id"):
            channel_hash = bytes.fromhex(kwargs["channel_id"])
            intersect(
                claim_hash
                for (
                    signing_hash,
                    _,
                ), claim_hashes in self.pending_claims_by_channel.items()
                if signing_hash == channel_hash
                for claim_hash in claim_hashes
            )
        if kwargs.get("channel_ids"):
            channel_hashes = {
                bytes.fromhex(channel_id) for channel_id in kwargs["channel_ids"]
            }
            intersect(
                claim_hash
                for (
                    signing_hash,
                    _,
                ), claim_hashes in self.pending_claims_by_channel.items()
                if signing_hash in channel_hashes
                for claim_hash in claim_hashes
            )
        if candidate_hashes is None:
            return [], []

        rows = []
        extras = {}
        for claim_hash in candidate_hashes:
            record = self.claims_by_hash.get(claim_hash)
            if not record:
                continue
            if not self._matches_search(record, kwargs):
                continue
            resolved = self._make_resolve_result(claim_hash)
            if not resolved:
                continue
            rows.append(resolved)
            if resolved.channel_hash:
                extra = self._get_claim_result(resolved.channel_hash)
                if extra:
                    extras[extra.claim_hash] = extra
            if resolved.reposted_claim_hash:
                extra = self._get_claim_result(resolved.reposted_claim_hash)
                if extra:
                    extras[extra.claim_hash] = extra
                    if extra.channel_hash:
                        repost_channel = self._get_claim_result(extra.channel_hash)
                        if repost_channel:
                            extras[repost_channel.claim_hash] = repost_channel

        rows.sort(key=lambda row: (-row.effective_amount, row.tx_num, row.position))
        limit = kwargs.get("limit", 10)
        return rows[:limit], list(extras.values())

    def _matches_search(self, record: PendingClaimRecord, kwargs: dict) -> bool:
        claim = record.claim
        claim_type = kwargs.get("claim_type")
        metadata = self._safe_metadata(record.tx.outputs[claim.position])
        if claim_type:
            if claim_type == "channel" and not claim.name.startswith("@"):
                return False
            if claim_type == "stream" and claim.name.startswith("@"):
                return False
            if claim_type == "repost" and not (metadata and metadata.is_repost):
                return False
        if (
            kwargs.get("is_controlling")
            and self.controlling_claims.get(claim.normalized_name) != claim.claim_hash
        ):
            return False
        has_signature = claim.signing_hash is not None
        if kwargs.get("has_channel_signature") and not has_signature:
            return False
        if (
            kwargs.get("valid_channel_signature")
            and not claim.channel_signature_is_valid
        ):
            return False
        if kwargs.get("invalid_channel_signature") and (
            not has_signature or claim.channel_signature_is_valid
        ):
            return False
        return True

    def _apply_transaction(self, tx_hash: bytes, tx: Tx):
        spent_claims = {}
        first_input = tx.inputs[0] if tx.inputs else None
        for txi in tx.inputs:
            if txi.is_generation():
                continue
            prev_txo = self._get_prev_txo(txi.prev_hash, txi.prev_idx)
            if prev_txo is None:
                continue
            if prev_txo.is_claim or prev_txo.is_update:
                claim_hash = self._claim_hash_for_txo(
                    txi.prev_hash, txi.prev_idx, prev_txo
                )
                previous_record = self.claims_by_hash.pop(claim_hash, None)
                if previous_record:
                    self._remove_pending_claim(previous_record.claim)
                    previous_claim = previous_record.claim
                else:
                    previous_claim = self._get_confirmed_claim(claim_hash)
                if previous_claim:
                    spent_claims[claim_hash] = previous_claim
                    self.removed_claim_hashes.add(claim_hash)
                    self.touched_names.add(previous_claim.normalized_name)
                    if (
                        previous_claim.signing_hash
                        and previous_claim.channel_signature_is_valid
                    ):
                        self.channel_count_delta[previous_claim.signing_hash] -= 1
                    if previous_claim.reposted_claim_hash:
                        self.reposted_delta[previous_claim.reposted_claim_hash] -= 1
            elif prev_txo.is_support:
                supported_claim_hash = prev_txo.support.claim_hash[::-1]
                self.support_delta[supported_claim_hash] -= prev_txo.value

        tx_num = self.tx_num_by_hash[tx_hash]
        for nout, txo in enumerate(tx.outputs):
            if txo.is_claim or txo.is_update:
                pending = self._build_pending_claim(
                    tx_hash, tx_num, tx, nout, txo, spent_claims, first_input
                )
                if pending:
                    self.claims_by_hash[pending.claim.claim_hash] = pending
                    self._index_pending_claim(pending.claim)
                    self.removed_claim_hashes.discard(pending.claim.claim_hash)
            elif txo.is_support:
                self._apply_support(txo)

    def _apply_support(self, txo: TxOutput):
        supported_claim_hash = txo.support.claim_hash[::-1]
        try:
            normalized_name = normalize_name(txo.support.name.decode())
        except UnicodeDecodeError:
            normalized_name = "".join(chr(x) for x in txo.support.name)
        supported_claim = self.claims_by_hash.get(supported_claim_hash)
        if supported_claim:
            if supported_claim.claim.normalized_name != normalized_name:
                return
        else:
            confirmed = self.db.get_claim_txo(supported_claim_hash)
            if not confirmed or confirmed.normalized_name != normalized_name:
                return
        self.support_delta[supported_claim_hash] += txo.value
        self.touched_names.add(normalized_name)

    def _build_pending_claim(
        self,
        tx_hash: bytes,
        tx_num: int,
        tx: Tx,
        nout: int,
        txo: TxOutput,
        spent_claims: dict,
        first_input,
    ) -> typing.Optional[PendingClaimRecord]:
        try:
            claim_name = txo.claim.name.decode()
        except UnicodeDecodeError:
            claim_name = "".join(chr(c) for c in txo.claim.name)
        try:
            normalized_name = normalize_name(claim_name)
        except UnicodeDecodeError:
            normalized_name = claim_name

        if txo.is_claim:
            claim_hash = hash160(tx_hash + pack(">I", nout))[::-1]
            root_tx_num, root_position = tx_num, nout
        else:
            claim_hash = txo.claim.claim_hash[::-1]
            previous = spent_claims.pop(claim_hash, None)
            if not previous or previous.normalized_name != normalized_name:
                return None
            claim_name = previous.name
            root_tx_num, root_position = previous.root_tx_num, previous.root_position

        signing_channel_hash = None
        channel_signature_is_valid = False
        reposted_claim_hash = None

        metadata = self._safe_metadata(txo)
        if metadata:
            if metadata.is_repost:
                reposted_claim_hash = metadata.repost.reference.claim_hash[::-1]
                self.reposted_delta[reposted_claim_hash] += 1
            if metadata.is_channel:
                self.pending_channel_keys[claim_hash] = (
                    metadata.channel.public_key_bytes
                )
            if metadata.is_signed:
                signing_channel_hash = metadata.signing_channel_hash[::-1]
                channel_signature_is_valid = self._is_valid_signature(
                    signing_channel_hash, txo, first_input
                )

        claim = StagedClaimtrieItem(
            claim_name,
            normalized_name,
            claim_hash,
            txo.value,
            self.coin.get_expiration_height(self.db.db_height + 1),
            tx_num,
            nout,
            root_tx_num,
            root_position,
            channel_signature_is_valid,
            signing_channel_hash,
            reposted_claim_hash,
        )
        self.touched_names.add(normalized_name)
        return PendingClaimRecord(claim, tx_hash, tx)

    def _is_valid_signature(
        self, signing_channel_hash: typing.Optional[bytes], txo: TxOutput, first_input
    ) -> bool:
        if not signing_channel_hash or first_input is None:
            return False
        channel_pub_key_bytes = self.pending_channel_keys.get(signing_channel_hash)
        if channel_pub_key_bytes is None:
            signing_channel = self.db.get_claim_txo(signing_channel_hash)
            if signing_channel:
                raw_channel_tx = self.db.get_raw_tx(
                    self.db.get_tx_hash(signing_channel.tx_num)
                )
                if raw_channel_tx:
                    channel_tx = self.parsed_tx_cache.get(
                        self.db.get_tx_hash(signing_channel.tx_num)
                    )
                    if channel_tx is None:
                        channel_tx = self.coin.transaction(raw_channel_tx)
                        self.parsed_tx_cache[
                            self.db.get_tx_hash(signing_channel.tx_num)
                        ] = channel_tx
                    try:
                        channel_pub_key_bytes = channel_tx.outputs[
                            signing_channel.position
                        ].metadata.channel.public_key_bytes
                    except Exception:
                        channel_pub_key_bytes = None
        if channel_pub_key_bytes is None:
            return False
        try:
            return self.coin.verify_signed_metadata(
                channel_pub_key_bytes, txo, first_input
            )
        except Exception:
            return False

    def _safe_metadata(self, txo: TxOutput):
        try:
            return txo.metadata
        except Exception:
            return None

    def _index_pending_claim(self, claim: StagedClaimtrieItem):
        self.pending_claims_by_name[claim.normalized_name].add(claim.claim_hash)
        if claim.signing_hash and claim.channel_signature_is_valid:
            self.pending_claims_by_channel[
                (claim.signing_hash, claim.normalized_name)
            ].add(claim.claim_hash)
            self.channel_count_delta[claim.signing_hash] += 1

    def _remove_pending_claim(self, claim: StagedClaimtrieItem):
        pending = self.pending_claims_by_name.get(claim.normalized_name)
        if pending and claim.claim_hash in pending:
            pending.remove(claim.claim_hash)
            if not pending:
                self.pending_claims_by_name.pop(claim.normalized_name, None)
        if claim.signing_hash and claim.channel_signature_is_valid:
            key = (claim.signing_hash, claim.normalized_name)
            pending = self.pending_claims_by_channel.get(key)
            if pending and claim.claim_hash in pending:
                pending.remove(claim.claim_hash)
                if not pending:
                    self.pending_claims_by_channel.pop(key, None)

    def _finalize(self):
        touched_names = set(self.touched_names).union(self.pending_claims_by_name)
        for claim_hash, record in list(self.claims_by_hash.items()):
            support_amount = max(
                0,
                self.db.get_support_amount(claim_hash)
                + self.support_delta.get(claim_hash, 0),
            )
            self.support_amounts[claim_hash] = support_amount
            self.effective_amounts[claim_hash] = record.claim.amount + support_amount

        for name in touched_names:
            best = None
            best_key = None
            combined = set(self.db.get_claims_for_name(name)).union(
                self.pending_claims_by_name.get(name, set())
            )
            for claim_hash in combined:
                if claim_hash in self.claims_by_hash:
                    tx_num = self.claims_by_hash[claim_hash].claim.tx_num
                    position = self.claims_by_hash[claim_hash].claim.position
                    amount = self.effective_amounts.get(claim_hash, 0)
                else:
                    if claim_hash in self.removed_claim_hashes:
                        continue
                    claim = self.db.get_claim_txo(claim_hash)
                    if not claim:
                        continue
                    tx_num = claim.tx_num
                    position = claim.position
                    amount = self.db.get_effective_amount(claim_hash)
                key = (amount, -tx_num, -position)
                if best is None or key > best_key:
                    best = claim_hash
                    best_key = key
            if best is not None:
                self.controlling_claims[name] = best

    def _select_claim_hash(
        self,
        segment: PathSegment,
        channel_hash: typing.Optional[bytes] = None,
        channel_only: bool = False,
        include_confirmed: bool = False,
    ) -> typing.Tuple[typing.Optional[bytes], bool]:
        name = segment.normalized
        if channel_hash is None:
            pending = set(self.pending_claims_by_name.get(name, set()))
            confirmed = (
                set(self.db.get_claims_for_name(name)) if include_confirmed else set()
            )
        else:
            pending = set(
                self.pending_claims_by_channel.get((channel_hash, name), set())
            )
            confirmed = (
                set(
                    claim_hash
                    for (claim_hash,) in self.db.prefix_db.channel_to_claim.iterate(
                        prefix=(channel_hash, name), include_key=False
                    )
                )
                if include_confirmed
                else set()
            )

        if channel_only:
            pending = {
                claim_hash
                for claim_hash in pending
                if self._claim_name(claim_hash).startswith("@")
            }
            confirmed = {
                claim_hash
                for claim_hash in confirmed
                if self._claim_name(claim_hash).startswith("@")
            }

        candidates = pending.union(confirmed)
        if segment.claim_id:
            candidates = {
                claim_hash
                for claim_hash in candidates
                if claim_hash.hex().startswith(segment.claim_id)
            }

        ordered = sorted(
            candidates,
            key=lambda claim_hash: self._claim_sort_key(claim_hash),
            reverse=True,
        )
        order = max(int(segment.amount_order or 1), 1)
        if len(ordered) < order:
            return None, False
        selected = ordered[order - 1]
        return selected, selected in self.claims_by_hash

    def _claim_sort_key(self, claim_hash: bytes):
        if claim_hash in self.claims_by_hash:
            claim = self.claims_by_hash[claim_hash].claim
            return (
                self.effective_amounts.get(claim_hash, 0),
                -claim.tx_num,
                -claim.position,
            )
        claim = self.db.get_claim_txo(claim_hash)
        if not claim:
            return -1, 0, 0
        return self.db.get_effective_amount(claim_hash), -claim.tx_num, -claim.position

    def _claim_name(self, claim_hash: bytes) -> str:
        if claim_hash in self.claims_by_hash:
            return self.claims_by_hash[claim_hash].claim.name
        claim = self.db.get_claim_txo(claim_hash)
        return "" if not claim else claim.name

    def _expand_claim(
        self, claim_hash: bytes, channel_hash: typing.Optional[bytes] = None
    ) -> ExpandedResolveResult:
        stream = self._get_claim_result(claim_hash)
        if not stream:
            return ExpandedResolveResult(None, None, None, None)
        channel_hash = channel_hash or stream.channel_hash
        channel = self._get_claim_result(channel_hash) if channel_hash else None
        repost = (
            self._get_claim_result(stream.reposted_claim_hash)
            if stream.reposted_claim_hash
            else None
        )
        reposted_channel = (
            self._get_claim_result(repost.channel_hash)
            if repost and repost.channel_hash
            else None
        )
        if stream.name.startswith("@") and not channel:
            return ExpandedResolveResult(None, stream, repost, reposted_channel)
        return ExpandedResolveResult(stream, channel, repost, reposted_channel)

    def _get_claim_result(
        self, claim_hash: typing.Optional[bytes]
    ) -> typing.Optional[ResolveResult]:
        if not claim_hash:
            return None
        if claim_hash in self.claims_by_hash:
            return self._make_resolve_result(claim_hash)
        return self.db._fs_get_claim_by_hash(claim_hash)

    def _make_resolve_result(self, claim_hash: bytes) -> typing.Optional[ResolveResult]:
        record = self.claims_by_hash.get(claim_hash)
        if not record:
            return None
        claim = record.claim
        short_url = f"{claim.name}#{claim_hash.hex()[:10]}"
        canonical_url = short_url
        channel_hash = claim.signing_hash if claim.channel_signature_is_valid else None
        channel_tx_hash = channel_height = channel_tx_position = None
        if channel_hash:
            channel = self._get_claim_result(channel_hash)
            if channel:
                channel_tx_hash = channel.tx_hash
                channel_tx_position = channel.position
                channel_height = channel.height
                canonical_url = f"{channel.short_url}/{short_url}"
        reposted_tx_hash = reposted_height = reposted_tx_position = None
        if claim.reposted_claim_hash:
            repost = self._get_claim_result(claim.reposted_claim_hash)
            if repost:
                reposted_tx_hash = repost.tx_hash
                reposted_tx_position = repost.position
                reposted_height = repost.height
        if claim.root_tx_num > self.db.db_tx_count:
            creation_height = 0
        else:
            creation_height = bisect_right(self.db.tx_counts, claim.root_tx_num)
        controlling = self.controlling_claims.get(claim.normalized_name)
        controlling_height = self.db.get_controlling_claim(claim.normalized_name)
        claims_in_channel = self.db.get_claims_in_channel_count(
            claim_hash
        ) + self.channel_count_delta.get(claim_hash, 0)
        return ResolveResult(
            claim.name,
            claim.normalized_name,
            claim_hash,
            claim.tx_num,
            claim.position,
            record.tx_hash,
            0,
            claim.amount,
            short_url,
            controlling == claim_hash,
            canonical_url,
            creation_height,
            0,
            0,
            self.effective_amounts.get(claim_hash, claim.amount),
            self.support_amounts.get(
                claim_hash, self.db.get_support_amount(claim_hash)
            ),
            self.db.get_reposted_count(claim_hash)
            + self.reposted_delta.get(claim_hash, 0),
            0
            if controlling == claim_hash and not controlling_height
            else (None if not controlling_height else controlling_height.height),
            claims_in_channel,
            channel_hash,
            claim.reposted_claim_hash,
            claim.channel_signature_is_valid if channel_hash else None,
            reposted_tx_hash,
            reposted_tx_position,
            reposted_height,
            channel_tx_hash,
            channel_tx_position,
            channel_height,
        )

    def _get_prev_txo(self, tx_hash: bytes, nout: int) -> typing.Optional[TxOutput]:
        tx = self.tx_by_hash.get(tx_hash)
        if tx is None:
            tx = self.parsed_tx_cache.get(tx_hash)
        if tx is None:
            raw_tx = self.db.get_raw_tx(tx_hash)
            if not raw_tx:
                return None
            tx = self.coin.transaction(raw_tx)
            self.parsed_tx_cache[tx_hash] = tx
        if nout >= len(tx.outputs):
            return None
        return tx.outputs[nout]

    def _get_confirmed_claim(
        self, claim_hash: bytes
    ) -> typing.Optional[StagedClaimtrieItem]:
        claim = self.db.get_claim_txo(claim_hash)
        if not claim:
            return None
        signing_hash = self.db.get_channel_for_claim(
            claim_hash, claim.tx_num, claim.position
        )
        reposted_claim_hash = self.db.get_repost(claim_hash)
        return StagedClaimtrieItem(
            claim.name,
            claim.normalized_name,
            claim_hash,
            claim.amount,
            self.coin.get_expiration_height(
                bisect_right(self.db.tx_counts, claim.tx_num)
            ),
            claim.tx_num,
            claim.position,
            claim.root_tx_num,
            claim.root_position,
            claim.channel_signature_is_valid,
            signing_hash,
            reposted_claim_hash,
        )

    @staticmethod
    def _claim_hash_for_txo(tx_hash: bytes, nout: int, txo: TxOutput) -> bytes:
        if txo.is_claim:
            return hash160(tx_hash + pack(">I", nout))[::-1]
        return txo.claim.claim_hash[::-1]
