import unittest
from collections import defaultdict

from hub.herald.mempool import HubMemPool, MemPoolTx


class HubMemPoolTests(unittest.TestCase):
    def test_balance_delta_subtracts_prevouts(self):
        hashX = b"12345678901"
        tx_hash = b"\x01" * 32
        mempool = object.__new__(HubMemPool)
        mempool.touched_hashXs = defaultdict(set, {hashX: {tx_hash}})
        mempool.txs = {
            tx_hash: MemPoolTx(
                prevouts=((hashX, 7),),
                in_pairs=(),
                out_pairs=((hashX, 2),),
                fee=0,
                size=0,
                raw_tx=b"",
            )
        }

        self.assertEqual(-5, mempool.balance_delta(hashX))
