#!/usr/bin/env python3
"""
Query Chainquery for recent unconfirmed claims or claim updates and verify
that herald resolves/searches return the pending tx instead of the older
confirmed claim.

Example:
  python scripts/test_mempool_against_herald.py --limit 10
  python scripts/test_mempool_against_herald.py --claim-type collection
  python scripts/test_mempool_against_herald.py --claim-id 1234abcd...
"""

from __future__ import annotations

import argparse
import base64
import importlib.util
import json
import socket
import sys
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

CHAINQUERY_SQL_API = "https://chainquery.lbry.com/api/sql"

MEMPOOL_CLAIMS_SQL = """
SELECT
  c.claim_id,
  c.name,
  c.type,
  c.claim_type,
  c.publisher_id,
  c.height,
  c.valid_at_height,
  c.title,
  c.description,
  c.claim_count,
  c.transaction_hash_id,
  c.vout,
  c.transaction_hash_update,
  c.vout_update,
  c.modified_at,
  c.created_at,
  CASE
    WHEN COALESCE(c.transaction_hash_update, c.transaction_hash_id) <> c.transaction_hash_id
      OR COALESCE(c.vout_update, c.vout) <> c.vout
    THEN 'update'
    ELSE 'create'
  END AS pending_kind,
  COALESCE(c.transaction_hash_update, c.transaction_hash_id) AS pending_tx_hash,
  COALESCE(c.vout_update, c.vout) AS pending_nout
FROM claim AS c
WHERE c.height = 0
"""


def load_result_pb2():
    module_path = REPO_ROOT / "hub" / "schema" / "types" / "v2" / "result_pb2.py"
    spec = importlib.util.spec_from_file_location("hub_result_pb2", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load protobuf module at {module_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


RESULT_PB2 = load_result_pb2()


class HeraldClient:
    def __init__(self, host: str, port: int, timeout: float):
        self.host = host
        self.port = port
        self.timeout = timeout
        self._socket: Optional[socket.socket] = None
        self._file = None
        self._request_id = 0

    def __enter__(self) -> "HeraldClient":
        self._socket = socket.create_connection(
            (self.host, self.port), timeout=self.timeout
        )
        self._file = self._socket.makefile("rwb")
        return self

    def __exit__(self, exc_type, exc, tb):
        if self._file is not None:
            self._file.close()
        if self._socket is not None:
            self._socket.close()

    def call(self, method: str, params: Any = None) -> Any:
        if self._file is None:
            raise RuntimeError("HeraldClient must be used as a context manager.")
        self._request_id += 1
        request = {
            "jsonrpc": "2.0",
            "id": self._request_id,
            "method": method,
        }
        if params is not None:
            request["params"] = params
        payload = (json.dumps(request, separators=(",", ":")) + "\n").encode("utf-8")
        self._file.write(payload)
        self._file.flush()
        raw = self._file.readline()
        if not raw:
            raise RuntimeError(
                f"No response from {self.host}:{self.port} for {method}."
            )
        response = json.loads(raw.decode("utf-8"))
        if "error" in response and response["error"] is not None:
            raise RuntimeError(f"{method} returned error: {response['error']}")
        return response.get("result")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Verify mempool claim resolve/search behavior against herald."
    )
    parser.add_argument("--host", default="c-hub1.odysee.com")
    parser.add_argument("--port", type=int, default=50001)
    parser.add_argument("--timeout", type=float, default=10.0)
    parser.add_argument("--chainquery-url", default=CHAINQUERY_SQL_API)
    parser.add_argument("--limit", type=int, default=10)
    parser.add_argument(
        "--claim-id", help="Only test one exact claim id from Chainquery."
    )
    parser.add_argument(
        "--name", help="Only test one exact claim name from Chainquery."
    )
    parser.add_argument(
        "--claim-type",
        help="Filter Chainquery rows by c.type, for example stream, channel, repost, or collection.",
    )
    parser.add_argument(
        "--channel-id",
        help="Filter Chainquery rows by publisher_id / signing channel claim id.",
    )
    parser.add_argument(
        "--skip-name-search",
        action="store_true",
        help="Skip the exact-name claim_search verification.",
    )
    parser.add_argument(
        "--skip-claim-id-search",
        action="store_true",
        help="Skip the exact claim_ids claim_search verification.",
    )
    return parser.parse_args()


def quote_sql(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "''") + "'"


def fetch_sql_via_api(api_url: str, sql: str, timeout: float) -> Dict[str, Any]:
    url = f"{api_url}?{urllib.parse.urlencode({'query': sql})}"
    request = urllib.request.Request(
        url,
        headers={
            "Accept": "application/json",
            "User-Agent": "hub-mempool-test-script/1.0",
        },
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        payload = json.loads(response.read().decode("utf-8"))
    if not payload.get("success"):
        raise RuntimeError(f"Chainquery API error: {payload.get('error')}")
    return payload


def fetch_mempool_claims(args: argparse.Namespace) -> List[Dict[str, Any]]:
    sql = [MEMPOOL_CLAIMS_SQL]
    if args.claim_id:
        sql.append(f"AND c.claim_id = {quote_sql(args.claim_id)}")
    if args.name:
        sql.append(f"AND c.name = {quote_sql(args.name)}")
    if args.claim_type:
        sql.append(f"AND c.type = {quote_sql(args.claim_type)}")
    if args.channel_id:
        sql.append(f"AND c.publisher_id = {quote_sql(args.channel_id)}")
    sql.append("ORDER BY c.modified_at DESC, c.created_at DESC")
    sql.append(f"LIMIT {int(args.limit)}")
    payload = fetch_sql_via_api(args.chainquery_url, "\n".join(sql), args.timeout)
    return list(payload.get("data") or [])


def outputs_from_base64(encoded: str):
    outputs = RESULT_PB2.Outputs()
    outputs.ParseFromString(base64.b64decode(encoded))
    return outputs


def txo_brief(txo_message) -> Dict[str, Any]:
    meta_name = txo_message.WhichOneof("meta")
    if meta_name == "error":
        return {
            "error_name": txo_message.error.Code.Name(txo_message.error.code),
            "error_text": txo_message.error.text,
        }
    claim = txo_message.claim
    claims_in_channel = claim.claims_in_channel or None
    return {
        "txid": txo_message.tx_hash[::-1].hex(),
        "nout": txo_message.nout,
        "height": txo_message.height,
        "short_url": claim.short_url,
        "canonical_url": claim.canonical_url or claim.short_url,
        "effective_amount": claim.effective_amount,
        "support_amount": claim.support_amount,
        "claims_in_channel": claims_in_channel,
    }


def make_url(name: str, claim_id: str) -> str:
    return f"lbry://{str(name)}#{claim_id}"


def resolve_one(
    client: HeraldClient, url: str
) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    encoded = client.call("blockchain.claimtrie.resolve", [url])
    outputs = outputs_from_base64(encoded)
    if not outputs.txos:
        return None, None
    row = txo_brief(outputs.txos[0])
    return row, encoded


def search_one(
    client: HeraldClient, params: Dict[str, Any]
) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    encoded = client.call("blockchain.claimtrie.search", params)
    outputs = outputs_from_base64(encoded)
    if not outputs.txos:
        return None, None
    row = txo_brief(outputs.txos[0])
    return row, encoded


def format_candidate(row: Dict[str, Any]) -> str:
    return (
        f"{row['claim_id']} | {row['name']} | {row.get('type') or row.get('claim_type')} | "
        f"{row['pending_kind']} | pending={row['pending_tx_hash']}:{row['pending_nout']}"
    )


def print_json(label: str, payload: Dict[str, Any]):
    print(f"{label}: {json.dumps(payload, sort_keys=True, default=str)}")


def verify_candidate(
    client: HeraldClient, row: Dict[str, Any], args: argparse.Namespace
) -> bool:
    candidate_name = row.get("name")
    if candidate_name is None:
        print()
        print(f"Candidate: {format_candidate(row)}")
        print("candidate_skipped: missing name")
        return False
    candidate_name = str(candidate_name)
    expected_txid = row["pending_tx_hash"]
    expected_nout = int(row["pending_nout"])
    url = make_url(candidate_name, row["claim_id"])
    print()
    print(f"Candidate: {format_candidate(row)}")
    print_json(
        "chainquery",
        {
            "claim_id": row["claim_id"],
            "name": candidate_name,
            "type": row.get("type"),
            "title": row.get("title"),
            "claim_count": row.get("claim_count"),
        },
    )
    if row.get("pending_kind") == "update":
        print(
            f"Update path: old={row['transaction_hash_id']}:{row['vout']} "
            f"pending={row['pending_tx_hash']}:{row['pending_nout']}"
        )
    else:
        print(f"Create path: pending={row['pending_tx_hash']}:{row['pending_nout']}")

    ok = True

    try:
        resolve_row, _ = resolve_one(client, url)
    except Exception as err:
        print(f"resolve_rpc_error: {err}")
        ok = False
    else:
        if resolve_row is None:
            print("resolve: no rows returned")
            ok = False
        else:
            resolve_match = (
                resolve_row.get("txid") == expected_txid
                and resolve_row.get("nout") == expected_nout
            )
            print_json("resolve", resolve_row)
            print(f"resolve_match_pending: {resolve_match}")
            ok = ok and resolve_match

    if not args.skip_claim_id_search:
        try:
            search_row, _ = search_one(
                client,
                {
                    "claim_ids": [row["claim_id"]],
                    "limit": 1,
                    "offset": 0,
                    "no_totals": True,
                },
            )
        except Exception as err:
            print(f"claim_search_claim_ids_rpc_error: {err}")
            ok = False
        else:
            if search_row is None:
                print("claim_search(claim_ids): no rows returned")
                ok = False
            else:
                search_match = (
                    search_row.get("txid") == expected_txid
                    and search_row.get("nout") == expected_nout
                )
                print_json("claim_search_claim_ids", search_row)
                print(f"claim_search_claim_ids_match_pending: {search_match}")
                ok = ok and search_match

    if args.channel_id:
        try:
            channel_search_row, _ = search_one(
                client,
                {
                    "channel_ids": [args.channel_id],
                    "claim_ids": [row["claim_id"]],
                    "limit": 1,
                    "offset": 0,
                    "no_totals": True,
                },
            )
        except Exception as err:
            print(f"claim_search_channel_ids_rpc_error: {err}")
            ok = False
        else:
            if channel_search_row is None:
                print("claim_search(channel_ids+claim_ids): no rows returned")
                ok = False
            else:
                channel_search_match = (
                    channel_search_row.get("txid") == expected_txid
                    and channel_search_row.get("nout") == expected_nout
                )
                print_json("claim_search_channel_ids", channel_search_row)
                print(f"claim_search_channel_ids_match_pending: {channel_search_match}")
                ok = ok and channel_search_match

    if not args.skip_name_search:
        try:
            encoded = client.call(
                "blockchain.claimtrie.search",
                {"name": candidate_name, "limit": 10, "offset": 0, "no_totals": True},
            )
        except Exception as err:
            print(f"claim_search_name_rpc_error: {err}")
            ok = False
        else:
            outputs = outputs_from_base64(encoded)
            name_rows = [txo_brief(txo) for txo in outputs.txos]
            name_match = any(
                item.get("txid") == expected_txid and item.get("nout") == expected_nout
                for item in name_rows
            )
            print(f"claim_search(name) returned {len(name_rows)} rows")
            for idx, item in enumerate(name_rows[:5], start=1):
                print_json(f"claim_search_name[{idx}]", item)
            print(f"claim_search_name_contains_pending: {name_match}")
            ok = ok and name_match

    print(f"candidate_passed: {ok}")
    return ok


def main() -> int:
    args = parse_args()
    try:
        rows = fetch_mempool_claims(args)
    except Exception as err:
        print(f"Failed to fetch mempool claims from Chainquery: {err}", file=sys.stderr)
        return 2

    if not rows:
        print("No matching mempool claims found in Chainquery.")
        return 1

    print(f"Found {len(rows)} Chainquery mempool candidate(s).")
    with HeraldClient(args.host, args.port, args.timeout) as client:
        server_version = client.call(
            "server.version", ["mempool-test-script", "0.107.0"]
        )
        print(f"Connected to {args.host}:{args.port} server.version={server_version}")
        passed = 0
        for row in rows:
            if verify_candidate(client, row, args):
                passed += 1
    print()
    print(f"Summary: {passed}/{len(rows)} candidates matched the pending tx on herald.")
    return 0 if passed == len(rows) else 1


if __name__ == "__main__":
    raise SystemExit(main())
