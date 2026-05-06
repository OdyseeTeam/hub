#!/usr/bin/env python3
"""
Benchmark a small set of confirmed, non-mempool herald queries.

By default this benchmarks a-hub1.odysee.com and compares it to c-hub1.odysee.com
using the same confirmed queries, so normal resolve/search latency can be compared
before and after mempool-related changes.

Examples:
  python scripts/benchmark_herald_non_mempool.py
  python scripts/benchmark_herald_non_mempool.py --runs 10
  python scripts/benchmark_herald_non_mempool.py --primary a-hub1.odysee.com:50001 --compare c-hub1.odysee.com:50001
  python scripts/benchmark_herald_non_mempool.py --no-compare
"""

from __future__ import annotations

import argparse
import base64
import importlib.util
import json
import socket
import statistics
import sys
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

REPO_ROOT = Path(__file__).resolve().parents[1]
CHAINQUERY_SQL_API = "https://chainquery.lbry.com/api/sql"


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
        request = {"jsonrpc": "2.0", "id": self._request_id, "method": method}
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
        if response.get("error") is not None:
            raise RuntimeError(f"{method} returned error: {response['error']}")
        return response.get("result")


@dataclass
class QueryCase:
    name: str
    method: str
    params: Any
    note: str


@dataclass
class RunSummary:
    host: str
    query_name: str
    note: str
    runs: int
    success_count: int
    avg_ms: float
    median_ms: float
    min_ms: float
    max_ms: float
    first_result: Optional[str]
    last_error: Optional[str]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Benchmark confirmed herald queries.")
    parser.add_argument("--primary", default="a-hub1.odysee.com:50001")
    parser.add_argument("--compare", default="c-hub1.odysee.com:50001")
    parser.add_argument("--no-compare", action="store_true")
    parser.add_argument("--runs", type=int, default=5)
    parser.add_argument("--warmup-runs", type=int, default=1)
    parser.add_argument("--timeout", type=float, default=15.0)
    parser.add_argument("--chainquery-url", default=CHAINQUERY_SQL_API)
    return parser.parse_args()


def parse_hostport(hostport: str) -> Tuple[str, int]:
    host, port = hostport.rsplit(":", 1)
    return host, int(port)


def fetch_sql_via_api(api_url: str, sql: str, timeout: float) -> List[Dict[str, Any]]:
    url = f"{api_url}?{urllib.parse.urlencode({'query': sql})}"
    request = urllib.request.Request(
        url,
        headers={
            "Accept": "application/json",
            "User-Agent": "hub-benchmark-script/1.0",
        },
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        payload = json.loads(response.read().decode("utf-8"))
    if not payload.get("success"):
        raise RuntimeError(f"Chainquery API error: {payload.get('error')}")
    return list(payload.get("data") or [])


def first_row(api_url: str, sql: str, timeout: float) -> Dict[str, Any]:
    rows = fetch_sql_via_api(api_url, sql, timeout)
    if not rows:
        raise RuntimeError(f"No rows returned for bootstrap query: {sql}")
    return rows[0]


def outputs_from_base64(encoded: str):
    outputs = RESULT_PB2.Outputs()
    outputs.ParseFromString(base64.b64decode(encoded))
    return outputs


def summarize_result(method: str, result: Any) -> str:
    if method in {
        "blockchain.claimtrie.resolve",
        "blockchain.claimtrie.search",
        "blockchain.claimtrie.getclaimbyid",
    }:
        outputs = outputs_from_base64(result)
        if not outputs.txos:
            return "no_rows"
        txo = outputs.txos[0]
        if txo.WhichOneof("meta") == "error":
            return f"error:{txo.error.Code.Name(txo.error.code)}"
        return f"{txo.tx_hash[::-1].hex()}:{txo.nout}"
    if isinstance(result, list):
        return json.dumps(result)
    return str(result)


def case_is_usable(hostport: str, case: QueryCase, timeout: float) -> Tuple[bool, str]:
    host, port = parse_hostport(hostport)
    with HeraldClient(host, port, timeout) as client:
        result = client.call(case.method, case.params)
    summary = summarize_result(case.method, result)
    usable = not summary.startswith("error:") and summary != "no_rows"
    return usable, summary


def bootstrap_cases(api_url: str, timeout: float) -> List[QueryCase]:
    channel = first_row(
        api_url,
        """
        SELECT name, claim_id
        FROM claim
        WHERE height > 0 AND name LIKE '@%' AND name REGEXP '^[@A-Za-z0-9._-]+$'
        LIMIT 1
        """,
        timeout,
    )
    stream = first_row(
        api_url,
        """
        SELECT name, claim_id
        FROM claim
        WHERE height > 0 AND name NOT LIKE '@%' AND name REGEXP '^[A-Za-z0-9._-]+$'
        LIMIT 1
        """,
        timeout,
    )
    signed_stream = first_row(
        api_url,
        """
        SELECT
          c.name AS stream_name,
          c.claim_id AS stream_claim_id,
          p.name AS channel_name,
          p.claim_id AS channel_claim_id
        FROM claim AS c
        LEFT JOIN claim AS p ON c.publisher_id = p.claim_id
        WHERE c.height > 0
          AND c.publisher_id IS NOT NULL
          AND c.name REGEXP '^[A-Za-z0-9._-]+$'
          AND p.name REGEXP '^[@A-Za-z0-9._-]+$'
        LIMIT 1
        """,
        timeout,
    )
    collection_rows = fetch_sql_via_api(
        api_url,
        """
        SELECT name, claim_id
        FROM claim
        WHERE height > 0 AND claim_id_list IS NOT NULL AND name REGEXP '^[A-Za-z0-9._-]+$'
        LIMIT 1
        """,
        timeout,
    )

    cases = [
        QueryCase(
            name="resolve_channel",
            method="blockchain.claimtrie.resolve",
            params=[f"lbry://{channel['name']}"],
            note="confirmed channel resolve",
        ),
        QueryCase(
            name="resolve_stream",
            method="blockchain.claimtrie.resolve",
            params=[f"lbry://{stream['name']}"],
            note="confirmed stream resolve",
        ),
        QueryCase(
            name="resolve_stream_in_channel",
            method="blockchain.claimtrie.resolve",
            params=[
                f"lbry://{signed_stream['channel_name']}/{signed_stream['stream_name']}"
            ],
            note="confirmed stream resolve through channel path",
        ),
        QueryCase(
            name="search_name_channel",
            method="blockchain.claimtrie.search",
            params={
                "name": str(channel["name"]),
                "limit": 10,
                "offset": 0,
                "no_totals": True,
            },
            note="exact channel-name search",
        ),
        QueryCase(
            name="search_name_stream",
            method="blockchain.claimtrie.search",
            params={
                "name": str(stream["name"]),
                "limit": 10,
                "offset": 0,
                "no_totals": True,
            },
            note="exact normalized-name search",
        ),
        QueryCase(
            name="search_channel_page",
            method="blockchain.claimtrie.search",
            params={
                "channel": signed_stream["channel_name"],
                "limit": 10,
                "offset": 0,
                "no_totals": True,
            },
            note="channel page search via channel URL",
        ),
    ]
    if collection_rows:
        collection = collection_rows[0]
        cases.append(
            QueryCase(
                name="resolve_collection",
                method="blockchain.claimtrie.resolve",
                params=[f"lbry://{collection['name']}"],
                note="confirmed collection resolve",
            )
        )
    return cases


def time_case(
    client: HeraldClient, case: QueryCase, runs: int, warmup_runs: int
) -> Tuple[List[float], int, Optional[str], Optional[str]]:
    last_error = None
    first_result = None
    durations: List[float] = []

    for _ in range(warmup_runs):
        try:
            client.call(case.method, case.params)
        except Exception:
            pass

    for _ in range(runs):
        started = time.perf_counter()
        try:
            result = client.call(case.method, case.params)
        except Exception as err:
            last_error = str(err)
        else:
            durations.append((time.perf_counter() - started) * 1000.0)
            if first_result is None:
                first_result = summarize_result(case.method, result)
    return durations, len(durations), first_result, last_error


def benchmark_host(
    hostport: str,
    cases: Sequence[QueryCase],
    runs: int,
    warmup_runs: int,
    timeout: float,
) -> List[RunSummary]:
    host, port = parse_hostport(hostport)
    summaries: List[RunSummary] = []
    with HeraldClient(host, port, timeout) as client:
        client.call("server.version", ["benchmark-script", "0.107.0"])
        for case in cases:
            durations, success_count, first_result, last_error = time_case(
                client, case, runs, warmup_runs
            )
            if durations:
                summaries.append(
                    RunSummary(
                        host=hostport,
                        query_name=case.name,
                        note=case.note,
                        runs=runs,
                        success_count=success_count,
                        avg_ms=statistics.mean(durations),
                        median_ms=statistics.median(durations),
                        min_ms=min(durations),
                        max_ms=max(durations),
                        first_result=first_result,
                        last_error=last_error,
                    )
                )
            else:
                summaries.append(
                    RunSummary(
                        host=hostport,
                        query_name=case.name,
                        note=case.note,
                        runs=runs,
                        success_count=0,
                        avg_ms=0.0,
                        median_ms=0.0,
                        min_ms=0.0,
                        max_ms=0.0,
                        first_result=first_result,
                        last_error=last_error,
                    )
                )
    return summaries


def print_summaries(
    primary: List[RunSummary], compare: Optional[List[RunSummary]]
) -> None:
    print(
        f"{'query':28} {'host':28} {'ok':>5} {'avg_ms':>10} {'p50_ms':>10} {'min_ms':>10} {'max_ms':>10}  result"
    )
    for summary in primary + (compare or []):
        print(
            f"{summary.query_name:28} {summary.host:28} "
            f"{summary.success_count:>5}/{summary.runs:<3} "
            f"{summary.avg_ms:>10.2f} {summary.median_ms:>10.2f} "
            f"{summary.min_ms:>10.2f} {summary.max_ms:>10.2f}  "
            f"{summary.first_result or summary.last_error or '-'}"
        )

    if not compare:
        return

    compare_map = {summary.query_name: summary for summary in compare}
    print()
    print("Delta vs primary:")
    print(f"{'query':28} {'compare-primary avg ms':>24} {'ratio':>10}")
    for base in primary:
        other = compare_map.get(base.query_name)
        if not other or base.success_count == 0 or other.success_count == 0:
            delta_text = "n/a"
            ratio_text = "n/a"
        else:
            delta = other.avg_ms - base.avg_ms
            ratio = other.avg_ms / base.avg_ms if base.avg_ms else 0.0
            delta_text = f"{delta:+.2f}"
            ratio_text = f"{ratio:.2f}x"
        print(f"{base.query_name:28} {delta_text:>24} {ratio_text:>10}")


def main() -> int:
    args = parse_args()
    try:
        cases = bootstrap_cases(args.chainquery_url, args.timeout)
    except Exception as err:
        print(f"Failed to bootstrap confirmed benchmark cases: {err}", file=sys.stderr)
        return 2

    usable_cases: List[QueryCase] = []
    skipped_cases: List[Tuple[str, str]] = []
    for case in cases:
        try:
            usable, summary = case_is_usable(args.primary, case, args.timeout)
        except Exception as err:
            skipped_cases.append((case.name, str(err)))
            continue
        if usable:
            usable_cases.append(case)
        else:
            skipped_cases.append((case.name, summary))

    if not usable_cases:
        print(f"No usable benchmark cases resolved on {args.primary}.", file=sys.stderr)
        return 2

    print("Benchmark cases:")
    for case in usable_cases:
        print(f"- {case.name}: {case.note}")
    if skipped_cases:
        print("Skipped cases:")
        for name, reason in skipped_cases:
            print(f"- {name}: {reason}")
    print()

    try:
        primary = benchmark_host(
            args.primary, usable_cases, args.runs, args.warmup_runs, args.timeout
        )
    except Exception as err:
        print(f"Primary benchmark failed for {args.primary}: {err}", file=sys.stderr)
        return 2

    compare: Optional[List[RunSummary]] = None
    if not args.no_compare:
        try:
            compare = benchmark_host(
                args.compare, usable_cases, args.runs, args.warmup_runs, args.timeout
            )
        except Exception as err:
            print(
                f"Compare benchmark failed for {args.compare}: {err}", file=sys.stderr
            )
            return 2

    print_summaries(primary, compare)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
