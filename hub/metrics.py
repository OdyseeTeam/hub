import time
import logging
import asyncio
import asyncio.tasks
import os
from contextlib import suppress
from aiohttp import web
from hub import PROMETHEUS_NAMESPACE
from prometheus_client import generate_latest as prom_generate_latest
from prometheus_client import Counter, Histogram, Gauge


PROBES_IN_FLIGHT = Counter("probes_in_flight", "Number of loop probes in flight", namespace='asyncio')
PROBES_FINISHED = Counter("probes_finished", "Number of finished loop probes", namespace='asyncio')
PROBE_TIMES = Histogram("probe_times", "Loop probe times", namespace='asyncio')
TASK_COUNT = Gauge("running_tasks", "Number of running tasks", namespace='asyncio')
SMAPS_ROLLUP_BYTES = Gauge(
    "process_smaps_rollup_bytes",
    "Process memory fields from /proc/self/smaps_rollup",
    namespace=PROMETHEUS_NAMESPACE,
    labelnames=("field",),
)
CGROUP_MEMORY_BYTES = Gauge(
    "cgroup_memory_bytes",
    "Current process cgroup memory fields",
    namespace=PROMETHEUS_NAMESPACE,
    labelnames=("field",),
)
OPEN_FDS_BY_TYPE = Gauge(
    "process_open_fds_by_type",
    "Open file descriptors grouped by target type",
    namespace=PROMETHEUS_NAMESPACE,
    labelnames=("type",),
)
SMAPS_ROLLUP_FIELDS = {
    "rss",
    "pss",
    "pss_anon",
    "pss_file",
    "anonymous",
    "private_dirty",
    "swap",
    "swap_pss",
}
FD_TYPES = ("socket", "sst", "other", "total")
CGROUP_MEMORY_STAT_FIELDS = {
    "anon",
    "file",
    "kernel",
    "pagetables",
    "sock",
    "slab",
    "swapcached",
    "inactive_anon",
    "active_anon",
    "inactive_file",
    "active_file",
}


def parse_smaps_rollup(text):
    metrics = {}
    for line in text.splitlines():
        if ":" not in line:
            continue
        raw_field, raw_value = line.split(":", 1)
        field = raw_field.strip()
        if field == "SwapPss":
            field = "Swap_Pss"
        field = field.lower().replace("_", " ")
        field = field.replace(" ", "_")
        if field not in SMAPS_ROLLUP_FIELDS:
            continue
        parts = raw_value.split()
        if not parts:
            continue
        try:
            value = int(parts[0])
        except ValueError:
            continue
        if len(parts) > 1 and parts[1].lower() == "kb":
            value *= 1024
        metrics[field] = value
    return metrics


def normalize_fd_target(target):
    suffix = " (deleted)"
    while target.endswith(suffix):
        target = target[: -len(suffix)]
    return target


def classify_fd_target(target):
    target = normalize_fd_target(target)
    if target.startswith("socket:"):
        return "socket"
    if target.endswith(".sst"):
        return "sst"
    return "other"


def read_process_smaps_rollup(path="/proc/self/smaps_rollup"):
    with open(path, "r") as smaps:
        return parse_smaps_rollup(smaps.read())


def parse_cgroup_memory_stat(text):
    metrics = {}
    for line in text.splitlines():
        parts = line.split()
        if len(parts) != 2:
            continue
        field, raw_value = parts
        if field not in CGROUP_MEMORY_STAT_FIELDS:
            continue
        try:
            metrics[field] = int(raw_value)
        except ValueError:
            continue
    return metrics


def read_int_file(path):
    with open(path, "r") as value_file:
        return int(value_file.read().strip())


def resolve_cgroup_memory_path(proc_cgroup_path="/proc/self/cgroup", cgroup_root="/sys/fs/cgroup"):
    try:
        with open(proc_cgroup_path, "r") as cgroup_file:
            lines = cgroup_file.readlines()
    except OSError:
        lines = []
    for line in lines:
        parts = line.strip().split(":", 2)
        if len(parts) != 3:
            continue
        _, controllers, relative_path = parts
        if controllers and "memory" not in controllers.split(","):
            continue
        relative_path = relative_path.lstrip("/")
        candidate = os.path.join(cgroup_root, relative_path)
        if os.path.exists(os.path.join(candidate, "memory.stat")):
            return candidate
    return cgroup_root


def read_cgroup_memory_metrics(path=None):
    path = path or resolve_cgroup_memory_path()
    metrics = {}
    stat_path = os.path.join(path, "memory.stat")
    try:
        with open(stat_path, "r") as stat_file:
            metrics.update(parse_cgroup_memory_stat(stat_file.read()))
    except OSError:
        return metrics
    for field, filename in (("current", "memory.current"), ("swap_current", "memory.swap.current")):
        try:
            metrics[field] = read_int_file(os.path.join(path, filename))
        except (OSError, ValueError):
            pass
    return metrics


def read_process_fd_counts(path="/proc/self/fd"):
    counts = {fd_type: 0 for fd_type in FD_TYPES}
    try:
        entries = os.listdir(path)
    except OSError:
        return counts
    counts["total"] = len(entries)
    for entry in entries:
        try:
            target = os.readlink(os.path.join(path, entry))
        except OSError:
            counts["other"] += 1
            continue
        counts[classify_fd_target(target)] += 1
    return counts


def get_loop_metrics(delay=1):
    loop = asyncio.get_event_loop()

    def callback(started):
        PROBE_TIMES.observe(time.perf_counter() - started - delay)
        PROBES_FINISHED.inc()

    async def monitor_loop_responsiveness():
        while True:
            now = time.perf_counter()
            loop.call_later(delay, callback, now)
            PROBES_IN_FLIGHT.inc()
            TASK_COUNT.set(len(asyncio.tasks._all_tasks))
            await asyncio.sleep(delay)

    return loop.create_task(monitor_loop_responsiveness())


def get_process_metrics(logger=None, delay=10):
    logger = logger or logging.getLogger(__name__)

    async def monitor_process():
        while True:
            try:
                for field, value in read_process_smaps_rollup().items():
                    SMAPS_ROLLUP_BYTES.labels(field=field).set(value)
            except OSError:
                logger.debug("failed to read process smaps_rollup", exc_info=True)
            try:
                for field, value in read_cgroup_memory_metrics().items():
                    CGROUP_MEMORY_BYTES.labels(field=field).set(value)
            except OSError:
                logger.debug("failed to read cgroup memory metrics", exc_info=True)
            try:
                for fd_type, value in read_process_fd_counts().items():
                    OPEN_FDS_BY_TYPE.labels(type=fd_type).set(value)
            except OSError:
                logger.debug("failed to read process file descriptors", exc_info=True)
            await asyncio.sleep(delay)

    return asyncio.get_event_loop().create_task(monitor_process())


class PrometheusServer:
    def __init__(self, logger=None):
        self.runner = None
        self.logger = logger or logging.getLogger(__name__)
        self._monitor_loop_task = None
        self._monitor_process_task = None

    async def start(self, interface: str, port: int):
        self.logger.info("start prometheus metrics")
        prom_app = web.Application()
        prom_app.router.add_get('/metrics', self.handle_metrics_get_request)
        self.runner = web.AppRunner(prom_app)
        await self.runner.setup()

        metrics_site = web.TCPSite(self.runner, interface, port, shutdown_timeout=.5)
        await metrics_site.start()
        self.logger.info(
            'prometheus metrics server listening on %s:%i', *metrics_site._server.sockets[0].getsockname()[:2]
        )
        self._monitor_loop_task = get_loop_metrics()
        self._monitor_process_task = get_process_metrics(self.logger)

    async def handle_metrics_get_request(self, request: web.Request):
        try:
            return web.Response(
                text=prom_generate_latest().decode(),
                content_type='text/plain; version=0.0.4'
            )
        except Exception:
            self.logger.exception('could not generate prometheus data')
            raise

    async def stop(self):
        if self._monitor_loop_task and not self._monitor_loop_task.done():
            self._monitor_loop_task.cancel()
        if self._monitor_process_task and not self._monitor_process_task.done():
            self._monitor_process_task.cancel()
        for task in (self._monitor_loop_task, self._monitor_process_task):
            if task:
                with suppress(asyncio.CancelledError):
                    await task
        self._monitor_loop_task = None
        self._monitor_process_task = None
        if self.runner:
            await self.runner.cleanup()
            self.runner = None
