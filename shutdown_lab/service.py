"""Admission, cooperative work and connection ownership on one event loop."""
from __future__ import annotations

import asyncio
import json
import math
import re
from collections.abc import Mapping

HOST = "127.0.0.1"
MAX_REQUEST_BYTES = 128


def bounded_seconds(value: float, *, allow_zero: bool, maximum: float) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError("interval must be a finite number")
    if value > maximum or value < 0 or (value == 0 and not allow_zero) or not math.isfinite(value):
        raise ValueError("interval outside supported range")
    return float(value)


def unique_object(pairs: list) -> dict:
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate field")
        result[key] = value
    return result


class BoundaryService:
    """Registered Event jobs only; no network-provided code or job duration.

    State checks and admission bookkeeping contain no await, so the first stop
    and admission form an event-loop ordering boundary. Not thread-safe.
    """

    def __init__(self, jobs: Mapping[str, asyncio.Event], *, io_timeout: float = .25,
                 close_timeout: float = .1, max_connections: int = 16):
        if not isinstance(jobs, Mapping) or not 1 <= len(jobs) <= 32 or any(
            type(name) is not str or re.fullmatch(r"[a-z][a-z0-9-]{0,31}", name) is None
            or type(gate) is not asyncio.Event for name, gate in jobs.items()
        ):
            raise ValueError("provide 1..32 named asyncio.Event jobs")
        if type(max_connections) is not int or not 1 <= max_connections <= 64:
            raise ValueError("connection bound must be 1..64")
        self.io_timeout = bounded_seconds(io_timeout, allow_zero=False, maximum=1)
        self.close_timeout = bounded_seconds(close_timeout, allow_zero=False, maximum=1)
        self.max_connections = max_connections
        self.jobs = dict(jobs)
        self.admitted = {name: asyncio.Event() for name in jobs}
        self.draining = asyncio.Event()
        self.drain_ready = asyncio.Event()
        self._changed = asyncio.Event()
        self.state = "NEW"
        self.port: int | None = None
        self._server: asyncio.Server | None = None
        self._startup: asyncio.Task | None = None
        self._stop: asyncio.Task | None = None
        self._handlers: set[asyncio.Task] = set()
        self._writers: set[asyncio.StreamWriter] = set()
        self._jobs: dict[str, asyncio.Task] = {}
        self.counts = dict(admitted=0, completed=0, cancelled=0, rejected=0,
                           protocol_errors=0, responses_written=0, response_errors=0,
                           connections_refused=0, cleanup_runs=0, handler_errors=0, writer_close_timeouts=0)
        self.grace_seconds: float | None = None
        self.deadline_monotonic: float | None = None
        self.deadline_reached = False
        self.elapsed_seconds: float | None = None
        self.cleanup_timeouts = 0

    async def start(self) -> int:
        if self.state in {"DRAINING", "STOPPED"}:
            raise RuntimeError("service cannot be restarted")
        if self._startup is None:
            if self.state != "NEW":
                raise RuntimeError("service cannot be restarted")
            self.state = "STARTING"
            self._startup = asyncio.create_task(self._open(), name="shutdown-lab-startup")
        return await asyncio.shield(self._startup)

    async def _open(self) -> int:
        server = None
        try:
            server = await asyncio.start_server(self._connected, HOST, 0,
                                               limit=MAX_REQUEST_BYTES, start_serving=False)
            self._server = server
            self.port = server.sockets[0].getsockname()[1]
            if self.state == "STARTING":
                await server.start_serving()
                if self.state == "STARTING":
                    self.state = "RUNNING"
            if self.state != "RUNNING":
                raise RuntimeError("startup interrupted by stop")
            return self.port
        except BaseException:
            if server is not None:
                server.close()
                await self._wait_server_closed(server)
            if self.state == "STARTING":
                self.state = "STOPPED"
            raise

    def _connected(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        if self.state != "RUNNING" or len(self._writers) >= self.max_connections:
            self.counts["connections_refused"] += 1
            writer.transport.abort()
            return
        self._writers.add(writer)
        task = asyncio.create_task(self._handle(reader, writer), name="shutdown-lab-handler")
        self._handlers.add(task)
        task.add_done_callback(self._handler_done)
        self._changed.set()

    def _handler_done(self, task: asyncio.Task) -> None:
        self._handlers.discard(task)
        if not task.cancelled():
            if task.exception() is not None:
                self.counts["handler_errors"] += 1
        self._changed.set()

    async def wait_connections(self, count: int) -> None:
        """Test/demo barrier; callers supply an outer timeout."""
        if type(count) is not int or not 0 <= count <= self.max_connections:
            raise ValueError("invalid connection count")
        while len(self._writers) < count:
            self._changed.clear()
            await self._changed.wait()

    async def _send(self, writer: asyncio.StreamWriter, response: dict) -> None:
        try:
            writer.write((json.dumps(response, sort_keys=True) + "\n").encode("utf-8"))
            async with asyncio.timeout(self.io_timeout):
                await writer.drain()
            self.counts["responses_written"] += 1
        except (ConnectionError, OSError, TimeoutError):
            self.counts["response_errors"] += 1

    async def _reject(self, writer: asyncio.StreamWriter, reason: str,
                      name: str | None = None, *, protocol: bool = False) -> None:
        self.counts["rejected"] += 1
        self.counts["protocol_errors"] += int(protocol)
        await self._send(writer, dict(job=name, status="rejected", reason=reason))

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            try:
                async with asyncio.timeout(self.io_timeout):
                    line = await reader.readline()
                if not line.endswith(b"\n") or len(line) > MAX_REQUEST_BYTES:
                    raise ValueError("invalid frame")
                request = json.loads(line.decode("utf-8"), object_pairs_hook=unique_object)
                if type(request) is not dict or set(request) != {"job"} or type(request["job"]) is not str:
                    raise ValueError("invalid schema")
                name = request["job"]
                if name not in self.jobs:
                    raise ValueError("unknown job")
            except (ValueError, UnicodeError, TimeoutError):
                await self._reject(writer, "invalid_request", protocol=True)
                return
            # Admission is distinct from accepting TCP or writing any response.
            if self.state != "RUNNING":
                await self._reject(writer, "draining", name)
                return
            if name in self._jobs:
                await self._reject(writer, "duplicate", name)
                return
            task = asyncio.create_task(self._work(name), name="shutdown-lab-job-" + name)
            task.add_done_callback(self._job_done)
            self._jobs[name] = task
            self.counts["admitted"] += 1
            self.admitted[name].set()
            try:
                status = await asyncio.shield(task)  # handler cancellation does not cancel admitted work
            except asyncio.CancelledError:
                if not task.cancelled():
                    raise
                status = "cancelled"
            await self._send(writer, dict(job=name, status=status))
        except (ConnectionError, OSError):
            self.counts["response_errors"] += 1
        finally:
            await self._close_writer(writer)

    def _job_done(self, task: asyncio.Task) -> None:
        if task.cancelled():  # cancellation can precede the coroutine's first instruction
            self.counts["cancelled"] += 1

    async def _work(self, name: str) -> str:
        try:
            await self.jobs[name].wait()
        except asyncio.CancelledError:
            self.counts["cancelled"] += 1
            return "cancelled"
        self.counts["completed"] += 1
        return "completed"

    async def _close_writer(self, writer: asyncio.StreamWriter) -> None:
        writer.close()
        try:
            async with asyncio.timeout(self.close_timeout):
                await writer.wait_closed()
        except TimeoutError:
            self.counts["writer_close_timeouts"] += 1
            writer.transport.abort()
        except (ConnectionError, OSError, asyncio.CancelledError):
            writer.transport.abort()
        finally:
            self._writers.discard(writer)
            self._changed.set()

    async def _wait_server_closed(self, server: asyncio.Server) -> None:
        try:
            async with asyncio.timeout(self.close_timeout):
                await server.wait_closed()
        except TimeoutError:
            self.cleanup_timeouts += 1

    async def stop(self, grace: float = .1) -> dict:
        grace = bounded_seconds(grace, allow_zero=True, maximum=5)
        if self._stop is None:
            self.grace_seconds = grace
            self.deadline_monotonic = asyncio.get_running_loop().time() + grace
            self.state = "DRAINING"
            if self._server is not None:
                self._server.close()  # cutoff now; wait_closed only after clients close
            self.draining.set()
            self._stop = asyncio.create_task(self._cleanup(), name="shutdown-lab-cleanup")
        return await asyncio.shield(self._stop)

    async def _cleanup(self) -> dict:
        started = asyncio.get_running_loop().time()
        self.counts["cleanup_runs"] += 1
        if self._startup is not None and not self._startup.done():
            try:
                async with asyncio.timeout(self.io_timeout):
                    await asyncio.shield(self._startup)
            except TimeoutError:
                self._startup.cancel()
                await asyncio.gather(self._startup, return_exceptions=True)
            except Exception:
                pass  # startup failed, but its partial listener cleanup still ran
        if self._server is not None:
            self._server.close()
        jobs = set(self._jobs.values())
        if jobs:
            remaining = max(0, self.deadline_monotonic - asyncio.get_running_loop().time())
            if remaining == 0:
                pending = {task for task in jobs if not task.done()}
                for task in pending:
                    task.cancel()
                self.drain_ready.set()
            else:
                self.drain_ready.set()
                _, pending = await asyncio.wait(jobs, timeout=remaining)
                for task in pending:
                    task.cancel()
            self.deadline_reached = bool(pending)
            await asyncio.gather(*jobs, return_exceptions=True)  # only our Event-wait jobs
        self.drain_ready.set()
        handlers = set(self._handlers)
        if handlers:
            _, pending = await asyncio.wait(handlers, timeout=self.io_timeout + self.close_timeout)
            for task in pending:
                task.cancel()
            await asyncio.gather(*handlers, return_exceptions=True)
        for writer in tuple(self._writers):
            await self._close_writer(writer)
        if self._server is not None:
            await self._wait_server_closed(self._server)
        self.state = "STOPPED"
        self.elapsed_seconds = asyncio.get_running_loop().time() - started
        return self.snapshot()

    def snapshot(self) -> dict:
        jobs = sum(not task.done() for task in self._jobs.values())
        handlers = sum(not task.done() for task in self._handlers)
        startup = int(self._startup is not None and not self._startup.done())
        return dict(state=self.state, **self.counts, grace_seconds=self.grace_seconds,
                    deadline_reached=self.deadline_reached, elapsed_seconds=self.elapsed_seconds,
                    cleanup_timeouts=self.cleanup_timeouts, remaining_jobs=jobs,
                    remaining_handlers=handlers, remaining_startup=startup,
                    remaining_owned_tasks=jobs + handlers + startup,
                    remaining_connections=len(self._writers))
