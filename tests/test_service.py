import asyncio
import functools
import json
import unittest
from unittest.mock import patch

from shutdown_lab import BoundaryService
from shutdown_lab.experiment import close_client, request, run_comparison, stable_result


def bounded_case(function):
    @functools.wraps(function)
    async def run(self):
        async with asyncio.timeout(3):
            await function(self)
    return run


class ShutdownTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.services = []
        self.clients = []
        self.writers = []

    async def asyncTearDown(self):
        async with asyncio.timeout(3):
            for service in self.services:
                await service.stop(0)
            for writer in self.writers:
                await close_client(writer)
            if self.clients:
                await asyncio.gather(*self.clients, return_exceptions=True)
            for service in self.services:
                self.clean(service.snapshot())

    def clean(self, snapshot):
        self.assertEqual(snapshot["state"], "STOPPED")
        for name in ("remaining_jobs", "remaining_handlers", "remaining_connections",
                     "remaining_startup", "remaining_owned_tasks", "cleanup_timeouts",
                     "handler_errors", "writer_close_timeouts"):
            self.assertEqual(snapshot[name], 0, name)

    async def service(self, names=("one", "two"), **kwargs):
        gates = {name: asyncio.Event() for name in names}
        service = BoundaryService(gates, **kwargs)
        self.services.append(service)
        await service.start()
        return service, gates

    def client(self, service, name):
        task = asyncio.create_task(request(service.port, name))
        self.clients.append(task)
        return task

    async def connection(self, service):
        reader, writer = await asyncio.open_connection("127.0.0.1", service.port, limit=128)
        self.writers.append(writer)
        return reader, writer

    @bounded_case
    async def test_empty_and_no_restart(self):
        service, _ = await self.service()
        result = await service.stop(0)
        self.clean(result)
        self.assertEqual(result["admitted"], 0)
        self.assertEqual(result["cleanup_runs"], 1)
        with self.assertRaises(RuntimeError):
            await service.start()
        self.assertEqual((await request(service.port, "one"))["status"], "client_error")

    @bounded_case
    async def test_normal_completion_and_separate_client_observation(self):
        service, gates = await self.service()
        task = self.client(service, "one")
        await service.admitted["one"].wait()
        self.assertEqual(service.snapshot()["completed"], 0)
        gates["one"].set()
        self.assertEqual((await task)["status"], "completed")
        result = await service.stop(.1)
        self.assertEqual((result["admitted"], result["completed"], result["cancelled"]), (1, 1, 0))
        self.assertEqual(result["responses_written"], 1)
        self.clean(result)

    @bounded_case
    async def test_existing_connection_is_rejected_after_admission_cutoff(self):
        service, gates = await self.service()
        client = self.client(service, "one")
        await service.admitted["one"].wait()
        reader, writer = await self.connection(service)
        await service.wait_connections(2)
        stopping = asyncio.create_task(service.stop(.1))
        await service.draining.wait()
        writer.write(b'{"job":"two"}\n'); await writer.drain()
        self.assertEqual(json.loads(await reader.readline())["reason"], "draining")
        gates["one"].set()
        self.assertEqual((await client)["status"], "completed")
        result = await stopping
        self.assertEqual((result["admitted"], result["rejected"], result["protocol_errors"]), (1, 1, 0))
        self.assertFalse(service.admitted["two"].is_set())
        self.clean(result)

    @bounded_case
    async def test_duplicate_request_is_not_second_work(self):
        service, gates = await self.service()
        original = self.client(service, "one")
        await service.admitted["one"].wait()
        duplicate = await request(service.port, "one")
        self.assertEqual(duplicate["reason"], "duplicate")
        gates["one"].set(); await original
        result = await service.stop(0)
        self.assertEqual((result["admitted"], result["completed"], result["rejected"]), (1, 1, 1))

    @bounded_case
    async def test_invalid_frames_schema_and_size(self):
        service, _ = await self.service()
        frames = [b'no-json\n', b'[]\n', b'{"job":2}\n', b'{"job":"unknown"}\n',
                  b'{"job":"one","extra":1}\n', b'{"job":"one","job":"two"}\n',
                  b'\xff\n', b'x' * 129 + b'\n']
        for frame in frames:
            reader, writer = await self.connection(service)
            writer.write(frame); await writer.drain()
            self.assertEqual(json.loads(await reader.readline())["status"], "rejected")
            await close_client(writer)
        result = await service.stop(0)
        self.assertEqual(result["admitted"], 0)
        self.assertEqual(result["protocol_errors"], len(frames))
        self.assertEqual(result["rejected"], len(frames))

    @bounded_case
    async def test_partial_eof_and_idle_request_are_bounded(self):
        service, _ = await self.service(io_timeout=.03)
        reader, writer = await self.connection(service)
        writer.write(b'{"job":'); await writer.drain(); writer.write_eof()
        self.assertEqual(json.loads(await reader.readline())["status"], "rejected")
        reader, _ = await self.connection(service)
        self.assertEqual(json.loads(await reader.readline())["status"], "rejected")
        result = await service.stop(0)
        self.assertEqual((result["admitted"], result["protocol_errors"]), (0, 2))

    @bounded_case
    async def test_repeated_and_concurrent_stop_keep_first_deadline(self):
        service, _ = await self.service()
        client = self.client(service, "one")
        await service.admitted["one"].wait()
        first = asyncio.create_task(service.stop(.04))
        await service.draining.wait()
        deadline = service.deadline_monotonic
        results = await asyncio.gather(first, service.stop(5), service.stop(0))
        self.assertEqual(service.deadline_monotonic, deadline)
        self.assertTrue(all(result == results[0] for result in results))
        self.assertEqual(results[0]["grace_seconds"], .04)
        self.assertEqual(results[0]["cleanup_runs"], 1)
        self.assertEqual((await client)["status"], "cancelled")
        self.assertEqual(await service.stop(1), results[0])

    @bounded_case
    async def test_cancelled_stop_caller_does_not_cancel_cleanup(self):
        service, _ = await self.service()
        client = self.client(service, "one")
        await service.admitted["one"].wait()
        first = asyncio.create_task(service.stop(.04))
        await service.draining.wait(); deadline = service.deadline_monotonic
        first.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await first
        result = await service.stop(5)
        self.assertEqual(service.deadline_monotonic, deadline)
        self.assertEqual(result["grace_seconds"], .04)
        self.assertEqual((result["cleanup_runs"], result["cancelled"]), (1, 1))
        self.assertEqual((await client)["status"], "cancelled")
        self.clean(result)

    @bounded_case
    async def test_disconnected_client_does_not_cancel_admitted_work(self):
        service, gates = await self.service()
        client = self.client(service, "one")
        await service.admitted["one"].wait()
        client.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await client
        gates["one"].set()
        result = await service.stop(.1)
        self.assertEqual((result["admitted"], result["completed"], result["cancelled"]), (1, 1, 0))
        # Client consumed no response. Server drain success cannot prove otherwise.
        self.clean(result)

    @bounded_case
    async def test_start_stop_overlap_closes_late_listener(self):
        service = BoundaryService({"one": asyncio.Event()})
        self.services.append(service)
        entered, release = asyncio.Event(), asyncio.Event()
        real_start = asyncio.start_server

        async def delayed(*args, **kwargs):
            entered.set(); await release.wait()
            return await real_start(*args, **kwargs)

        with patch("shutdown_lab.service.asyncio.start_server", side_effect=delayed):
            starting = asyncio.create_task(service.start())
            await entered.wait()
            stopping = asyncio.create_task(service.stop(0))
            await service.draining.wait(); release.set()
            with self.assertRaises(RuntimeError):
                await starting
            result = await stopping
        self.clean(result)
        self.assertFalse(service._server.is_serving())
        self.assertEqual((await request(service.port, "one"))["status"], "client_error")

    @bounded_case
    async def test_stop_before_start_and_start_failure(self):
        service = BoundaryService({"one": asyncio.Event()})
        self.services.append(service)
        self.clean(await service.stop(0))
        with self.assertRaises(RuntimeError):
            await service.start()
        other = BoundaryService({"one": asyncio.Event()})
        self.services.append(other)
        with patch("shutdown_lab.service.asyncio.start_server", side_effect=OSError("synthetic bind failure")):
            with self.assertRaises(OSError):
                await other.start()
        self.clean(await other.stop(0))

    @bounded_case
    async def test_connection_capacity_is_separate_from_request_rejects(self):
        service, _ = await self.service(max_connections=1, io_timeout=.03)
        reader, _ = await self.connection(service)
        await service.wait_connections(1)
        refused = await request(service.port, "one")
        self.assertIn(refused["status"], {"client_error", "eof"})
        await reader.readline()
        result = await service.stop(0)
        self.assertEqual(result["connections_refused"], 1)
        self.assertEqual(result["admitted"], 0)

    @bounded_case
    async def test_controlled_comparison(self):
        result = stable_result(await run_comparison())
        for policy, completed, cancelled in (("immediate", 0, 3), ("finite_grace", 2, 1)):
            service = result[policy]["service"]
            self.assertEqual((service["admitted"], service["completed"], service["cancelled"]), (3, completed, cancelled))
            self.assertEqual((service["rejected"], service["deadline_reached"]), (1, True))
            observed = result[policy]["client_responses_observed"]
            self.assertEqual((observed["completed"], observed["cancelled"], observed["rejected"]), (completed, cancelled, 1))
            self.assertEqual((observed["eof"], observed["client_error"]), (0, 0))
            self.clean(service)

    @bounded_case
    async def test_invalid_configuration_and_stop_preserves_running(self):
        for value in (-1, float("nan"), float("inf"), True, 10**1000):
            with self.assertRaises(ValueError):
                BoundaryService({"one": asyncio.Event()}, io_timeout=value)
        for jobs in ({}, {"bad name": asyncio.Event()}, {"one": object()}, None):
            with self.assertRaises(ValueError):
                BoundaryService(jobs)
        service, _ = await self.service()
        for grace in (-1, float("nan"), float("inf"), True, 6):
            with self.assertRaises(ValueError):
                await service.stop(grace)
            self.assertEqual(service.state, "RUNNING")
        with self.assertRaises(ValueError):
            await service.wait_connections(17)


if __name__ == "__main__":
    unittest.main()
