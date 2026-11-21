"""Real loopback requests with deterministic admission/release barriers."""
from __future__ import annotations

import asyncio
import json

from .service import BoundaryService, HOST


async def close_client(writer: asyncio.StreamWriter) -> None:
    writer.close()
    try:
        async with asyncio.timeout(.25):
            await writer.wait_closed()
    except (OSError, TimeoutError):
        writer.transport.abort()


async def request(port: int, job: str) -> dict:
    """One bounded local client's observed response, not a server-side delivery claim."""
    if type(port) is not int or not 1 <= port <= 65535:
        raise ValueError("invalid local port")
    writer = None
    try:
        async with asyncio.timeout(2):
            reader, writer = await asyncio.open_connection(HOST, port, limit=128)
            writer.write((json.dumps({"job": job}) + "\n").encode("utf-8"))
            await writer.drain()
            line = await reader.readline()
            return json.loads(line) if line else {"status": "eof"}
    except (OSError, TimeoutError, ValueError):
        return {"status": "client_error"}
    finally:
        if writer is not None:
            await close_client(writer)


async def scenario(grace: float) -> dict:
    gates = {name: asyncio.Event() for name in ("fast-a", "fast-b", "slow", "late")}
    service = BoundaryService(gates)
    clients = []
    late_writer = None
    try:
        port = await service.start()
        clients = [asyncio.create_task(request(port, name)) for name in ("fast-a", "fast-b", "slow")]
        async with asyncio.timeout(2):
            await asyncio.gather(*(service.admitted[name].wait() for name in ("fast-a", "fast-b", "slow")))
            late_reader, late_writer = await asyncio.open_connection(HOST, port, limit=128)
            await service.wait_connections(4)
            stopping = asyncio.create_task(service.stop(grace))
            await service.drain_ready.wait()
            gates["fast-a"].set()
            gates["fast-b"].set()
            # Existing TCP connection is not admitted work; test the cutoff there.
            late_writer.write(b'{"job":"late"}\n')
            await late_writer.drain()
            late = json.loads(await late_reader.readline())
            snapshot = await stopping
            responses = await asyncio.gather(*clients)
        observed = {status: 0 for status in ("completed", "cancelled", "rejected", "eof", "client_error")}
        for response in [*responses, late]:
            observed[response["status"]] += 1
        return dict(service=snapshot, client_responses_observed=observed)
    finally:
        await service.stop(0)  # joins the first cleanup; never changes its deadline
        if clients:
            await asyncio.gather(*clients, return_exceptions=True)
        if late_writer is not None:
            await close_client(late_writer)


async def run_comparison() -> dict:
    return dict(scenario="three-admitted-event-jobs-plus-one-late-request",
                controls="same registered jobs; two released after drain-ready, one held past grace",
                immediate=await scenario(0), finite_grace=await scenario(.05),
                interpretation="controlled completion/cancellation trade-off; not a performance benchmark")


def stable_result(result: dict) -> dict:
    """Omit variable elapsed observations from a result actually produced by a run."""
    copied = json.loads(json.dumps(result))
    for policy in ("immediate", "finite_grace"):
        copied[policy]["service"].pop("elapsed_seconds")
    return copied
