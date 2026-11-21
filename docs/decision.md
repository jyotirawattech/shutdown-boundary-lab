# Decision: separate admission, work and response observations

Status: original reproducible experiment. [CI results](https://github.com/jyotirawattech/shutdown-boundary-lab/actions/workflows/ci.yml)
apply to their recorded revisions; local preparation observations remain separately labelled.

The useful question is whether a bounded drain window permits already-admitted
work to finish after new admission stops. A listener alone cannot express that
boundary: existing connections can still deliver requests. We therefore place
an event-loop state check next to the admission increment and job-task creation,
with no intervening await. The first stop changes state before yielding.

## Owned resources

- The service owns one shielded startup task and the listener it creates.
- The synchronous accept callback registers each handler and writer immediately.
  At the connection limit or outside RUNNING it aborts the new transport rather
  than allocating another handler. Those events are connection refusals.
- The key-to-job task map owns admitted jobs, at most one per registered name.
  A handler awaits its job through `shield`; a disconnected/cancelled handler
  does not silently cancel work the service already admitted.
- Each handler closes its writer in `finally`, using a bounded wait then abort.
  Shutdown joins handlers and closes remaining tracked writers before it waits
  for server closure. Unexpected handler and timeout observations have counters.
- The single cleanup task is shielded from stop-caller cancellation. The snapshot
  counts startup/job/handler tasks, excluding the cleanup task producing its own
  final snapshot; after callers join stop, that cleanup is done too.

## First deadline and policy readiness

Grace is finite, 0–5 seconds; I/O and close intervals are finite, positive and at
most one second. The first valid stop fixes `deadline_monotonic`. Later valid
calls join the same task, including calls after it finishes, and cannot change
that deadline. Cancelling a caller cancels its wait, not the shared cleanup.

The listener closes immediately. Startup already in progress is settled with an
I/O bound; if stopping won, it closes a late listener and cannot publish RUNNING.
Elapsed startup settlement consumes the original grace rather than resetting it.
The cleanup then waits only for the remaining grace. With zero remaining grace it
selects/cancels unfinished jobs directly, without an `asyncio.wait(..., timeout=0)`
yield that would introduce an accidental scheduling grace.

`draining` marks the admission cutoff. The experiment uses the stronger
`drain_ready` barrier: immediate policy has issued cancellation, or finite policy
is about to wait for cooperative work. Both runs release the same two gates after
this defined policy step. One held gate forces the finite deadline case. This is
an explicit controlled ordering, not a claim about natural request arrival times.

After the job decision, response writes and handler/writer closure have their own
bounds. Therefore 0.05 seconds is a configured job-drain allowance, not a promise
that process shutdown or every client response takes at most 0.05 seconds.
`deadline_reached` means unfinished jobs remained at the cancellation decision;
it is not a hard-real-time measurement.

## Why a small line protocol

A 128-byte UTF-8 JSON line selects a known synthetic Event job. The schema rejects
unknown/duplicate fields; there is no network-provided callable, file, endpoint or
sleep duration. One request per connection avoids a second queue/protocol layer.
No credentials, private data, external traffic or production authentication are
needed for this fixed IPv4 loopback-only lab. Other local processes can reach a
loopback socket; this is not a sandbox or a production security boundary.

## Baseline and observed quantities

Immediate cancellation is the named baseline. Both policies use the same admitted
jobs, registered gates and release barrier. The stable output records counts and
configuration from actual executions, removing only varying elapsed observations.
The observed local clients read every response in the controlled worked case.
Completion and a server drain count remain separate from those client reads.

A finite window completes two additional jobs here and spends time awaiting the
held job. We do not derive a speedup or memory percentage from those counts, and
we do not claim a generally superior grace. This is not a paired wall-clock
benchmark: workloads, scheduler variability and repeated-run distributions have
not been characterized for performance conclusions.

## Python and cancellation boundary

The source targets Python 3.11+ (`asyncio.timeout`, standard streams/tasks). The
[Python server lifecycle documentation](https://docs.python.org/3.13/library/asyncio-eventloop.html#asyncio.Server.wait_closed)
describes closure of listeners versus active connections; Python 3.12+ waits for
active connections too. We close owned clients first. The implementation does not
use the server client-close helpers introduced in Python 3.13.

Configured CI covers 3.11/3.13, but a saved workflow is not an executed compatibility
result. Local run evidence names the actual interpreter. All jobs are ordinary
Event waits and respond to cancellation; arbitrary blocking/native code or code
that suppresses cancellation requires process isolation and a different design.
No hard deadline, external deployment, adoption or production reliability is
established by this package.
