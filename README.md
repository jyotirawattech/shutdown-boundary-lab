# Shutdown Boundary Lab

[![CI](https://github.com/jyotirawattech/shutdown-boundary-lab/actions/workflows/ci.yml/badge.svg)](https://github.com/jyotirawattech/shutdown-boundary-lab/actions/workflows/ci.yml)

What happens to admitted work when a service stops accepting requests?

This original, AI-assisted Python experiment uses only the standard library and runs a real **127.0.0.1** TCP service on an
ephemeral port. Cooperative synthetic jobs let you inspect the difference between
immediate cancellation and a finite drain window. It is a local engineering lab;
it has no public endpoint, account integration or deployment step.

## Run it

Requires Python 3.11+ and Make; no package download is needed.

```sh
make verify
python3 -m shutdown_lab
python3 -m shutdown_lab --observations
```

`make verify` runs the assertions and freshly checks the stable demo against
[results/worked.json](results/worked.json). The default CLI omits variable elapsed
observations; `--observations` includes the actual monotonic elapsed values from
that run. These values are observations, not paired performance measurements.
The 13 November 2026 local preparation check used Python 3.14.3. CI targets
Python 3.11 and 3.13; [view its actual revision-specific results](https://github.com/jyotirawattech/shutdown-boundary-lab/actions/workflows/ci.yml).
A configured version target alone is not a compatibility result.

## A finished, controlled comparison

Both policies admit the same three jobs before shutdown: `fast-a`, `fast-b` and
`slow`. Two Event gates are released at the service's drain-ready barrier. The
third stays closed beyond the configured grace. An already-owned fourth TCP
connection then requests `late` to test the admission cutoff.

The exact barrier matters: immediate cancellation has been issued when its
`drain_ready` event is set; the finite-grace policy has closed the listener and is
about to await its jobs. Both experiments use this same policy-ready release rule.
This controls a reproducible boundary case; it does not model arbitrary arrival
or scheduler timing. Jobs are otherwise identical and all three are admitted in
both runs. No per-policy job duration is tuned to create a winner.

The worked run records:

- Immediate baseline, zero grace: 0 completed, 3 cancelled, 1 late request rejected.
- Finite grace, 0.05 seconds configured: 2 completed, 1 cancelled, 1 late request rejected.
- Both: one cleanup, no remaining owned startup/job/handler tasks or connections.

The finite window permits **two additional completions out of the three admitted
jobs** in this scenario because their gates open while they may still run. It also
waits for the held job before cancelling it. This is a completion-versus-waiting
trade-off, not a speedup, general reliability gain or production result. An
all-fast, all-held or differently ordered workload can change the result.

## The admission boundary

The lifecycle is `NEW → STARTING → RUNNING → DRAINING → STOPPED`; startup failure
or a stop during startup can reach STOPPED without admitting work. The first valid
`stop(grace)` fixes its monotonic deadline, changes admission state and closes the
listener without yielding. An already accepted connection still needs a valid
request to be admitted. A post-cutoff request on an owned connection is rejected;
a new TCP connection may instead be refused and has no admitted request to count.

One connection carries one newline-terminated UTF-8 JSON object, at most 128 bytes:

```json
{"job":"fast-a"}
```

Only a registered synthetic job name is accepted, once per service lifetime.
Unknown/extra/duplicate fields, unknown names and malformed/oversized/partial/idle
requests are rejected. Network input cannot select code, URLs, paths or job delays.
Responses contain `job`, `status` (`completed`, `cancelled`, `rejected`) and a bounded
reason for rejection. The service limits owned connections to 16 by default.

## Read counters accurately

`admitted`, `completed` and `cancelled` describe work. After complete shutdown,
`admitted == completed + cancelled` for these cooperative jobs. `rejected` counts
unadmitted request attempts, including invalid/partial/idle requests;
`protocol_errors` is an overlapping subset. `connections_refused` counts excess or
post-cutoff connection callbacks separately.

`responses_written` means the server's bounded stream drain succeeded. It does
**not** prove that a client received a response. The experiment's separate
`client_responses_observed` counts responses its clients actually read (their local
acknowledgement observation), including cancellation and rejection. Even a read
response does not prove an external application persisted an outcome.

Snapshots also expose remaining owned resources, cleanup/error/close-timeout
counts, the first configured grace and whether unfinished jobs needed cancellation
at its deadline. The grace controls job draining, not the total shutdown duration:
startup settlement, bounded response writes and connection closure add work.

## Reuse and limits

`BoundaryService` exposes `start()`, `stop(grace)`, `snapshot()`, per-job `admitted`
events, `draining`, `drain_ready` and `wait_connections(n)` for controlled tests.
Provide 1–32 named, ordinary `asyncio.Event` objects on one event loop. Get a port
from `await start()` and always join `await stop(...)` in your cleanup path.
Repeated/concurrent stops join one shielded cleanup; cancelling a stop caller does
not cancel it or extend the first deadline. Restarting a stopped service rejects.

The core owns its startup, admitted job tasks, handlers and writers. It closes
writers before awaiting server closure, including Python 3.12+ server semantics.
See the [decision and ownership record](docs/decision.md) for the cancellation and
compatibility boundaries. The test suite uses Event barriers and bounded outer
waits, rather than sleeps to guess admission ordering.

Cancellation here is cooperative: jobs only await their registered Event. Python
cannot forcibly terminate arbitrary blocking or cancellation-suppressing code.
This is not a hard OS deadline, adversarial service, TLS/authentication layer,
exactly-once system, throughput benchmark or proof for every possible schedule.
All inputs are local and synthetic; no employer/customer material is included.
