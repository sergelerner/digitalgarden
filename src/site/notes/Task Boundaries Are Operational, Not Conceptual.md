---
{"dg-publish":true,"permalink":"/task-boundaries-are-operational-not-conceptual/","tags":["code"]}
---

A design doc describes a pipeline as logical steps: fetch, dedup, decide, compute paths, dispatch. A common mistake is translating that diagram 1:1 into orchestrator tasks — one Airflow task per box.

Those are two different graphs. The spec's boxes mark concepts. A task boundary is an operational contract: it's the unit of retry, the unit of scheduling overhead, and the unit you can observe in the UI. Every boundary you add costs something real — an XCom serialization round-trip, scheduler latency, one more mapped instance per element — and buys something real only if you need what a boundary provides.

Draw a boundary when at least one of these is true:

I/O. The step talks to the outside world — an API, a database, object storage, a Spark cluster. External calls fail independently and should retry independently, without re-running their neighbors. This is the hard boundary: never let one task's retry re-execute another step's side effects.

Retry semantics. The step should be re-runnable in isolation — cleared and re-executed against its upstream's cached output — because that's how you'll debug it in production.

Visibility. A human will need this step's output on its own. Not its logs — its output, inspectable in the UI, answering an operational question directly.

Everything else — pure transformations, dedup, string formatting, argument assembly — belongs inside a task, implemented in a plain, unit-tested library function. You lose nothing: testability lives in the library, not in the task boundary. A pure function with good tests is more verifiable than a task, not less.

The visibility criterion is the judgment call, so apply a concrete test: what question will someone ask at 3am, and which step's output answers it? In our pipeline, the "decide what runs this tick" step is pure Python — no I/O — yet it stayed a task, because "why didn't X dispatch?" is answered by exactly that output, in the UI, per run. Meanwhile "dedup the ID list" and "format the output path" were folded into their I/O neighbors: nobody has ever debugged a path formatter independently of the call that consumed it.

Rule of thumb: boundaries where the world can fail or where a human will look; libraries everywhere else. Your DAG gets shorter, your tests get faster, and the graph in the UI starts reading like an operational runbook instead of a transcription of the design doc.