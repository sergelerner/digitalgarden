---
{"dg-publish":true,"permalink":"/code/one-stop-two-hops-10-000-rows/","dg-note-properties":{}}
---

*From partitions to parquet files — a tour of the decisions that actually determine your job's speed and your data's shape.*

## The job we'll follow

Every example below comes from one realistic pipeline: a **two-hop enrichment**.

A regional transit partner sends us a catalog of bus stops — the physical signposts and shelters, one row each. We want to attach, to every stop, the scheduled trips that call there.

A _trip_ is one single run of one vehicle: the 08:15 departure of route 47, specifically, as distinct from the 08:25 and the 08:35. A _route_ is the line on the map ("route 47, downtown to the airport"); a _trip_ is one bus actually driving it at one particular time. A route running every ten minutes from 5 a.m. to midnight produces well over a hundred trips a day, and the schedule feed lists every one of them.

So for the stop on Elm Street, the question we're answering is: _which individual bus departures call here?_ The partner's file doesn't say — it only knows where the stop is. We have to look it up in two hops: first find the routes that serve Elm Street, then find every scheduled trip on those routes. That two-hop lookup, and the enormous fan-out it produces, is the pipeline this post is about.

Three inputs, all landing in object storage as roughly 1 GB files. Their **format** is whatever the upstream systems happen to produce — parquet from one, CSV from another, sometimes gzip-compressed, sometimes not. Nobody on our team chooses it. As §1 and §2 show, that choice has more influence on this job than any config we set.

**`partner_stops`** — the partner's data, the thing being enriched
```
stop_ref        string     partner's own identifier
zone_code       int        4-digit fare zone (unpadded)
stop_code       int        5-digit stop number within the zone (unpadded)
shelter_type    string     dimension we carry through
surveyed_at     timestamp
```

**`route_stops`** — hop 1: which routes serve which stop
```
route_id        string
zone            int
stop            int
direction       string
```

**`trip_schedule`** — hop 2: which trips run on each route (the fan-out)
```
route_id        string
trip_id         string
service_day     string
```


<iframe src="/img/user/Code/spark-enrichment-map.html" width="100%" height="800px" title="spark-enrichment-map.html" style="border:1px solid #ccc;" loading="lazy"></iframe>

The output is parquet: every original stop column, plus one row per `trip_id` found. The fan-out is severe and entirely realistic — a stop served by 5 routes, each running ~2,000 trips across the schedule period, expands into **10,000 output rows**. The output dwarfs the input.

The cluster: executors with **4 cores and 21 GB** each, scaling up to **200 executors** — so at full stretch, **800 tasks running at once**.

> **Two planners, don't confuse them.** A scheduler (Airflow, or whatever you use) decides *which batch to process* and launches the Spark job. It never touches the data. Inside the job, the **Spark driver** does the planning we care about here. And when we say the driver "reads metadata from storage," that's literal: it does a **LIST**, collects **object sizes**, and takes the **schema** from a config or declaration. It does not read a single row to build the plan.

---

## §1 · Partitions: the unit of everything

Spark never processes "a DataFrame" as one thing. It processes partitions, in parallel.

▎ **Partition** — one chunk of a dataset, small enough for a single CPU core to process on its own. A DataFrame is really a recipe for computing data plus a decision about how many chunks it's split into. If a DataFrame has 200 partitions, up to 200 things can happen at once.

▎ **Task / Executor / Core** — a task is the work of processing one partition through one stage of the recipe. An executor is a JVM process on a cluster machine that runs tasks; each executor has some number of cores, and each core runs one task at a time. Our executors have 4 cores and 21 GB each; up to 200 executors → 800 tasks can run simultaneously.

When the job reads `trip_schedule`, Spark decides the *initial* partition count from the files on disk. There's a default rule, and a property that decides whether the rule even applies:

▎ **Split size** — Spark's default rule is to cut input into partitions of `spark.sql.files.maxPartitionBytes` (default **128 MB**). Small files get packed together toward 128 MB; big files get sliced. A 1 GB file → ~8 partitions. Parallelism scales with data size, automatically.

▎ **Splittability** — but that rule only applies if Spark can *start reading at an arbitrary byte offset*. That property, not file size, is what actually decides your read parallelism:

| Format | Splittable? | 1 GB file yields |
|---|---|---|
| Parquet | yes — at row-group boundaries (compression is per-block, so snappy/zstd inside parquet is fine) | ~8 partitions |
| Uncompressed CSV / JSON | yes — any byte offset | ~8 partitions |
| **gzip-compressed CSV** | **no** — gzip must be decompressed from byte 0 | **1 partition** |
| bzip2 | yes — has block markers | ~8 partitions |

▎ **What this means for our job** — take `trip_schedule`, three files of ~1 GB each, and read it two ways:

- **As parquet or uncompressed CSV:** ~24 partitions, and that number grows as the feed grows. Healthy.
- **As gzip CSV:** the rule collapses to **one file = one partition, regardless of size**. The stage runs **3 tasks wide** on a cluster with 800 slots, each task pinning a single core to ~1 GB of serial decompression. The other 797 slots sit idle.

Same bytes, same query, same cluster — an order-of-magnitude difference in read parallelism, decided entirely by a packaging choice made upstream of us. This is one reason the code repartitions immediately after reading (see §6); it's also the first thing worth checking when a stage inexplicably runs a handful of tasks wide.

The partition count at any moment determines your parallelism, your memory pressure per task, and — the punchline of this post — how many output files you write.

---

## §2 · Pruning: the data you never read

§1 asked how the input gets *divided*. The prior question is how much of it gets *read* at all.

▎ **Pruning** — the general term for a query engine discarding data it can prove the query doesn't need. It isn't a heuristic or an approximation: if a column, file, or row cannot possibly affect the result, reading it is pure waste, so the optimizer eliminates it. Catalyst does this on its own. Your only job is to write the query so you don't block it.

Three flavors, and our pipeline touches all three.

▎ **Column pruning** (projection pushdown) — narrows *which columns* are read. Our `route_stops` feed carries a `direction` column we never reference, and `trip_schedule` carries `service_day`. Because the job selects only the columns it uses, Catalyst pushes that set down into the file scan and the rest are never materialized.

How much this saves **depends on the format**. Parquet stores column-by-column, so unread columns are bytes never fetched from storage. CSV stores row-by-row, so every byte still crosses the wire and gets parsed — you save materialization, not I/O.

▎ **Partition pruning** — narrows *which files* are read, using directory structure. Feeds laid out as `service_date=…/version=…` let the reader resolve to just the newest version folder; every older snapshot is skipped without a single file being opened. This one is **format-independent** — it's just not listing directories.

▎ **Predicate pushdown** — narrows *which rows* are read, by moving filters as close to the source as possible. A `filter(col("zone_key").isNotNull)` is the candidate. In parquet, the footer carries per-row-group min/max statistics and null counts, so the reader can skip entire row groups that provably can't contain a match. In CSV there's nothing to consult, so the filter still runs — just *after* parsing rather than *instead of* it.****

Notice the pattern: **two of the three only pay off if the format cooperates.** The same upstream packaging decision that capped your parallelism in §1 also decides whether the engine can skip work it has proven unnecessary. Format isn't a storage detail; it's a query-planning capability.

▎ **Doing it by hand: broadcast pre-filtering** — sometimes you know a pruning opportunity the planner can't see. Our partner catalog covers a handful of fare zones; the reference feed covers the whole country. Before joining, we can compute the zones the partner data *actually contains* and use them to discard reference rows that provably can't match:

```scala
val presentZones = broadcast(
  stopsPrepared.select(col("zone_key")).distinct()
)

val routeStopsPruned = routeStopsPrepared
  .join(presentZones, Seq("zone_key"), "inner")
```

This is exactly the shape of what Spark calls **dynamic partition pruning** — using values from the small side of a join to eliminate data on the big side. Spark can sometimes do this automatically, but only when the big side is *directory-partitioned* on the join key. Ours isn't, so the code does it explicitly.

Semantically this join is a no-op: an inner join against the keys we're about to match on anyway removes nothing the outer join wouldn't have dropped. In practice it can be the difference between shuffling a national feed and shuffling a few cities' worth of it.


****
---

## §3 · Narrow vs. wide operations

Operations divide into two families, and the difference is whether data has to move between machines.

▎ **Narrow operation** — each output partition is computed from one input partition. `filter` (keep some rows), `select` (keep/derive some columns), and our `explode` (turn one row holding an array of 3 trips into 3 rows) are all narrow: to produce piece #7 of the result, a task only needs piece #7 of the input, already sitting in its memory. No network, and the partition count simply carries over.

▎ **Wide operation** — an output partition needs rows from many (potentially all) input partitions. Our join on `(zone, stop)` is the canonical case: every row for zone `0042`, stop `01337` — whether it started in partition #3 of the partner stops or partition #191 of the route-stops feed — must end up on the same machine to be matched. `join`, `groupBy`, `distinct`, and `repartition` are wide.

A useful instinct: narrow ops are free-flowing; wide ops are checkpoints where the whole cluster synchronizes. Which brings us to what a wide op actually does:

---

## §4 · The shuffle

Every wide operation is really three sub-phases — a lineage Spark inherited directly from Hadoop MapReduce. (The term *shuffle* is not a Spark invention; it's the name MapReduce already gave the middle phase.)

▎ **Map → Shuffle → Reduce**
- **Map:** each task processes its own input partition locally (no network) and, for each row, computes a key and writes the row into one of N local buckets.
- **Shuffle:** the all-to-all transport in between — buckets are written to local disk, then fetched over the network so all rows with the same key converge on one machine.
- **Reduce:** N new tasks each pick up one bucket's worth of same-key rows and finish the work (the join, the groupBy, the aggregate).

The shuffle is the most expensive thing Spark does: disk + network + serialization for the entire dataset.

And the crucial question: how many buckets (N) should the shuffle produce? Spark can't derive this from the data — the plan is made before the data has been seen. So historically it came from a static setting:

```
spark.sql.shuffle.partitions = 2000
```

▎ **What this number actually is** — it's the **width of the shuffle's output**: the number of buckets the map side writes into, which is exactly the number of reduce-side tasks that read them. It isn't a step that happens "after" the shuffle; it *defines* the shuffle. The next stage merely inherits those N partitions, which is why it can look like it applies afterward.

The number is a cluster-shape decision: with 800 parallel slots (200 × 4), 2000 tasks ≈ 2.5 waves of work — healthy utilization when a national trip schedule brings hundreds of millions of rows.

▎ **Dynamic allocation** (`minExecutors=0`, `maxExecutors=200`) — executors are requested and released as demand changes. A test run against one city's feed spins up two executors, not 200. So the *cost* side self-adjusts; the *partition count* side is what needs the next section.

> **Two knobs, two layers.** Dynamic allocation governs **resources** — how many executors exist. `shuffle.partitions` and AQE govern the **query plan** — how the work is cut up. They're orthogonal: dynamic allocation decides *how many workers you hire*; the shuffle settings decide *how the work is divided among them*. Tune both; neither substitutes for the other.

---

## §5 · AQE: making the static number adaptive

A fixed 2000 is right for a national feed's big joins and absurd for a single-city test: shuffling five thousand rows into 2000 buckets means 2000 tasks holding ~2 rows each — pure scheduling overhead. Even within one production run, different shuffles carry wildly different volumes.

▎ **AQE (Adaptive Query Execution)** — Spark re-plans queries mid-flight using statistics collected as stages finish. The relevant feature: once a shuffle's *write* side has run, Spark knows exactly how many bytes landed in each bucket. **Partition coalescing** then merges adjacent small buckets until each merged partition approaches a target size, before the read side starts.

```
spark.sql.adaptive.coalescePartitions.enabled       = true
spark.sql.adaptive.advisoryPartitionSizeInBytes     = 256m
```

Read the settings as one sentence: *"start every shuffle at up to 2000 buckets, then let AQE merge them until partitions are roughly 256 MB."* AQE flips the meaning of `shuffle.partitions` from a **mandate** into a **ceiling** — the finest granularity a shuffle may start from. On one city's data the joins begin at 2000 and collapse to a handful of real tasks; on the national feed they genuinely use the width. One number, both worlds.

> **Sequencing matters.** `shuffle.partitions` is the *up-front guess*, made blind, before running. `advisoryPartitionSizeInBytes` is the *runtime correction*, made after AQE measures real bytes. They fire one after another on the same shuffle — 2000 buckets created, then merged toward 256 MB each — not as competing settings.

---

## §6 · Where adaptivity stops: `repartition`

▎ **repartition** — an explicit command to shuffle a DataFrame into a chosen shape. Because *you* specified the shape, AQE treats it as authoritative and won't coalesce it. It's the escape hatch from adaptivity — sometimes you want that, sometimes it bites you. It has two overloads, and our job uses both:

▎ **`repartition(n)`** — "give me exactly n partitions," distributed **round-robin**. It ignores row *values*; it only rebalances the *count* into n even chunks. Use it when the goal is purely "restore or lock parallelism."

▎ **`repartition(n, col…)`** — "give me n partitions, **grouped by these columns**." Spark computes `hash(cols) % n` per row, so every row with the same key value lands in the same partition, deterministically.

Here's how the job prepares its two join inputs. The zone and stop numbers arrive as unpadded integers while the reference feed stores them zero-padded, so we normalize **into real columns** before doing anything else — that detail turns out to matter enormously:

```scala
val shufflePartitions = spark.conf.get("spark.sql.shuffle.partitions").toInt

// partner side — normalize the legacy integer codes into stable string columns, once
val stopsPrepared = partnerStops
  .withColumn("stop_row_id", monotonically_increasing_id())
  .withColumn("zone_key", lpad(col("zone_code").cast("string"), 4, "0"))
  .withColumn("stop_key", lpad(col("stop_code").cast("string"), 5, "0"))
  .repartition(shufflePartitions, col("zone_key"), col("stop_key"))
  .cache()

// route-stops side — SAME normalization, SAME key columns, SAME partition count
val routeStopsPrepared = routeStops
  .select(
    lpad(col("zone").cast("string"), 4, "0").alias("zone_key"),
    lpad(col("stop").cast("string"), 5, "0").alias("stop_key"),
    col("route_id")
  )
  .repartition(shufflePartitions, col("zone_key"), col("stop_key"))
  .cache()

// hand-rolled dynamic partition pruning (§2): drop reference rows whose
// zone can't appear in the partner catalog before paying to join them
val presentZones = broadcast(stopsPrepared.select(col("zone_key")).distinct())

val stopsWithRoutes = stopsPrepared.join(
  routeStopsPrepared.join(presentZones, Seq("zone_key"), "inner"),
  Seq("zone_key", "stop_key"),
  "left"
)
```

**Why repartition — keyed — before a join?** In theory: a join must bring matching keys onto one machine, which is inherently a shuffle. If both sides *already* carry a hash partitioning on the join key with the same partition count, Spark can recognize they're **co-partitioned** and skip inserting its own exchange.

But the normalization above is doing more work than it looks like, and getting it wrong fails in two different ways at once.

> ### ⚠️ The normalization trap (it costs correctness *and* performance)
>
> When two sides of a join encode the same key differently, you have to normalize — and *where* you normalize decides whether you get burned once or twice.
>
> Our partner sends `zone_code` and `stop_code` as **integers**, so leading zeros are already gone: zone `42`, stop `1337`. The reference feed stores them **zero-padded**: `"0042"`, `"01337"`. Normalizing with `lpad` is mandatory. The tempting shortcut is to apply it inline wherever it's needed:
>
> ```scala
> // BROKEN — two ways at once
> val stopsPrepared = partnerStops
>   .repartition(shufflePartitions,
>     lpad(col("zone_code").cast("string"), 4, "0"),
>     lpad(col("stop_code").cast("string"), 5, "0"))
>   .cache()
>
> stopsPrepared.join(routeStopsPrepared,
>   col("zone_code") === col("zone_key") && col("stop_code") === col("stop_key"))
> ```
>
> **Failure 1 — silent data loss.** The join compares the *raw* `zone_code` (`"42"`) against the *padded* `zone_key` (`"0042"`). Nothing matches. No exception, no warning: the left join just fills nulls, your enrichment output comes back empty or thin, and your match-rate metric faithfully reports the loss as though it were a property of the data. This is the expensive one, and it's the one that ships.
>
> **Failure 2 — the shuffle you paid for and didn't get.** Even in a version where the values *do* line up, the partitioning is on the expression `lpad(zone_code, …)` while the join's required distribution is on the attribute `zone_key`. Spark compares canonicalized *expressions*, not the values they produce, so it can't tell they're equivalent — it inserts a fresh exchange and re-shuffles anyway. You bought a full shuffle and got nothing back. (And a skip requires *both* sides aligned, so it doesn't help that the other side happens to match.)
>
> **One fix covers both: normalize once, at the ingestion boundary, into real columns** — which is exactly what the working snippet above does. Every subsequent reference — partition key, join key, and any statistics you compute — is then the same plain attribute. Values can't diverge, and the planner can see that the partitioning satisfies the join.
>
> **Two obligations come with that fix.**
>
> First, your **metrics must use the normalized columns too**. Counting distinct keys on the raw column while joining on the padded one puts your numerator and denominator in different units, and your match rate becomes fiction — a pipeline that's correct while its own reporting lies about it.
>
> Second, those key columns are now **part of your DataFrame**. Strip them before writing, or they leak into your published schema forever (see §8). It's worth a test assertion — `output.columns should not contain "zone_key"` — because a leaked bookkeeping column is a schema change every downstream consumer inherits.
>
> And regardless of which path you take: check `.explain(true)` or the SQL tab in the Spark UI for whether an `Exchange hashpartitioning` still sits above your join. Don't take co-partitioning on faith.

There's a second, more reliable reason for these `repartition` calls, and it has nothing to do with the join: **normalizing the read before caching**. Whatever §1 handed us — three fat gzip partitions, or a healthy two hundred from parquet — `repartition(2000, …).cache()` converts it into a known, even, in-memory base *once*. When the input was non-splittable, this is a rescue: it's the only way those three serialized tasks stop dictating the width of everything downstream. When the input was already wide, it's still worth it, because each prepared frame is consumed more than once (`stopsPrepared` feeds the pruning broadcast, the route join, *and* the final join-back), and caching an evenly-distributed base means no reuse re-reads or re-parses the source. **Even if the join skip never fires, this alone justifies the call.**

▎ **Why read the number from config instead of hardcoding `2000`** — `spark.conf.get("spark.sql.shuffle.partitions")`. The static conf only governs shuffles Spark inserts *automatically*. These manual repartitions live in two places the conf can't reach on its own: right after the read (no shuffle has happened yet) and just before a cache (where AQE would otherwise coalesce). Reading the value keeps the manual calls in lockstep with the single source of truth — change the conf and they follow, instead of drifting from a magic number.

Now the second hop and the fan-out, ending in the third repartition — the **plain** overload, which exists to fight AQE:

```scala
val stopTrips = stopsWithRoutes
  .join(tripSchedule, Seq("route_id"), "left")
  .filter(col("trip_id").isNotNull)
  .select(col("stop_row_id"), col("trip_id"))

val tripsByStop = stopTrips
  .groupBy("stop_row_id")
  .agg(collect_set("trip_id").alias("trip_ids"))

val enriched = stopsPrepared
  .join(tripsByStop, Seq("stop_row_id"), "left")
  .repartition(shufflePartitions)   // no key — just restore width
  .cache()
```

This one leans on two more concepts:

▎ **Lazy evaluation / actions** — DataFrame operations don't run when you write them; they build a recipe (the plan). Only an **action** — `count()`, `collect()`, `.write` — triggers computation, and each action re-cooks the recipe from scratch.

▎ **cache()** — marks a DataFrame to be kept in executor memory after its first computation, so later actions reuse it instead of re-cooking. Essential when one intermediate feeds several downstream computations — here, `enriched` feeds both the write **and** the statistics pass. (And per the trap above, that statistics pass must count on `zone_key`/`stop_key`, not the raw codes, or it will measure something the join never compared.)

Left to itself, AQE would look at this modest intermediate (one row per stop, each holding an array), compare it to the 256 MB target, and coalesce it to **~2 partitions**. Then everything computed from that cached frame — every action, for the rest of the job — runs **2 cores wide**. We don't need key grouping here, just parallelism, so the plain `repartition(n)` is exactly right: it re-spreads the count, and being an explicit number, AQE leaves it alone.

> ### The same move, one level up: validate the schema at the boundary
>
> Normalizing values once at ingestion has a structural twin: deciding once, at ingestion, **which columns are guaranteed to exist**.
>
> The tempting alternative is to stay flexible — accept whatever schema arrives and branch on it everywhere. But "this column might be absent" is not a local condition; it propagates. Every key-building expression needs a conditional, every join condition needs a variant, every statistic needs a guard, and the fallback paths are the ones your tests exercise least.
>
> The cheaper contract is a single `require` at the top: *these columns must be present* (values may still be null per row — that's data, not schema). Everything downstream then reads as one unbranched path.
>
> This is worth real line count. In the pipeline this post is drawn from, tightening that one check from "either column" to "both columns" deleted roughly a quarter of the file — a `foldLeft` that conditionally added columns, an `identity` no-op for a skipped broadcast filter, a four-way pattern match on column presence, and two branching join conditions. None of it was wrong. All of it existed to serve a flexibility nobody had asked for.
>
> **Null-per-row is data. Absent-column is schema.** Handle the first in your logic and the second at your front door, and the two never tangle.

That's three legitimate repartitions. The fourth one was a bug.

---

## §7 · The small files problem

The write path did `repartition(2000)` immediately before `.write.parquet(...)`. One rule makes that fatal:

▎ **One partition = one output file.** When Spark writes a DataFrame, each partition's task writes its own file. Nothing merges them afterwards. **The file count of your output is the partition count at the moment of writing.**

So a single-city test run's few thousand rows, force-spread across 2000 partitions, became hundreds of ~1.6 KB parquet files. Why so small yet not empty?

▎ **Parquet footer** — every parquet file ends with a metadata block: the schema, plus per-column statistics and byte offsets. It's the same footer that powers predicate pushdown in §2 — readers fetch it first, then pull only the byte ranges they need. But it's a fixed overhead of a few KB per file, so a file holding 3 rows is almost entirely footer.

▎ **Small files problem** — the classic object-storage pathology: many tiny files make every future read pay one request + one footer parse **per file**. A thousand 2 KB files can be slower to scan than a single 100 MB file. Worse, the pruning from §2 degrades too: min/max statistics are only useful when a row group contains enough rows for the range to be selective, and a file holding three rows prunes nothing. And unlike task counts — which evaporate when the job ends — **files persist and tax every reader forever**: the next scanner, the next schema probe, the next re-enrichment, the analyst's ad-hoc query.

That asymmetry is the heart of the bug: `repartition(2000)` leaked a **transient cluster-tuning decision** into a **permanent storage artifact**.

---

## §8 · The fix: size the write by the data

The principle: **output file count should be a function of data volume, not cluster configuration.** The job can simply look at the volume before writing.

```scala
// The normalized keys and the row id were internal bookkeeping (§6) — they must not
// leak into the published schema, which is permanent in a way task counts are not.
val internalColumns = Set("stop_row_id", "zone_key", "stop_key", "trip_ids")

val exploded = enriched.select(
  enriched.columns.filterNot(internalColumns.contains).map(col) :+
    explode_outer(col("trip_ids")).alias("trip_id"): _*
)

val explodedRows = exploded.count()                    // an action — but cheap, see below
val targetFiles  = math.max(1, math.ceil(explodedRows / 5000000.0).toInt)

exploded.coalesce(targetFiles).write.mode("overwrite").parquet(outputPath)
logger.info(s"Wrote $explodedRows rows into $targetFiles files")
```

Each line leans on a concept from above:

- The **column filtering** pays off the debt incurred in §6. Materializing normalized keys was the right call for the join, but those columns are ours, not the consumer's — the same "transient decision, permanent artifact" trap as the file count, just applied to schema instead of layout.
- The `count()` would normally mean computing everything twice (lazy evaluation — every action re-cooks). But `enriched`, the cached parent, is already in memory, so the count only replays the **narrow** `explode` over in-memory data. **Caching is what makes "peek, then decide" affordable.**
- Why 5M rows instead of a byte target? Compressed size is only knowable *after* writing; row count is knowable *before*. For this narrow output schema (two codes, a dimension, one id) 5M rows lands around 100–250 MB compressed — comfortably inside the range object-storage readers like, and big enough that row-group statistics can actually prune.

▎ **coalesce(n)** — reduces the partition count **without a shuffle**: existing partitions are glued together on the machines where they already live. Contrast with `repartition`, which re-mails every row across the network. The distinction from §6, in one line:

| | `repartition(n)` | `repartition(n, cols)` | `coalesce(n)` |
|---|---|---|---|
| Shuffle? | full | full | **none** (merge only) |
| Can grow? | yes | yes | **no** — shrink only |
| Placement | round-robin, even | `hash(cols) % n` | glued in place, possibly uneven |
| Use for | restoring parallelism | co-locating keys | cheap output file sizing |

Rule of thumb: **shrinking → coalesce; growing, rebalancing skew, or co-locating for a join → repartition.** (One caveat: `coalesce` concentrates work, so `coalesce(1)` on huge data serializes your final stage into a single core. Deriving the target from volume makes it degrade gracefully.)

The result: a single-city run writes **1 file**; a national run with 80M exploded stop-trip rows writes **~16 well-sized ones**. And the job logs its own decision, so every run explains itself.

---

## The through-line

Seven decisions determine the shape of this job, and each is made by a different mechanism at a different moment:

| Moment | Decided by | In our job |
|---|---|---|
| Reading files | file sizes + **splittability** | splittable → scales with data; gzip → 1 partition per file |
| …and how much of them | **pruning** (columns, files, rows) | format decides whether the engine can skip proven-unneeded work |
| Preparing inputs | your `repartition(n, cols)` | normalize keys once, co-locate, cache a known width |
| Any join / groupBy | `spark.sql.shuffle.partitions` | 2000 buckets — the shuffle's output width |
| …then re-judged | AQE `advisoryPartitionSizeInBytes` | merged toward 256 MB each |
| Before a reused cache | your `repartition(n)` | lock width so AQE can't collapse it to 2 |
| Writing output | your `coalesce(n)` + column pruning | **1 partition = 1 file** — size by rows, don't leak internal columns |

Three things are worth internalizing, and the first two are the same lesson wearing different clothes.

**Do it once, at the boundary.** A key encoded two ways will cost you silently — first your matches, then your shuffle. A column that might be absent will spread conditionals through every function it touches. Normalize values and validate schema at ingestion, and neither problem propagates.

**Never let a transient decision become a permanent artifact.** Task counts vanish when the job exits. Files and schemas don't — they bill every reader, forever. A partition count chosen for the cluster's benefit has no business determining how many files you write, and a bookkeeping column added for the planner's benefit has no business appearing in your published schema.

**Don't block the optimizer.** Almost everything in §2 happens for free, and the way to lose it is to shuffle or cache before you've projected and filtered. The cheapest work is the work the engine proves it can skip.

---

Two notes on this revision. I folded in the **boundary-validation callout** at the end of §6 — you hadn't explicitly asked for it, so it's easy to cut as a single block if you'd rather keep §6 focused on partitioning alone. And §7's small-files section picked up one new sentence connecting tiny files back to **degraded row-group pruning**, which strengthens the argument: small files don't just cost you requests, they cost you the very optimization §2 promised.