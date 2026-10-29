# Spec: Batched/Paginated Cleaner Plans for Hudi (0.15.0 fork)

## Status

Implemented and available on `release-0.15.0-heap-fork`. This document started as the
design plan; the sections below describe what was built, and where it differs from the
original plan.

| Commit | What |
|---|---|
| `6590f9bc320` | Feature: `hoodie.clean.plan.partitions.batch.size`, `BatchOverride`, `cleanInBatches()` |
| `5a3735b3b21` | Unit tests for chunking, resume parsing and `BatchOverride`; chunking and parsing extracted into package-private helpers |
| `be37a0ab48a` | Integration tests for legacy behavior, multiple instants, and resume |
| `1d2287f9112` | `heapfork.version` bump to 0.2-SNAPSHOT |

The datalake repo enables it for the events cleaner: `EventsCleanerUSE1`
(`datalake_api/internal/cron/datalakecron_definitions.go`) passes
`hoodie.clean.plan.partitions.batch.size=2000`, and the k8s cleaner builder
(`datalake_api/internal/k8s/cleaner.go`) passes `CleanerPlanPartitionsBatchSize`.

## Context

Our MOR table `events-v1` has ~6.7M partitions. A backlog accumulated because the cleaner
ran with an incorrect `hoodie.cleaner.hours.retained=720` for a period before being
reverted to `168`. When the cleaner tries to catch up, it computes a candidate partition
list (currently via a full-table scan, since the incremental-mode checkpoint was
invalidated by an over-aggressive archiver run), and serializes the resulting
`HoodieCleanerPlan` as a single Avro-encoded byte array before persisting it. This blows
past the JVM's hard array-size ceiling (`Integer.MAX_VALUE` bytes):

```
java.lang.OutOfMemoryError: Required array length 2147483639 + 139 is too large
	at java.io.ByteArrayOutputStream.ensureCapacity
	at org.apache.avro.io.DirectBinaryEncoder.writeFixed
	at org.apache.hudi.common.table.timeline.TimelineMetadataUtils.serializeCleanerPlan
	at org.apache.hudi.table.action.clean.CleanPlanActionExecutor.requestClean
```

Working around this by manually ratcheting `hoodie.cleaner.commits.retained` down in small
steps is slow, imprecise (the "N commits" framing doesn't actually bound how much
historical debt gets swept per run), and doesn't fix the underlying limitation for future
incidents.

## Goal

Add native support for **paginating a single logical clean operation into multiple
bounded internal batches**, each going through Hudi's normal request → inflight → execute
→ complete instant lifecycle. This removes the single-giant-plan bottleneck directly,
rather than working around it via retention-window tricks.

This is an **opt-in** capability (new config, defaults to disabled/current behavior)
since this code is shared across many tables in our fork, and we don't want to change
behavior for callers that don't ask for it.

## Non-goals

- Do not change how `partitionsToClean` is computed (incremental vs. full-scan decision
  in `CleanPlanner.getPartitionPathsForCleanByCommits` stays untouched).
- Do not change cleaning *policy* semantics (`KEEP_LATEST_BY_HOURS` /
  `KEEP_LATEST_COMMITS` continue to work exactly as before).
- Do not attempt to fix the incremental-mode/archiver-interaction issue as part of this
  change, that's a separate operational concern.

## Design overview

1. Compute `earliestCommitToRetain` and the full `partitionsToClean` list **once** per
   invocation, exactly as today.
2. If `hoodie.clean.plan.partitions.batch.size` is a positive value, sort the list and
   split it into deterministic chunks of at most that size.
3. For each chunk, run a **separate** clean instant through the existing
   request/inflight/execute/complete lifecycle, using the *same* `earliestCommitToRetain`
   for every chunk in this run (see "Correctness requirement").
4. Tag each resulting clean instant's `extraMetadata` with batch bookkeeping so a
   restarted job can skip batches that already completed.
5. Everything else (per-file delete idempotency, crash-resume of a single instant) is
   inherited because each batch is a real, independent instant on the timeline.

### Correctness requirement (must enforce)

All batches produced by one "logical" paginated clean run **must** share the same
`earliestCommitToRetain`. If the boundary were allowed to advance between batches, any
partitions not yet covered by an earlier batch could become permanently invisible to
future incremental cleaning (their old debris would never be revisited). See internal
discussion doc for the full reasoning; this is the most important invariant to test for.

## Implementation (as built)

### Config

`HoodieCleanConfig.CLEANER_PLAN_PARTITIONS_BATCH_SIZE`

- Key: `hoodie.clean.plan.partitions.batch.size`
- Default `-1` (disabled, legacy single-plan behavior). Any value `> 0` enables batching.
- Advanced config, `sinceVersion("0.15.0-heap-fork")`.
- Builder: `HoodieCleanConfig.Builder#withCleanerPlanPartitionsBatchSize(int)`.
  Getter: `HoodieWriteConfig#getCleanerPlanPartitionsBatchSize()`.

### `CleanPlanActionExecutor`

- New `BatchOverride(List<String> partitions, Option<HoodieInstant> earliestInstant)`.
  When present, `requestClean(HoodieEngineContext)` uses the supplied `earliestInstant`
  and partition list instead of calling `planner.getEarliestCommitToRetain()` and
  `planner.getPartitionPathsToClean(...)`.
- The two values travel together because `CleanPlanner.getDeletePaths(partition,
  earliestInstant)` drives per-file delete decisions off `earliestInstant`. Pinning only
  the partitions would let each batch recompute the boundary and break the correctness
  requirement.
- The original five-argument constructor is kept and delegates with
  `Option.empty()`, so existing callers are unaffected.
- `requestClean(String startCleanTime)` is public and is called directly by the
  orchestrator, skipping `execute()`'s trigger gate (see "Behavior notes").
- Also fixes an existing dead-code bug in the same class: the
  `Option<Map<String,String>> extraMetadata` constructor argument was stored but never
  written into the plan. It is now merged into the plan's `extraMetadata` alongside the
  savepointed timestamps. This is what carries the batch bookkeeping.

### `BaseHoodieTableServiceClient#cleanInBatches`

`clean(String, boolean)` runs its usual preamble (table services enabled check, failed
write rollback, `hoodie.cleaner.rollback_only` exit) and then, when the batch size is
`> 0`, delegates to `cleanInBatches(cleanInstantTime, timerContext)`:

1. Create the table. If any `.clean.requested` or `.clean.inflight` instant is pending
   from a prior crashed run, finish it first with `CleanActionExecutor` and reload the
   timeline, so its completion is visible to the resume scan in step 5.
2. Compute `earliestInstant = planner.getEarliestCommitToRetain()` and the full partition
   list with `planner.getPartitionPathsToClean(earliestInstant)`, once.
3. If the list is empty, release resources and return `null`.
4. `chunkForBatchedClean(list, batchSize)`: copy, sort by natural string order, and slice.
   Sorting makes chunk boundaries identical across restarts. Throws
   `IllegalArgumentException` if `batchSize <= 0`.
5. `findCompletedBatchIndices(...)`: scan completed `.clean` instants on the active
   timeline and collect `batchIndex` values from those whose `extraMetadata` matches this
   run's `targetEarliestCommitToRetain` and `totalBatches`.
6. For each chunk index not already done:
   - build `extraMetadata` with the three keys below;
   - create a new instant time and a `CleanPlanActionExecutor` with
     `BatchOverride(chunk, earliestInstant)`;
   - call `requestClean(instantTime)`; if it returns no plan (nothing to delete in this
     chunk) log and continue;
   - run `CleanActionExecutor.execute()` to move the instant to inflight and complete it;
   - update clean metrics and log `batch i/N`, files deleted, and
     `earliestCommitToRetain`.
7. Release resources and return the metadata of the last batch that produced one.

Bookkeeping keys, written to each batch's `extraMetadata`:

| Key | Value |
|---|---|
| `hoodie.clean.batch.targetEarliestCommitToRetain` | the pinned boundary's timestamp (empty string if none) |
| `hoodie.clean.batch.totalBatches` | number of chunks in this run |
| `hoodie.clean.batch.batchIndex` | zero-based index of this chunk |

`parseCompletedBatchIndex(extra, target, totalBatches)` returns an index only when all
three keys are present, the target and total match, and both numbers parse. Legacy
(non-batched) instants, instants from other runs, and malformed entries are ignored.

### `HoodieCleaner` (utilities)

No change. The new key is read from `--hoodie-conf` or the properties file like other
cleaner configs, and `client.clean()` reaches the batched path through
`clean(String, boolean)`.

## Differences from the original plan

| Original plan | As built |
|---|---|
| `CleanPlanActionExecutor` takes `Option<List<String>> partitionsOverride` | Takes `Option<BatchOverride>` carrying the partitions **and** the pinned `earliestInstant` |
| Batch bookkeeping in `extraMetadata`, with a marker file under `.hoodie/.aux/` as a fallback | `extraMetadata` only. The marker file was not needed |
| Orchestrator method name "TBD" | `cleanInBatches(String, Timer.Context)`, private, in `BaseHoodieTableServiceClient` |
| Resume starts from the completed-batch scan | Also finishes any pending requested/inflight clean instant **before** the scan |
| Chunking and resume parsing inline | Extracted to package-private `chunkForBatchedClean` and `parseCompletedBatchIndex` for unit testing |
| Not mentioned | Fixes the dropped `extraMetadata` constructor argument in `CleanPlanActionExecutor` |

## Behavior notes and limitations

These come from reading the code; they are not all covered by tests.

- **The commits-since-last-clean trigger is bypassed.** `execute()` only plans when
  `commitsSinceLastClean >= hoodie.clean.max.commits` (`needsCleaning`). The orchestrator
  calls `requestClean(String)` directly for every batch, including the first, so with
  batching on, each `clean()` invocation plans and runs regardless of that setting.
  Calling it less often than before is therefore the caller's responsibility.
- **`scheduleInline` is not used** in the batched path, and `hoodie.clean.allow.multiple`
  is not consulted. Pending clean instants are always finished first.
- **Batches with nothing to delete are not recorded.** They are re-planned on a resumed
  run and produce nothing again, which costs a listing, not a delete.
- **Resume identity is `(targetEarliestCommitToRetain, totalBatches)`.** If new commits
  move the boundary, or the partition count changes the chunk count, a re-run is treated
  as a new run and does not skip earlier batches. Deletes are idempotent, so this repeats
  work instead of causing wrong deletes.
- **Resume only sees completed clean instants still on the active timeline.** Batches
  whose instants have been archived are not counted.
- **Return value:** only the metadata of the last non-empty batch is returned; `null`
  if every batch was empty.
- **Metrics:** the same timer context is stopped once per batch, so the clean duration
  reported for batch *k* is measured from the start of the whole run, not from the start
  of that batch.

## Testing

Unit tests (`hudi-client-common`):

- `TestBatchedCleanHelpers`
  - chunking: empty list, smaller than batch, exactly one batch, even multiples, a
    smaller final chunk, sort-before-chunk (so a restart gives the same layout), no
    mutation of the input, and rejection of batch size `0` or negative;
  - `parseCompletedBatchIndex`: null, empty, missing keys, mismatched target, mismatched
    total, malformed numbers, a matching entry, and a legacy instant with only
    savepoint metadata;
  - the combined case: remaining chunks skip done indices without duplicating any.
- `TestCleanPlanActionExecutorBatchOverride`: `BatchOverride` stores its fields, and
  accepts an empty `earliestInstant`.

Integration tests (`TestCleanPlanExecutor`):

- `testCleanInBatchesDisabledPreservesLegacyPath`
- `testCleanInBatchesProducesMultipleInstants`
- `testCleanInBatchesResumeSkipsCompletedBatches`

Not covered by tests in this repo:

- A scale test at or near the ~6.7M-partition table size, to confirm the plan-size fix
  avoids the OOM and that per-batch throughput is acceptable.
- The behavior notes above, including the trigger bypass and the cumulative
  per-batch timing.

## Rollout

- Ships with the config defaulted to `-1`, so other tables using this fork are
  unaffected.
- The datalake repo enables it for the events cleaner (`EventsCleanerUSE1`, batch size
  2000). Enable it elsewhere only per table, through that table's cleaner job config.
- Whether to recommend it as a default safeguard for other very-large-partition tables
  (for example, auto-enabling above a partition-count threshold) remains a separate
  follow-up decision.
