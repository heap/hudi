# Spec: Batched/Paginated Cleaner Plans for Hudi (0.15.0 fork)

## Context

Our MOR table `events-v1` has ~6.7M partitions. A backlog accumulated because the
cleaner ran with an incorrect `hoodie.cleaner.hours.retained=720` for a period
before being reverted to `168`. When the cleaner tries to catch up, it computes
a candidate partition list (currently via a full-table scan, since the
incremental-mode checkpoint was invalidated by an over-aggressive archiver run),
and serializes the resulting `HoodieCleanerPlan` as a single Avro-encoded byte
array before persisting it. This blows past the JVM's hard array-size ceiling
(`Integer.MAX_VALUE` bytes):

```
java.lang.OutOfMemoryError: Required array length 2147483639 + 139 is too large
	at java.io.ByteArrayOutputStream.ensureCapacity
	at org.apache.avro.io.DirectBinaryEncoder.writeFixed
	at org.apache.hudi.common.table.timeline.TimelineMetadataUtils.serializeCleanerPlan
	at org.apache.hudi.table.action.clean.CleanPlanActionExecutor.requestClean
```

Working around this today by manually ratcheting `hoodie.cleaner.commits.retained`
down in small steps is slow, imprecise (the "N commits" framing doesn't actually
bound how much historical debt gets swept per run), and doesn't fix the
underlying limitation for future incidents.

## Goal

Add native support for **paginating a single logical clean operation into
multiple bounded internal batches**, each going through Hudi's normal
request → inflight → execute → complete instant lifecycle. This removes the
single-giant-plan bottleneck directly, rather than working around it via
retention-window tricks.

This should be implemented as an **opt-in** capability (new config, defaults
to disabled/current behavior) since this code is shared across many tables in
our fork, and we don't want to change behavior for callers that don't ask for it.

## Non-goals

- Do not change how `partitionsToClean` is computed (incremental vs. full-scan
  decision in `CleanPlanner.getPartitionPathsForCleanByCommits` stays untouched).
- Do not change cleaning *policy* semantics (`KEEP_LATEST_BY_HOURS` /
  `KEEP_LATEST_COMMITS` continue to work exactly as before).
- Do not attempt to fix the incremental-mode/archiver-interaction issue as part
  of this change, that's a separate operational concern.

## Design overview

1. Compute `earliestCommitToRetain` and the full `partitionsToClean` list
   **once** per invocation, exactly as today.
2. If a new config (`hoodie.clean.plan.partitions.batch.size`) is set and the
   partition list exceeds it, split the list into deterministically-ordered
   chunks of that size.
3. For each chunk, run a **separate** clean instant through the existing
   request/inflight/execute/complete lifecycle, using the *same*
   `earliestCommitToRetain` for every chunk in this run (this is important:
   the boundary must stay fixed across the whole sweep, see "Correctness
   requirement" below).
4. Tag each resulting `HoodieCleanMetadata`'s `extraMetadata` with batch
   bookkeeping so a restarted job can skip batches that already completed.
5. Everything else (per-file delete idempotency, `hoodie.clean.allow.multiple`
   crash-resume behavior) is inherited for free because each batch is a real,
   independent instant on the timeline.

### Correctness requirement (must enforce)

All batches produced by one "logical" paginated clean run **must** share the
same `earliestCommitToRetain`. If the boundary were allowed to advance between
batches, any partitions not yet covered by an earlier batch could become
permanently invisible to future incremental cleaning (their old debris would
never be revisited). See internal discussion doc for the full reasoning; this
is the most important invariant to test for.

## Files to change

### 1. `hudi-client/hudi-client-common/.../config/HoodieCleanConfig.java`

Add a new config property:

```java
public static final ConfigProperty<Integer> CLEANER_PLAN_PARTITIONS_BATCH_SIZE = ConfigProperty
    .key("hoodie.clean.plan.partitions.batch.size")
    .defaultValue(-1) // -1 = disabled, preserves current single-plan behavior
    .markAdvanced()
    .sinceVersion("0.15.0-heap-fork")
    .withDocumentation("When set to a positive value, paginates a single "
        + "cleaner invocation into multiple internally-batched clean instants, "
        + "each covering at most this many partitions, to avoid building an "
        + "oversized single HoodieCleanerPlan. Each batch shares the same "
        + "earliestCommitToRetain boundary. Default (-1) preserves legacy "
        + "single-plan behavior.");
```

Expose the usual `getCleanerPlanPartitionsBatchSize()` getter on
`HoodieWriteConfig`, following the existing pattern for other clean configs
in this file (see `CLEANER_POLICY`, `CLEANER_COMMITS_RETAINED`, etc. for the
established style).

### 2. `hudi-client/hudi-client-common/.../table/action/clean/CleanPlanActionExecutor.java`

- Add an optional constructor/field allowing an explicit partition list
  override (`Option<List<String>> partitionsOverride`), used instead of
  calling `planner.getPartitionPathsToClean(earliestInstant)` inside
  `requestClean()` when present. This lets the batching orchestrator (below)
  reuse this class per-chunk without re-deriving the candidate list each time.
- No other changes needed here, the existing `requestClean()` body (building
  `cleanOps` via `getDeletePaths` per partition, filtering empty results,
  building the `HoodieCleanerPlan`) works unchanged on a smaller partition
  list.

### 3. New orchestration method, e.g. `hudi-client/hudi-client-common/.../client/BaseHoodieTableServiceClient.java`

Add `cleanInBatches(String cleanInstantTimePrefix)` (name TBD), invoked from
`clean()` when `config.getCleanerPlanPartitionsBatchSize() > 0`:

```
1. table = createTable(config, hadoopConf)
2. planner = new CleanPlanner(context, table, config)
3. earliestInstant = planner.getEarliestCommitToRetain()
4. fullPartitionList = planner.getPartitionPathsToClean(earliestInstant)
     // this is the expensive full-scan/incremental step; still done once
5. sort fullPartitionList deterministically (e.g. natural string order)
6. chunks = partition fullPartitionList into batches of config batch size
7. totalBatches = chunks.size()
8. alreadyDone = scan completed clean timeline for instants whose extraMetadata
     contains matching {"targetEarliestCommitToRetain": earliestInstant, "totalBatches": totalBatches}
     -> collect their "batchIndex" values into a set
9. for i in 0 until totalBatches:
     if i in alreadyDone: continue   // resume support, see below
     instantTime = table.getMetaClient().createNewInstantTime()  // must be unique + increasing
     executor = new CleanPlanActionExecutor(context, config, table, instantTime,
                    Option.empty(), partitionsOverride = Option.of(chunks[i]))
     plan = executor.requestClean(context)   // writes .clean.requested
     // extraMetadata tagging: add batchIndex, totalBatches, targetEarliestCommitToRetain
     // to the plan's extraMetadata map before/while persisting
     table.getActiveTimeline().transitionCleanRequestedToInflight(...)
     metadata = new CleanActionExecutor(context, config, table, instantTime).execute()
     // completes the instant as usual
     log/emit metrics: batch i/totalBatches, partitions=chunks[i].size(),
                       filesDeleted=metadata.getTotalFilesDeleted()
10. return combined/last HoodieCleanMetadata for caller compatibility
```

**Resumability note**: step 8-9 means that if the whole job process crashes
and is re-invoked, it will recompute `fullPartitionList` again (this repeats
the relatively cheap listing step, not the risky serialization step) and then
skip any batch index already recorded as completed via `extraMetadata`, so no
duplicate work and no re-litigating of already-cleaned partitions. Within a
single batch's own execution, the existing `hoodie.clean.allow.multiple=false`
resume-on-crash behavior in `clean()` still applies unchanged.

If tagging `extraMetadata` this way turns out to be awkward with the existing
`HoodieCleanerPlan`/`HoodieCleanMetadata` avro schemas, an acceptable
alternative is a small dedicated marker file under `.hoodie/.aux/` recording
`{targetEarliestCommitToRetain, totalBatches, completedBatchIndices}`, updated
after each batch completes. Prefer the extraMetadata approach first since it
avoids introducing a new file format/location, fall back to the marker file
only if schema changes are impractical.

### 4. `hudi-utilities/.../HoodieCleaner.java`

No functional change should be required here if `cleanInBatches` is invoked
transparently from `clean()` based on the new config, this keeps the existing
CLI invocation, orchestration job definition, metrics reporter, and Zookeeper
lock provider config all working exactly as-is. Just confirm the new config
key is picked up from the same `--hoodie-conf` / properties file mechanism
already used for other cleaner configs.

## Testing plan

1. **Unit tests** for the chunking/resume logic: given a partition list and a
   simulated set of "already completed" batch indices, verify the correct
   remaining chunks are processed and none are skipped/duplicated.
2. **Unit test** that all chunks in a single run use an identical
   `earliestCommitToRetain` (the correctness invariant above).
3. **Integration test** against a Hudi test table with a synthetically large
   number of partitions and induced backlog, verifying: (a) legacy behavior
   (`batch.size=-1`) is unchanged, (b) batched mode produces multiple
   completed `.clean` instants, (c) simulating a crash mid-run (kill the job
   after N batches) and re-running completes the remainder without
   reprocessing completed batches or double-deleting files.
4. **Before running against `events-v1` production**, validate against an
   isolated clone of this table's `.hoodie` metadata (or a scaled-down
   synthetic table with a comparable partition count) to confirm the
   plan-size fix actually avoids the OOM at this scale, and that per-batch
   timing/throughput is acceptable given ~6.7M partitions.

## Rollout recommendation

- Ship with the new config defaulted to `-1` (disabled) so no other table
  using this fork is affected.
- Enable it explicitly only for `events-v1`'s cleaner job config initially.
- Once validated in production for this table over a few cycles, consider
  whether to recommend it as a default safeguard for other very-large-partition
  tables in the fork (e.g., auto-enable above some partition-count threshold),
  as a separate follow-up decision.
