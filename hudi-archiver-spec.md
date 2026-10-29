# Spec: Heap-fork changes to the Hudi timeline archiver (0.15.0 fork)

## About this document

This is a **retroactive** spec. The changes below were made without a written design, and
this document was reconstructed afterwards from the commits, their javadoc and config
documentation, and how the datalake repo (`pa_system-of-record_datalake`) uses them. Where
a statement is an inference or was not verified, it says so. For the cleaner counterpart
see `hudi-cleaner-batching-spec.md` (the two are related: both exist because of the same
very-large-partition table).

## Summary

Compared with upstream `release-0.15.0`, the fork changes the archiver in two places, and
changes how the CLI command that runs it reports failure:

| # | Commit | Change |
|---|---|---|
| 1 | `7adbe431b6b` | New opt-in config `hoodie.archive.trim.clean.action.metadata` that strips per-partition file lists from archived clean instants |
| 2 | `5f82a31f3bf` | The CLI `ARCHIVE` command now merges JVM system properties into its write config, so `-Dhoodie.*=...` reaches the archiver |
| 3 | `cfd3c2f7120`, `93a67b42109` (DE-27803) | `SparkMain` (the CLI entry point the archiver runs through) now surfaces failures on YARN. Not archiver logic, see "Related" |

Released as heap-fork version `0.3` (`9d2f33dabc4`); change 2 bumped the version to
`0.3-SNAPSHOT`.

Not changed: which instants get archived, the archival policy (`hoodie.keep.min.commits`,
`hoodie.keep.max.commits`, archive-beyond-savepoint), the archive log format, and the
archived timeline's readers.

## Change 1: trim clean metadata when archiving

### Problem

Archiving a clean instant writes its `HoodieCleanerPlan` (for requested or inflight
instants) or `HoodieCleanMetadata` (for completed ones) into an archive log block. Both
carry per-partition maps, which dominate the size on a table with millions of partitions.
According to the commit's javadoc, a single instant's Avro block can then exceed the JVM's
`Integer.MAX_VALUE` byte-array limit and fail the archive. It is the same ceiling that the
cleaner hit when serializing one huge plan, see the cleaner spec.

### Behavior

Config: `hoodie.archive.trim.clean.action.metadata`

- Type boolean, default `false`, advanced. Builder method
  `HoodieArchivalConfig.Builder#withArchiveTrimCleanActionMetadata(boolean)`; getter
  `HoodieWriteConfig#shouldArchiveTrimCleanActionMetadata()`.
- When `false`, behavior is identical to upstream.

When `true`, `MetadataConversionUtils.createMetaWrapper(instant, metaClient, true)` trims
the clean action before it is wrapped for the archive log:

| Instant state | Object | Fields cleared | Fields kept |
|---|---|---|---|
| requested / inflight | `HoodieCleanerPlan` | `filesToBeDeletedPerPartition` and `filePathsToBeDeletedPerPartition` set to `null`; `partitionsToBeDeleted` set to `null` | policy, version, retention boundary (`earliestInstantToRetain`), last completed commit, `extraMetadata` |
| completed | `HoodieCleanMetadata` | `partitionMetadata` set to an **empty map**; `bootstrapPartitionMetadata` set to `null` | `startCleanTime`, `timeTakenInMillis`, `totalFilesDeleted`, `earliestCommitToRetain`, `lastCompletedCommitTimestamp`, version, `extraMetadata` |

Notes:

- `partitionMetadata` is a required (non-null) Avro field, so it is emptied rather than
  nulled. The three cleaner-plan fields and `bootstrapPartitionMetadata` are nullable
  with a null default (checked in the `.avsc` files).
- The objects are deserialized fresh inside `createMetaWrapper` and don't outlive the
  call, so they are mutated in place instead of copied. Copying would allocate the very
  maps the option exists to avoid holding.
- Other action types (commit, delta commit, replace commit, rollback, and so on) are
  unaffected.

### Wiring

- `HoodieTimelineArchiver#convertToAvroRecord` passes
  `config.shouldArchiveTrimCleanActionMetadata()` to the three-argument
  `createMetaWrapper`. This is the only caller that passes `true`.
- The original two-argument `createMetaWrapper` is kept and delegates with `false`. Its
  other caller, `ConcurrentOperation` (conflict resolution), is therefore unchanged.
- Because the flag is read in `HoodieTimelineArchiver`, any writer that archives inline
  with this config set also trims. Only the CLI archiver is wired to set it in the
  datalake repo, as far as I read.

### Trade-off

Trimming is **irreversible**: an archived clean instant permanently loses its
per-partition delete lists. The summary counts and the retention boundary survive. Anything
that wants to replay or audit which files an archived clean deleted can no longer do so.
I did not search for consumers of archived clean detail, so I can't say whether any exist.

### Tests

None. The commit changed no test files, and nothing under `src/test` references the config.

## Change 2: system properties reach the CLI archiver

### Problem

`ArchiveExecutorUtils.archive(...)`, which backs the CLI `ARCHIVE` command, built its
write config only from its positional arguments (`minCommits`, `maxCommits`,
`commitsRetained`, `enableMetadata`, `basePath`). There was no way to set any other
config, including the new trim option, for a CLI-run archiver.

### Behavior

The builder now ends with `.withProperties(System.getProperties())` before `.build()`.
`-Dhoodie.*=...` flags in `spark.driver.extraJavaOptions` are therefore copied into the
write config. The commit says it has no effect when no such properties are set.

Things to know:

- **Precedence.** `withProperties` is a plain `putAll` on the config's properties, and it
  runs after the archival, clean and metadata sub-configs are applied. A `-D` value
  therefore **overrides** the positional argument for the same key (for example
  `hoodie.keep.min.commits`). This follows from the code; I haven't run it.
- **Breadth.** It copies *all* JVM system properties, not just `hoodie.*`, into the
  config's property bag. Spark and JVM properties come along. This looks harmless, but it
  is wider than the stated purpose.
- **Driver only.** The archiver runs in the driver, and the datalake passes the flag
  through `spark.driver.extraJavaOptions` only, so executors are not given it.

## Related: `SparkMain` failure handling (DE-27803)

`SparkMain` is the entry point for the `ARCHIVE` command, among others. Two commits changed
how it reports failure:

- `cfd3c2f7120`: if the Spark master is not `yarn`, call `System.exit(returnCode)` as
  before. On `yarn`, don't exit; instead throw an `Error` for a non-zero code, so the YARN
  application status reflects the failure.
- `93a67b42109`: capture the exception from the command and rethrow it, and improve the
  error message ("Check application logs for details"). It also changes the `hudi-cli`
  `pom.xml` to the heap-fork version label and pins the other Hudi module dependencies to
  `0.15.0`.

This only matters for runs whose master is `yarn`. The datalake's k8s archiver rewrites the
master argument to the Kubernetes URL (`k8sSparkMaster` in `cron/datalakecron.go`), so for
those runs the original `System.exit` path applies.

## How the datalake runs it

(From the datalake repo's `cron/hudi_archiver.go`, `cron/datalakecron.go` and
`k8s/client.go`; included because this is where these flags are used.)

- Each archiver cron runs `org.apache.hudi.cli.commands.SparkMain` from the Hudi CLI jar
  with positional arguments `ARCHIVE <master> <driverMem> <min> <max> <retained> false
  <tablePath>`. `min`, `max` and `retained` come from `GetCommitsToKeep`, computed from
  the table's timeline on S3.
- The archiver cron definitions I read (`EventsArchiverUSE1` and `EventsArchiverEUC1`) set
  `spark.driver.extraJavaOptions=-Dhoodie.archive.trim.clean.action.metadata=true`. I did
  not read the other archiver definitions.
- The k8s client appends per-job `extraJavaOptions` to its base flags (G1GC,
  `-Daws.region`) instead of replacing them, so the `-D` flag survives.
- Datalake comments record a constraint worth keeping next to this spec: the archiver's
  minimum-days-to-keep **must exceed the cleaner's retention window**, otherwise the
  archiver can archive past the cleaner's `earliestCommitToRetain` and break incremental
  cleaning. They also note that Hudi ties `.clean` archival to the same threshold, so a
  high commit floor pins clean instants on the timeline.

## Limitations and open questions

- No tests cover the trim option or the `System.getProperties()` merge.
- Trimming cannot be undone for instants already archived with it enabled.
- Unknown: whether anything reads archived clean plans or metadata at file level.
- The merged properties are broader than `hoodie.*` and can override positional arguments.
- The trim reduces the size of the clean action only. A very large commit, replace-commit
  or other instant could still hit the same limit, and this change doesn't address it.
