# Incremental Processing Architecture

## Scope

Phase 6 implements incremental ingestion into the six Bronze Delta tables. Silver
transformations and Gold analytics remain full-snapshot rebuilds; this phase does not
claim end-to-end incremental propagation.

## Processing flow

1. Read all six CSV files from `/Volumes/<catalog>/commerce_lakehouse/raw_files`.
2. Validate exact schemas, castability, primary-key quality, duplicate keys, and
   foreign-key relationships before the first Bronze mutation.
3. Hash ordered business columns with SHA-256, excluding operational metadata.
4. Merge each source into `bronze_<dataset>` by its declared primary key: insert a
   missing key, update a matching key only when the record hash changed, and do not
   rewrite unchanged records.
5. Record per-dataset state and counts in
   `<catalog>.commerce_lakehouse.incremental_batch_audit`.
6. Check target key uniqueness/non-nullability and batch completion.

## Idempotency and recovery

Set `batch_id_override` to a stable ID to replay a logical batch. A blank override
creates a new UUID. Business processing is idempotent for identical input regardless
of batch ID; the audit identity differs unless the override is reused. Audit rows are
upserted by `(batch_id, dataset)`.

Each table merge and audit merge is a separate Delta transaction. There is no
cross-table transaction. All six source datasets are validated before writes begin,
but a later table write can fail after earlier tables commit. Inspect audit states
(`STARTED`, `SUCCESS`, `FAILED`) and rerun with the same batch ID. Run one writer at a
time for this catalog/schema; operation counts are calculated immediately before the
merge and assume no concurrent writer.

## Deletion semantics

Rows absent from an input snapshot are retained. The pipeline does not assume that
source files are authoritative deletion snapshots. Deletion handling requires an
explicit source contract distinguishing full snapshots from change-data feeds.

## Lineage

`_ingested_at`, `_source_file`, `_source_system`, `_ingestion_batch_id`, and
`_record_hash` are set on insert and changed-record updates. Unchanged rows retain
their original lineage values.

## Limitations

- The notebook updates Bronze only; Silver and Gold require their existing notebooks
  to be run separately when downstream refresh is needed.
- Audit metrics are not platform-level monitoring or alerting.
- The source contract currently represents the synthetic CSV datasets.
