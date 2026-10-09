# ADR 0006: Incremental Bronze Merge Strategy

- **Status:** Accepted
- **Context:** The original Bronze pipeline overwrites complete managed Delta tables.
  Phase 6 requires safe reprocessing without rewriting unchanged records.
- **Decision:** Add a separate notebook that validates all source data, hashes business
  columns, merges by primary key, and records per-dataset outcomes in a Delta audit
  table.
- **Consequences:** Repeated input is business-idempotent; changed records update and
  new records insert. Each target table is its own transaction, so partial runs remain
  possible and must be diagnosed through audit state and retried.
- **Deletion policy:** Missing source keys do not delete target rows. Deletion behavior
  is deferred until authoritative snapshot versus change-feed semantics are specified.
- **Downstream scope:** Silver and Gold remain full-snapshot transformations.
