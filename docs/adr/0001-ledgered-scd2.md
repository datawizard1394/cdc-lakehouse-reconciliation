# ADR 0001: Ledger CDC outcomes and materialize SCD2 incrementally

- **Status:** Accepted
- **Date:** 2026-07-28

## Context

At-least-once delivery can duplicate events, events can arrive out of order, and
schema evolution can make some events unsafe to apply. A portfolio demo must
show observable behavior for each case without claiming distributed guarantees
it does not provide.

## Decision

Use event ID as the replay identity and source sequence as the authoritative
per-entity order. Persist an outcome ledger for both applied and quarantined
events. Sort unseen events deterministically, validate versioned contracts, and
apply valid changes to an SCD Type 2 history. Close the active record on UPDATE
or DELETE; open a merged replacement only for INSERT or UPDATE.

Reconcile the resulting active state to a separately generated authoritative
snapshot on every run.

## Consequences

Benefits:

- replay has no duplicate target effect;
- quarantine itself is idempotent;
- ordering and delete semantics are reviewable;
- every delivered identity has a durable outcome; and
- source/target drift is reported at key and field level.

Trade-offs:

- the local files are not a distributed transaction;
- a crash between atomic file promotions can leave a run without its final
  manifest, requiring safe replay;
- source sequence must be trustworthy; and
- ledger retention must be at least as long as source replay retention.

## Production adaptation

Use a transactional lakehouse merge and commit source offsets plus event ledger
state atomically. Partition history by business access patterns, compact small
files, expire ledger entries only after replay risk closes, and run both
incremental hash reconciliation and scheduled full reconciliation.

