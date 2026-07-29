# CDC reconciliation runbook

This runbook describes the local synthetic reference. Production thresholds and
ownership must be defined for the actual platform.

## Successful run criteria

- `reconciliation_passed` is `true`;
- `difference_count` is zero;
- all delivered event IDs appear once in the event ledger after batch dedupe;
- quarantine volume matches understood contract exceptions; and
- the run manifest exists after all target files.

## Reconciliation failure

1. Preserve the events, source snapshot, ledger, and history.
2. Classify differences as missing target keys, unexpected target keys, or field
   mismatches.
3. Trace each entity through `source_event_id` and `source_sequence`.
4. Check whether source snapshot time and consumed CDC boundary are aligned.
5. Correct logic or replay boundaries in a reviewed change with a regression
   fixture.
6. Rebuild in an isolated output directory and compare before promotion.

Never “fix” a mismatch by deleting evidence or editing the target manually.

## Quarantine increase

Group by `reason`, schema version, and source identity. Unknown versions should
remain quarantined until their compatibility rules are reviewed. A compatible
additive field requires:

1. updated versioned contract;
2. explicit default/backfill semantics;
3. SCD2 merge behavior;
4. tests for old and new versions; and
5. controlled replay of quarantined identities.

## Interrupted run

Individual files are atomically promoted, and the final manifest is the commit
marker. If it is absent or inconsistent, rerun the same bounded events. The
ledger prevents duplicate effects already recorded. In production, prefer a
single transactional commit for state and offset.

## Ledger/history disagreement

Do not advance the source boundary. Reconstruct expected history from retained
events in a separate location, compare checksums and entity histories, then
repair using an audited transaction. Ledger retention must cover the maximum
source replay window.

## Suggested production observability

These are design targets, not measured claims for this demo:

- CDC lag by source partition;
- delivered, deduplicated, stale, applied, and quarantined event rates;
- schema version distribution;
- active-row uniqueness;
- reconciliation difference count and age;
- SCD rows per entity and small-file pressure; and
- ledger/state commit duration and failure rate.

