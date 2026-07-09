# ABCD Detector RFC

## Purpose

This RFC resolves implementation blockers identified in `ABCD_IMPLEMENTATION_AUDIT.md` by defining deterministic implementation rules for the ABCD detector.

This document is intentionally limited to detector mechanics. It does not redesign the ABCD hypothesis, change thresholds, introduce datasets, run backtests, or alter strategy behavior.

## Non-Goals

- No implementation code.
- No research execution.
- No threshold changes.
- No hypothesis redesign.
- No strategy promotion or baseline modification.

## Canonical Data Model Assumptions

The detector consumes an ordered stream of source rows and emits candidate lifecycle records. Each source row is normalized before detector evaluation.

Required normalized source fields:

- `symbol`
- `timeframe`
- `timestamp`
- `source_row_ordinal`
- numeric price fields required by the existing ABCD hypothesis
- any existing swing/pivot classification fields required by the existing ABCD hypothesis

No new economic threshold, ratio, or strategy condition is introduced by this RFC.

## Deterministic Rules

### 1. Timestamp Parsing and Ordering

**Rule**

1. All input timestamps MUST be parsed as timezone-aware UTC instants.
2. Accepted timestamp inputs are:
   - timezone-aware ISO-8601 strings;
   - integer Unix epoch milliseconds;
   - integer Unix epoch seconds only when explicitly declared by the caller as seconds.
3. Timezone-naive strings are invalid source rows.
4. Parsed timestamps are represented internally as UTC epoch milliseconds.
5. Source rows MUST be sorted for detector processing by this total key:
   1. `symbol` ascending bytewise UTF-8;
   2. `timeframe` ascending bytewise UTF-8;
   3. `timestamp_ms` ascending numeric;
   4. `source_row_ordinal` ascending numeric.
6. If two rows have identical `symbol`, `timeframe`, `timestamp_ms`, and `source_row_ordinal`, the input is invalid because deterministic ordering cannot be proven.

**Determinism guarantee**

Every implementation parses to the same UTC millisecond value and uses the same complete tie-break chain before detector state is updated.

### 2. `source_row_ordinal` Scope

**Rule**

1. `source_row_ordinal` is scoped to the canonical input artifact after normalization and sorting.
2. The first canonical row in an artifact has ordinal `0`.
3. Ordinals increment by `1` across the entire artifact, not separately per symbol or timeframe.
4. A resumed run MUST reuse persisted ordinals from the original artifact manifest and MUST NOT recompute them from a filtered suffix.
5. Live mode MUST assign ordinals from the durable append log, not from transient process memory.

**Determinism guarantee**

Two engineers processing the same artifact manifest assign the same ordinal to every row, including rows for different symbols sharing the same timestamp.

### 3. Pivot Identity Generation

**Rule**

A `pivot_id` MUST be generated when a normalized source row is accepted as a pivot by existing detector logic.

Canonical format:

```text
pivot:v1:{symbol}:{timeframe}:{timestamp_ms}:{source_row_ordinal}:{pivot_role}:{price_decimal}
```

Where:

- `symbol` and `timeframe` are canonical strings after trimming outer whitespace and preserving case.
- `timestamp_ms` is the UTC epoch millisecond integer.
- `source_row_ordinal` is the artifact-scoped ordinal.
- `pivot_role` is the existing role/classification assigned by the detector.
- `price_decimal` is the canonical decimal string defined in the Numeric/Decimal Arithmetic section.

No random UUIDs, process-local counters, object addresses, or database surrogate keys may appear in `pivot_id`.

**Determinism guarantee**

The same accepted pivot row and role always produces the same identifier across machines, restarts, and replays.

### 4. Candidate Identity Generation

**Rule**

A `candidate_id` MUST be generated from the ordered pivot identity tuple that defines the candidate.

Canonical format:

```text
candidate:v1:{symbol}:{timeframe}:{A_pivot_id}:{B_pivot_id}:{C_pivot_id}:{direction}
```

If the existing hypothesis requires D before candidate materialization, `D_pivot_id` is appended after `C_pivot_id`. If the existing hypothesis materializes candidates before D, D MUST NOT be included in `candidate_id`; D-related updates are lifecycle events for the same candidate.

`candidate_id` MUST NOT include:

- discovery order;
- mutable lifecycle state;
- invalidation reason;
- wall-clock run time;
- file path;
- process id;
- row count outside the defining pivots.

**Determinism guarantee**

Two implementations that identify the same A/B/C pivot tuple and direction derive the same candidate key and therefore deduplicate or update the same logical candidate.

### 5. Rolling Window and Concurrent Candidate Policy

**Rule**

1. The rolling window is evaluated independently per `(symbol, timeframe)` state partition.
2. A window contains only pivots whose source rows have been accepted into that partition state.
3. The maximum age, pivot count, and hypothesis thresholds are exactly those already defined by the existing ABCD hypothesis; this RFC does not change them.
4. Multiple valid candidates may coexist when they have different `candidate_id` values.
5. A newer candidate MUST NOT evict, overwrite, or mutate an older active candidate unless an explicit lifecycle rule invalidates the older candidate.
6. When multiple candidate transitions are triggered by the same source row, transitions are computed against the pre-row active set and then emitted in the global output order defined below.
7. Candidate evaluation order within one `(symbol, timeframe, source_row_ordinal)` is ascending by `candidate_id`.

**Determinism guarantee**

Concurrent candidates are treated as a set keyed by `candidate_id`; no engineer can accidentally produce different output by using insertion order or a single active-candidate slot.

### 6. Candidate Lifecycle

**Rule**

A candidate has exactly one of these lifecycle states:

1. `OPEN`
2. `CONFIRMED`
3. `INVALIDATED`
4. `EXPIRED`
5. `ARTIFACT_OPEN`

Allowed transitions:

- `OPEN -> CONFIRMED`
- `OPEN -> INVALIDATED`
- `OPEN -> EXPIRED`
- `OPEN -> ARTIFACT_OPEN`
- `CONFIRMED -> INVALIDATED` only if existing detector rules already permit post-confirmation invalidation
- `CONFIRMED -> ARTIFACT_OPEN` if no terminal outcome is reached before artifact end

Disallowed transitions:

- Any transition out of `INVALIDATED`, `EXPIRED`, or `ARTIFACT_OPEN` within the same artifact.
- Any transition that changes candidate geometry after `candidate_id` is assigned.

Each emitted lifecycle record MUST include:

- `candidate_id`
- current `state`
- `event_type`
- `event_timestamp_ms`
- `event_source_row_ordinal`
- `reason`, using `null` when not applicable

**Determinism guarantee**

The state machine is finite and terminal states are absorbing, so independent implementations cannot reopen or mutate closed candidates differently.

### 7. Invalidation Reason Precedence

**Rule**

If multiple invalidation conditions are true on the same source row, the emitted `reason` MUST be the first applicable reason in this precedence order:

1. `INPUT_INVALID`
2. `TIMESTAMP_ORDER_VIOLATION`
3. `MISSING_REQUIRED_FIELD`
4. `NON_NUMERIC_REQUIRED_FIELD`
5. `GEOMETRY_BROKEN`
6. `BOUNDARY_BREACH`
7. `WINDOW_EXPIRED`
8. `ARTIFACT_END`

If an existing detector already names a more specific condition within one of these categories, that specific condition may be emitted as `reason_detail`, but `reason` MUST remain one of the canonical values above.

**Determinism guarantee**

When a row simultaneously triggers multiple failures, every implementation selects the same primary reason before emission.

### 8. Artifact-End Behavior

**Rule**

At end of artifact:

1. No synthetic future price action is created.
2. Every non-terminal active candidate emits exactly one terminal artifact-end lifecycle record.
3. If existing timeout/window rules mark the candidate expired at or before the final source row, emit `EXPIRED` with `WINDOW_EXPIRED`.
4. Otherwise emit `ARTIFACT_OPEN` with reason `ARTIFACT_END`.
5. The event timestamp for artifact-end records is the final processed row `timestamp_ms` for that candidate's `(symbol, timeframe)` partition.
6. The event source ordinal is the final processed row ordinal for that candidate's `(symbol, timeframe)` partition.
7. If the partition has no processed rows after candidate creation, use the candidate creation row as the event row.

**Determinism guarantee**

All active candidates receive exactly one deterministic closing record tied to the final available real row, not to process end time.

### 9. Restart/Resume Behavior

**Rule**

A resumed detector run MUST restore:

- artifact manifest identity;
- input normalization version;
- last fully committed `source_row_ordinal`;
- per `(symbol, timeframe)` rolling window state;
- all active candidates and lifecycle states;
- emitted record high-water mark keyed by global output ordering fields.

On resume:

1. Reprocess from the last committed ordinal plus one.
2. If durable state is unavailable, restart from ordinal `0` and reproduce the full artifact output.
3. Never resume from a timestamp alone.
4. If the artifact manifest hash differs from the persisted manifest hash, resume is forbidden; the run must be a new replay.
5. Emission is idempotent by `(candidate_id, event_type, event_source_row_ordinal, state, reason)`.

**Determinism guarantee**

A crash and resume produces the same final records as uninterrupted replay because resume uses ordinal and state snapshots rather than wall-clock or timestamp-only cursors.

### 10. Replay Prefix Boundary Rules

**Rule**

For replaying a prefix ending at ordinal `N`:

1. Process only rows with `source_row_ordinal <= N`.
2. Emit normal lifecycle records caused by those rows.
3. Apply artifact-end behavior at ordinal `N` for candidates still active in the prefix.
4. Prefix output MUST be identical to the output that a full replay would have produced up to and including ordinal `N`, except that active candidates at `N` are additionally closed as `ARTIFACT_OPEN` or `EXPIRED` for the prefix artifact.
5. A later replay with a longer prefix is a distinct artifact unless it uses persisted resume state from the shorter prefix before artifact-end closure.

**Determinism guarantee**

Engineers can compare partial replays because the prefix boundary is an explicit artificial artifact end with defined closure behavior.

### 11. Live/Replay Buffering Assumptions

**Rule**

1. Replay mode receives a complete immutable artifact and sorts before processing.
2. Live mode receives append-only events and MUST buffer per `(symbol, timeframe)` until an event is past the configured lateness horizon.
3. The lateness horizon is an operational parameter, not a strategy threshold; it must be recorded in the run manifest.
4. Within the committed live stream, events are processed by the same canonical ordering key used for replay.
5. A late event that arrives after its ordinal position has been committed is rejected with `TIMESTAMP_ORDER_VIOLATION` and MUST NOT mutate prior candidates.
6. Live output is guaranteed replay-equivalent only for the committed append log, not for uncommitted buffer contents.

**Determinism guarantee**

Replay and live mode share the same committed ordering semantics, and late data handling is explicit rather than implementation-dependent.

### 12. Numeric/Decimal Arithmetic

**Rule**

1. All detector geometry arithmetic MUST use base-10 decimal arithmetic, not binary floating point.
2. Input numeric strings are parsed as exact decimals.
3. Integer numeric fields are parsed as exact decimals with scale `0`.
4. Decimal context MUST support at least 28 significant digits.
5. Canonical decimal output uses plain notation with:
   - no thousands separators;
   - no scientific notation;
   - no leading plus sign;
   - no trailing zeros after the decimal point;
   - no decimal point when scale is zero;
   - `0` instead of `-0`.
6. Rounding is forbidden for comparisons unless the existing hypothesis already defines a rounding step.
7. If division is required for existing ratios, intermediate precision MUST use at least 28 significant digits and final comparison must use the exact unrounded decimal result.

**Determinism guarantee**

Every implementation compares the same decimal values and serializes identifiers from the same canonical decimal strings.

### 13. Boundary Comparisons

**Rule**

Boundary inclusivity MUST be explicit:

1. Existing threshold checks using “at least,” “minimum,” or “greater than or equal” are inclusive: `value >= threshold`.
2. Existing threshold checks using “at most,” “maximum,” or “less than or equal” are inclusive: `value <= threshold`.
3. Existing threshold checks using “greater than,” “above,” or “exceeds” are exclusive: `value > threshold`.
4. Existing threshold checks using “less than,” “below,” or “under” are exclusive: `value < threshold`.
5. If existing documentation is ambiguous, the implementation MUST choose inclusive comparison and record that choice in the run manifest.
6. Equality at an inclusive boundary passes; equality at an exclusive boundary fails.

**Determinism guarantee**

Values exactly on thresholds are no longer interpreted differently by different engineers.

### 14. Null and Missing Field Representation

**Rule**

1. Missing fields and explicit null fields are distinct during input validation.
2. In emitted output, both are represented as JSON `null` only for optional fields.
3. A missing required field invalidates the row with `MISSING_REQUIRED_FIELD`.
4. An explicit null in a required field invalidates the row with `MISSING_REQUIRED_FIELD`.
5. Empty strings in required numeric fields invalidate the row with `NON_NUMERIC_REQUIRED_FIELD`.
6. Optional fields that are absent are emitted as JSON `null` if the output schema includes them.
7. No sentinel strings such as `"NA"`, `"None"`, `"null"`, or empty string may represent null in output.

**Determinism guarantee**

Input validation and output serialization do not depend on language-specific missing-value conventions.

### 15. Output Record Emission Policy

**Rule**

1. The detector emits one record for each candidate lifecycle event.
2. Candidate discovery emits `event_type = CANDIDATE_OPENED` with state `OPEN`.
3. Confirmation emits `event_type = CANDIDATE_CONFIRMED` with state `CONFIRMED`.
4. Invalidation emits `event_type = CANDIDATE_INVALIDATED` with state `INVALIDATED`.
5. Expiration emits `event_type = CANDIDATE_EXPIRED` with state `EXPIRED`.
6. Artifact closure emits `event_type = CANDIDATE_ARTIFACT_OPEN` with state `ARTIFACT_OPEN`.
7. No duplicate lifecycle event may be emitted for the same `(candidate_id, event_type, event_source_row_ordinal, state, reason)` tuple.
8. Intermediate diagnostic records may be emitted only to a separate diagnostic artifact and MUST NOT be mixed into canonical detector output.

**Determinism guarantee**

The canonical output is a lifecycle event log with deduplicated event keys, not an implementation-specific debug trace.

### 16. Global Output Ordering

**Rule**

Canonical output records MUST be sorted by:

1. `event_timestamp_ms` ascending;
2. `event_source_row_ordinal` ascending;
3. `symbol` ascending bytewise UTF-8;
4. `timeframe` ascending bytewise UTF-8;
5. `candidate_id` ascending bytewise UTF-8;
6. `event_type` ascending bytewise UTF-8;
7. `state` ascending bytewise UTF-8;
8. `reason`, with `null` sorting before strings and strings sorted bytewise UTF-8.

Writers MUST apply this ordering before persisting canonical output, even if internal processing was partitioned or parallelized.

**Determinism guarantee**

Parallel and single-threaded implementations produce byte-stable record order.

## Audit FAIL Resolution Matrix

### FAIL: rolling window / concurrent candidate policy was underspecified

**Issue quote**: “rolling window / concurrent candidate policy” was an implementation blocker.

**Final deterministic rule**: Rolling windows are partitioned per `(symbol, timeframe)`, concurrent candidates coexist by unique `candidate_id`, candidate evaluation order is ascending `candidate_id`, and new candidates do not overwrite older active candidates except through explicit lifecycle transitions.

**Why identical output follows**: Both engineers maintain the same active candidate set and process same-row candidate transitions in the same sorted order.

### FAIL: candidate lifecycle was underspecified

**Issue quote**: “candidate lifecycle” was an implementation blocker.

**Final deterministic rule**: Candidates use the finite state machine `OPEN`, `CONFIRMED`, `INVALIDATED`, `EXPIRED`, and `ARTIFACT_OPEN`, with explicit allowed and disallowed transitions.

**Why identical output follows**: Terminal states are absorbing and every lifecycle mutation maps to exactly one event type.

### FAIL: `candidate_id` generation was underspecified

**Issue quote**: “candidate_id generation” was an implementation blocker.

**Final deterministic rule**: `candidate_id` is generated from canonical symbol, timeframe, ordered defining pivot IDs, and direction, excluding mutable or runtime-specific fields.

**Why identical output follows**: The same candidate geometry produces the same key regardless of runtime order, restart, or storage backend.

### FAIL: `pivot_id` generation was underspecified

**Issue quote**: “pivot_id generation” was an implementation blocker.

**Final deterministic rule**: `pivot_id` is generated from canonical symbol, timeframe, timestamp, source ordinal, pivot role, and canonical decimal price.

**Why identical output follows**: Pivots are identified by immutable source facts and detector-assigned role, not by process-local counters.

### FAIL: timestamp parsing and ordering were underspecified

**Issue quote**: “timestamp parsing and ordering” was an implementation blocker.

**Final deterministic rule**: Timestamps parse to timezone-aware UTC epoch milliseconds and rows sort by symbol, timeframe, timestamp, then source ordinal.

**Why identical output follows**: All implementations evaluate rows in the same order and reject ambiguous timestamp inputs.

### FAIL: `source_row_ordinal` scope was underspecified

**Issue quote**: “source_row_ordinal scope” was an implementation blocker.

**Final deterministic rule**: `source_row_ordinal` is artifact-scoped after canonical normalization and sorting, starting at zero and incrementing across the entire artifact.

**Why identical output follows**: Ordinal assignment is not partition-local, filter-local, or process-local.

### FAIL: global output ordering was underspecified

**Issue quote**: “global output ordering” was an implementation blocker.

**Final deterministic rule**: Canonical output sorts by event timestamp, event ordinal, symbol, timeframe, candidate ID, event type, state, and reason.

**Why identical output follows**: Output order is independent of thread scheduling, dictionary iteration, or database insertion order.

### FAIL: invalidation reason precedence was underspecified

**Issue quote**: “invalidation reason precedence” was an implementation blocker.

**Final deterministic rule**: Simultaneous invalidations use the fixed precedence list beginning with `INPUT_INVALID` and ending with `ARTIFACT_END`.

**Why identical output follows**: Every implementation chooses the same primary reason when multiple conditions are true on one row.

### FAIL: artifact-end behavior was underspecified

**Issue quote**: “artifact-end behavior” was an implementation blocker.

**Final deterministic rule**: Active candidates emit exactly one final `EXPIRED` or `ARTIFACT_OPEN` lifecycle record tied to the final real row in the candidate partition.

**Why identical output follows**: End-of-file behavior no longer depends on whether an implementation drops, silently preserves, or synthetically resolves active candidates.

### FAIL: restart/resume behavior was underspecified

**Issue quote**: “restart/resume behavior” was an implementation blocker.

**Final deterministic rule**: Resume restores durable state and continues from last committed ordinal plus one; timestamp-only resume is forbidden.

**Why identical output follows**: Crash recovery and uninterrupted replay converge to the same state and emitted event set.

### FAIL: replay prefix boundary rules were underspecified

**Issue quote**: “replay prefix boundary rules” was an implementation blocker.

**Final deterministic rule**: Prefix replay processes rows through ordinal `N`, emits normal events, then applies artifact-end closure at `N`.

**Why identical output follows**: Partial artifacts have a deterministic terminal boundary instead of relying on full-artifact future data.

### FAIL: live/replay buffering assumptions were underspecified

**Issue quote**: “live/replay buffering assumptions” was an implementation blocker.

**Final deterministic rule**: Replay sorts a complete immutable artifact; live mode buffers until lateness horizon and commits by canonical order, rejecting late committed-position mutations.

**Why identical output follows**: Live and replay share committed ordering semantics, and late events have a fixed non-mutating outcome.

### FAIL: numeric/decimal arithmetic was underspecified

**Issue quote**: “numeric/decimal arithmetic” was an implementation blocker.

**Final deterministic rule**: Geometry arithmetic uses exact base-10 decimals with canonical decimal serialization and no implicit rounding.

**Why identical output follows**: Boundary checks and IDs are not affected by binary floating-point representation or locale formatting.

### FAIL: boundary comparisons were underspecified

**Issue quote**: “boundary comparisons” was an implementation blocker.

**Final deterministic rule**: Inclusive and exclusive comparisons are mapped explicitly from threshold language, with ambiguous thresholds defaulting to inclusive and recorded in the manifest.

**Why identical output follows**: Equality at each boundary has one defined pass/fail outcome.

### FAIL: null/missing field representation was underspecified

**Issue quote**: “null/missing field representation” was an implementation blocker.

**Final deterministic rule**: Missing and explicit null are distinct during validation; optional nulls emit as JSON `null`; sentinel strings are forbidden.

**Why identical output follows**: Serialization and validation do not vary by language or dataframe library conventions.

### FAIL: output record emission policy was underspecified

**Issue quote**: “output record emission policy” was an implementation blocker.

**Final deterministic rule**: Canonical output is a deduplicated lifecycle event log with one record per lifecycle event and diagnostics separated from canonical output.

**Why identical output follows**: Implementations agree on which records belong in the canonical detector artifact.

## Readiness Status

Engineering implementation:
IMPLEMENTATION_READY

Scientific hypothesis:
NOT_VALIDATED

Production deployment:
NOT_APPROVED

Interpretation:

- The detector specification is deterministic and sufficiently defined for independent engineers to implement byte-identical behavior.
- This RFC does NOT claim that the ABCD hypothesis has been validated.
- This RFC does NOT establish profitability, expectancy, robustness, or production suitability.
- Production implementation remains gated by successful completion of the validation plan described in `ABCD_VALIDATION_PLAN.md`.
