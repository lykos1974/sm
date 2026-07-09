# Observations Registry

## Purpose
Research observations extracted from source artifacts.

## Required Fields
- `id`
- `title` or `name`
- `status`
- `source_artifacts`
- `related_records`
- `created_at`
- `updated_at`
- `owner`
- `confidence` or `classification` where relevant
- `notes`
- `unknown_lost_never_recorded` where relevant

## Status Values
DRAFT, OBSERVED, NEEDS_REVIEW, SUPERSEDED, DISCARDED

## Relationship Fields
Use `related_records` to link upstream and downstream records. Preserve lineage across Observation → Belief → Experiment/Hypothesis → Validation Run → Evidence → Decision → Knowledge.

## Must NOT Store
Do not store executable code, detector logic, strategy rules, validation outputs, datasets, exports, or database files here.
