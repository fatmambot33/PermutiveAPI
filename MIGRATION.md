# Migration Guide

## Canonical client

New code should create one `PermutiveClient` and access resource families from it:

```python
from PermutiveAPI import PermutiveClient

client = PermutiveClient("api-key")
cohort = client.cohorts.get("cohort-id")
segments = client.segments.list(page_size=100)
```

The canonical client provides shared authentication, timeouts, retries, decoding, pagination, batching primitives, and structured SDK errors.

## Compatibility exports

The package-root resource classes such as `Cohort`, `Import`, `Segment`, `Workspace`, `Identity`, `Segmentation`, and `ContextSegment` remain supported for existing applications. They are compatibility APIs rather than the direction for new resource development.

No compatibility export will be removed without:

1. a runtime warning where practical;
2. a changelog entry;
3. a documented replacement;
4. at least one minor-release deprecation period;
5. removal in a major release only.

## Errors

New code should catch the structured `SDKError` hierarchy. Legacy `PermutiveAPIError` classes remain available for compatibility.

## Pagination

Use `Resource.list()` for one page or `Resource.iter_all()` for bounded lazy iteration. Prefer `max_items` for jobs that must cap work.

## Batch execution

Use `execute_batch()` for generic ordered bounded work. Existing resource-specific `batch_*` helpers remain available while they are migrated to the canonical execution primitive.
