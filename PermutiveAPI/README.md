# PermutiveAPI Package

This directory contains the core source code for the `PermutiveAPI` Python package. It is the central hub for all functionality related to the Permutive API.

## Structure

The package is organized into several modules and sub-packages, each responsible for a specific part of the Permutive API:

-   **`Workspace.py`**: The primary entry point for most interactions. The `Workspace` class allows you to list resources like cohorts, imports, and segments.
-   **`Cohort.py`**: Provides the `Cohort` and `CohortList` classes for creating, retrieving, and managing cohorts. Cohorts are often managed within a `Workspace`.
-   **`Audience/`**: A sub-package for managing audience-related resources.
    -   `Import.py`: Manages data imports.
    -   `Segment.py`: Manages audience segments, which are often tied to an `Import`.
    -   `Source.py`: Represents the source of an import.
-   **`Identify/`**: A sub-package for user identification.
    -   `Identity.py` and `Alias.py`: Manage user identities and their aliases.
-   **`Utils.py`**: Contains utility functions, such as custom JSON encoders and request handlers, used across the package.
-   **`errors.py`**: Defines custom exception classes (`PermutiveAPIError`, `PermutiveRateLimitError`, etc.) to provide detailed error information.

## Convenience Imports

For ease of use, the most common classes from the sub-packages and modules are exposed directly at the top level of the `PermutiveAPI` package. This means you can import them as follows:

```python
from PermutiveAPI import Workspace, Cohort, Segment, Import, Identity, Alias
```

This avoids the need for longer imports like `from PermutiveAPI.Audience.Segment import Segment`.

## Batch helpers

The high-volume helpers such as `Cohort.batch_create`, `Segment.batch_create`,
and `Identity.batch_identify` delegate to the shared
`PermutiveAPI._Utils.http.process_batch` runner. Each helper accepts
`max_workers` to tune the thread pool and `progress_callback` to surface
aggregate throughput metrics while the batch runs.

```python
from PermutiveAPI import Cohort
from PermutiveAPI._Utils.http import Progress


def on_progress(progress: Progress) -> None:
    avg = progress.average_per_thousand_seconds
    avg_display = f"{avg:.2f}s" if avg is not None else "n/a"
    print(
        f"{progress.completed}/{progress.total} "
        f"(errors: {progress.errors}, avg/1000: {avg_display})"
    )


cohorts = [
    Cohort(name="VIP Customers", query={"type": "users"}),
    Cohort(name="Returning Visitors", query={"type": "visitors"}),
]

responses, failures = Cohort.batch_create(
    cohorts,
    api_key="your-api-key",
    max_workers=4,  # recommended starting point for concurrent writes
    progress_callback=on_progress,
)

if failures:
    for failed_request, error in failures:
        print("Inspect retry candidate:", failed_request.url, error)
```

Starting with four workers typically provides a good balance between
throughput and Permutive's `HTTP 429` rate limits. Increase the worker count
gradually only after monitoring latency and error rates. Because the helpers
reuse the package-wide retry/backoff strategy, they automatically retry failed
requests (including `429` responses) before reporting them in the `failures`
list.

For detailed usage instructions and installation, please refer to the main `README.md` in the root of the repository.
