# PermutiveAPI

PermutiveAPI is a Python module to interact with the Permutive API. It provides a set of classes and methods to manage users, imports, cohorts, and workspaces within the Permutive ecosystem.

## Table of Contents

- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
  - [Importing the Module](#importing-the-module)
  - [Managing Workspaces](#managing-workspaces)
  - [Managing Cohorts](#managing-cohorts)
  - [Managing Segments](#managing-segments)
- [Managing Imports](#managing-imports)
- [Managing Users](#managing-users)
- [Working with pandas DataFrames](#working-with-pandas-dataframes)
- [Batch Helpers and Progress Callbacks](#batch-helpers-and-progress-callbacks)
- [Error Handling](#error-handling)
- [Development](#development)
- [Contributing](#contributing)
- [License](#license)

## Installation

You can install the PermutiveAPI module using pip:

```sh
pip install PermutiveAPI --upgrade
```

> **Note**
> PermutiveAPI depends on [`pandas`](https://pandas.pydata.org/) for its DataFrame
> export helpers. The dependency is installed automatically with the package,
> but make sure your runtime environment includes it before using the
> `to_pd_dataframe` utilities described below.

## Configuration

Before using the library, you need to configure your credentials.

1.  **Copy the environment file**:
    ```sh
    cp _env .env
    ```
2.  **Set your credentials path**:
    Edit the `.env` file and set the `PERMUTIVE_APPLICATION_CREDENTIALS` environment variable to the absolute path of your workspace JSON file.
    ```sh
    PERMUTIVE_APPLICATION_CREDENTIALS="/absolute/path/to/your/workspace.json"
    ```
The workspace credentials JSON can be downloaded from the Permutive dashboard under **Settings → API keys**. Save the file somewhere secure. The `apiKey` inside this JSON is used to authenticate API calls.

## Usage

### Importing the Module

To use the PermutiveAPI module, import the necessary classes. The main classes are exposed at the top level of the `PermutiveAPI` package:

```python
from PermutiveAPI import (
    Alias,
    Cohort,
    Identity,
    Import,
    Segment,
    Source,
    Workspace,
)
```

### Managing Workspaces

The `Workspace` class is the main entry point for interacting with your Permutive workspace.

```python
# Create a workspace instance
workspace = Workspace(
    name="Main",
    organisation_id="your-org-id",
    workspace_id="your-workspace-id",
    api_key="your-api-key",
)

# List all cohorts in a workspace (includes child workspaces)
all_cohorts = workspace.cohorts()
for cohort in all_cohorts:
    print(f"Cohort ID: {cohort.id}, Name: {cohort.name}")

# List all imports in a workspace
all_imports = workspace.imports()
for imp in all_imports:
    print(f"Import ID: {imp.id}, Name: {imp.name}")

# List segments for a specific import
segments_in_import = workspace.segments(import_id="your-import-id")
for segment in segments_in_import:
    print(f"Segment ID: {segment.id}, Name: {segment.name}")
```

### Managing Cohorts

You can create, retrieve, and list cohorts using the `Cohort` class.

```python
# List all cohorts
all_cohorts = Cohort.list(api_key="your_api_key")
print(f"Found {len(all_cohorts)} cohorts.")

# Get a specific cohort by ID
cohort_id = "your-cohort-id"
cohort = Cohort.get_by_id(id=cohort_id, api_key="your_api_key")
print(f"Retrieved cohort: {cohort.name}")

# Create a new cohort
new_cohort = Cohort(
    name="High-Value Customers",
    query={"type": "segment", "id": "segment-id-for-high-value-customers"}
)
new_cohort.create(api_key="your_api_key")
print(f"Created cohort with ID: {new_cohort.id}")
```

### Managing Segments

The `Segment` class allows you to interact with audience segments.

```python
# List all segments for a given import
import_id = "your-import-id"
segments = Segment.list(api_key="your_api_key", import_id=import_id)
print(f"Found {len(segments)} segments in import {import_id}.")

# Get a specific segment by ID
segment_id = "your-segment-id"
segment = Segment.get_by_id(import_id=import_id, segment_id=segment_id, api_key="your_api_key")
print(f"Retrieved segment: {segment.name}")
```

### Managing Imports

You can list and retrieve imports using the `Import` class.

```python
# List all imports
all_imports = Import.list(api_key="your_api_key")
for imp in all_imports:
    print(f"Import ID: {imp.id}, Code: {imp.code}, Source Type: {imp.source.type}")

# Get a specific import by ID
import_id = "your-import-id"
import_instance = Import.get_by_id(id=import_id, api_key="your_api_key")
print(f"Retrieved import: {import_instance.id}, Source Type: {import_instance.source.type}")
```

### Managing Users

The `Identity` and `Alias` classes are used to manage user profiles.

```python
# Create an alias for a user
alias = Alias(id="user@example.com", tag="email", priority=1)

# Create an identity for the user
identity = Identity(user_id="internal-user-id-123", aliases=[alias])

# Send the identity information to Permutive
try:
    identity.identify(api_key="your-api-key")
    print("Successfully identified user.")
except Exception as e:
    print(f"Error identifying user: {e}")

```

### Working with pandas DataFrames

The list models expose helpers for quick DataFrame exports when you need to
analyze your data using pandas. Each list class provides a `to_pd_dataframe`
method that returns a `pandas.DataFrame` populated with the model attributes:

```python
from PermutiveAPI import Cohort, CohortList

cohorts = CohortList(
    [
        Cohort(name="C1", id="1", code="c1", tags=["t1"]),
        Cohort(name="C2", id="2", description="second cohort"),
    ]
)

df = cohorts.to_pd_dataframe()
print(df[["id", "name"]])
```

The same helper is available on `SegmentList` and `ImportList` for consistency
across the API.

### Batch Helpers and Progress Callbacks

High-volume workflows often rely on the ``batch_*`` helpers to run requests
concurrently. Every helper accepts an optional ``progress_callback`` that is
invoked after each request completes with a
:class:`~PermutiveAPI._Utils.http.Progress` snapshot describing aggregate
throughput. The dataclass includes counters for completed requests, failure
totals, elapsed time, and the estimated seconds required to process 1,000
requests, making it straightforward to surface both reliability and latency
trends in dashboards or logs. Most workloads achieve a good balance between
throughput and API friendliness with ``max_workers=4``. Increase the pool size
gradually (for example to 6 or 8 workers) only after observing stable latency
and error rates because the Permutive API enforces rate limits.

```python
from PermutiveAPI import Cohort
from PermutiveAPI._Utils.http import Progress


def on_progress(progress: Progress) -> None:
    avg = progress.average_per_thousand_seconds
    avg_display = f"{avg:.2f}s" if avg is not None else "n/a"
    print(
        f"{progress.completed}/{progress.total} "
        f"(errors: {progress.errors}, avg/1000: {avg_display}): "
        f"{progress.batch_request.method} {progress.batch_request.url}"
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
        print("Retry or inspect:", failed_request.url, error)
```

The same callback shape is shared across helpers such as
``Identity.batch_identify`` and ``Segment.batch_create``, enabling reuse of
progress reporting utilities that surface throughput, error counts, and
latency projections. The helpers delegate to
:func:`PermutiveAPI._Utils.http.process_batch`, so they automatically benefit
from the shared retry/backoff configuration used by the underlying request
helpers. When the API responds with ``HTTP 429`` (rate limiting), the helper
retries using the exponential backoff already built into the package before
surfacing the error in the ``failures`` list.

#### Configuring batch defaults

Two environment variables allow you to tune the default behaviour without
touching application code:

- ``PERMUTIVE_BATCH_MAX_WORKERS`` controls the worker pool size used by the
  shared batch runner when ``max_workers`` is omitted. Provide a positive
  integer to cap concurrency or leave it unset to use Python's default
  heuristic.
- ``PERMUTIVE_BATCH_TIMEOUT_SECONDS`` controls the default timeout applied to
  each ``PermutiveAPI._Utils.http.BatchRequest``. Set it to a positive
  float (in seconds) to align the HTTP timeout with your infrastructure's
  expectations.

Invalid values raise ``ValueError`` during initialisation to surface mistakes
early in the development cycle.

#### Configuring retry defaults

Transient failure handling can also be adjusted through environment variables.
When unset, the package uses the standard ``RetryConfig`` defaults.

- ``PERMUTIVE_RETRY_MAX_RETRIES`` sets the number of attempts performed by the
  HTTP helpers before surfacing an error. Provide a positive integer.
- ``PERMUTIVE_RETRY_BACKOFF_FACTOR`` controls the exponential multiplier applied
  after each failed attempt. Provide a positive number (floats are accepted).
- ``PERMUTIVE_RETRY_INITIAL_DELAY_SECONDS`` specifies the starting delay in
  seconds before retrying. Provide a positive number.

Supplying invalid values for any of these variables raises ``ValueError`` when
the retry configuration is evaluated, helping catch misconfiguration early.

Segmentation workflows follow the same pattern. For example, you can create
multiple segments for a given import in one request batch while reporting
progress back to an observability system:

```python
from PermutiveAPI import Segment


segments = [
    Segment(
        import_id="import-123",
        name="Frequent Flyers",
        query={"type": "users", "filter": {"country": "US"}},
    ),
    Segment(
        import_id="import-123",
        name="Dormant Subscribers",
        query={"type": "users", "filter": {"status": "inactive"}},
    ),
]

segment_responses, segment_failures = Segment.batch_create(
    segments,
    api_key="your-api-key",
    max_workers=4,
    progress_callback=on_progress,
)

if segment_failures:
    for failed_request, error in segment_failures:
        print("Segment creation retry candidate:", failed_request.url, error)
```

### Error Handling

The package raises purpose-specific exceptions that are also available at the
top level of the package for convenience:

```python
from PermutiveAPI import (
    PermutiveAPIError,
    PermutiveAuthenticationError,
    PermutiveBadRequestError,
    PermutiveRateLimitError,
    PermutiveResourceNotFoundError,
    PermutiveServerError,
)

try:
    # make an API call via the high-level classes
    Cohort.list(api_key="your_api_key")
except PermutiveBadRequestError as e:
    # e.status, e.url, and e.response are available for debugging
    print(e.status, e.url, e)
except PermutiveAPIError as e:
    print("Unhandled API error:", e)
```
```

## Development

To set up a development environment, install the required dependencies:

```sh
pip install -r requirements-dev.txt
```

### Running Tests

Before committing any changes, please run the following checks to ensure code quality and correctness.

**Style Checks:**
```bash
pydocstyle PermutiveAPI
black --check .
```

**Static Type Analysis:**
```bash
pyright PermutiveAPI
```

**Unit Tests and Coverage:**
```bash
pytest -q --cov=PermutiveAPI --cov-report=term-missing --cov-fail-under=70
```

All checks must pass before a pull request can be merged.

## Contributing

Contributions are welcome! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for development setup and pull request guidelines.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.
