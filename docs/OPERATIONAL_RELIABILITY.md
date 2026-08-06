# Operational Reliability

PermutiveAPI 6.7 makes upstream contracts, replay safety, coordinated runtime behavior, performance budgets, and release identity executable.

## Versioned API contracts

`contracts/api-samples-v1.json` is the maintained source evidence. It generates:

- `contracts/api-contract-v1.json` for machine consumers;
- `API_COVERAGE.md` for maintainers;
- stable structural fingerprints for each supported operation.

Validate both generated files with:

```bash
python scripts/generate_api_contracts.py --check
```

`classify_response_schema()` reports `none`, `additive`, or `breaking`. Additive fields remain compatible. Removed fields, removed response variants, and type changes are breaking. `validate_response_schema()` raises `SchemaDriftError` only for breaking drift.

## Sanitized recording and replay

`RecordingTransport` captures only:

- HTTP method;
- URL without query strings or fragments;
- status code;
- an allow-list of response headers;
- recursively redacted JSON response values.

It never stores request bodies, API keys, authorization headers, cookies, or query parameters. `ReplayTransport` then exercises the canonical client without a network request.

```python
from pathlib import Path

from PermutiveAPI import PermutiveClient, Recording, ReplayTransport, RetryPolicy

recording = Recording.read(Path("recordings/core-v1.json"))
transport = ReplayTransport(recording)
client = PermutiveClient(
    "local-placeholder",
    base_url="https://api.permutive.test",
    retry_policy=RetryPolicy(max_attempts=1),
    transport=transport,
)
result = client.request("GET", "v1/cohorts")
```

Validate committed recordings with:

```bash
python scripts/validate_recordings.py
```

## Coordinated rate limits and atomic rotation

One `AtomicCredentials` and one `RateLimitCoordinator` can be shared across synchronous and asynchronous transports.

```python
import requests

from PermutiveAPI import (
    AtomicCredentials,
    CoordinatedTransport,
    PermutiveClient,
    RateLimitCoordinator,
)

credentials = AtomicCredentials("initial-key")
coordinator = RateLimitCoordinator(requests_per_second=10)
transport = CoordinatedTransport(requests.Session(), credentials, coordinator)
client = PermutiveClient("managed-placeholder", transport=transport)

client.request("GET", "v1/cohorts")
credentials.rotate("rotated-key")
client.request("GET", "v1/cohorts")
```

The wrapper replaces the placeholder at the transport boundary. Every attempt uses one immutable credential generation. A rotation changes future attempts without mutating an in-flight attempt. HTTP 429 `Retry-After` values defer every caller sharing the coordinator.

`CoordinatedAsyncTransport` provides the same behavior for `AsyncPermutiveClient`. Cancellation propagates normally and is never converted into a retry.

## Performance budgets

`benchmarks/budgets-v1.json` defines generous local regression limits for contract generation, query serialization, recipe discovery, and recording loading.

```bash
python scripts/validate_performance.py
```

The budgets detect major regressions rather than compare machines. The validator warms each operation, measures repeated executions, and evaluates the median.

## Live integration

Default tests are network-free. Live testing is manual through `.github/workflows/live-integration.yml` and the protected `permutive-live` environment.

The environment must provide:

- secret `PERMUTIVE_API_KEY`;
- variable `PERMUTIVE_LIVE_READ_PATH` containing an explicitly approved read-only endpoint.

No credential or live endpoint is committed to the repository.

## Immutable releases

The release workflow builds distributions once and creates:

- wheel and source distribution;
- reproducible CycloneDX SBOM;
- `release-manifest.json` containing project, version, source commit, size, and SHA-256 for every distribution and the SBOM;
- GitHub artifact attestations.

The manifest is verified after build, before Trusted Publishing, and before GitHub Release creation. PyPI and GitHub therefore receive the exact candidate that passed release validation.
