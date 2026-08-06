# Local integration testing

PermutiveAPI ships a standard-library loopback server for deterministic SDK and agent integration tests. It never contacts Permutive and requires no credentials.

## Start the canonical fixtures

```python
from PermutiveAPI import PermutiveClient, RetryPolicy
from PermutiveAPI.testing import MockPermutiveServer

retry = RetryPolicy(
    max_attempts=3,
    initial_delay=0.001,
    multiplier=1.0,
    max_delay=0.001,
    jitter=0.0,
)

with MockPermutiveServer.standard() as server:
    with PermutiveClient(
        "test-api-key",
        base_url=server.base_url,
        retry_policy=retry,
    ) as client:
        payload = client.request("GET", "v1/success")

    requests = server.requests
```

The server binds only to `127.0.0.1` on an operating-system-selected port. Request logs are immutable snapshots and the server releases its socket when the context exits.

## Version 1 scenarios

`mock_fixtures/v1.json` is the machine-readable catalog for:

- success and creation;
- request validation failure;
- authentication failure;
- not found;
- conflict;
- rate-limit recovery;
- temporary server recovery;
- persistent server failure;
- two-page iteration;
- repeated continuation-token protection.

Queued responses let the real retry implementation receive a failure followed by success. The final response remains reusable, which keeps repeated tests deterministic.

## Custom routes

```python
from PermutiveAPI.testing import MockPermutiveServer, MockResponse, MockRoute

routes = (
    MockRoute(
        method="GET",
        path="/custom",
        responses=(MockResponse(body={"state": "custom"}),),
    ),
)

with MockPermutiveServer(routes) as server:
    print(server.url("/custom"))
```

Routes are exact method-and-path matches. Query parameters and JSON bodies are captured in `MockRequest` records. Unknown routes return a deterministic JSON `404` response.

## Sync and async parity

The same server accepts both `PermutiveClient` and `AsyncPermutiveClient`. Tests should run equivalent success, error, retry, and pagination assertions against both clients rather than maintaining separate fake transports.

## Fixture safety

Fixture content must use synthetic identifiers and payloads. Do not copy production requests, credentials, workspace identifiers, or customer data into the catalog. Live integration tests remain opt-in under the `integration` marker.
