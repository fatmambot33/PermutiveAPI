# Structured diagnostics

PermutiveAPI exposes framework-neutral transport wrappers for safe request diagnostics. The wrappers are optional and add no cost unless explicitly installed around a transport.

```python
import logging
import requests

from PermutiveAPI import PermutiveClient
from PermutiveAPI.diagnostics import DiagnosticTransport

logger = logging.getLogger("permutive")
transport = DiagnosticTransport(requests.Session(), lambda event: logger.info("permutive_request", extra={"permutive": event}))
client = PermutiveClient("api-key", transport=transport)
```

Use `AsyncDiagnosticTransport` around an async transport for the same event contract.

Events contain only the request phase, HTTP method, endpoint without query parameters, attempt number, duration, status, request ID, and exception type. Request payloads, query parameters, credentials, and exception messages are never emitted.
