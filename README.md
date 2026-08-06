# PermutiveAPI

[![PyPI version](https://img.shields.io/pypi/v/PermutiveAPI.svg)](https://pypi.org/project/PermutiveAPI/)
[![Python versions](https://img.shields.io/pypi/pyversions/PermutiveAPI.svg)](https://pypi.org/project/PermutiveAPI/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

PermutiveAPI is a typed, governed Python SDK and AI-agent platform for the Permutive API. It supports Python 3.9 through 3.13 and provides synchronous and asynchronous clients, typed queries, a safe Codex plugin, deterministic evaluations and scenarios, executable recipes, capability negotiation, generated API contracts, sanitized replay, coordinated runtime resilience, and immutable release evidence.

## Install

```bash
python -m pip install --upgrade PermutiveAPI
```

Optional integrations:

```bash
python -m pip install --upgrade "PermutiveAPI[async]"
python -m pip install --upgrade "PermutiveAPI[dataframe]"
```

The core package does not require HTTPX or pandas.

## Configure safely

```bash
permutiveapi configure
permutiveapi doctor
permutiveapi validate
permutiveapi eval
```

`configure` writes only `PERMUTIVE_API_KEY` to a local `.env` file without echoing it. `doctor` verifies the variable, permissions, and Git ignore policy. `validate` and `eval` are credential-free and network-free.

Credential lookup order is explicit input, process environment, project `.env`, then `~/.config/permutive/.env`. Credentials are never uploaded, logged, or included in object representations.

## Canonical client

```python
from PermutiveAPI import PermutiveClient

with PermutiveClient("api-key") as client:
    cohort = client.cohorts.get("cohort-id")
    page = client.segments.list(page_size=50)
```

Canonical resource namespaces are `cohorts`, `imports`, `segments`, `sources`, and `workspaces`. Their 25 CRUD/list operations and structural response fingerprints are generated in [API_COVERAGE.md](API_COVERAGE.md).

## Async client

Install `PermutiveAPI[async]`:

```python
import asyncio

from PermutiveAPI import AsyncPermutiveClient


async def main() -> None:
    async with AsyncPermutiveClient("api-key") as client:
        result = await client.request("GET", "cohorts-api/v2/cohorts")
        print(result)


asyncio.run(main())
```

The async client shares typed JSON, errors, retries, pagination, and bounded-batch semantics with the synchronous SDK.

## Typed queries

```python
from PermutiveAPI import all_of, event, in_segment

query = all_of((event("pageview"), in_segment("high-intent")))
payload = query.to_json()
```

## Governed agent platform

The Codex plugin and Python integration reuse the canonical SDK:

```python
from PermutiveAPI.plugins.codex import CodexPlugin

plugin = CodexPlugin.from_env()
tools = plugin.tools().as_openai_tools()
agent_kit = plugin.agent_kit()
```

Read-only is the default. Write tools require explicit read-write mode and confirmation. Adaptive integrations negotiate capabilities before execution. `CodexPlugin.invoke_safe()` returns stable secret-safe error codes, retryability, recommended actions, and safe context.

Run deterministic platform proof:

```bash
permutiveapi eval
permutiveapi examples
permutiveapi examples --name reviewed-cohort-write
```

The scorecard covers tool selection, unsupported capabilities, approvals, allow and deny policy, redaction, idempotency, workflow bounds, partial failures, and audit completeness. Seven credential-free recipes cover SDK, async, queries, plugin, and governed workflows. A fresh installed interpreter must complete the canonical recipe within five seconds.

## Operational reliability

### API drift and replay

```bash
python scripts/generate_api_contracts.py --check
python scripts/validate_recordings.py
```

Versioned samples generate the machine contract and `API_COVERAGE.md`. Additive fields remain compatible; removals and type changes fail validation. Recordings exclude request payloads, query strings, credentials, authorization headers, cookies, and sensitive response values.

### Coordinated limits and rotation

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

client.request("GET", "cohorts-api/v2/cohorts")
credentials.rotate("rotated-key")
```

One coordinator can be shared by synchronous and asynchronous transports. Every request attempt receives one immutable credential generation, `Retry-After` deferrals apply across all callers, and transport exceptions redact the real rotating key before reaching the client.

### Performance and releases

```bash
python scripts/validate_performance.py
```

Performance budgets detect material local regressions. The release workflow builds once, generates an SBOM and SHA-256 manifest, attests the artifacts, and verifies the exact candidate before PyPI Trusted Publishing and GitHub Release creation.

See [docs/OPERATIONAL_RELIABILITY.md](docs/OPERATIONAL_RELIABILITY.md) for the complete contract.

## CLI

| Command | Purpose |
|---|---|
| `permutiveapi configure` | Create a protected local credential file. |
| `permutiveapi doctor` | Check credential safety without showing values. |
| `permutiveapi validate` | Validate the installed product surface. |
| `permutiveapi test` | Run deterministic installed-package checks. |
| `permutiveapi eval` | Print governed-platform scorecard JSON. |
| `permutiveapi docs` | Print canonical documentation paths. |
| `permutiveapi examples` | Discover or print executable recipes. |
| `permutiveapi upgrade` | Print the explicit upgrade command. |
| `permutiveapi uninstall` | Print the explicit removal command. |

## Development

```bash
git clone https://github.com/fatmambot33/PermutiveAPI.git
cd PermutiveAPI
python -m pip install -e ".[dev]"

python scripts/validate_release_metadata.py
python scripts/validate_typing_scope.py
python scripts/generate_api_contracts.py --check
python scripts/validate_recordings.py
python scripts/validate_performance.py
python scripts/generate_evaluation_scorecard.py --check evals/scorecard.json
black --check --diff src tests typing_examples scripts
pydocstyle src/PermutiveAPI
pyright
pytest -q
python -m build
python -m twine check dist/*
```

Supported changes require NumPy-style docstrings, strict typing, deterministic network-free tests, secret redaction, clean installation, and machine-readable evidence.

## Project contracts

- [PRODUCT.md](PRODUCT.md) — mission and decision rules.
- [PUBLIC_API.md](PUBLIC_API.md) — canonical and compatibility surfaces.
- [API_COVERAGE.md](API_COVERAGE.md) — generated endpoint coverage.
- [MIGRATION.md](MIGRATION.md) — supported migration paths.
- [docs/AI_NATIVE.md](docs/AI_NATIVE.md) — governed platform architecture.
- [docs/EVALUATIONS.md](docs/EVALUATIONS.md) — deterministic evidence.
- [docs/OPERATIONAL_RELIABILITY.md](docs/OPERATIONAL_RELIABILITY.md) — drift, replay, resilience, budgets, and releases.
- [ROADMAP.md](ROADMAP.md) — completed roadmap and non-goals.
- [SECURITY.md](SECURITY.md) — reporting and guarantees.
- [RELEASING.md](RELEASING.md) — immutable publication process.
- [CHANGELOG.md](CHANGELOG.md) — released behavior.

## Compatibility

[PUBLIC_API.md](PUBLIC_API.md) is the source of truth. Legacy resource classes remain supported as compatibility APIs, but new code should prefer `PermutiveClient`, `AsyncPermutiveClient`, typed queries, and canonical tool surfaces. Review [MIGRATION.md](MIGRATION.md) before replacing older call patterns.

## License

MIT. See [LICENSE](LICENSE).
