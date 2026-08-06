# PermutiveAPI

[![PyPI version](https://img.shields.io/pypi/v/PermutiveAPI.svg)](https://pypi.org/project/PermutiveAPI/)
[![Python versions](https://img.shields.io/pypi/pyversions/PermutiveAPI.svg)](https://pypi.org/project/PermutiveAPI/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

PermutiveAPI is a typed, governed Python SDK and AI-agent platform for the Permutive API.

It provides one canonical synchronous client, optional asynchronous support, typed queries and errors, a safe Codex plugin, OpenAI-compatible tools, and optional hosted MCP configuration. Python 3.9 through 3.13 are supported.

## Install

Core SDK and CLI:

```bash
python -m pip install --upgrade PermutiveAPI
```

Optional asynchronous transport:

```bash
python -m pip install --upgrade "PermutiveAPI[async]"
```

Optional pandas integration:

```bash
python -m pip install --upgrade "PermutiveAPI[dataframe]"
```

The core package does not import or require pandas or HTTPX.

## Configure safely

Run the local credential wizard from the project where you will use the SDK:

```bash
permutiveapi configure
permutiveapi doctor
permutiveapi validate
```

`configure` writes `PERMUTIVE_API_KEY` to a local `.env` file without echoing it. Existing files are not overwritten unless `--force` is supplied. `doctor` checks that the variable is present, the file has restrictive permissions where supported, and `.env` is ignored by Git. `validate` checks the installed SDK, CLI, Python plugin entry point, and tool contract without making a network request or requiring credentials.

Credential lookup is deterministic:

1. Explicit API key passed by the application.
2. `PERMUTIVE_API_KEY` in the process environment.
3. `.env` in the current project.
4. `~/.config/permutive/.env`.

Credentials are never uploaded, logged, or included in object representations.

## First request

`PermutiveClient` is the canonical synchronous entry point:

```python
from PermutiveAPI import PermutiveClient

with PermutiveClient("api-key") as client:
    cohort = client.cohorts.get("cohort-id")
    first_page = client.segments.list(page_size=50)

print(cohort)
print(first_page.items)
```

The resource namespaces are:

- `client.cohorts`
- `client.imports`
- `client.segments`
- `client.sources`
- `client.workspaces`

Supported resource operations use consistent `get`, `list`, `create`, `update`, and `delete` methods where the Permutive endpoint supports them. See [API_COVERAGE.md](API_COVERAGE.md) for the maintained endpoint matrix.

## Async usage

Install the `async` extra, then use the asynchronous client as a context manager:

```python
import asyncio

from PermutiveAPI import AsyncPermutiveClient


async def main() -> None:
    async with AsyncPermutiveClient("api-key") as client:
        cohort = await client.request(
            "GET",
            "cohorts-api/v2/cohorts/cohort-id",
        )
        print(cohort)


asyncio.run(main())
```

The async client shares the same typed JSON, error, retry, redaction, pagination, and bounded-batch contracts as the synchronous SDK.

## Typed queries

Query helpers compose immutable, deterministic JSON payloads while preserving raw payload compatibility:

```python
from PermutiveAPI import all_of, event, property_condition

query = all_of(
    event("Pageview"),
    property_condition("client.country", "equals", "FR"),
)

payload = query.to_json()
```

Invalid operator and value combinations fail before the request is sent.

## Codex plugin

Install the repository-backed Codex marketplace plugin:

```bash
codex plugin marketplace add fatmambot33/PermutiveAPI --ref main
codex plugin add permutiveapi@fatmambot33-permutiveapi
```

The Python plugin surface is also available directly:

```python
from PermutiveAPI.plugins.codex import CodexPlugin

plugin = CodexPlugin.from_env()
tools = plugin.tools().as_openai_tools()
agent_kit = plugin.agent_kit()
```

The default policy is read-only. To expose write tools, applications must explicitly select `mode="read_write"`; each mutating invocation still requires `confirmed=True` unless a deliberately approved policy says otherwise.

```python
plugin = CodexPlugin.from_env(mode="read_write")
result = plugin.invoke(
    "permutive_create_cohort",
    {"payload": {"name": "Example", "query": {}}},
    confirmed=True,
)
```

The plugin reuses the canonical SDK. It does not duplicate transport, authentication, models, or business rules. See [docs/AI_NATIVE_PLUGIN.md](docs/AI_NATIVE_PLUGIN.md), [docs/AI_NATIVE.md](docs/AI_NATIVE.md), and [docs/MCP.md](docs/MCP.md).

## CLI lifecycle

| Command | Purpose |
| --- | --- |
| `permutiveapi configure` | Create a protected local `.env` credential file. |
| `permutiveapi doctor` | Check local credential safety without showing values. |
| `permutiveapi validate` | Validate the installed product surface. |
| `permutiveapi test` | Run the deterministic installed-package self-test. |
| `permutiveapi docs` | Print canonical documentation locations. |
| `permutiveapi examples` | Print minimal SDK and plugin examples. |
| `permutiveapi upgrade` | Print the explicit interpreter-specific upgrade command. |
| `permutiveapi uninstall` | Print the explicit interpreter-specific removal command. |

`upgrade` and `uninstall` print commands only. They never mutate the active environment automatically. Full behavior and exit codes are documented in [docs/CLI.md](docs/CLI.md).

## Compatibility

[PUBLIC_API.md](PUBLIC_API.md) is the source of truth for canonical, compatibility, deprecated, and internal exports. Legacy resource classes remain available as compatibility APIs, but new code should use `PermutiveClient`, `AsyncPermutiveClient`, typed queries, and the canonical plugin/tool surfaces.

See [MIGRATION.md](MIGRATION.md) before replacing older call patterns and [COMPATIBILITY_MATRIX.md](COMPATIBILITY_MATRIX.md) for the supported Python and optional-dependency matrix.

## Development

Clone and install the repository in editable mode:

```bash
git clone https://github.com/fatmambot33/PermutiveAPI.git
cd PermutiveAPI
python -m pip install -e ".[dev]"
```

Run the same primary checks used by CI:

```bash
black --check src tests
pydocstyle src/PermutiveAPI
pyright
pytest -q
python scripts/validate_ai_native_platform.py
python scripts/validate_release_metadata.py
python -m build
python -m twine check dist/*
```

NumPy-style docstrings, strict typing, deterministic network-free tests, clean package installation, and secret redaction are required for supported changes.

## Project contracts

- [PRODUCT.md](PRODUCT.md) — product mission and decision rules.
- [PUBLIC_API.md](PUBLIC_API.md) — supported public surface.
- [API_COVERAGE.md](API_COVERAGE.md) — endpoint coverage.
- [ROADMAP.md](ROADMAP.md) — active milestones and non-goals.
- [SECURITY.md](SECURITY.md) — security reporting and guarantees.
- [RELEASING.md](RELEASING.md) — validated release process.
- [CHANGELOG.md](CHANGELOG.md) — released behavior.

## License

MIT. See [LICENSE](LICENSE).
