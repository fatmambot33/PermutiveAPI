# AI-Native Plugin Guide

## Codex-first experience

For Codex users, the plugin is the preferred entry point. The repository exposes a Codex marketplace at `.agents/plugins/marketplace.json`.

For a managed workspace, import it from **Workspace settings > Plugins > Add > Import marketplace** using:

```text
https://github.com/fatmambot33/PermutiveAPI
```

Leave the path empty because the marketplace manifest is at the repository root, and use `main` when a branch is requested. After import, make the PermutiveAPI plugin available or installed for the intended role. Codex users can then select it from **Sources > Use plugins**.

Then work in natural language:

- `Set up PermutiveAPI.`
- `Check my Permutive connection and credentials.`
- `List my cohorts.`
- `Inspect this segment.`
- `Explain my workspace configuration.`
- `Prepare a cohort change.`

The plugin bootstraps the stable PyPI package only when it is missing, reuses the canonical `CodexPlugin` tool surface, and keeps normal use free of Python boilerplate.

## One-step readiness check

The normal setup and troubleshooting preflight is:

```bash
permutiveapi check --json
```

This single command resolves credentials without displaying them and performs one bounded read-only Permutive API request. The result reports credential status and connection status separately with a stable error code and a secret-safe recommended action.

A successful result means the environment is ready for normal read-only plugin use.

If credentials are missing, launch the local non-echoing setup:

```bash
permutiveapi configure
```

Enter the API key only into the local prompt. Never paste it into chat or hosted plugin configuration. Then rerun:

```bash
permutiveapi check --json
```

`PERMUTIVE_API_KEY` is resolved from the process environment, the selected project `.env`, or `~/.config/permutive/.env`. Secret values are never printed, committed, remotely stored, or included in object representations.

Use `permutiveapi doctor` only when the readiness check reports unsafe local `.env` protections or when local credential storage itself needs inspection.

## Welcome and troubleshooting skills

The plugin includes dedicated skill surfaces for onboarding and recovery:

- `welcome` drives first-use setup and readiness with the shortest safe flow.
- `troubleshooting` maps readiness error codes to one corrective action at a time.
- `permutiveapi` remains the primary execution skill for normal reads and confirmed writes.

Troubleshooting starts with `permutiveapi check --json`; it does not begin by reinstalling the SDK, asking for a key, or running unrelated validation commands.

## Error routing

The readiness check uses stable categories including:

- `credentials_missing`
- `credential_file_unsafe`
- `authentication_failed`
- `authorization_denied`
- `transport_unavailable`
- `rate_limited`
- `upstream_server_error`
- `invalid_response`

The returned `recommended_action` and `safe_context` are designed for agent use and exclude credential values and raw sensitive payloads.

## Package bootstrap

When the package is unavailable, install the stable release from PyPI:

```bash
python -m pip install --upgrade PermutiveAPI
```

Do not reinstall PermutiveAPI on every request. Git installs are reserved for unreleased development validation.

## Safety model

The default policy is read-only. Write workflows must show the exact mutation and require explicit confirmation before execution. The plugin delegates HTTP, authentication, resources, serialization, policy enforcement, and actionable error handling to the canonical SDK rather than maintaining a separate implementation.

## Python integration

Python users can install the package directly:

```bash
python -m pip install --upgrade PermutiveAPI
```

Programmatic integrations can use the same underlying plugin surface:

```python
from PermutiveAPI.plugins.codex import CodexPlugin

plugin = CodexPlugin.from_env()
tools = plugin.tools().as_openai_tools()
agent_kit = plugin.agent_kit()
```

For explicitly confirmed writes:

```python
plugin = CodexPlugin.from_env(mode="read_write")
result = plugin.invoke(
    "permutive_create_cohort",
    {"payload": {"name": "Example", "query": {}}},
    confirmed=True,
)
```

## Development installs

Install from Git only when validating unreleased repository code:

```bash
python -m pip install --upgrade git+https://github.com/fatmambot33/PermutiveAPI.git
```

For editable local development:

```bash
git clone https://github.com/fatmambot33/PermutiveAPI.git
cd PermutiveAPI
python -m pip install -e '.[dev]'
```
