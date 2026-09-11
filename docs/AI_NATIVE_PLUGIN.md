# AI-Native Plugin Guide

## Codex-first experience

For Codex users, the plugin is the preferred entry point. The repository already exposes a Codex marketplace at `.agents/plugins/marketplace.json`.

For a managed workspace, import it from **Workspace settings > Plugins > Add > Import marketplace** using:

```text
https://github.com/fatmambot33/PermutiveAPI
```

Leave the path empty because the marketplace manifest is at the repository root, and use `main` when a branch is requested. After import, make the PermutiveAPI plugin available or installed for the intended role. Codex users can then select it from **Sources > Use plugins**.

Then work in natural language:

- `Set up PermutiveAPI.`
- `List my cohorts.`
- `Inspect this segment.`
- `Explain my workspace configuration.`
- `Prepare a cohort change.`

The plugin bootstraps the stable PyPI package when it is not already available, runs local credential diagnostics, and reuses the canonical `CodexPlugin` tool surface. Normal Codex use does not require users to write Python or import the SDK manually.

## First-use setup

The plugin installs the stable package from PyPI when needed:

```bash
python -m pip install --upgrade PermutiveAPI
```

It then runs:

```bash
permutiveapi doctor
```

If credentials are missing, the local interactive setup is launched:

```bash
permutiveapi configure
```

Enter the API key only into the local non-echoing prompt. The plugin must never request secret values in chat or copy them into hosted configuration.

Run `permutiveapi doctor` again before any network operation. `PERMUTIVE_API_KEY` is resolved from explicit application input, the process environment, a project `.env`, or `~/.config/permutive/.env`, in that order. Secret values are never printed, committed, remotely stored, or included in object representations.

The plugin does not reinstall PermutiveAPI on every request. Package bootstrap is only needed when the package is unavailable or the user explicitly asks to upgrade it.

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
