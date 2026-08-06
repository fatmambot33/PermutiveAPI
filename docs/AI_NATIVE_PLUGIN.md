# AI-Native Plugin Guide

## Install from PyPI

```bash
python -m pip install --upgrade PermutiveAPI
```

## Install from Git

```bash
python -m pip install git+https://github.com/fatmambot33/PermutiveAPI.git
```

For local development:

```bash
git clone https://github.com/fatmambot33/PermutiveAPI.git
cd PermutiveAPI
python -m pip install -e '.[dev]'
```

## Install the Codex plugin

```bash
codex plugin marketplace add fatmambot33/PermutiveAPI --ref main
codex plugin add permutiveapi@fatmambot33-permutiveapi
```

The plugin exposes a typed, versioned contract with deterministic entry-point discovery and capability metadata. Before network operations, configure and validate the local API key:

```bash
permutiveapi configure
permutiveapi doctor
permutiveapi validate
```

`PERMUTIVE_API_KEY` is resolved from explicit application input, the process environment, a project `.env`, or `~/.config/permutive/.env`, in that order. Secret values are never printed, committed, remotely stored, or included in object representations.

## Python integration

```python
from PermutiveAPI.plugins.codex import CodexPlugin

plugin = CodexPlugin.from_env()
tools = plugin.tools().as_openai_tools()
agent_kit = plugin.agent_kit()
```

The default policy is read-only. Write tools require an explicit `read_write` policy and confirmation at invocation time:

```python
plugin = CodexPlugin.from_env(mode="read_write")
result = plugin.invoke(
    "permutive_create_cohort",
    {"payload": {"name": "Example", "query": {}}},
    confirmed=True,
)
```

The plugin delegates all HTTP, authentication, resource, error, and serialization behavior to the canonical SDK.
