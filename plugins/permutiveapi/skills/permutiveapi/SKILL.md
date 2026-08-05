---
name: permutiveapi
description: Install and use PermutiveAPI from Git with credentials loaded only from a local .env file. Use for workspace inspection, cohorts, segments, imports, sources, diagnostics, and confirmed writes.
---

# PermutiveAPI

## Local-only credential policy

Use only credentials stored in the user's local project `.env`. Never request, upload, persist, or copy secrets into Codex, Git, plugin files, chat messages, hosted configuration, MCP URLs, or command history. Never print credential values.

Before the first API operation:

1. Check whether `.env` exists in the current working directory.
2. If it is missing, copy the included template:

```bash
cp plugins/permutiveapi/.env.example .env
```

3. Ask the user to edit `.env` locally and provide values for:

```dotenv
PERMUTIVE_API_KEY=
PERMUTIVE_WORKSPACE_ID=
```

4. Ensure `.env` is ignored by Git. Add it to `.gitignore` when necessary.
5. Verify only that required variables are present and non-empty. Report missing variable names, never their values.
6. Stop before making API calls until local configuration is complete.

Load the local file with `python-dotenv` or the package's `from_env()` path. Do not fall back to hosted secrets, Codex-managed credentials, remote secret stores, or credentials supplied in prompts.

## Setup

Install the repository version locally:

```bash
python -m pip install --upgrade "git+https://github.com/fatmambot33/PermutiveAPI.git"
```

Use `PermutiveAPI.plugins.codex.CodexPlugin` as the primary integration surface. Start read-only. Enable write mode only for an explicitly requested mutation and require confirmation immediately before execution.

## Workflow

1. Complete the local `.env` check and configuration guidance.
2. Run diagnostics without printing secrets.
3. Prefer typed SDK methods and plugin tools over raw HTTP.
4. Inspect before changing.
5. Present the exact intended mutation and obtain confirmation for writes.
6. Redact tokens, credentials, and authorization headers from every output and error.

## Example

```python
from dotenv import load_dotenv
from PermutiveAPI.plugins.codex import CodexPlugin

load_dotenv(".env")
plugin = CodexPlugin.from_env()
print(plugin.diagnostics())
```

Keep outputs concise and include only non-secret object identifiers needed for follow-up work.
