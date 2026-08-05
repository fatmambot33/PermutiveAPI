---
name: permutiveapi
description: Install and use PermutiveAPI from Git with credentials loaded only from a local .env file. Use for workspace inspection, cohorts, segments, imports, sources, diagnostics, and confirmed writes.
---

# PermutiveAPI

## Local-only credential policy

Use only credentials stored in the user's local project `.env`. Never request secret values in chat, upload them, print them, or copy them into Codex, Git, plugin files, hosted configuration, MCP URLs, or command history.

## First-use setup

Install the repository version locally:

```bash
python -m pip install --upgrade "git+https://github.com/fatmambot33/PermutiveAPI.git"
```

Before every API workflow, run:

```bash
permutiveapi doctor
```

When the check fails, guide the user through the local interactive wizard:

```bash
permutiveapi configure
```

The wizard:

1. Prompts for the API key without echoing it.
2. Prompts for the workspace ID.
3. Writes only to the current project's `.env`.
4. Uses restrictive file permissions where supported.
5. Refuses to overwrite an existing file unless `--force` is supplied.
6. Warns when `.env` is not ignored by Git.

Run `permutiveapi doctor` again after configuration. It verifies the file, required variable names, permissions, and Git-ignore protection without displaying values. Stop before API calls until it passes.

Use `--env-file PATH` for a deliberate non-default local file. Do not fall back to hosted secrets, Codex-managed credentials, remote secret stores, or prompt-supplied credentials.

## Workflow

1. Run `permutiveapi doctor`.
2. Run plugin diagnostics without printing secrets.
3. Use `PermutiveAPI.plugins.codex.CodexPlugin` and typed tools.
4. Start read-only and inspect before changing.
5. Present the exact mutation and obtain confirmation for writes.
6. Redact authorization data from every output and error.

```python
from dotenv import load_dotenv
from PermutiveAPI.plugins.codex import CodexPlugin

load_dotenv(".env")
plugin = CodexPlugin.from_env()
print(plugin.diagnostics())
```
