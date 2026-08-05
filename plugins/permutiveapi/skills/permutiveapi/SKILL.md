---
name: permutiveapi
description: Install and use PermutiveAPI from Git for typed, safe Permutive workflows. Use for workspace inspection, cohorts, segments, imports, sources, diagnostics, and confirmed write operations.
---

# PermutiveAPI

## Setup

Ensure the current Python environment has the repository version installed:

```bash
python -m pip install --upgrade "git+https://github.com/fatmambot33/PermutiveAPI.git"
```

Use `PermutiveAPI.plugins.codex.CodexPlugin` as the primary integration surface. Start read-only. Enable write mode only when the user explicitly requests a mutation, and require confirmation immediately before executing it.

## Workflow

1. Check configuration and credentials without printing secrets.
2. Run plugin diagnostics before API work.
3. Prefer typed SDK methods and plugin-provided tools over raw HTTP.
4. Inspect before changing.
5. For writes, present the exact intended change and obtain confirmation.
6. Report API errors with secrets and tokens redacted.

## Examples

```python
from PermutiveAPI.plugins.codex import CodexPlugin

plugin = CodexPlugin.from_env()
print(plugin.diagnostics())
tools = plugin.tools()
```

Keep outputs concise and include object identifiers needed for follow-up work.
