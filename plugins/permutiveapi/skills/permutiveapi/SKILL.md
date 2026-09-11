---
name: permutiveapi
description: Use PermutiveAPI directly from Codex for workspace inspection, cohorts, segments, imports, sources, diagnostics, and confirmed writes. Bootstrap the stable PyPI package automatically when needed and keep credentials local.
---

# PermutiveAPI

## User experience

Treat this plugin as the Codex front door to PermutiveAPI. Users should be able to ask for outcomes such as:

- `Set up PermutiveAPI.`
- `List my cohorts.`
- `Inspect this segment.`
- `Explain my workspace configuration.`
- `Prepare a cohort change.`

Do not require users to write Python, import `CodexPlugin`, or manually install the SDK during the normal workflow.

## Local-only credential policy

Use only credentials resolved by PermutiveAPI's local credential provider. Never request secret values in chat, upload them, print them, or copy them into Codex, Git, plugin files, hosted configuration, MCP URLs, or command history.

## First-use bootstrap

Before the first Permutive workflow in a working environment:

1. Check whether the `PermutiveAPI` package is importable.
2. If it is missing, install or upgrade the stable release from PyPI:

   ```bash
   python -m pip install --upgrade PermutiveAPI
   ```

3. Run:

   ```bash
   permutiveapi doctor
   ```

4. If local credentials are missing or invalid, launch the local interactive setup:

   ```bash
   permutiveapi configure
   ```

   The user enters secrets only into the local non-echoing prompt. Never ask them to paste an API key into chat.

5. Run `permutiveapi doctor` again. Stop before API calls until it passes.

Do not reinstall the package on every request. Bootstrap only when the package is unavailable or when the user explicitly asks to upgrade it.

Use `--env-file PATH` only when the user deliberately selects a non-default local credential file. Do not fall back to hosted secrets, Codex-managed credentials, remote secret stores, or prompt-supplied credentials.

## Execution workflow

Use the existing `PermutiveAPI.plugins.codex.CodexPlugin` as the implementation layer; do not create a parallel API surface.

1. Start in read-only mode.
2. Use the typed plugin tools for discovery and reads.
3. Prefer `invoke_safe()` at the plugin boundary so errors stay structured and secret-safe.
4. For a write request, prepare and show the exact mutation first.
5. Enable read-write mode only for that requested workflow.
6. Obtain explicit user confirmation before invoking a write tool.
7. Redact authorization data from every output and error.

The normal user-facing response should describe results and required confirmations, not the Python plumbing used to obtain them.

## Safety invariants

- Read-only is the default.
- Writes require explicit confirmation.
- Never expose credentials in output, logs, traces, exceptions, or generated files.
- Reuse the canonical SDK clients, typed resources, policy, and error handling.
- If bootstrap or diagnostics fail, explain the actionable local fix instead of bypassing the checks.
