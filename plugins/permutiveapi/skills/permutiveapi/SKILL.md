---
name: permutiveapi
description: Use PermutiveAPI directly from Codex for workspace inspection, cohorts, segments, imports, sources, diagnostics, and confirmed writes. Bootstrap the stable PyPI package automatically when needed and keep credentials local.
---

# PermutiveAPI

## User experience

Treat this plugin as the Codex front door to PermutiveAPI. Users should be able to ask for outcomes such as:

- `Set up PermutiveAPI.`
- `Check my Permutive connection.`
- `List my cohorts.`
- `Inspect this segment.`
- `Explain my workspace configuration.`
- `Prepare a cohort change.`

Do not require users to write Python, import `CodexPlugin`, or manually install the SDK during the normal workflow.

Use the dedicated welcome skill for first-use setup and readiness requests. Use the troubleshooting skill when setup, authentication, authorization, connection, or API execution fails.

## Local-only credential policy

Use only credentials resolved by PermutiveAPI's local credential provider. Never request secret values in chat, upload them, print them, or copy them into Codex, Git, plugin files, hosted configuration, MCP URLs, or command history.

## Readiness check

The canonical preflight is one command:

```bash
permutiveapi check --json
```

It resolves credentials without displaying them and performs one bounded read-only API request. A successful result means both credentials and connection are ready.

Do not run `doctor`, `validate`, or Python snippets before ordinary API work. Use `permutiveapi doctor` only when the readiness check reports unsafe local credential-file protections or when local storage itself is being investigated.

If credentials are missing, use the local non-echoing setup:

```bash
permutiveapi configure
```

Then rerun `permutiveapi check --json`. Never ask the user to paste an API key into chat.

## Execution workflow

Use the existing `PermutiveAPI.plugins.codex.CodexPlugin` as the implementation layer; do not create a parallel API surface.

1. Confirm readiness with the canonical check when setup has not yet been established in the current environment.
2. Start in read-only mode.
3. Use the typed plugin tools for discovery and reads.
4. Prefer `invoke_safe()` at the plugin boundary so errors stay structured and secret-safe.
5. For a write request, prepare and show the exact mutation first.
6. Enable read-write mode only for that requested workflow.
7. Obtain explicit user confirmation before invoking a write tool.
8. Redact authorization data from every output and error.

The normal user-facing response should describe results and required confirmations, not the Python plumbing used to obtain them.

## Package bootstrap

If the package is unavailable, install the stable PyPI release:

```bash
python -m pip install --upgrade PermutiveAPI
```

Do not reinstall the package on every request. Bootstrap only when the package is unavailable or when the user explicitly asks to upgrade it.

Use `--env-file PATH` only when the user deliberately selects a non-default local credential file. Do not fall back to hosted secrets, Codex-managed credentials, remote secret stores, or prompt-supplied credentials.

## Safety invariants

- Read-only is the default.
- Writes require explicit confirmation.
- Never expose credentials in output, logs, traces, exceptions, or generated files.
- Reuse the canonical SDK clients, typed resources, policy, and error handling.
- If readiness fails, follow the troubleshooting skill rather than bypassing the check.
