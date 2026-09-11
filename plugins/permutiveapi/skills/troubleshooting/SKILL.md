---
name: troubleshooting
description: Diagnose and repair PermutiveAPI setup, credentials, authentication, authorization, connection, network, rate-limit, or API failures in Codex. Use the canonical readiness check first, keep secrets local, and give one actionable next step at a time.
---

# PermutiveAPI Troubleshooting

## First diagnostic

Run exactly one canonical readiness check before suggesting fixes:

```bash
permutiveapi check --json
```

Do not begin by reinstalling the package, asking for an API key, or running a long checklist. Use the returned error code to choose the next action.

## Error routing

- `credentials_missing`: run `permutiveapi configure`, then rerun `permutiveapi check --json`.
- `credential_file_unsafe`: run `permutiveapi doctor`, repair file permissions or `.gitignore`, then rerun the readiness check.
- `authentication_failed`: the configured API key was rejected. Ask the user to replace it locally with `permutiveapi configure --force`; never ask them to paste the key into chat.
- `authorization_denied`: authentication worked but the credential lacks access for the requested Permutive operation or workspace. Do not rotate credentials blindly; explain that permissions must be corrected in Permutive.
- `transport_unavailable`: check local network reachability and retry once after the network issue is resolved.
- `rate_limited`: respect the server delay and retry later; do not create a busy retry loop.
- `upstream_server_error`: retry with the SDK's bounded retry behavior and report the safe request identifier when available.
- `invalid_response`: preserve the safe request identifier and investigate upstream schema compatibility.
- any other code: use the returned `recommended_action` and `safe_context`; do not expose raw exception messages when they may contain sensitive data.

## Package problems

Only inspect installation when the `permutiveapi` command or `PermutiveAPI` import is unavailable. In that case install or repair the stable package with:

```bash
python -m pip install --upgrade PermutiveAPI
```

Then rerun `permutiveapi check --json`.

## Execution discipline

Keep remediation short:

1. run the readiness check;
2. apply the one repair implied by its error code;
3. rerun the readiness check;
4. continue the original user task once both credentials and connection report `ok`.

Do not run unrelated validation, evaluation, build, or development commands during ordinary user troubleshooting.

## Safety

- Never request, print, echo, upload, or log the API key.
- Never copy credentials into Codex settings, plugin files, MCP URLs, Git, or command history.
- Keep troubleshooting read-only.
- Preserve explicit confirmation for any write workflow after recovery.
