---
name: welcome
description: Welcome and set up PermutiveAPI in Codex. Use on first use, setup requests, onboarding, connection checks, or when the user asks whether PermutiveAPI is ready. Keep setup minimal, verify credentials and live connectivity, and never request secrets in chat.
---

# PermutiveAPI Welcome

## Goal

Get the user from plugin installation to a verified read-only Permutive connection with the fewest steps and no Python boilerplate.

## Default flow

1. Check whether `PermutiveAPI` is installed. Install the stable PyPI release only when the package is unavailable:

   ```bash
   python -m pip install --upgrade PermutiveAPI
   ```

2. Run the single canonical readiness check:

   ```bash
   permutiveapi check --json
   ```

   This resolves credentials without displaying them and performs one bounded read-only API request. Do not run separate diagnostic commands first.

3. If the result is ready, tell the user the connection is working and continue directly with their requested Permutive task.

4. If the result reports `credentials_missing`, launch the local non-echoing setup:

   ```bash
   permutiveapi configure
   ```

   Never ask the user to paste an API key into chat. After local configuration, rerun:

   ```bash
   permutiveapi check --json
   ```

5. If the result reports another error code, hand off to the PermutiveAPI troubleshooting workflow instead of improvising around the safety checks.

## Welcome response

Keep successful onboarding concise. Confirm these two facts only:

- credentials were resolved from an approved local source;
- the read-only Permutive API connection succeeded.

Then move on to the user's actual goal. Useful next requests include listing cohorts, inspecting a segment, or explaining workspace configuration.

## Safety

- Never display, echo, log, upload, or request the API key in chat.
- Never require a workspace ID for authentication.
- Never enable write mode during setup.
- Do not reinstall or upgrade the package on every request.
- Use `permutiveapi doctor` only when `permutiveapi check` reports unsafe local credential-file protections or when the user explicitly asks to inspect local credential storage.
