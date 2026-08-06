# PermutiveAPI CLI

The `permutiveapi` command provides a small, deterministic lifecycle surface. It never uploads credentials and does not mutate the active Python environment without an explicit external package-manager command.

## Credential commands

### `permutiveapi configure`

Interactively writes `PERMUTIVE_API_KEY` to a local `.env` file. Secret input is not echoed. Existing files are protected unless `--force` is supplied.

The API key is the only credential required by the canonical SDK and Codex plugin. Workspace identifiers belong in individual API requests when an endpoint requires one; they are not global authentication material.

### `permutiveapi doctor`

Checks that the local credential file exists, contains `PERMUTIVE_API_KEY`, has restrictive permissions where supported, and is ignored by Git. Credential values are never displayed.

## Product commands

### `permutiveapi validate`

Runs installed-package checks for distribution metadata, deterministic Python plugin discovery, and the public tool-registry contract. It is network-free and does not require credentials.

### `permutiveapi test`

Runs the deterministic installed-package self-test. Repository contributors should continue to use `pytest` for the complete source test suite.

### `permutiveapi eval`

Prints the versioned governed-platform evaluation scorecard as deterministic JSON. The command verifies tool selection, policy enforcement, secret redaction, idempotency, workflow bounds, partial failures, and audit completeness. It requires no credentials or network access and exits with `1` when any case fails.

### `permutiveapi docs`

Prints the canonical repository documentation locations.

### `permutiveapi examples`

Prints minimal canonical SDK and Codex plugin examples.

## Environment lifecycle guidance

### `permutiveapi upgrade`

Prints the exact interpreter-specific `pip install --upgrade PermutiveAPI` command. It does not modify the environment automatically.

### `permutiveapi uninstall`

Prints the exact interpreter-specific `pip uninstall PermutiveAPI` command. It does not remove the package automatically.

## Exit codes

- `0`: the command completed successfully.
- `1`: validation or an evaluation failed, or local configuration needs repair.
- `2`: required input is missing or an unsafe overwrite was refused.
