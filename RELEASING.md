# Releasing

`pyproject.toml` is the authoritative package version. The changelog heading, release notes, built artifact metadata, tag, GitHub Release, and published PyPI version must match it.

## Release readiness

Before merging a release candidate:

1. Confirm all protected Python, AI-native, and security checks pass.
2. Confirm `CHANGELOG.md` and `docs/releases/X.Y.Z.md` contain the target version.
3. Validate generated API contracts and coverage.
4. Validate sanitized recordings and deterministic replay.
5. Validate governed evaluations, recipes, first success, and performance budgets.
6. Build the wheel and source distribution once.
7. Install the wheel in a clean environment and exercise the documented product surface.

## Immutable candidate evidence

The release workflow generates:

- wheel and source distribution under `release-dist/`;
- reproducible CycloneDX SBOM under `release-evidence/`;
- `release-manifest.json` containing the project, version, source commit, relative path, size, and SHA-256 for every distribution and the SBOM;
- GitHub artifact attestations for distributions and evidence.

The candidate manifest is verified:

1. immediately after build;
2. after artifact download and before PyPI Trusted Publishing;
3. after artifact download and before tag and GitHub Release creation.

PyPI and GitHub Release therefore consume the exact candidate that passed build validation. Artifacts are never rebuilt in publication jobs.

## Automation

Merging a version change to `main` starts `.github/workflows/python-publish.yml`. It uses PyPI Trusted Publishing and no long-lived repository publication token.

The workflow:

1. resolves the package version and checks whether it already exists on PyPI;
2. validates source contracts and performance budgets;
3. builds and smoke-tests distributions;
4. generates SBOM, manifest, and attestations;
5. verifies and publishes the exact candidate;
6. verifies again and creates or reconciles the annotated tag and GitHub Release.

## Version policy

- Patch: backward-compatible fixes.
- Minor: backward-compatible features and deprecations.
- Major: removals or breaking public API changes.

## Recovery

A broken release must not be overwritten. Yank it on PyPI, document the reason, fix forward with a new patch version, and retain the original tag and release evidence for traceability.
