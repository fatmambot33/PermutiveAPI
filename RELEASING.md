# Releasing

`pyproject.toml` is the authoritative package version. The tag, changelog heading, built artifact metadata, and published PyPI version must match it.

## Release readiness

Before tagging a release:

1. Confirm required CI checks pass on `main`.
2. Confirm `CHANGELOG.md` contains the target version and migration notes.
3. Run `python -m build` and `python -m twine check dist/*`.
4. Install the wheel in a clean virtual environment and import the documented public API.
5. Create an annotated `vX.Y.Z` tag matching `pyproject.toml`.

## Automation

The release workflow builds artifacts from the tag commit and publishes them through PyPI trusted publishing. The workflow does not use a long-lived repository token.

## Version policy

- Patch: backward-compatible fixes.
- Minor: backward-compatible features and deprecations.
- Major: removals or breaking public API changes.

## Recovery

A broken release must not be overwritten. Yank it on PyPI, document the reason, fix forward with a new patch version, and retain the original tag for traceability.
