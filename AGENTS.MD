# AGENTS

This repository uses a small set of quality gates to keep the codebase healthy.

## Commit Checklist
Before committing, ensure all of the following pass:
- `pydocstyle PermutiveAPI`
- `pyright PermutiveAPI`
- `pytest -q`

## Style Notes
- Use NumPy-style docstrings with explicit `Args` and `Returns` sections.
- Keep the implementation pythonic and maintainable.
- Write commit messages in the imperative mood.

