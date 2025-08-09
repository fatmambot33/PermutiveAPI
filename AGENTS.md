# AGENTS

This repository uses a small set of quality gates to keep the codebase healthy.

## Commit Checklist
Before committing, ensure all of the following pass:
- `pydocstyle PermutiveAPI`
- `pyright PermutiveAPI`
- `pytest -q`

## Style Notes
- Use NumPy-style docstrings following PEP 257 conventions:
  - Start with a short one-line summary.
  - Include a blank line after the summary, followed by a more detailed description if needed.
  - Use `Parameters` and `Returns` sections (not `Args`).
- Keep the implementation Pythonic and maintainable.
- Write commit messages in the imperative mood.


