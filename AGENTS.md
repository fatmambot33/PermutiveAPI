# AGENTS.md

These instructions apply to any automated agent contributing to this repository.

## 1. Code Style

- Use standard PEP 8 formatting for all Python code.
- Write commit messages in the imperative mood (e.g., "Add feature" not "Added feature").
- Keep the implementation Pythonic and maintainable.

## 2. Docstring Style

- Use **NumPy-style** docstrings following [PEP 257](httpshttps://peps.python.org/pep-0257/) conventions.
- **Do not** use `:param` / `:type` syntax (reST/Sphinx style) or Google-style `Args`.
- Always include type hints in function signatures and **do not** duplicate them in docstrings.
- Begin docstrings with a **short, one-line summary**, followed by a blank line and an optional extended description.
- Use the following NumPy-style sections when applicable:
  - `Parameters`
  - `Returns`
  - `Raises`
  - `Examples`
- Document all public classes, methods, and functions.
- **For classes, the main docstring should include a `Methods` section summarizing each public method and its one-line description.**
- For optional parameters, note the default value in the description.
- Use present tense and active voice (“Return…”, “Fetch…”).

## 3. Code Quality and Testing

Before running tests, install the development dependencies:

```bash
pip install -r requirements-dev.txt
```

To ensure your changes will pass the automated checks in our Continuous Integration (CI) pipeline, run the following commands locally before committing. All checks must pass.

**Style Checks:**
```bash
pydocstyle PermutiveAPI
black --check .
```

**Static Type Analysis:**
```bash
pyright PermutiveAPI
```

**Unit Tests and Coverage:**
```bash
pytest -q --cov=PermutiveAPI --cov-report=term-missing --cov-fail-under=70
```

## 4. Directory Layout

- Production code lives in `PermutiveAPI/`.
- Tests live in `tests/`.
- Keep imports relative within the package (e.g., `from PermutiveAPI...`).

## 5. Pull Request Messages

Each pull request should include:

1. **Summary** – brief description of the change.
2. **Testing** – commands run and confirmation that the tests passed.

Example PR body:

```
### Summary
- add new helper to utils.list
- expand tests for list chunking

### Testing
- `pytest` (all tests passed)
```

## 6. General Guidelines

- Avoid pushing large data files to the repository.
- Prefer small, focused commits over sweeping changes.
- Update or add tests whenever you modify functionality.
