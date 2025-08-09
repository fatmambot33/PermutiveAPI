# AGENTS

This repository uses a small set of quality gates to keep the codebase healthy.

## Commit Checklist
Before committing, ensure all of the following pass:
- `pydocstyle PermutiveAPI`
- `pyright PermutiveAPI`
- `pytest -q`

## Style Notes
- Use **NumPy-style** docstrings following [PEP 257](https://peps.python.org/pep-0257/) conventions.
- **Do not** use `:param` / `:type` syntax (reST/Sphinx style) or Google-style `Args`.
- Always include type hints in function signatures and **do not** duplicate them in docstrings.
- Begin docstrings with a **short, one-line summary**, followed by a blank line and an optional extended description.
- Use the following NumPy-style sections when applicable:
  - `Parameters`
  - `Returns`
  - `Raises`
  - `Examples`
- Document all public classes, methods, and functions.
- For optional parameters, note the default value in the description.
- Use present tense and active voice (“Return…”, “Fetch…”).
- Keep the implementation Pythonic and maintainable.
- Write commit messages in the imperative mood.

### Function Example
```python
def connect(host: str, port: int = 5432) -> bool:
    """
    Connect to a database server.

    Parameters
    ----------
    host : str
        Hostname or IP address of the server.
    port : int, optional
        Port number to connect to. Defaults to 5432.

    Returns
    -------
    bool
        True if the connection is successful, False otherwise.

    Raises
    ------
    ConnectionError
        If the server is unreachable.

    Examples
    --------
    >>> connect("localhost")
    True
    """
```
### Class Example
```python
class Import:
    """
    Represents an Import in the system.

    Attributes
    ----------
    id : str
        Unique identifier of the import.
    name : str
        Display name of the import.
    active : bool
        Whether the import is active.
    """
```
