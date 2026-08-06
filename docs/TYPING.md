# Typing contract

PermutiveAPI ships a PEP 561 `py.typed` marker and treats static typing as part of the supported product contract.

## Strict surface

`TYPING_SCOPE.json` lists every module that must pass Pyright in strict mode. The strict group contains the canonical SDK, async client, typed models and queries, configuration and credentials, diagnostics, CLI, tools, agent platform, MCP configuration, plugin runtime, and local validation surface.

The Pyright `include` list in `pyproject.toml` must exactly match this group. CI runs `scripts/validate_typing_scope.py` before Pyright, so a new canonical module cannot be added without an explicit typing decision.

## Compatibility surface

Legacy resources and utility modules remain available for backward compatibility. They are listed separately in `TYPING_SCOPE.json` rather than being silently excluded.

Compatibility modules:

- remain covered by runtime and public API regression tests;
- may not define a second canonical transport or client contract;
- must not be used as the foundation for new features;
- should migrate into the strict group when they are modernized;
- cannot grow without changing the machine-readable scope and its bounded regression test.

This boundary permits incremental modernization without falsely claiming that legacy internals already satisfy the canonical strict contract.

## Downstream consumers

Applications should import supported names from `PermutiveAPI`. The built wheel includes `py.typed`, so type checkers can consume the package directly.

```python
from PermutiveAPI import PermutiveClient, QueryExpression

client: PermutiveClient = PermutiveClient("api-key")
query: QueryExpression
```

`PUBLIC_API.md` classifies package-root exports as canonical or compatibility. `TYPING_SCOPE.json` classifies their implementation modules. Both contracts are validated independently.
