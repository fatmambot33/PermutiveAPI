# Developer experience

PermutiveAPI provides one deterministic path from installation to a useful result, plus structured guidance when an operation cannot complete.

## First useful result

List the installed executable recipes:

```bash
permutiveapi examples
```

Print one complete copy-paste recipe:

```bash
permutiveapi examples --name workspace-inspection
```

Run the installed first-success proof:

```bash
python -m PermutiveAPI.first_success
```

The first-success metric measures a fresh Python interpreter from process start through package import and completion of the credential-free `workspace-inspection` recipe. The enforced budget is five seconds. It requires neither external network access nor credentials.

The metric contract is committed at `metrics/first-success-v1.json` and validated in source CI and again from the built wheel.

## Recipe discovery

Recipes are available for these categories:

- `sdk`
- `async`
- `queries`
- `plugin`
- `governed`

Filter by category:

```bash
permutiveapi examples --category governed
```

Return machine-readable metadata for humans or agents:

```bash
permutiveapi examples --json
```

Use the Python catalog directly:

```python
from PermutiveAPI import find_recipes

recipe = find_recipes(name="reviewed-cohort-write")[0]
print(recipe.source)
```

Every published recipe compiles in CI. Core recipes execute from the core wheel, and the async recipe executes from the wheel installed with the `async` extra.

## Actionable errors

Classify an SDK exception without exposing its message or payload:

```python
from PermutiveAPI import classify_exception
from PermutiveAPI import RateLimitError

error = RateLimitError("not exposed", status_code=429, retryable=True)
guidance = classify_exception(error, operation="list_cohorts")
print(guidance.to_dict())
```

`ErrorGuidance` includes:

- a stable error code;
- whether retrying can reasonably succeed;
- one recommended next action;
- safe context such as operation, status code, request ID, endpoint without query data, and attempt count.

It never includes credentials, request payloads, response payloads, or raw exception messages.

Governed `InvocationResult` failures retain the existing `error_type` and generic `error_message`, and add `error_code`, `retryable`, `recommended_action`, and `safe_context`.

## Codex plugin results

Existing `CodexPlugin.invoke()` behavior remains unchanged. Use `invoke_safe()` when an agent or UI needs structured actionable results rather than exceptions:

```python
from PermutiveAPI.credentials import LocalCredentialsProvider
from PermutiveAPI.plugins.codex import CodexPlugin

plugin = CodexPlugin(LocalCredentialsProvider(api_key="local-only-key"))
try:
    result = plugin.invoke_safe("missing_tool")
    print(result)
finally:
    plugin.close()
```

The safe result explains what happened, whether retrying is useful, and what to do next without echoing the local credential.

## Validation

Repository validation runs:

```bash
python scripts/validate_recipes.py
python scripts/validate_first_success.py
```

The same contracts are checked in Python CI, the AI-native platform workflow, and clean installed-wheel smoke tests.
