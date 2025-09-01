# PermutiveAPI Package

This directory contains the core source code for the `PermutiveAPI` Python package. It is the central hub for all functionality related to the Permutive API.

## Structure

The package is organized into several modules and sub-packages, each responsible for a specific part of the Permutive API:

-   **`Workspace.py`**: The primary entry point for most interactions. The `Workspace` class allows you to list resources like cohorts, imports, and segments.
-   **`Cohort.py`**: Provides the `Cohort` and `CohortList` classes for creating, retrieving, and managing cohorts. Cohorts are often managed within a `Workspace`.
-   **`Audience/`**: A sub-package for managing audience-related resources.
    -   `Import.py`: Manages data imports.
    -   `Segment.py`: Manages audience segments, which are often tied to an `Import`.
    -   `Source.py`: Represents the source of an import.
-   **`Identify/`**: A sub-package for user identification.
    -   `Identity.py` and `Alias.py`: Manage user identities and their aliases.
-   **`Utils.py`**: Contains utility functions, such as custom JSON encoders and request handlers, used across the package.
-   **`errors.py`**: Defines custom exception classes (`PermutiveAPIError`, `PermutiveRateLimitError`, etc.) to provide detailed error information.

## Convenience Imports

For ease of use, the most common classes from the sub-packages and modules are exposed directly at the top level of the `PermutiveAPI` package. This means you can import them as follows:

```python
from PermutiveAPI import Workspace, Cohort, Segment, Import, Identity, Alias
```

This avoids the need for longer imports like `from PermutiveAPI.Audience.Segment import Segment`.

For detailed usage instructions and installation, please refer to the main `README.md` in the root of the repository.
