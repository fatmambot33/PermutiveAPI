# PermutiveAPI

PermutiveAPI is a Python module to interact with the Permutive API. It provides a set of classes and methods to manage users, imports, cohorts, and workspaces within the Permutive ecosystem.

## Table of Contents

- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
  - [Importing the Module](#importing-the-module)
  - [Managing Workspaces](#managing-workspaces)
  - [Managing Cohorts](#managing-cohorts)
  - [Managing Segments](#managing-segments)
  - [Managing Imports](#managing-imports)
  - [Managing Users](#managing-users)
- [Development](#development)
- [Contributing](#contributing)
- [License](#license)

## Installation

You can install the PermutiveAPI module using pip:

```sh
pip install PermutiveAPI --upgrade
```

## Configuration

Before using the library, you need to configure your credentials.

1.  **Copy the environment file**:
    ```sh
    cp _env .env
    ```
2.  **Set your credentials path**:
    Edit the `.env` file and set the `PERMUTIVE_APPLICATION_CREDENTIALS` environment variable to the absolute path of your workspace JSON file.
    ```sh
    PERMUTIVE_APPLICATION_CREDENTIALS="/absolute/path/to/your/workspace.json"
    ```
The workspace credentials JSON can be downloaded from the Permutive dashboard under **Settings → API keys**. Save the file somewhere secure. The `apiKey` inside this JSON is used to authenticate API calls.

## Usage

### Importing the Module

To use the PermutiveAPI module, import the necessary classes. The main classes are exposed at the top level of the `PermutiveAPI` package:

```python
from PermutiveAPI import (
    Alias,
    Cohort,
    Identity,
    Import,
    Segment,
    Source,
    Workspace,
)
```

### Managing Workspaces

The `Workspace` class is the main entry point for interacting with your Permutive workspace.

```python
# Create a workspace instance
# You can pass credentials explicitly or let the client find them in your .env file
workspace = Workspace(workspace_id="your-workspace-id", api_key="your-api-key")

# List all cohorts in a workspace
all_cohorts = workspace.list_cohorts(include_child_workspaces=True)
for cohort in all_cohorts:
    print(f"Cohort ID: {cohort.id}, Name: {cohort.name}")

# List all imports in a workspace
all_imports = workspace.imports
for imp in all_imports:
    print(f"Import ID: {imp.id}, Status: {imp.status}")

# List segments for a specific import
segments_in_import = workspace.list_segments(import_id="your-import-id")
for segment in segments_in_import:
    print(f"Segment ID: {segment.id}, Name: {segment.name}")
```

### Managing Cohorts

You can create, retrieve, and list cohorts using the `Cohort` class.

```python
# List all cohorts
all_cohorts = Cohort.list(api_key="your_api_key")
print(f"Found {len(all_cohorts)} cohorts.")

# Get a specific cohort by ID
cohort_id = "your-cohort-id"
cohort = Cohort.get_by_id(id=cohort_id, api_key="your_api_key")
print(f"Retrieved cohort: {cohort.name}")

# Create a new cohort
new_cohort = Cohort(
    name="High-Value Customers",
    query={"type": "segment", "id": "segment-id-for-high-value-customers"}
)
created_cohort = new_cohort.create(api_key="your_api_key")
print(f"Created cohort with ID: {created_cohort.id}")
```

### Managing Segments

The `Segment` class allows you to interact with audience segments.

```python
# List all segments for a given import
import_id = "your-import-id"
segments = Segment.list(api_key="your_api_key", import_id=import_id)
print(f"Found {len(segments)} segments in import {import_id}.")

# Get a specific segment by ID
segment_id = "your-segment-id"
segment = Segment.get_by_id(id=segment_id, api_key="your_api_key")
print(f"Retrieved segment: {segment.name}")
```

### Managing Imports

You can list and retrieve imports using the `Import` class.

```python
# List all imports
all_imports = Import.list(api_key="your_api_key")
for imp in all_imports:
    print(f"Import ID: {imp.id}, Status: {imp.status}, Source: {imp.source.name}")

# Get a specific import by ID
import_id = "your-import-id"
import_instance = Import.get_by_id(id=import_id, api_key="your_api_key")
print(f"Retrieved import: {import_instance.id}, Source: {import_instance.source.name}")
```

### Managing Users

The `Identity` and `Alias` classes are used to manage user profiles.

```python
# Create an alias for a user
alias = Alias(id="user@example.com", tag="email", priority=1)

# Create an identity for the user
identity = Identity(user_id="internal-user-id-123", aliases=[alias])

# Send the identity information to Permutive
# The private_key is different from the api_key and is used for this specific endpoint
try:
    identity.identify(private_key="your-private-key")
    print("Successfully identified user.")
except Exception as e:
    print(f"Error identifying user: {e}")
```

## Development

To set up a development environment, install the required dependencies:

```sh
pip install -r requirements-dev.txt
```

### Running Tests

Before committing any changes, please run the following checks to ensure code quality and correctness.

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

All checks must pass before a pull request can be merged.

## Contributing

Contributions are welcome! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for development setup and pull request guidelines.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.
