# PermutiveAPI

[![Python CI](https://github.com/fatmambo33/PermutiveAPI/actions/workflows/ci.yml/badge.svg)](https://github.com/fatmambo33/PermutiveAPI/actions/workflows/ci.yml)

PermutiveAPI is a Python module to interact with the Permutive API. It provides a set of classes and methods to manage users, imports, cohorts, and workspaces within the Permutive ecosystem.

## Installation

You can install the PermutiveAPI module using pip:

```sh
pip install PermutiveAPI --upgrade
```

## Configuration

Copy the `_env` file as `.env` and set the `PERMUTIVE_APPLICATION_CREDENTIALS` environment variable to the absolute path of your workspace JSON file:

```sh
cp _env .env
```

Edit the `.env` file to include the path to your workspace JSON file:

```sh
PERMUTIVE_APPLICATION_CREDENTIALS="/absolute/path/to/workspace.json"
```

The workspace credentials JSON can be downloaded from the Permutive dashboard
under **Settings \u2192 API keys**. Save the file somewhere secure and set the
`PERMUTIVE_APPLICATION_CREDENTIALS` variable to its absolute path. The `apiKey`
inside this JSON is used to authenticate API calls.

## Usage

### Importing the Module

To use the PermutiveAPI module, import the necessary classes. The main classes are exposed at the top level of the `PermutiveAPI` package:

```python
from PermutiveAPI import (
    Alias,
    Cohort,
    CohortList,
    Identity,
    Import,
    ImportList,
    Segment,
    SegmentList,
    Source,
    Workspace,
    WorkspaceList,
)
```

### Managing Users

You can manage user identities and aliases using the `Identity` and `Alias` classes:

```python
# Create an alias
alias = Alias(id="alias_id", tag="alias_tag", priority=1)

# Create an identity
identity = Identity(user_id="user_id", aliases=[alias])

# Convert identity to JSON
identity_json = identity.to_json()

# Identify a user
identity.identify(api_key="your_private_key")
```

### Managing Imports

You can manage imports using the `Import` and `ImportList` classes. An `Import` object may also contain a `Source` object.

```python
# Fetch an import by ID
import_instance = Import.get_by_id(id="import_id", api_key="your_api_key")

# The source of the import can be accessed via the .source attribute
source_info = import_instance.source

# List all imports
imports = Import.list(api_key="your_api_key")
```

### Managing Cohorts

You can manage cohorts using the `Cohort` and `CohortList` classes:

```python
# Create a new cohort
cohort = Cohort(name="cohort_name", query={"property": "value"})
cohort.create(api_key="your_api_key")

# Fetch a cohort by ID
cohort_instance = Cohort.get_by_id(id="cohort_id", api_key="your_api_key")

# List all cohorts
cohorts = Cohort.list(api_key="your_api_key")
```

### Managing Workspaces

You can manage workspaces using the `Workspace` and `WorkspaceList` classes:

```python
# Create a workspace instance
workspace = Workspace(name="workspace_name", organization_id="org_id", workspace_id="workspace_id", api_key="your_api_key")

# List cohorts in a workspace
cohorts = workspace.list_cohorts(include_child_workspaces=True)

# List imports in a workspace
imports = workspace.imports

# List segments in a workspace
segments = workspace.list_segments(import_id="import_id")
```
## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for development setup and pull request guidelines.


## Development

Install the dependencies and set up your environment:

```sh
pip install -r requirements.txt
cp _env .env  # update the path to your workspace.json
```

After configuring the `.env` file you can import the package to verify
everything is configured correctly:

```sh
python -c "import PermutiveAPI"
```

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.
