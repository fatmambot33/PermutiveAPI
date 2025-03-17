# PermutiveAPI

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

## Usage

### Importing the Module

To use the PermutiveAPI module, import the necessary classes:

```python
from PermutiveAPI import Import, ImportList, Segment, SegmentList, Identity, Alias, Cohort, CohortList, Workspace, WorkspaceList
```

### Managing Users

You can manage user identities and aliases using the `Identity` and `Alias` classes:

```python
from PermutiveAPI.User import Identity, Alias

# Create an alias
alias = Alias(id="alias_id", tag="alias_tag", priority=1)

# Create an identity
identity = Identity(user_id="user_id", aliases=[alias])

# Convert identity to JSON
identity_json = identity.to_json()

# Identify a user
identity.Identify(api_key="your_private_key")
```

### Managing Imports

You can manage imports using the `Import` and `ImportList` classes:

```python
from PermutiveAPI.Import import Import, ImportList

# Fetch an import by ID
import_instance = Import.get_by_id(id="import_id", api_key="your_api_key")

# List all imports
imports = Import.list(api_key="your_api_key")
```

### Managing Cohorts

You can manage cohorts using the `Cohort` and `CohortList` classes:

```python
from PermutiveAPI.Cohort import Cohort, CohortList

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
from PermutiveAPI.Workspace import Workspace, WorkspaceList

# Create a workspace instance
workspace = Workspace(name="workspace_name", organization_id="org_id", workspace_id="workspace_id", api_key="your_api_key")

# List cohorts in a workspace
cohorts = workspace.list_cohorts(include_child_workspaces=True)

# List imports in a workspace
imports = workspace.imports

# List segments in a workspace
segments = workspace.list_segments(import_id="import_id")
```

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.