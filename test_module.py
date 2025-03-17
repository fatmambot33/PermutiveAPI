import os
from dotenv import load_dotenv

import random
load_dotenv()
PERMUTIVE_APPLICATION_CREDENTIALS=os.getenv("PERMUTIVE_APPLICATION_CREDENTIALS")
from PermutiveAPI.Workspace import Workspace , WorkspaceList
print("from PermutiveAPI.Workspace import Workspace , WorkspaceList")
print("Create dummy instances")
workspace_instance = Workspace(
                         name="workspace_name",
                         organisation_id="Act",
                         workspace_id="workspace_description",
                         api_key="workspace_updated_at")
workspace_list_instance_json = workspace_instance.to_json()
workspace_instance = Workspace.from_json(workspace_list_instance_json)
workspace_list_instance = WorkspaceList(items_list=[workspace_instance])
workspace_list_instance_json = workspace_list_instance.to_json()
workspace_list_instance = WorkspaceList.from_json(workspace_list_instance)
print(f"workspace_instance::{workspace_instance}")
print(f"workspace_list_instance::{workspace_list_instance}")

print("Load credentials")
workspace_list_instance=WorkspaceList.from_json_file(PERMUTIVE_APPLICATION_CREDENTIALS)
print(f"workspace_list_instance::{workspace_list_instance}")
random_workspace = random.choice(workspace_list_instance)
print(f"random_workspace::{random_workspace}")
print("====================================================")
print("from PermutiveAPI.Cohort import Cohort, CohortList")
from PermutiveAPI.Cohort import Cohort, CohortList
print("Create dummy instances")
cohort_instance = Cohort(id="cohort_id",
                         code="cohort_code",
                         name="cohort_name",
                         state="Act",
                         description="cohort_description",
                         last_updated_at="cohort_updated_at")
cohort_list_instance_json = cohort_instance.to_json()
cohort_instance = Cohort.from_json(cohort_list_instance_json)
cohort_list_instance = CohortList(items_list=[cohort_instance])
cohort_list_instance_json = cohort_list_instance.to_json()
cohort_list_instance = CohortList.from_json(cohort_list_instance)
print(f"cohort_instance::{cohort_instance}")
print(f"cohort_list_instance::{cohort_list_instance}")
print("get cohort_list")
include_child_workspaces_true = Cohort.list(api_key=random_workspace.api_key,
                                            include_child_workspaces=True)
include_child_workspaces_true.to_json_file(os.path.join("scratch", "cohort_list.json"))
include_child_workspaces_false = Cohort.list(api_key=random_workspace.api_key,
                                             include_child_workspaces=False)

print("Comparing cohorts with include_child_workspaces=True and include_child_workspaces=False")
print(f"include_child_workspaces_true::{include_child_workspaces_true}")
print(f"include_child_workspaces_false::{include_child_workspaces_false}")

if include_child_workspaces_true == include_child_workspaces_false:
    print("Both lists are identical.")
else:
    print("The lists are different.")

# Counting the number of items
count_true = len(include_child_workspaces_true)
count_false = len(include_child_workspaces_false)
print(f"Number of items with include_child_workspaces=True: {count_true}")
print(f"Number of items with include_child_workspaces=False: {count_false}")

# Convert Cohort objects to (id, name) tuples for easy comparison
set_true = {(cohort.id, cohort.name) for cohort in include_child_workspaces_true}
set_false = {(cohort.id, cohort.name) for cohort in include_child_workspaces_false}

# Compute intersection (common cohorts)
intersection = set_true & set_false

# Compute exclusive elements
exclusive_true = set_true - set_false  # Elements only in include_child_workspaces_true
exclusive_false = set_false - set_true  # Elements only in include_child_workspaces_false

# Convert back to list of Cohort objects
intersection_list = [cohort for cohort in include_child_workspaces_true if (cohort.id, cohort.name) in intersection]

exclusive_true_list = [cohort for cohort in include_child_workspaces_true if (cohort.id, cohort.name) in exclusive_true]
exclusive_false_list = [cohort for cohort in include_child_workspaces_false if (cohort.id, cohort.name) in exclusive_false]

# Print results
print("Intersection:", [(c.id, c.name) for c in intersection_list])
print("Exclusive to include_child_workspaces_true:", [(c.id, c.name) for c in exclusive_true_list])
print("Exclusive to include_child_workspaces_false:", [(c.id, c.name) for c in exclusive_false_list])
