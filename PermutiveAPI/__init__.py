from dotenv import load_dotenv

import os
from .Workspace import WorkspaceList


load_dotenv()


env_var = ["PERMUTIVE_APPLICATION_CREDENTIALS"]
for var in env_var:
    if os.environ.get(var) is None:
        raise ValueError(f"Missing {var} in .env")


WORKSPACES = WorkspaceList.read_json(
    os.environ.get("PERMUTIVE_APPLICATION_CREDENTIALS"))
