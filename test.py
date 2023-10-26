
from dotenv import load_dotenv

from PermutiveAPI import WorkspaceList

import os
import logging
load_dotenv()

WORKSPACES_PATH = os.getenv("PERMUTIVE_APPLICATION_CREDENTIALS")
WORKSPACES = WorkspaceList.from_json(WORKSPACES_PATH)


def test():
    for workspace in WORKSPACES:
        logging.info(f"{workspace.name}")
        print(workspace.list_cohorts())


test()
