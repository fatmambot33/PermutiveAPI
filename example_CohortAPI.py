import logging
from PermutiveAPI.Workspace import WorkspaceList

from dotenv import load_dotenv
import logging
logging.getLogger().setLevel(logging.INFO)
load_dotenv()
ws_list = WorkspaceList.from_json()
master = ws_list.Masterworkspace
for ws in ws_list:
    logging.info(ws.name)
    cohorts = ws.list_cohorts()
    print(cohorts)
