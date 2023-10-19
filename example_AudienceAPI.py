import logging
from PermutiveAPI.Workspace import WorkspaceList
from PermutiveAPI.AudienceAPI import AudienceAPI
from dotenv import load_dotenv
import logging
logging.getLogger().setLevel(logging.INFO)
load_dotenv()
ws_list = WorkspaceList.from_json()
master = ws_list.get_Masterworkspace()
masterKey = master.privateKey
masterName = master.name
for ws in ws_list:
    logging.info(ws.name)
    api = AudienceAPI(api_key=ws.privateKey)
    api.sync_imports_cohorts(
        prefix=f"{ws.name} | Import | ")
