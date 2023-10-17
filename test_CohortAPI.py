import logging
from PermutiveAPI.Workspace import WorkspaceList
from PermutiveAPI.CohortAPI import CohortAPI
from dotenv import load_dotenv
import logging
logging.getLogger().setLevel(logging.INFO)
load_dotenv()
ws_list = WorkspaceList.read_json()
masterKey = ws_list.get_MasterprivateKey()
for ws in ws_list:
    audience_api = CohortAPI(api_key=ws.privateKey)
    audience_api.list()
