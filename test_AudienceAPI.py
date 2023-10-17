import logging
from PermutiveAPI.Workspace import WorkspaceList
from PermutiveAPI.AudienceAPI import AudienceAPI
from dotenv import load_dotenv
import logging
logging.getLogger().setLevel(logging.INFO)
load_dotenv()
ws_list = WorkspaceList.read_json()
masterKey = ws_list.get_MasterprivateKey()
for ws in ws_list:
    logging.info(ws.name)
    audience_api = AudienceAPI(api_key=ws.privateKey)
    audience_api.sync_cohorts(masterKey=masterKey)
