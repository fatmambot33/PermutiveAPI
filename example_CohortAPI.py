import logging
from PermutiveAPI.Workspace import WorkspaceList
from PermutiveAPI.CohortAPI import CohortAPI
from dotenv import load_dotenv
import logging
logging.getLogger().setLevel(logging.INFO)
load_dotenv()
ws_list = WorkspaceList.from_json()
masterKey = ws_list.get_MasterprivateKey()
for ws in ws_list:
    logging.info(ws.name)
    api = CohortAPI(api_key=ws.privateKey)
    api_list=api.list()
    print(api_list)
