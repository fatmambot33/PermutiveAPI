import logging


import PermutiveAPI

from dotenv import load_dotenv
import logging
logging.getLogger().setLevel(logging.INFO)
load_dotenv()
ws_list = PermutiveAPI.WorkspaceList.from_json()
for ws in ws_list:
    imports=ws.AudienceAPI.list_imports()
    print(imports)
    for item in imports:
        print(ws.AudienceAPI.list_segments(import_id=item.id))

