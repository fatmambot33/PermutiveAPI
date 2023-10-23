import logging


from PermutiveAPI.Workspace import WorkspaceList

from dotenv import load_dotenv
import logging
logging.getLogger().setLevel(logging.INFO)
load_dotenv()
ws_list = WorkspaceList.from_json()
for ws in ws_list:
    imports=ws.list_imports()
    print(ws.list_imports())
    for item in imports:
        print(ws.list_segments(import_id=item.id))

