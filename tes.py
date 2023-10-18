from PermutiveAPI.Workspace import WorkspaceList
from PermutiveAPI.CohortAPI import CohortAPI
from PermutiveAPI.AudienceAPI import AudienceAPI
from PermutiveAPI.Utils import ListHelper
from PermutiveAPI.Query import Query
import logging
import os
from dotenv import load_dotenv

load_dotenv()
workspaceID = "1dfc40bb-d155-4f15-970e-99450dbfa0e2"

logging.getLogger().setLevel(logging.INFO)
WORKSPACES_PATH = os.getenv("PERMUTIVE_APPLICATION_CREDENTIALS")
WORKSPACES = WorkspaceList.from_json(WORKSPACES_PATH)
api_key = WORKSPACES.get_privateKey(workspaceID)

query = Query(name="Liveramp - Conde Nast US (3P)",
              workspace_id=workspaceID,
              second_party_segments=[])
query.second_party_segments = []
api = AudienceAPI(api_key=api_key)


imports = api.list_imports()
segments = api.list_segments(import_id='b7656e83-ae34-4380-a3fc-72476fdc6062')

for segment in segments:
    query.second_party_segments.append(segment)
    print(segment)
query.sync(api_key=api_key)
