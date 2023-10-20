from PermutiveAPI.Workspace import WorkspaceList
from PermutiveAPI.CohortAPI import CohortAPI
from PermutiveAPI.AudienceAPI import AudienceAPI
from PermutiveAPI.Utils import ListHelper
from PermutiveAPI.Query import Query
import logging
import os
from dotenv import load_dotenv

load_dotenv()


logging.getLogger().setLevel(logging.INFO)

WORKSPACES_PATH = os.getenv("PERMUTIVE_APPLICATION_CREDENTIALS")
WORKSPACES = WorkspaceList.from_json(WORKSPACES_PATH)


workspace_id = "1dfc40bb-d155-4f15-970e-99450dbfa0e2"

master_workspace = WORKSPACES.get_Masterworkspace()

api_key = WORKSPACES.get_privateKey(workspace_id)
master_key = WORKSPACES.get_MasterprivateKey()


import_id = 'b7656e83-ae34-4380-a3fc-72476fdc6062'

query_id = "374f823e-71a3-46aa-baac-79532eaa9223"
master_query_id = "4ecbf8b0-88f3-43d1-bc5c-f49ec8b24f96"


second_party_segments = []
api = AudienceAPI(api_key=api_key)
oImport = api.get_import(import_id=import_id)

second_party_segments = [(oImport.code, segment.code)
                         for segment in api.list_segments(import_id=import_id)]
cohorts = {cohort.name: cohort.id for cohort in api.list_cohorts()}

market = "US"
name = f"Import | {oImport.name}"
query_name = f"{market} | {name}"
tags = ["#imports", oImport.name]
description = f"Wrap-up of all {oImport.name}'s segments"
workspace_query = Query(id=cohorts.get(query_name, None),
                        name=query_name,
                        second_party_segments=second_party_segments,
                        market=market,
                        workspace_id=workspace_id,
                        tags=tags,
                        description=description)
workspace_query.sync(api_key=api_key, new_tags=tags)

master_query_name = f"{master_workspace.name} | {name}"
master_query = Query(id=cohorts.get(master_query_name, None),
                     name=master_query_name,
                     second_party_segments=second_party_segments,
                     workspace_id=master_workspace.workspaceID,
                     tags=tags,
                     description=description)
master_query.sync(api_key=master_key, new_tags=tags)
