
from dotenv import load_dotenv


from GoogleCloudPlatformAPI.CloudStorage import CloudStorage
from GoogleCloudPlatformAPI.BigQuery import BigQuery
from GoogleCloudPlatformAPI.AdManager import CustomTargeting
from PermutiveAPI.Utils import FileHelper, ListHelper
from PermutiveAPI import Query, QueryList,UserAPI,CohortAPI,AudienceAPI,WorkspaceList

from datetime import date
import tarfile
import os
import logging
load_dotenv()


DEFAULT_GCS_BUCKET = os.getenv("DEFAULT_GCS_BUCKET")
WORKSPACES_PATH = os.getenv("PERMUTIVE_APPLICATION_CREDENTIALS")
WORKSPACES = WorkspaceList.from_json(WORKSPACES_PATH)
Masterworkspace = WORKSPACES.Masterworkspace


def daily_description(last_90d=False):
    logging.info(f"daily_description")
    WORKSPACES.from_description()


def daily_imports():
    logging.info(f"daily_imports")
    dt = date.today()
    folder_path = f"data/imports"
    bucket_name = "permutive-customer-tpd-upload-conde_nast_international"
    prefix = "2p/"
    cs = CloudStorage()
    files = cs.list_files(bucket_name=bucket_name,
                          prefix=prefix)
    for file in files:
        try:
            cs.copy_file(bucket_name=bucket_name,
                        file_name=file,
                        destination_bucket_name=DEFAULT_GCS_BUCKET)  # type: ignore
        except:
            logging.warning(f"Unable to copy {file} from {bucket_name} to {DEFAULT_GCS_BUCKET}")
    for workspace in WORKSPACES:
        for import_item in workspace.AudienceAPI.list_imports():
            filepath = f"{folder_path}/{import_item.id}.json"
            filepath_bck = f"{folder_path}/{dt}/{import_item.id}.json"
            import_item.to_json(import_item,filepath=filepath)
            
            if not FileHelper.file_exists(filepath_bck):
                if import_item.name != "Liveramp - Conde Nast US (3P)":
                    workspace.sync_import_cohorts(import_detail=import_item,
                                                   prefix=f"{workspace.name} | Import | ")
                import_item.to_json(import_item,filepath=filepath_bck)
                cs.upload_file_from_filename(
                    local_file_path=filepath_bck,
                    destination_file_path=f"imports/{dt}/{import_item.id}.json",
                    bucket_name=DEFAULT_GCS_BUCKET)  # type: ignore


def daily_cohorts():
    dt = date.today()
    logging.info(f"daily_cohorts")
    cs = CloudStorage()
    cohort_files = []
    folder_path = f"data/cohorts"
    destination_path = f"cohorts/{dt}/{dt}.tgz"
    tar_filename = f"{folder_path}/{dt}/{dt}.tgz"
    if not FileHelper.file_exists(tar_filename):
        cohorts = Masterworkspace.CohortAPI.list(include_child_workspaces=True)
        for cohort in cohorts:
            cohort_filename = f"{folder_path}/{cohort.id}.json"
            cohort_bck = f"{folder_path}/{dt}/{cohort.id}.json"
            if not FileHelper.file_exists(cohort_bck) and cohort.id is not None:
                cohort_details = Masterworkspace.CohortAPI.get(cohort_id=cohort.id)
                cohort_details.to_json(cohort_details,cohort_filename)
                cohort_details.to_json(cohort_details,cohort_bck)
            cohort_files.append(cohort_bck)

        with tarfile.open(tar_filename, "w:gz") as tar:
            for name in cohort_files:
                tar.add(name)
        for name in cohort_files:
            os.remove(name)
    cs.upload_file_from_filename(
        local_file_path=f'{os.path.dirname(os.path.realpath("__file__"))}/{tar_filename}',
        destination_file_path=destination_path,
        bucket_name=DEFAULT_GCS_BUCKET)  # type: ignore



def daily_wrap():
    dt = date.today()
    cs = CloudStorage()
    wrap_up = {}
    master_api = CohortAPI(api_key=Masterworkspace.privateKey)  # type: ignore
    cohorts = master_api.list(include_child_workspaces=True)
    cohort_names_to_id = {cohort.name: cohort.id for cohort in cohorts}
    for workspace in WORKSPACES:
        logging.info(f"daily_wrap::{workspace.name}")
        market_def_file = f'data/query/{workspace.name}.json'
        if workspace.name != "CN":
            query_list = QueryList.from_file(market_def_file)
            if query_list is not None:
                for query in query_list:
                    if query.cohort_global is not None:
                        wrap_query = Query(
                            name=f"CN | {query.cohort_global}",
                            market="CN",
                            segments=ListHelper.merge_list(
                                [query.number], query.segments),
                            accurate_segments=ListHelper.merge_list(
                                [query.accurate_id], query.accurate_segments),
                            volume_segments=ListHelper.merge_list(
                                [query.volume_id], query.volume_segments),
                            obsidian_segments=ListHelper.merge_list(
                                [query.obsidian_id], query.obsidian_segments),
                            keywords=query.keywords,
                            taxonomy=query.taxonomy,
                            domains=query.domains,
                            urls=query.urls,
                            cohort_global=query.cohort_global
                        )

                        if wrap_query.cohort_global not in wrap_up.keys():
                            seed_name = f"{wrap_query.market} | {wrap_query.cohort_global} | Seed"
                            wrap_query.id = cohort_names_to_id.get(
                                seed_name, None)

                            accurate_name = f"{wrap_query.market} | {wrap_query.cohort_global} | Accurate"
                            wrap_query.accurate_id = cohort_names_to_id.get(
                                accurate_name, None)

                            volume_name = f"{wrap_query.market} | {wrap_query.cohort_global} | Volume"
                            wrap_query.volume_id = cohort_names_to_id.get(
                                volume_name, None)

                            wrap_up[wrap_query.cohort_global] = wrap_query
                        else:
                            wrap_up[wrap_query.cohort_global].merge(wrap_query)

    for wrap in wrap_up:
        market_def_file = f'data/wrap/{wrap}.json'
        market_def_backup = f'data/wrap/{dt}/{wrap}.json'
        if not FileHelper.file_exists(market_def_backup):
            q = wrap_up[wrap]
            if q.keywords is not None:
                q.description = ",".join(sorted(q.keywords, key=str.casefold))

            q_seed = Query(name=f'CN | {wrap} | Seed',
                           id=q.id,
                           market="CN",
                           segments=q.segments,
                           cohort_global=wrap,
                           description=q.description)
            q_seed.sync(api_key=Masterworkspace.privateKey, new_tags=[
                'Seed', f'{q_seed.cohort_global}', 'CNI Only', '#spirewrap'], override_tags=True)

            if q.accurate_segments is not None:
                q_accurate = Query(name=f'CN | {wrap} | Accurate',
                                   id=q.accurate_id,
                                   market="CN",
                                   segments=q.accurate_segments,
                                   cohort_global=wrap)
                q_accurate.sync(api_key=Masterworkspace.privateKey, new_tags=[
                                'Accurate', f'{q_accurate.cohort_global}', 'CNI Only', '#spirewrap'], override_tags=True)

            if q.volume_segments is not None:
                q_volume = Query(name=f'CN | {wrap} | Volume',
                                 id=q.volume_id,
                                 market="CN",
                                 segments=q.volume_segments,
                                 cohort_global=wrap)
                q_volume.sync(api_key=Masterworkspace.privateKey, new_tags=[
                    'Volume', f'{q_volume.cohort_global}', 'CNI Only', '#spirewrap'], override_tags=True)
            wrap_up[wrap].to_json(market_def_file)
            wrap_up[wrap].to_json(market_def_backup)
            cs.upload_file_from_filename(
                local_file_path=market_def_backup,
                destination_file_path=f"wrapup/{dt}/{wrap}.json",
                bucket_name=DEFAULT_GCS_BUCKET)  # type: ignore


def daily_gam_kvp():
    logging.info(f"main:sync_gam_kvp")
    customTargetingKeyId = 11741567
    custom_targeting_client = CustomTargeting()
    cohort_api = CohortAPI(api_key=Masterworkspace.privateKey)  
    permutive_kvps = custom_targeting_client.get_key_value_pairs(
        customTargetingKeyId)
    cohorts_list = cohort_api.list(include_child_workspaces=True)
    permutive_cohorts = {
        str(cohort.code): cohort.name for cohort in cohorts_list}
    update_kvp = []
    delete_kvp = []
    for permutive_kvp in permutive_kvps:
        kvp_code = permutive_kvp['name']
        if kvp_code in permutive_cohorts.keys():
            cohort_name = permutive_cohorts[kvp_code].strip()
            if cohort_name != permutive_kvp['displayName']:
                permutive_kvp['displayName'] = cohort_name
                update_kvp.append(permutive_kvp)
                logging.info('Added:' + permutive_kvp['displayName'])
        else:
            if 'Deprecated' not in permutive_kvp['displayName']:
                delete_kvp.append(permutive_kvp)
                logging.info('Deleted:' + permutive_kvp['displayName'])

    custom_targeting_client.update_key_value_pairs(
        key_value_pairs=update_kvp)
    custom_targeting_client.delete_key_value_pairs(
        customTargetingKeyId, delete_kvp)


def daily_identities():
    bq = BigQuery()
    with open('/Users/pfourcat/Documents/GitHub/ColabNotebooks/sql/daily_identities.sql') as f:
        query = f.read()
    bq_identities_list = bq.execute_query(query=query)
    user_api = UserAPI(api_key=Masterworkspace.privateKey)  
    for bq_identity in bq_identities_list:
        identity=UserAPI.Identity(user_id=bq_identity["user_id"],
                                  aliases=[UserAPI.Identity.Alias(id=bq_identity["uID"],tag="uID", priority=0)])
        user_api.identify(identity=identity)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)

    daily_identities()
