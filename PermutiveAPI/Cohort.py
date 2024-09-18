
import json
import logging
from typing import Dict, List, Optional, Union
from dataclasses import dataclass, field,asdict
from datetime import datetime, timedelta
from .Utils import RequestHelper, FileHelper
from collections import defaultdict

_API_VERSION = "v2"
_API_ENDPOINT = f'https://api.permutive.app/cohorts-api/{_API_VERSION}/cohorts/'
_API_PAYLOAD = ["id", "name", "query", "description", "tags"]


@dataclass
class Cohort():
    """
    Represents a cohort entity in the Permutive ecosystem.

    Attributes:
        name (str): The name of the cohort.
        id (str, optional): The unique identifier of the cohort.
        code (str, optional): The code associated with the cohort.
        query (Dict, optional): The query used to define the cohort.
        tags (List[str], optional): Tags associated with the cohort.
        description (str, optional): A description of the cohort.
        state (str, optional): The state of the cohort.
        segment_type (str, optional): The type of segment.
        live_audience_size (int, optional): The size of the live audience.
        created_at (datetime, optional): The creation date of the cohort.
        last_updated_at (datetime, optional): The last update date of the cohort.
        workspace_id (str, optional): The ID of the associated workspace.
        request_id (str, optional): The request ID associated with cohort operations.
        error (str, optional): An error message, if an error occurs during operations.

    Methods:
        create(privateKey: str) -> None:
            Creates a new cohort in the Permutive ecosystem.

        update(privateKey: str) -> Cohort:
            Updates an existing cohort.

        delete(privateKey: str) -> None:
            Deletes the current cohort.

        get_by_id(id: str, privateKey: str) -> Cohort:
            Retrieves a cohort by its unique identifier.

        get_by_name(name: str, privateKey: str) -> Optional[Cohort]:
            Retrieves a cohort by its name.

        get_by_code(code: Union[int, str], privateKey: str) -> Optional[Cohort]:
            Retrieves a cohort by its code.

        list(include_child_workspaces: bool = False, privateKey: str) -> List[Cohort]:
            Retrieves a list of all cohorts.
    """

    name: str
    id: Optional[str] = None
    code: Optional[str] = None
    query: Optional[Dict] = None
    tags: Optional[List[str]] = None
    description: Optional[str] = None
    state: Optional[str] = None
    segment_type: Optional[str] = None
    live_audience_size: Optional[int] = 0
    created_at: Optional[datetime] = field(default_factory=datetime.now)
    last_updated_at: Optional[datetime] = field(default_factory=datetime.now)
    workspace_id: Optional[str] = None
    request_id: Optional[str] = None
    error: Optional[str] = None

    def create(self,
               privateKey: str):
        """
        Creates a new cohort.

        :param cohort: Cohort to be created.
        :return: Created cohort object.
        """
        logging.debug(f"{datetime.now()}::CohortAPI::create::{self.name}")
        if not self.query:
            raise ValueError('query must be specified')
        if self.id:
            logging.warning("id is specified")
        url = f"{_API_ENDPOINT}"
        response = RequestHelper.postRequest_static(privateKey=privateKey,
                                                    url=url,
                                                    data=RequestHelper.to_payload_static(self, _API_PAYLOAD))
        created = Cohort(**response.json())
        self.id = created.id
        self.code = created.code

    def update(self,
               privateKey: str):
        """
        Updates an existing cohort.

        :param cohort_id: ID of the cohort to be updated.
        :param updated_cohort: Updated cohort data.
        :return: Updated cohort object.
        """
        logging.debug(f"{datetime.now()}::CohortAPI::update::{self.name}")
        if not self.id:
            raise ValueError("Cohort ID must be specified for update.")
        url = f"{_API_ENDPOINT}{self.id}"

        response = RequestHelper.patchRequest_static(privateKey=privateKey,
                                                     url=url,
                                                     data=RequestHelper.to_payload_static(self, _API_PAYLOAD))

        return Cohort(**response.json())

    def delete(self,
               privateKey) -> None:
        """
        Deletes a specific cohort.
        :param cohort_id: ID of the cohort to be deleted.
        :return: None
        """
        logging.debug(f"{datetime.now()}::CohortAPI::delete::{self.name}")
        if not self.id:
            raise ValueError("Cohort ID must be specified for deletion.")
        url = f"{_API_ENDPOINT}{self.id}"
        RequestHelper.deleteRequest_static(privateKey=privateKey,
                                           url=url)

    @staticmethod
    def get_by_id(id: str,
                  privateKey: str) -> 'Cohort':
        """
        Fetches a specific cohort from the API using its ID.

        :param cohort_id: ID of the cohort.
        :return: Cohort object or None if not found.
        """
        logging.debug(f"{datetime.now()}::CohortAPI::get::{id}")
        url = f"{_API_ENDPOINT}{id}"
        response = RequestHelper.getRequest_static(privateKey=privateKey,
                                                   url=url)

        return Cohort(**response.json())

    @staticmethod
    def get_by_name(
        name: str,
        privateKey: str
    ) -> Optional['Cohort']:
        '''
            Object Oriented Permutive Cohort seqrch
            :rtype: Cohort object
            :param cohort_name: str Cohort Name. Required
            :return: Cohort object
        '''
        logging.debug(f"{datetime.now()}::CohortAPI::get_by_name::{name}")

        for cohort in Cohort.list(include_child_workspaces=True,
                                  privateKey=privateKey):
            if name == cohort.name and cohort.id:
                return Cohort.get_by_id(id=cohort.id,
                                        privateKey=privateKey)

    @staticmethod
    def get_by_code(
            code: Union[int, str],
            privateKey: str) -> Optional['Cohort']:
        '''
        Object Oriented Permutive Cohort seqrch
        :rtype: Cohort object
        :param cohort_code: Union[int, str] Cohort Code. Required
        :return: Cohort object
        '''
        logging.debug(f"{datetime.now()}::CohortAPI::get_by_code::{code}")
        for cohort in Cohort.list(include_child_workspaces=True,
                                  privateKey=privateKey):
            if code == cohort.code and cohort.id:
                return Cohort.get_by_id(id=cohort.id,
                                        privateKey=privateKey)

    @staticmethod
    def list(privateKey: str, include_child_workspaces=False) -> 'CohortList':
        """
            Fetches all cohorts from the API.

            :return: List of all cohorts.
        """
        logging.debug(f"CohortAPI::list")

        url = RequestHelper.gen_url_with_key(_API_ENDPOINT, privateKey)
        if include_child_workspaces:
            url = f"{url}&include-child-workspaces=true"

        response = RequestHelper.getRequest_static(privateKey, url)
        cohort_list = CohortList([Cohort(**cohort)
                                 for cohort in response.json()])
        return cohort_list

    def to_json(self, filepath: str):
        FileHelper.check_filepath(filepath)
        with open(file=filepath, mode='w', encoding='utf-8') as f:
            json.dump(asdict(self), f,
                      ensure_ascii=False, indent=4, default=FileHelper.json_default)

    @staticmethod
    def from_json(filepath: str) -> 'Cohort':
        if not FileHelper.file_exists(filepath):
            raise ValueError(f'{filepath} does not exist')
        with open(file=filepath, mode='r') as json_file:
            return Cohort(**json.load(json_file))

    def dict_to_sql_databricks(self, table_mapping,
                               start_date: datetime = datetime.now() - timedelta(days=31),
                               end_date: datetime = datetime.now() - timedelta(days=1)):

        def condition_to_sql(condition: Dict[str, Union[str, list]], property_name: str) -> Optional[str]:
            # Remove 'properties.' prefix
            property_name = property_name.replace('properties.', '')
            sql_parts = []
            if 'contains' in condition:
                values = condition['contains']
                if isinstance(values, str):
                    values = [values]
                for value in values:
                    sql_parts.append(
                        "{} LIKE '%{}%'".format(property_name, value))
            elif 'equal_to' in condition:
                values = condition['equal_to']
                if isinstance(values, list):
                    sql_parts.append("{} IN ({})".format(
                        property_name, ', '.join("'{}'".format(v) for v in values)))
                else:
                    sql_parts.append("{} = '{}'".format(property_name, values))
            elif 'list_contains' in condition:
                values = condition['list_contains']
                if isinstance(values, list):
                    sql_parts.append("ARRAYS_OVERLAP({}, ARRAY({}))".format(
                        property_name, ', '.join("'{}'".format(v) for v in values)))
                else:
                    sql_parts.append(
                        "ARRAYS_OVERLAP({}, ARRAY('{}'))".format(property_name, values))
            elif 'condition' in condition:
                logging.debug(f"Condition: {condition}")
            if len(sql_parts) > 0:
                return " OR ".join(sql_parts)

        def parse_where(where_clause):
            if 'or' in where_clause:
                or_conditions = []
                for clause in where_clause['or']:
                    or_conditions.append(parse_where(clause))
                return " OR ".join(f"({cond})" for cond in or_conditions)
            elif 'and' in where_clause:
                and_conditions = []
                for clause in where_clause['and']:
                    and_conditions.append(parse_where(clause))
                return " AND ".join(f"({cond})" for cond in and_conditions)
            elif 'condition' in where_clause and 'property' in where_clause:
                return condition_to_sql(where_clause['condition'], where_clause['property'])
            else:
                print(where_clause)

        def parse_event(event, table_name) -> Optional[str]:
            start_d = start_date.strftime('%Y-%m-%d')
            end_d = end_date.strftime('%Y-%m-%d')
            event_conditions = [f"dt BETWEEN '{start_d}' AND '{end_d}'"]
            where_sql = None
            if 'where' in event:
                where_sql = parse_where(event['where'])
                event_conditions.append(f"({where_sql})")
            base_query = f"SELECT user_id FROM {table_name} WHERE " + \
                " AND ".join(event_conditions) + " GROUP BY user_id"
            if 'frequency' in event:
                frequency = event['frequency']
                if 'greater_than_or_equal_to' in frequency:
                    base_query += f" HAVING COUNT(*) >= {frequency['greater_than_or_equal_to']}"
                if 'greater_than' in frequency:
                    base_query += f" HAVING COUNT(*) > {frequency['greater_than']}"
                if 'equal_to' in frequency:
                    base_query += f" HAVING COUNT(*) = {frequency['equal_to']}"
            if where_sql:
                return base_query
            else:
                print(f"{table_name}:no where")

        def parse_query(query):
            if 'or' in query:
                queries = []
                for event in query['or']:
                    parsed_query = parse_query(event)
                    if parsed_query:
                        queries.append(parsed_query)
                if len(queries) == 0:
                    return None
                sqlquery = " UNION ".join(queries)
                return f"({sqlquery})"
            elif 'and' in query:
                queries = []
                for event in query['and']:
                    parsed_query = parse_query(event)
                    if parsed_query:
                        queries.append(parsed_query)
                if len(queries) == 0:
                    return None
                sqlquery = " INTERSECT ".join(queries)
                return f"({sqlquery})"
            elif 'event' in query:
                event_name = query['event']
                table_name = table_mapping.get(event_name)
                if table_name:
                    parsed_event = parse_event(query, table_name)
                    return parsed_event
                else:
                    print(f"Missing mapping for {event_name}")
            else:
                print(query)

        return parse_query(self.query)


class CohortList(List[Cohort]):

    def __init__(self, cohorts: Optional[List[Cohort]] = None):
        super().__init__(cohorts if cohorts is not None else [])
        self._id_dictionary_cache: Dict[str, Cohort] = {}
        self._name_dictionary_cache: Dict[str, Cohort] = {}
        self._tag_dictionary_cache: Dict[str, List[Cohort]] = defaultdict(list)
        self._workspace_dictionary_cache: Dict[str, List[Cohort]] = defaultdict(list)
        self.rebuild_cache()

    def rebuild_cache(self):
        """Rebuilds all caches based on the current state of the list."""
        self._id_dictionary_cache = {
            cohort.id: cohort for cohort in self if cohort.id}
        self._name_dictionary_cache = {
            cohort.name: cohort for cohort in self if cohort.name}
        self._tag_dictionary_cache = defaultdict(CohortList)
        self._workspace_dictionary_cache = defaultdict(CohortList)
        for cohort in self:
            if cohort.tags:
                for tag in cohort.tags:
                    self._tag_dictionary_cache[tag].append(cohort)
            if cohort.workspace_id:
                self._workspace_dictionary_cache[cohort.workspace_id].append(
                    cohort)

    @property
    def id_dictionary(self) -> Dict[str, Cohort]:
        """Returns a dictionary of cohorts indexed by their IDs."""
        if not self._id_dictionary_cache:
            self.rebuild_cache()
        return self._id_dictionary_cache

    @property
    def name_dictionary(self) -> Dict[str, Cohort]:
        """Returns a dictionary of cohorts indexed by their names."""
        if not self._name_dictionary_cache:
            self.rebuild_cache()
        return self._name_dictionary_cache

    @property
    def tag_dictionary(self) -> Dict[str, List[Cohort]]:
        """Returns a dictionary of cohorts indexed by their tags."""
        if not self._tag_dictionary_cache:
            self.rebuild_cache()
        return self._tag_dictionary_cache

    @property
    def workspace_dictionary(self) -> Dict[str, List[Cohort]]:
        """Returns a dictionary of cohorts indexed by their workspace IDs."""
        if not self._workspace_dictionary_cache:
            self.rebuild_cache()
        return self._workspace_dictionary_cache

    def to_json(self, filepath: str):
        """Saves the CohortList to a JSON file at the specified filepath."""
        FileHelper.check_filepath(filepath)
        with open(file=filepath, mode='w', encoding='utf-8') as f:
            json.dump([asdict(cohort) for cohort in self], f, ensure_ascii=False, indent=4,
                      default=FileHelper.json_default)

    @staticmethod
    def from_json(filepath: str) -> 'CohortList':
        """Creates a new CohortList from a JSON file at the specified filepath."""
        cohort_list = FileHelper.from_json(filepath)
        return CohortList([Cohort(**cohort) for cohort in cohort_list])
