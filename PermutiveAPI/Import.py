import logging
from typing import Dict, List, Optional
from dataclasses import dataclass, field
from datetime import datetime
from collections import defaultdict


from PermutiveAPI.Utils import RequestHelper,  JSONSerializable


_API_VERSION = "v1"
_API_ENDPOINT = f'https://api.permutive.app/audience-api/{_API_VERSION}/imports'
_API_PAYLOAD = ['name', 'code', 'description', 'cpm', 'categories']


@dataclass
class Source(JSONSerializable):
    """
    Dataclass for the Source entity in the Permutive ecosystem.

    Attributes:
        id (str): Unique identifier for the source.
        state (Dict): State information of the source.
        type (str): Type of the source.
        schema_id (Optional[str]): Schema identifier associated with the source.
        cohorts (Optional[List[str]]): List of cohorts associated with the source.
        bucket (Optional[str]): Bucket information for the source.
        permissions (Optional[Dict]): Permissions associated with the source.
        phase (Optional[str]): Phase information of the source.
        errors (Optional[List[str]]): List of errors associated with the source.
        advertiser_name (Optional[str]): Name of the advertiser associated with the source.
        organization_id (Optional[str]): Organization identifier associated with the source.
        version (Optional[str]): Version information of the source.

    Methods:
        __str__(): Returns a pretty-printed JSON string representation of the source.
        to_json() -> dict: Converts the source object to a JSON-serializable dictionary.
        to_json_file(filepath: str): Writes the JSON representation of the source to a file.
        from_json_file(filepath: str) -> 'Source': Creates a Source object from a JSON file.

    """

    id: str
    state: Dict
    type: str
    schema_id: Optional[str] = None
    cohorts: Optional[List[str]] = None
    bucket: Optional[str] = None
    permissions: Optional[Dict] = None
    phase: Optional[str] = None
    errors: Optional[List[str]] = None
    advertiser_name: Optional[str] = None
    organization_id: Optional[str] = None
    version: Optional[str] = None


@dataclass
class Import(JSONSerializable):
    """
    Dataclass for the Import in the Permutive ecosystem.
    """
    id: str
    name: str
    code: str
    relation: str
    identifiers: List[str]
    source: 'Source'
    description: Optional[str] = None
    inheritance: Optional[str] = None
    segments: Optional['SegmentList'] = None
    updated_at: Optional[datetime] = datetime.now()

    @classmethod
    def get_by_id(cls,
                  id: str,
                  api_key: str) -> 'Import':
        """
        Fetches a specific import by its id.

        :param import_id: ID of the import.
        :return: The requested Importt.
        """
        logging.debug(f"{datetime.now()}::AudienceAPI::get_import::{id}")
        url = f"{_API_ENDPOINT}/{id}"
        response = RequestHelper.get_static(url=url,
                                            api_key=api_key)
        if not response:
            raise ValueError('Unable to get_import')
        return cls(**response.json())

    @classmethod
    def list(cls,
             api_key: str) -> List['Import']:
        logging.debug(f"{datetime.now()}::AudienceAPI::list_imports")
        url = _API_ENDPOINT
        response = RequestHelper.get_static(
            api_key=api_key, url=url)
        imports = response.json()

        def create_import(item):
            source_data = item.get('source')
            if source_data:
                source_instance = Source.from_json(source_data)
                item['source'] = source_instance
            return cls(**item)

        return [create_import(item) for item in imports['items']]


class ImportList(List[Import],
                 JSONSerializable):
    """
    A class representing a list of Import objects with additional functionality for caching and serialization.

    Attributes:
        _id_dictionary_cache (Dict[str, Import]): A cache dictionary of imports indexed by their IDs.
        _name_dictionary_cache (Dict[str, Import]): A cache dictionary of imports indexed by their names.
        _identifier_dictionary_cache (defaultdict[ImportList]): A cache dictionary of imports indexed by their identifiers.

    Methods:
        __init__(imports: Optional[List[Import]] = None):
            Initializes the ImportList with an optional list of Import objects and rebuilds the cache.

        rebuild_cache():
            Rebuilds all caches based on the current state of the list.

        id_dictionary() -> Dict[str, Import]:
            Returns a dictionary of imports indexed by their IDs.

        name_dictionary() -> Dict[str, Import]:
            Returns a dictionary of imports indexed by their names.

        identifier_dictionary() -> Dict[str, 'ImportList']:
            Returns a dictionary of imports indexed by their identifiers.

        to_json() -> List[dict]:
            Serializes the ImportList to a list of dictionaries.

        from_json(cls, data: List[dict]) -> 'ImportList':
            Deserializes a list of dictionaries to an ImportList object.
    """

    def __init__(self, items_list: Optional[List[Import]] = None):
        super().__init__(items_list if items_list is not None else [])
        self._id_dictionary_cache: Dict[str, Import] = {}
        self._name_dictionary_cache: Dict[str, Import] = {}
        self._identifier_dictionary_cache: Dict[str, ImportList] = defaultdict(
            list)
        self.rebuild_cache()

    def rebuild_cache(self):
        """Rebuilds all caches based on the current state of the list."""
        for _import in self:
            self._id_dictionary_cache[_import.id] = _import
            self._name_dictionary_cache[_import.id] = _import
            for identifier in _import.identifiers:
                self._identifier_dictionary_cache[identifier].append(_import)
        return self

    @property
    def id_dictionary(self) -> Dict[str, Import]:
        """Returns a dictionary of imports indexed by their IDs."""
        if not self._id_dictionary_cache:
            self.rebuild_cache()
        return self._id_dictionary_cache

    @property
    def name_dictionary(self) -> Dict[str, Import]:
        """Returns a dictionary of imports indexed by their names."""
        if not self._name_dictionary_cache:
            self.rebuild_cache()
        return self._name_dictionary_cache

    @property
    def identifier_dictionary(self) -> Dict[str, 'ImportList']:
        """Returns a dictionary of imports indexed by their identifiers."""
        if not self._identifier_dictionary_cache:
            self.rebuild_cache()
        return self._identifier_dictionary_cache

    def to_list(self) -> List[Import]:
        """Returns the list of imports."""
        return list(self)


@dataclass
class Segment(JSONSerializable):
    """
    A class to represent a Segment in the Permutive API.
    Attributes:
        code (str): The code of the segment.
        name (str): The name of the segment.
        import_id (str): The import ID of the segment.
        id (Optional[str]): The ID of the segment.
        description (Optional[str]): The description of the segment.
        cpm (Optional[float]): The cost per mille (CPM) of the segment.
        categories (Optional[List[str]]): A list of categories associated with the segment.
        updated_at (Optional[datetime]): The date and time when the segment was last updated.
    Methods:
        create(api_key: str):
        update(api_key: str):
        delete(api_key: str) -> bool:
        get(import_id: str, segment_id: str, api_key: str) -> 'Segment':
        get_by_code(import_id: str, segment_code: str, api_key: str) -> 'Segment':
        get_by_id(id: str, api_key: str) -> 'Segment':
        list(import_id: str, api_key: str) -> List['Segment']:
        to_json_file(filepath: str):
        from_json(data: dict) -> 'Segment':
        from_json_file(filepath: str) -> 'Segment'
    """

    code: str
    name: str
    import_id: str
    id: Optional[str] = None
    description: Optional[str] = None
    cpm: Optional[float] = 0.0
    categories: Optional[List[str]] = None
    updated_at: Optional[datetime] = field(default_factory=datetime.now)

    def create(self,
               api_key: str):
        """
        Creates a new segment using the provided private key.

        Args:
            api_key (str): The private key used for authentication.

        Raises:
            ValueError: If the segment creation fails.

        Returns:
            None
        """

        logging.debug(
            f"SegmentAPI::create_segment::{self.import_id}::{self.name}")
        url = f"{_API_ENDPOINT}/{self.import_id}/segments"
        response = RequestHelper.post_static(api_key=api_key,
                                             url=url,
                                             data=RequestHelper.to_payload_static(dataclass_obj=self,
                                                                                  api_payload=_API_PAYLOAD))
        if not response:
            raise ValueError('Unable to create_segment')

        self = Segment.from_json(response.json())

    def update(self,
               api_key: str):
        """
        Updates the segment using the provided private key.

        Args:
            api_key (str): The private key used for authentication.

        Raises:
            ValueError: If the segment update fails.

        Returns:
            None
        """

        logging.debug(
            f"SegmentAPI::update_segment::{self.import_id}::{self.name}")
        url = f"{_API_ENDPOINT}/{self.import_id}/segments/{self.id}"
        response = RequestHelper.patch_static(api_key=api_key,
                                              url=url,
                                              data=RequestHelper.to_payload_static(dataclass_obj=self,
                                                                                   api_payload=_API_PAYLOAD))
        if not response:
            raise ValueError('Unable to update_segment')
        self = Segment.from_json(response.json())

    def delete(self,
               api_key: str) -> bool:
        """
        Deletes a segment using the provided private key.

        Args:
            api_key (str): The private key used for authentication.

        Returns:
            bool: True if the segment was successfully deleted (status code 204), False otherwise.
        """

        logging.debug(
            f"SegmentAPI::delete_segment::{self.import_id:}::{self.id}")
        url = f"{_API_ENDPOINT}/{self.import_id}/segments/{self.id}"
        response = RequestHelper.delete_static(api_key=api_key,
                                               url=url)
        return response.status_code == 204

    @classmethod
    def get(import_id: str,
            segment_id: str,
            api_key: str) -> 'Segment':
        """
            Retrieve a segment by its import ID and segment ID.

            Args:
                import_id (str): The ID of the import.
                segment_id (str): The ID of the segment.
                api_key (str): The private key for authentication.

            Returns:
                Segment: The segment object retrieved from the API.

            Raises:
                ValueError: If the segment cannot be retrieved.
        """

        logging.debug(
            f"SegmentAPI::get_segment_by_id::{import_id}::{segment_id}")
        url = f"{_API_ENDPOINT}/{import_id}/segments/{segment_id}"
        response = RequestHelper.get_static(api_key,
                                            url=url)
        if not response:
            raise ValueError('Unable to get_segment')
        return Segment.from_json(response.json())

    @staticmethod
    def get_by_code(import_id: str,
                    segment_code: str,
                    api_key: str) -> 'Segment':
        """
        Retrieve a segment by its code.

        Args:
            import_id (str): The ID of the import.
            segment_code (str): The code of the segment to retrieve.
            api_key (str): The private key for authentication.

        Returns:
            Segment: The segment object retrieved by the given code.

        Raises:
            ValueError: If the segment cannot be retrieved.
        """

        logging.debug(
            f"SegmentAPI::get_segment_by_code::{import_id}::{segment_code}")
        url = f"{_API_ENDPOINT}/{import_id}/segments/code/{segment_code}"
        response = RequestHelper.get_static(url=url, api_key=api_key
                                            )
        if not response:
            raise ValueError('Unable to get_segment')
        return Segment.from_json(response.json())

    @staticmethod
    def get_by_id(id: str,
                  api_key: str) -> 'Segment':
        """
        Retrieve a Segment by its ID.

        Args:
            id (str): The ID of the segment to retrieve.
            api_key (str): The private key for authentication.

        Returns:
            Segment: The segment object retrieved by the given ID.

        Raises:
            ValueError: If the segment cannot be retrieved.
        """
        logging.debug(f"{datetime.now()}::SegmentAPI::get_segment:{id}")
        url = f"{_API_ENDPOINT}/{id}"
        response = RequestHelper.get_static(url=url,
                                            api_key=api_key)
        if not response:
            raise ValueError('Unable to get_by_id')
        return Segment.from_json(response.json())

    @staticmethod
    def list(import_id: str,
             api_key: str) -> List['Segment']:
        """
        Retrieves a list of segments for a given import ID.

        Args:
            import_id (str): The ID of the import to retrieve segments for.
            api_key (str): The private key used for authentication.

        Returns:
            List[Segment]: A list of Segment objects retrieved from the API.

        Raises:
            requests.exceptions.RequestException: If an error occurs while making the API request.
        """
        logging.debug(f"{datetime.now()}::SegmentAPI::list")

        base_url = f"{_API_ENDPOINT}/{import_id}/segments"
        all_segments = []
        next_token = None

        while True:
            # Construct the URL with the pagination token
            url = f"{base_url}?pagination_token={next_token}" if next_token else base_url
            response = RequestHelper.get_static(
                api_key=api_key, url=url)
            data = response.json()

            # Extract elements and add them to the list
            all_segments.extend([Segment.from_json(element)
                                for element in data.get('elements', [])])

            # Check for next_token in the pagination metadata
            next_token = data.get('pagination', {}).get('next_token')

            if not next_token:
                break  # Stop when there are no more pages

        return all_segments


class SegmentList(List[Segment],
                  JSONSerializable):
    """
    SegmentList is a custom list that holds Segment objects and provides additional functionality
    for caching and serializing the segments.
    Attributes:
        _id_dictionary_cache (Dict[str, Segment]): A cache dictionary mapping segment IDs to segment objects.
        _name_dictionary_cache (Dict[str, Segment]): A cache dictionary mapping segment names to segment objects.
        _code_dictionary_cache (Dict[str, Segment]): A cache dictionary mapping segment codes to segment objects.
    Methods:
        __init__(segments: Optional[List[Segment]] = None):
        rebuild_cache():
        id_dictionary() -> Dict[str, Segment]:
        name_dictionary() -> Dict[str, Segment]:
        code_dictionary() -> Dict[str, Segment]:
        to_json() -> List[dict]:
            Serializes the SegmentList to a list of dictionaries.
        from_json(data: List[dict]) -> 'SegmentList':
            Deserializes a list of dictionaries to a SegmentList."""

    def __init__(self,
                 items_list: Optional[List[Segment]] = None):
        """
        Initializes the SegmentList with an optional list of Segment objects.

        Args:
            segments (Optional[List[Segment]]): A list of Segment objects to initialize the SegmentList with. 
                                                If None, initializes with an empty list.
        """
        super().__init__(items_list if items_list is not None else [])
        self.rebuild_cache()

    def rebuild_cache(self):
        """
        Rebuilds all caches based on the current state of the list.

        This method updates the following caches:
        - _id_dictionary_cache: A dictionary mapping segment IDs to segment objects.
        - _name_dictionary_cache: A dictionary mapping segment names to segment objects.
        - _code_dictionary_cache: A dictionary mapping segment codes to segment objects.
        """
        self._id_dictionary_cache = {
            segment.id: segment for segment in self if segment.id}
        self._name_dictionary_cache = {
            segment.name: segment for segment in self if segment.name}
        self._code_dictionary_cache = {
            segment.code: segment for segment in self if segment.name}

    @property
    def id_dictionary(self) -> Dict[str, Segment]:
        """
        Returns a dictionary of segments indexed by their IDs.

        This method checks if the cache for the ID dictionary is empty. If it is,
        it rebuilds the cache by calling the `rebuild_cache` method. Finally, it
        returns the cached dictionary of segments.

        Returns:
            Dict[str, Segment]: A dictionary where the keys are segment IDs (str)
            and the values are Segment objects.
        """
        if not self._id_dictionary_cache:
            self.rebuild_cache()
        return self._id_dictionary_cache

    @property
    def name_dictionary(self) -> Dict[str, Segment]:
        """
        Returns a dictionary of segments indexed by their names.

        This method checks if the cache for the name dictionary is empty. If it is,
        it rebuilds the cache by calling the `rebuild_cache` method. Finally, it
        returns the cached dictionary of segments.

        Returns:
            Dict[str, Segment]: A dictionary where the keys are segment names and
            the values are Segment objects.
        """
        if not self._name_dictionary_cache:
            self.rebuild_cache()
        return self._name_dictionary_cache

    @property
    def code_dictionary(self) -> Dict[str, Segment]:
        """
        Returns a dictionary of segments indexed by their codes.

        This method checks if the cache for the code dictionary is empty. If it is,
        it rebuilds the cache by calling the `rebuild_cache` method. Finally, it 
        returns the cached dictionary of segments.

        Returns:
            Dict[str, Segment]: A dictionary where the keys are segment codes and 
            the values are Segment objects.
        """
        if not self._code_dictionary_cache:
            self.rebuild_cache()
        return self._code_dictionary_cache

    def to_list(self) -> List[Segment]:
        """Returns the list of segments."""
        return list(self)
