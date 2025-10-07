"""Import management for the Permutive API."""

import logging
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import (
    Any,
    Callable,
    DefaultDict,
    Dict,
    Iterable,
    List,
    Optional,
    Tuple,
    Type,
    Union,
    cast,
)

from requests import Response
import pandas as pd

from PermutiveAPI._Utils import http
from PermutiveAPI._Utils.http import BatchRequest, Progress, process_batch
from PermutiveAPI._Utils.json import JSONSerializable, load_json_list
from . import _API_ENDPOINT
from .Source import Source
from .Segment import SegmentList


@dataclass
class Import(JSONSerializable[Dict[str, Any]]):
    """Represents an Import in the Permutive ecosystem.

    Parameters
    ----------
    id : str
        The ID of the import.
    name : str
        The name of the import.
    code : str
        The code of the import.
    relation : str
        The relation of the import.
    identifiers : List[str]
        A list of identifiers for the import.
    source : Source
        The source of the import.
    description : Optional[str], optional
        An optional description of the import (default: None).
    inheritance : Optional[str], optional
        An optional inheritance of the import (default: None).
    segments : Optional[SegmentList], optional
        An optional list of segments in the import (default: None).
    created_at : Optional[datetime], optional
        The timestamp of the creation (default: `datetime.now(tz=timezone.utc)`).
    updated_at : Optional[datetime], optional
        The timestamp of the last update (default: `datetime.now(tz=timezone.utc)`).

    Methods
    -------
    get_by_id(id, api_key)
        Fetch a specific import by its ID.
    list(api_key)
        Retrieve a list of all imports.
    batch_get_by_id(ids, api_key, max_workers=None, progress_callback=None)
        Fetch multiple imports concurrently using the shared batch runner.

    """

    _request_helper = http

    id: str
    name: str
    code: str
    relation: str
    identifiers: List[str]
    source: "Source"
    description: Optional[str] = None
    inheritance: Optional[str] = None
    segments: Optional["SegmentList"] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    def __post_init__(self) -> None:
        """Normalize timestamps for deterministic serialization.

        If one of ``created_at`` or ``updated_at`` is missing, it is copied
        from the other. If both are missing, they are initialized to the
        same current UTC timestamp. This avoids microsecond-level drift
        between the two fields.
        """
        if self.created_at is None and self.updated_at is None:
            now = datetime.now(tz=timezone.utc)
            self.created_at = now
            self.updated_at = now
        elif self.created_at is None:
            self.created_at = self.updated_at
        elif self.updated_at is None:
            self.updated_at = self.created_at

    @staticmethod
    def get_by_id(id: str, api_key: str) -> "Import":
        """Fetch a specific import by its ID.

        Parameters
        ----------
        id : str
            ID of the import.
        api_key : str
            The API key for authentication.

        Returns
        -------
        Import
            The requested import.

        Raises
        ------
        ValueError
            If the import cannot be fetched.
        """
        logging.debug(f"AudienceAPI::get_import::{id}")
        url = f"{_API_ENDPOINT}/{id}"
        response = Import._request_helper.get(url=url, api_key=api_key)
        if not response:
            raise ValueError("Unable to get_import")
        return Import.from_json(response.json())

    @staticmethod
    def list(api_key: str) -> "ImportList":
        """Retrieve a list of all imports.

        Parameters
        ----------
        api_key : str
            The API key for authentication.

        Returns
        -------
        ImportList
            A list of Import objects.

        Raises
        ------
        ValueError
            If the import list cannot be fetched.
        """
        logging.debug(f"AudienceAPI::list_imports")
        url = _API_ENDPOINT
        response = Import._request_helper.get(api_key=api_key, url=url)
        if response is None:
            raise ValueError("Response is None")
        imports = response.json()
        return ImportList.from_json(imports["items"])

    @classmethod
    def batch_get_by_id(
        cls,
        ids: Iterable[str],
        *,
        api_key: str,
        max_workers: Optional[int] = None,
        progress_callback: Optional[Callable[[Progress], None]] = None,
    ) -> Tuple[Dict[str, "Import"], List[Tuple[BatchRequest, Exception]]]:
        """Fetch multiple imports concurrently.

        Parameters
        ----------
        ids : Iterable[str]
            Import identifiers to retrieve.
        api_key : str
            API key for authentication.
        max_workers : int | None, optional
            Maximum number of worker threads (default: ``None`` to defer to the
            shared batch runner's default).
        progress_callback : Callable[[Progress], None] | None, optional
            Invoked after each request completes. Receives a
            :class:`~PermutiveAPI._Utils.http.Progress` snapshot describing the
            batch throughput and latency (including the estimated seconds per
            1,000 requests).

        Returns
        -------
        dict[str, Import]
            Mapping of successfully fetched IDs to :class:`Import` instances.
        list[tuple[BatchRequest, Exception]]
            Requests that raised exceptions alongside their originating
            descriptors.

        Notes
        -----
        This helper delegates work to :func:`PermutiveAPI._Utils.http.process_batch`.

        Examples
        --------
        >>> ids = ["import-1", "import-2"]
        >>> imports, failures = Import.batch_get_by_id(
        ...     ids,
        ...     api_key="test-key",
        ... )  # doctest: +SKIP
        >>> sorted(imports.keys())  # doctest: +SKIP
        ['import-1', 'import-2']
        >>> failures  # doctest: +SKIP
        []
        >>> def on_progress(progress):
        ...     avg = progress.average_per_thousand_seconds
        ...     avg_display = f"{avg:.2f}s" if avg is not None else "n/a"
        ...     print(
        ...         f"{progress.completed}/{progress.total} (errors: {progress.errors}) "
        ...         f"avg/1000: {avg_display}"
        ...     )
        >>> _imports, _failures = Import.batch_get_by_id(
        ...     ids,
        ...     api_key="test-key",
        ...     max_workers=4,
        ...     progress_callback=on_progress,
        ... )  # doctest: +SKIP
        """
        results: Dict[str, "Import"] = {}
        batch_requests: List[BatchRequest] = []

        for import_id in ids:
            url = f"{_API_ENDPOINT}/{import_id}"

            def _make_callback(target_id: str) -> Callable[[Response], None]:
                def _callback(response: Response) -> None:
                    results[target_id] = cls.from_json(response.json())

                return _callback

            batch_requests.append(
                BatchRequest(
                    method="GET",
                    url=url,
                    callback=_make_callback(import_id),
                )
            )

        _, errors = process_batch(
            batch_requests,
            api_key=api_key,
            max_workers=max_workers,
            progress_callback=progress_callback,
        )
        return results, errors


class ImportList(List[Import], JSONSerializable[List[Any]]):
    """Manage a list of Import objects.

    Provide caching for quick lookup and JSON (de)serialization helpers.

    Methods
    -------
    from_json(data)
        Deserialize a list of imports from various JSON representations.
    id_dictionary()
        Return a dictionary of imports indexed by their IDs.
    name_dictionary()
        Return a dictionary of imports indexed by their names.
    code_dictionary()
        Return a dictionary of imports indexed by their codes.
    identifier_dictionary()
        Return a dictionary of imports indexed by their identifiers.
    to_pd_dataframe()
        Convert the import list into a pandas ``DataFrame``.
    """

    @classmethod
    def from_json(
        cls: Type["ImportList"],
        data: Union[dict, List[dict], str, Path],
    ) -> "ImportList":
        """Deserialize a list of imports from various JSON representations.

        Parameters
        ----------
        data : Union[dict, List[dict], str, Path]
            The JSON data to deserialize. It can be a dictionary, a list of
            dictionaries, a JSON string, or a path to a JSON file.

        Returns
        -------
        ImportList
            An `ImportList` instance created from the provided JSON data.
        """
        import_list = load_json_list(data, cls.__name__, "Import")

        # Special handling for 'source' which is a nested JSONSerializable
        def create_import(item) -> Import:
            source_data = item.get("source")
            if source_data:
                source_instance = Source.from_json(source_data)
                item["source"] = source_instance
            return Import.from_json(item)

        return cls([create_import(item) for item in import_list])

    def __init__(self, items_list: Optional[List[Import]] = None):
        """Initialize the ImportList with an optional list of Import objects.

        Parameters
        ----------
        items_list : Optional[List[Import]], optional
            Import objects to initialize with (default: None).
        """
        super().__init__(items_list if items_list is not None else [])
        self._id_dictionary_cache: Dict[str, Import] = {}
        self._name_dictionary_cache: Dict[str, Import] = {}
        self._code_dictionary_cache: Dict[str, Import] = {}
        self._identifier_dictionary_cache: DefaultDict[str, ImportList] = defaultdict(
            ImportList
        )
        self._refresh_cache()

    def _refresh_cache(self) -> None:
        """Refresh all caches based on the current state of the list."""
        self._id_dictionary_cache = {}
        self._name_dictionary_cache = {}
        self._code_dictionary_cache = {}
        self._identifier_dictionary_cache = defaultdict(ImportList)
        for _import in self:
            self._id_dictionary_cache[_import.id] = _import
            self._name_dictionary_cache[_import.name] = _import
            self._code_dictionary_cache[_import.code] = _import
            for identifier in _import.identifiers:
                self._identifier_dictionary_cache[identifier].append(_import)

    @property
    def id_dictionary(self) -> Dict[str, Import]:
        """Return a dictionary of imports indexed by their IDs.

        Returns
        -------
        Dict[str, Import]
            Mapping of import IDs to ``Import`` objects.
        """
        if not self._id_dictionary_cache:
            self._refresh_cache()
        return self._id_dictionary_cache

    @property
    def name_dictionary(self) -> Dict[str, Import]:
        """Return a dictionary of imports indexed by their names.

        Returns
        -------
        Dict[str, Import]
            Mapping of import names to ``Import`` objects.
        """
        if not self._name_dictionary_cache:
            self._refresh_cache()
        return self._name_dictionary_cache

    @property
    def code_dictionary(self) -> Dict[str, Import]:
        """Return a dictionary of imports indexed by their codes.

        Returns
        -------
        Dict[str, Import]
            Mapping of import codes to ``Import`` objects.
        """
        if not self._code_dictionary_cache:
            self._refresh_cache()
        return self._code_dictionary_cache

    @property
    def identifier_dictionary(self) -> Dict[str, "ImportList"]:
        """Return a dictionary of imports indexed by their identifiers.

        Returns
        -------
        Dict[str, ImportList]
            Mapping of identifiers to lists of imports.
        """
        if not self._identifier_dictionary_cache:
            self._refresh_cache()
        return self._identifier_dictionary_cache

    def to_pd_dataframe(self) -> "pd.DataFrame":
        """Convert the import list into a pandas ``DataFrame``.

        Returns
        -------
        pandas.DataFrame
            A dataframe containing one row per import with serialized fields.
        """
        records = [cast(Dict[str, Any], _import.to_json()) for _import in self]
        return pd.DataFrame(records)
