import logging
from typing import Dict, List, Optional
from dataclasses import dataclass
from datetime import datetime

from .APIRequestHandler import APIRequestHandler
from .Segment import Segment
from .Utils import FileHelper

_API_VERSION = "v1"
_API_ENDPOINT = f'https://api.permutive.app/audience-api/{_API_VERSION}/imports'

@dataclass
class Import(FileHelper):
    """
    Dataclass for the Import in the Permutive ecosystem.
    """
    id: str
    name: str
    code: str
    relation: str
    identifiers: List[str]
    description: Optional[str] = None
    source: Optional['Source'] = None
    inheritance: Optional[str] = None
    segments: Optional[List['Segment']] = None
    updated_at: Optional[datetime] = datetime.now()

    @dataclass
    class Source(FileHelper):
        """
        Dataclass for the Source entity in the Permutive ecosystem.
        """
        id: str
        state: Dict
        bucket: str
        permissions: Dict
        phase: str
        type: str
    @staticmethod
    def get_by_id(id: str,
                  privateKey: str) -> 'Import':
        """
        Fetches a specific import by its id.

        :param import_id: ID of the import.
        :return: The requested Importt.
        """
        logging.info(f"AudienceAPI::get_import::{id}")
        url = f"{_API_ENDPOINT}/{id}"
        response = APIRequestHandler.getRequest_static(url=url,
                                                       privateKey=privateKey)
        if response is None:
            raise ValueError('Unable to get_import')
        return Import(**response.json())
    
    @staticmethod
    def list(privateKey:str) -> List['Import']:
        """
        Fetches all imports from the API.

        :return: List of all imports.
        """
        logging.info(f"AudienceAPI::list_imports")
        url = _API_ENDPOINT
        response = APIRequestHandler.getRequest_static(privateKey=privateKey,
                                                       url=url)
        imports = response.json()
        return [Import(**item) for item in imports['items']]

    
