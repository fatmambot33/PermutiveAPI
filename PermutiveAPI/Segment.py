import logging
from typing import List, Optional
from dataclasses import dataclass
from datetime import datetime
import json

from .APIRequestHandler import APIRequestHandler
from .Utils import FileHelper

_API_VERSION = "v1"
_API_ENDPOINT = f'https://api.permutive.app/audience-api/{_API_VERSION}/imports'
_API_PAYLOAD = ['name', 'code', 'description', 'cpm', 'categories']


@dataclass
class Segment():
    """
    Dataclass for the Segment entity in the Permutive ecosystem.
    """
    id: str
    code: str
    name: str
    import_id: str
    description: Optional[str] = None
    cpm: Optional[float] = 0.0
    categories: Optional[List[str]] = None
    updated_at: Optional[datetime] = datetime.now()

    def create(self, privateKey: str):
        """
        Creates a new segment

        :return: The created Segment.
        """
        logging.debug(
            f"SegmentAPI::create_segment::{self.import_id}::{self.name}")
        url = f"{_API_ENDPOINT}/{self.import_id}/segments"
        response = APIRequestHandler.postRequest_static(privateKey=privateKey,
                                                        url=url,
                                                        data=APIRequestHandler.to_payload_static(dataclass_obj=self,
                                                                                                 api_payload=_API_PAYLOAD))
        if response is None:
            raise ValueError('Unable to create_segment')

        self = Segment(**response.json())

    def update(self, privateKey: str):
        """
        PATCH
        https://api.permutive.app/audience-api/v1/imports/{importId}/segments/{segmentId}
        Updates a segment for an import. The segment is identified by its globally unique public ID.
        https://developer.permutive.com/reference/patchimportsimportidsegmentssegmentid
        :param segment: SegmentAPI.Import.Segment to update
        :return: The updated Segment.
        """

        logging.debug(
            f"SegmentAPI::update_segment::{self.import_id}::{self.name}")
        url = f"{_API_ENDPOINT}/{self.import_id}/segments/{self.id}"
        response = APIRequestHandler.patchRequest_static(privateKey=privateKey,
                                                         url=url,
                                                         data=APIRequestHandler.to_payload_static(dataclass_obj=self,
                                                                                                  api_payload=_API_PAYLOAD))
        if response is None:
            raise ValueError('Unable to update_segment')
        self = Segment(**response.json())

    def delete(self, privateKey: str) -> bool:
        """
        Deletes a specific segment by its id.

        :param import_id: ID of the import.
        :param segment_id: ID of the segment.
        :return: True if deletion was successful, otherwise False.
        """
        logging.debug(
            f"SegmentAPI::delete_segment::{self.import_id:}::{self.id}")
        url = f"{_API_ENDPOINT}/{self.import_id}/segments/{self.id}"
        response = APIRequestHandler.deleteRequest_static(privateKey=privateKey,
                                                          url=url)
        return response.status_code == 204

    @staticmethod
    def get(import_id: str,
            segment_id: str,
            privateKey: str) -> 'Segment':
        """
        Fetches a specific segment by its id.
        https://developer.permutive.com/reference/getimportsimportidsegmentssegmentid
        :param import_id: ID of the import.
        :param segment_id: UUID of the segment.
        :return: The requested Segment.
        """
        logging.debug(
            f"SegmentAPI::get_segment_by_id::{import_id}::{segment_id}")
        url = f"{_API_ENDPOINT}/{import_id}/segments/{segment_id}"
        response = APIRequestHandler.getRequest_static(privateKey,
                                                       url=url)
        if response is None:
            raise ValueError('Unable to get_segment')
        return Segment(**response.json())

    @staticmethod
    def get_by_code(
        import_id: str,
        segment_code: str,
            privateKey: str) -> 'Segment':
        """
        Fetches a specific segment by its code.
        https://developer.permutive.com/reference/getimportsimportidsegmentscodesegmentcode
        :param import_id: ID of the import.
        :param segment_code: Public code of the segment.
        :return: The requested Segment.
        """
        logging.debug(
            f"SegmentAPI::get_segment_by_code::{import_id}::{segment_code}")
        url = f"{_API_ENDPOINT}/{import_id}/segments/code/{segment_code}"
        response = APIRequestHandler.getRequest_static(url=url, privateKey=privateKey
                                                       )
        if response is None:
            raise ValueError('Unable to get_segment')
        return Segment(**response.json())

    @staticmethod
    def get_by_id(id: str, privateKey: str) -> 'Segment':
        """
        Fetches a specific Segment by its id.

        :param import_id: ID of the import.
        :return: The requested Segment.
        """
        logging.debug(f"SegmentAPI::get_segment:{id}")
        url = f"{_API_ENDPOINT}/{id}"
        response = APIRequestHandler.getRequest_static(url=url,
                                                       privateKey=privateKey)
        if response is None:
            raise ValueError('Unable to get_by_id')
        return Segment(**response.json())

    @staticmethod
    def list(id: str, privateKey: str) -> List['Segment']:
        """
        Fetches all imports from the API.

        :return: List of all imports.
        """
        logging.debug(f"SegmentAPI::list")
        url = f"{_API_ENDPOINT}/{id}"
        response = APIRequestHandler.getRequest_static(privateKey=privateKey,
                                                       url=url)
        segments = response.json()
        return [Segment(**item) for item in segments.get('items', [])]

    def to_json(self, filepath: str):
        FileHelper.check_filepath(filepath)
        with open(file=filepath, mode='w', encoding='utf-8') as f:
            json.dump(self, f,
                      ensure_ascii=False, indent=4, default=FileHelper.json_default)

    @staticmethod
    def from_json(filepath: str) -> 'Segment':
        with open(file=filepath, mode='r') as json_file:
            return Segment(**json.load(json_file))
